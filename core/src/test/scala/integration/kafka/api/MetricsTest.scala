/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import kafka.log.LogConfig
import kafka.network.RequestMetrics
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{JaasTestUtils, TestUtils}

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._

class MetricsTest extends IntegrationTestHarness with SaslSetup {

  override val producerCount = 1
  override val consumerCount = 1
  override val serverCount = 1

  override protected def listenerName = new ListenerName("CLIENT")
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KafkaServerContextName}"
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "false")
  this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableDoc, "false")
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties =
    Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties =
    Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, kafkaServerJaasEntryName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  /**
   * Verifies some of the metrics of producer, consumer as well as server.
   */
  @Test
  def testMetrics(): Unit = {
    val topic = "topicWithOldMessageFormat"
    val props = new Properties
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    TestUtils.createTopic(this.zkUtils, topic, numPartitions = 1, replicationFactor = 1, this.servers, props)
    val tp = new TopicPartition(topic, 0)

    // Clear all error metrics to test new errors since RequestMetrics is a static object
    RequestMetrics.metricsMap.values.foreach { requestMetrics =>
      requestMetrics.errorMeters.values.foreach(_.removeMeter())
    }

    // Produce and consume some records
    val numRecords = 10
    val recordSize = 1000
    val producer = producers.head
    sendRecords(producer, numRecords, recordSize, tp)

    val consumer = this.consumers.head
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    consumeRecords(consumer, numRecords)

    verifyKafkaRateMetricsHaveCumulativeCount()
    verifyClientVersionMetrics(consumer.metrics, "Consumer")
    verifyClientVersionMetrics(this.producers.head.metrics, "Producer")

    val server = servers.head
    verifyBrokerMessageConversionMetrics(server, recordSize)
    verifyBrokerErrorMetrics(servers.head)
    verifyBrokerZkMetrics(server, topic)

    generateAuthenticationFailure(tp)
    verifyBrokerAuthenticationMetrics(server)
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
      recordSize: Int, tp: TopicPartition) = {
    val bytes = new Array[Byte](recordSize)
    (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, bytes))
    }
    producer.flush()
  }

  protected def consumeRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int): Unit = {
    val maxIters = numRecords * 300
    var iters = 0
    var received = 0
    while (received < numRecords && iters < maxIters) {
      received += consumer.poll(50).count
      iters += 1
    }
    assertEquals(numRecords, received)
  }

  // Create a producer that fails authentication to verify authentication failure metrics
  private def generateAuthenticationFailure(tp: TopicPartition): Unit = {
    val producerProps = new Properties()
    val saslProps = new Properties()
     // Temporary limit to reduce blocking before KIP-152 client-side changes are merged
    saslProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000")
    saslProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000")
    saslProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    // Use acks=0 to verify error metric when connection is closed without a response
    saslProps.put(ProducerConfig.ACKS_CONFIG, "0")
    val producer = TestUtils.createNewProducer(brokerList, securityProtocol = securityProtocol,
        trustStoreFile = trustStoreFile, saslProperties = Some(saslProps), props = Some(producerProps))

    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "key".getBytes, "value".getBytes)).get
    } catch {
      case _: Exception => // expected exception
    } finally {
      producer.close()
    }
  }

  private def verifyKafkaRateMetricsHaveCumulativeCount(): Unit =  {

    def exists(name: String, rateMetricName: MetricName, allMetricNames: Set[MetricName]): Boolean = {
      allMetricNames.contains(new MetricName(name, rateMetricName.group, "", rateMetricName.tags))
    }

    def verify(rateMetricName: MetricName, allMetricNames: Set[MetricName]): Unit = {
      val name = rateMetricName.name
      val totalExists = exists(name.replace("-rate", "-total"), rateMetricName, allMetricNames)
      val totalTimeExists = exists(name.replace("-rate", "-time"), rateMetricName, allMetricNames)
      assertTrue(s"No cumulative count/time metric for rate metric $rateMetricName",
          totalExists || totalTimeExists)
    }

    val consumer = this.consumers.head
    val consumerMetricNames = consumer.metrics.keySet.asScala.toSet
    consumerMetricNames.filter(_.name.endsWith("-rate"))
        .foreach(verify(_, consumerMetricNames))

    val producer = this.producers.head
    val producerMetricNames = producer.metrics.keySet.asScala.toSet
    val producerExclusions = Set("compression-rate") // compression-rate is an Average metric, not Rate
    producerMetricNames.filter(_.name.endsWith("-rate"))
        .filterNot(metricName => producerExclusions.contains(metricName.name))
        .foreach(verify(_, producerMetricNames))

    // Check a couple of metrics of consumer and producer to ensure that values are set
    verifyKafkaMetricRecorded("records-consumed-rate", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("records-consumed-total", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("record-send-rate", producer.metrics, "Producer")
    verifyKafkaMetricRecorded("record-send-total", producer.metrics, "Producer")
  }

  private def verifyClientVersionMetrics(metrics: java.util.Map[MetricName, _ <: Metric], entity: String): Unit = {
    Seq("commit-id", "version").foreach { name =>
      verifyKafkaMetric(name, metrics, entity) { matchingMetrics =>
        assertEquals(1, matchingMetrics.size)
        val metric = matchingMetrics.head
        val value = metric.metricValue
        assertNotNull(s"$entity metric not recorded $name", value)
        assertNotNull(s"$entity metric $name should be a non-empty String",
            value.isInstanceOf[String] && !value.asInstanceOf[String].isEmpty)
        assertTrue("Client-id not specified", metric.metricName.tags.containsKey("client-id"))
      }
    }
  }

  private def verifyBrokerAuthenticationMetrics(server: KafkaServer): Unit = {
    val metrics = server.metrics.metrics
    TestUtils.waitUntilTrue(() =>
      maxKafkaMetricValue("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics")) > 0,
      "failed-authentication-total not updated")
    verifyKafkaMetricRecorded("successful-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("successful-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
  }

  private def verifyBrokerMessageConversionMetrics(server: KafkaServer, recordSize: Int): Unit = {
    val requestSize = verifyYammerMetricRecorded(s"RequestSize,request=Produce")
    val tempSize = verifyYammerMetricRecorded(s"TemporaryMemorySize,request=Produce")
    assertTrue(s"Unexpected temporary memory size requestSize $requestSize tempSize $tempSize",
        tempSize >= recordSize)

    verifyYammerMetricRecorded(s"ProduceMessageConversionsPerSec")
    verifyYammerMetricRecorded(s"MessageConversionsTimeMs,request=Produce", value => value > 0.0)

    verifyYammerMetricRecorded(s"RequestSize,request=Fetch")
    // Temporary size for fetch should be zero after KAFKA-5968 is fixed
    verifyYammerMetricRecorded(s"TemporaryMemorySize,request=Fetch", value => value >= 0.0)

    verifyYammerMetricRecorded(s"RequestSize,request=Metadata") // request size recorded for all request types, check one
  }

  private def verifyBrokerZkMetrics(server: KafkaServer, topic: String): Unit = {
    // Latency is rounded to milliseconds, so we may need to retry some operations to get latency > 0.
    val (_, recorded) = TestUtils.computeUntilTrue({
      servers.head.zkUtils.getLeaderAndIsrForPartition(topic, 0)
      yammerMetricValue("ZooKeeperLatency").asInstanceOf[Double]
    })(latency => latency > 0.0)
    assertTrue("ZooKeeper latency not recorded", recorded)

    assertEquals(s"Unexpected ZK state ${server.zkUtils.zkConnection.getZookeeperState}",
        "CONNECTED", yammerMetricValue("SessionState"))
  }

  private def verifyBrokerErrorMetrics(server: KafkaServer): Unit = {

    def errorMetricCount = Metrics.defaultRegistry.allMetrics.keySet.asScala.filter(_.getName == "ErrorsPerSec").size

    val startErrorMetricCount = errorMetricCount
    verifyYammerMetricRecorded("name=ErrorsPerSec,request=Metadata,error=NONE")

    try {
      consumers.head.partitionsFor("12{}!")
    } catch {
      case e: InvalidTopicException => // expected
    }
    verifyYammerMetricRecorded("name=ErrorsPerSec,request=Metadata,error=INVALID_TOPIC_EXCEPTION")

    // Check that error metrics are registered dynamically
    val currentErrorMetricCount = errorMetricCount
    assertEquals(startErrorMetricCount + 1, currentErrorMetricCount)
    assertTrue(s"Too many error metrics $currentErrorMetricCount" , currentErrorMetricCount < 10)

    // Verify that error metric is updated with producer acks=0 when no response is sent
    sendRecords(producers.head, 1, 100, new TopicPartition("non-existent", 0))
    verifyYammerMetricRecorded("name=ErrorsPerSec,request=Metadata,error=LEADER_NOT_AVAILABLE")
  }

  private def verifyKafkaMetric[T](name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None)(verify: Iterable[Metric] => T) : T = {
    val matchingMetrics = metrics.asScala.filter {
      case (metricName, _) => metricName.name == name && group.forall(_ == metricName.group)
    }
    assertTrue(s"Metric not found $name", matchingMetrics.size > 0)
    verify(matchingMetrics.values)
  }

  private def maxKafkaMetricValue(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None): Double = {
    // Use max value of all matching metrics since Selector metrics are recorded for each Processor
    verifyKafkaMetric(name, metrics, entity, group) { matchingMetrics =>
      matchingMetrics.foldLeft(0.0)((max, metric) => Math.max(max, metric.value))
    }
  }

  private def verifyKafkaMetricRecorded(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None): Unit = {
    val value = maxKafkaMetricValue(name, metrics, entity, group)
    assertTrue(s"$entity metric not recorded correctly for $name value $value", value > 0.0)
  }

  private def yammerMetricValue(name: String): Any = {
    val allMetrics = Metrics.defaultRegistry.allMetrics.asScala
    val (metricName, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(n => n.getMBeanName)}"))
    metric match {
      case m: Meter => m.count.toDouble
      case m: Histogram => m.max
      case m: Gauge[_] => m.value
      case m => fail(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  private def verifyYammerMetricRecorded(name: String, verify: Double => Boolean = d => d > 0): Double = {
    val metricValue = yammerMetricValue(name).asInstanceOf[Double]
    assertTrue(s"Broker metric not recorded correctly for $name value $metricValue", verify(metricValue))
    metricValue
  }
}

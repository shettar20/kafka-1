/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package kafka.api

import java.util
import java.util.Collections

import kafka.api.CustomQuotaCallbackTest._
import kafka.network.RequestChannel
import kafka.quota._
import kafka.server._
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{MetricName, TopicPartition}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.mutable

object CustomQuotaCallbackTest {
  val UserTag = "session-user-principal"
  val ClientIdTag = "request-client-id"
  val CriticalTopic = "criticalTopic"
  val CriticalClientId = "Critical Client"

  class CustomQuotaCallback extends ClientQuotaCallback {
    var brokerId: Int = -1
    var currentPartitions =  Map[TopicPartition, Option[Int]]()
    val currentQuotas = Map[ClientQuotaType, mutable.Map[QuotaId, Double]](
      ClientQuotaType.Fetch -> mutable.Map[QuotaId, Double](),
      ClientQuotaType.Produce -> mutable.Map[QuotaId, Double](),
      ClientQuotaType.Request -> mutable.Map[QuotaId, Double]()
    )
    override def configure(configs: util.Map[String, _]): Unit = {
      brokerId = configs.get(KafkaConfig.BrokerIdProp).toString.toInt
    }

    override def quota(session: RequestChannel.Session, clientId: String, quotaType: ClientQuotaType): ClientQuota = synchronized {
      val quotaLimit = currentQuota(Some(session.principal.getName), Some(clientId), quotaType)
      ClientQuota(quotaLimit.getOrElse(Long.MaxValue),
        Map(UserTag -> session.principal.getName, ClientIdTag -> clientId))
    }

    override def quotaLimit(metricTags: Map[String, String], quotaType: ClientQuotaType): Option[Double] = {
      currentQuota(metricTags.get(UserTag), metricTags.get(ClientIdTag), quotaType)
    }

    private def currentQuota(user: Option[String], clientId: Option[String], quotaType: ClientQuotaType): Option[Double] = {
      val quotas = currentQuotas(quotaType)
      var quotaId = QuotaId(user, clientId)
      if (!quotas.contains(quotaId))
        quotaId = QuotaId(user, None)
      if (!quotas.contains(quotaId))
        quotaId = QuotaId(None, clientId)
      if (!quotas.contains(quotaId))
        quotaId = QuotaId(None, None)
      quotas.get(quotaId).map { quota =>
        val multiplier = if (clientId.exists(_ == CriticalClientId)) {
          currentPartitions.count { case (tp, leader) =>
            tp.topic == CriticalTopic && leader == Some(this.brokerId)
          } * 1000 + 1
        } else
          1
        quota * multiplier
      }
    }

    override def updatePartitionMetadata(partitions: Map[TopicPartition, kafka.quota.PartitionMetadata]): Boolean = synchronized {
      currentPartitions = partitions.map { case (tp, metadata) => (tp, metadata.leader) }
      true
    }

    override def updateQuota(quotaEntity: ClientQuotaEntity, quotaType: ClientQuotaType, newValue: Option[Double]): Boolean = synchronized {
      assertEquals(2, quotaEntity.configEntities.size)
      val user = if (quotaEntity.configEntities(0).entityType == QuotaConfigEntityType.DefaultUser)
        None
      else
        Some(quotaEntity.configEntities(0).name)
      val clientId = if (quotaEntity.configEntities(1).entityType == QuotaConfigEntityType.DefaultClientId)
        None
      else
        Some(quotaEntity.configEntities(1).name)
      val quotaId = QuotaId(user, clientId)
      val quotaMap = currentQuotas(quotaType)
      newValue match {
        case Some(value) => quotaMap.put(quotaId, value)
        case None => quotaMap.remove(quotaId)
      }
      true
    }

    override def close(): Unit = {}
  }
}
class CustomQuotaCallbackTest extends UserClientIdQuotaTest {

  override def producerClientId = "CustomQuotaCallbackTest Producer"
  override def consumerClientId = "CustomQuotaCallbackTest Consumer"
  override def producerQuotaId = QuotaId(Some(userPrincipal), Some(producerClientId))
  override def consumerQuotaId = QuotaId(Some(userPrincipal), Some(consumerClientId))

  @Before
  override def setUp() {
    this.serverConfig.setProperty(KafkaConfig.ClientQuotaCallbackClassProp, classOf[CustomQuotaCallback].getName)
    super.setUp()
  }

  override def throttleMetricName(quotaType: ClientQuotaType, quotaId: QuotaId): MetricName = {
    leaderNode.metrics.metricName("throttle-time",
      quotaType.toString,
      "Tracking throttle-time per user/client-id",
      UserTag, quotaId.sanitizedUser.getOrElse(""),
      ClientIdTag, quotaId.clientId.getOrElse(""))
  }

  @Test
  def testCustomQuotaManagement() {
    val topic1Leader = leaderNode
    val criticalTopicLeader = followerNode
    val criticalTopicAssignment = Map(0 -> Seq(criticalTopicLeader.config.brokerId))
    TestUtils.createTopic(zkClient, CriticalTopic, criticalTopicAssignment, servers)
    val numRecords = 1000

    this.producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, CriticalClientId)
    val criticalProducer = createNewProducer
    producers += criticalProducer
    val criticalClientQuotaId = QuotaId(Some(userPrincipal), Some(CriticalClientId))
    sendRecords(criticalProducer, numRecords, CriticalTopic)
    assertEquals("Should not have been throttled", 0.0,
      throttleMetric(ClientQuotaType.Produce, criticalClientQuotaId, criticalTopicLeader).value, 0.0)

    this.consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, CriticalClientId)
    this.consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "critical-group")
    val criticalConsumer = createNewConsumer
    consumers += criticalConsumer
    consumeRecords(criticalConsumer, numRecords, CriticalTopic)
    assertEquals("Should not have been throttled", 0.0,
      throttleMetric(ClientQuotaType.Fetch, criticalClientQuotaId, criticalTopicLeader).value, 0.0)

    val nonCriticalProducer = producers.head
    val produced = produceUntilThrottled(nonCriticalProducer, numRecords)
    assertTrue("Should have been throttled",
      throttleMetric(ClientQuotaType.Produce, producerQuotaId, topic1Leader).value > 0)
    verifyProducerThrottleTimeMetric(nonCriticalProducer)

    val nonCriticalConsumer = consumers.head
    consumeUntilThrottled(nonCriticalConsumer, produced)
    assertTrue("Should have been throttled",
      throttleMetric(ClientQuotaType.Fetch, consumerQuotaId, topic1Leader).value > 0)
    verifyConsumerThrottleTimeMetric(nonCriticalConsumer)
  }

  def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int, topic: String): Unit = {
    (0 to numRecords).foreach { i =>
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, null, null, i.toString.getBytes)).get()
    }
  }

  def consumeRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int, topic: String): Unit = {
    consumer.subscribe(Collections.singleton(topic))
    val endTimeMs = System.currentTimeMillis + 5000
    var numConsumed = 0
    while (numConsumed < numRecords && System.currentTimeMillis < endTimeMs)
        numConsumed += consumer.poll(100).count
  }
}

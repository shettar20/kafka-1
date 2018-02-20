/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.quota

import kafka.network.RequestChannel.Session
import org.apache.kafka.common.{Configurable, TopicPartition}

trait ClientQuotaCallback extends Configurable {

  /**
    * Quota callback invoked to determine the quota limit to be applied for a request.
    *
    * @param session The session for which quota is requested
    * @param clientId The client id associated with the request
    * @param quotaType Type of quota requested
    *
    * @return the quota including the limit and metric tags that indicate which other entities share this quota
    */
  def quota(session: Session, clientId: String, quotaType: ClientQuotaType): ClientQuota

  /**
    * Returns the quota limit associated with the provided metric tags. These tags were returned from
    * a previous call to [[ClientQuotaCallback.quota()]]. This method is invoked by quota managers to
    * obtain the current quota limit applied to a metric after a quota update or partition metadata change.
    * If the tags are no longer in use after the update, (e.g. this is a {user, client-id) quota metric
    * and the quota now in use is a (user) quota), None is returned.
    *
    * @param metricTags Metric tags for a quota metric of type `quotaType`
    * @param quotaType Type of quota requested
    * @return the quota limit for the provided metric tags or None if the metric tags are no longer in use
    */
  def quotaLimit(metricTags: Map[String, String], quotaType: ClientQuotaType): Option[Double]

  /**
    * Metadata update callback that is invoked whenever UpdateMetadata request is received from
    * the controller. This is useful if quota computation is takes partitions into account.
    * Deleted partitions are not included in `partitions`.
    *
    * @param partitions Partitions and their metadata including partition leader
    * @return true if quotas have changed
    */
  def updatePartitionMetadata(partitions: Map[TopicPartition, PartitionMetadata]): Boolean

  /**
    * Quota configuration update callback that is invoked whenever quota configuration in ZooKeeper
    * is updated. This is useful to track configured quotas if  built-in quota configuration tools
    * are used for quota management.
    *
    * @param quotaEntity The quota entity for which quota is being updated.
    * @param quotaType Type of quota being updated.
    * @param newValue The new quota value. If None, the quota configuration for `quotaEntity` is deleted.
    * @return true if quotas have changed
    */
  def updateQuota(quotaEntity: ClientQuotaEntity, quotaType: ClientQuotaType, newValue: Option[Double]) : Boolean

  /**
    * Closes this instance.
    */
  def close(): Unit

}


object ClientQuotaType  {
  case object Fetch extends ClientQuotaType
  case object Produce extends ClientQuotaType
  case object Request extends ClientQuotaType
}
sealed trait ClientQuotaType

/**
  * Client quota returned by `ClientQuotaCallback`.
  *
  * @param quotaLimit The quota bound to be applied
  * @param metricTags The tags to be added to the quota metric for this request. All entities
  *                   which have the same `metricTags` share the `quotaLimit`
  */
case class ClientQuota(quotaLimit: Double, metricTags: Map[String, String])


object QuotaConfigEntityType  {
  case object User extends QuotaConfigEntityType
  case object ClientId extends QuotaConfigEntityType
  case object DefaultUser extends QuotaConfigEntityType
  case object DefaultClientId extends QuotaConfigEntityType
}
sealed trait QuotaConfigEntityType

trait QuotaConfigEntity {
  def name: String
  def entityType: QuotaConfigEntityType
}

/**
  * The metadata for an entity for which quota is configured. Quotas may be defined at
  * different levels and `configEntities` gives the config entities that define the level.
  * For example, if quota is configured for <userA, clientB>, `configEntities` will be
  * List(userA, clientB). For <clientC> quota, 'configEntities` will be List(clientC).
  */
trait ClientQuotaEntity {
  def configEntities: List[QuotaConfigEntity]
}

/**
  * Partition metadata that may be used in quota computation. This is provided
  * by the broker when UpdateMetadata request is received from the controller.
  */
trait PartitionMetadata {
  def leader: Option[Int]
}

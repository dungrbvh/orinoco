package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.application.sessionize.NonRobotEventsWithPartitionerDS
import com.orinoco.commons.CustomPartitionerUtils
import com.orinoco.config.base.PartitionConfig
import com.orinoco.schema.normalize.NormalizedWithUser
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object FilterRoboticEventsWithPartitioner {
  /*
   * About this function
   * Purpose is to do two things:
   *   1. Make better assumptions about executor memory requirements
   *   2. Make all executors do even amounts of work to optimize application runtime
   *
   * Session sizes vary wildly, from a single event to tens of thousands of events.
   * Sessions with more than 10,000 events, due to robotic activity, are filtered, to ensure executors run to completion.
   * Sessions with 1,000s of events are pre-profiled, then passed to a custom partitioner.
   * This guarantees that sessions with many events do not randomly get assigned to a few executors.
   * Ensures that data is distributed evenly across executors.
   */

  /*
   * About the Custom Partitioner
   * Ensures that sessions with a great number of events are evenly distributed during Sessionization.
   * The remaining 'small' sessions are hash-partitioned across executors.
   * Overall goal is to prevent key skew from causing one executors from slowing down Sessionization.
   */

  /*
   * Bot filtering and custom partitioner creation
   *
   * @param normalizedWithUser Input Dataset Normalize
   * @param sessionCache Input Dataset Session Cache
   * @param useCustomPartitioner if process will use custom partitioner
   * @param partitionConfig partitioning configuration
   * @param normalizedWithUserCount Input Dataset Normalize count
   * @param sessionCacheCount Input Dataset Session Cache count
   * @param spark Necessary for spark usage
   * @return
   */

  def asDataset(
               normalizedWithUser: Dataset[NormalizedWithUser],
               sessionCache: Dataset[SessionizationCache],
               useCustomPartitioner: Boolean,
               partitionConfig: PartitionConfig,
               normalizedWithUserCount: Long,
               sessionCacheCount: Long
               )(implicit spark: SparkSession): NonRoboticEventsWithPartitionerDS = {
    import spark.implicits._

    if (!useCustomPartitioner) {
      val partitioner = new HashPartitioner(300)
      NonRobotEventsWithPartitionerDS(
        normalizedWithUser,
        sessionCache,
        normalizedWithUserCount + sessionCacheCount,
        partitioner
      )
    }
    else {
      // Setup
      val totalEventCount = normalizedWithUserCount + sessionCacheCount
      val normalizedLargeSessionsList: Dataset[(String, Long)] = normalizedWithUser
        .map(n => (n.norm.sessionization_key, 1L))
        .groupByKey(_._1)
        .reduceGroups((a,b) => (a._1, a._2 + b._2))
        .map(_._2)
      val cachedLargeSessionList: Dataset[(String, Long)] =
        if (sessionCache.isEmpty)
          Seq.empty[(String, Long)].toDS
        else
          sessionCache
            .map(s => (s.sessionization_key, s.event_count.getOrElse(1).toLong))
            .groupByKey(_._1)
            .reduceGroups((a,b) => (a._1, a._2 + b._2))
            .map(_._2)

      // Prior calculations
      val largeSessions: Dataset[(String, Long)] = normalizedLargeSessionsList
        .union(cachedLargeSessionsList)
        .groupByKey(_._1)
        .mapGroups {
          case (sessionization_key, sessionCountList) =>
            (sessionization_key, sessionCountList.map(_._2).sum)
        }
        .persist(StorageLevel.MEMORY_AND_DISK)

      val largeSessionsNonRobotic: Dataset[(String, Long)] = largeSessions
        .filter(x => x._2 < partitionConfig.roboticSessionSize && x._2 >= partitionConfig.largeSessionSize)

      // For custom partitioning
      val partitionDetails: List[((String, Long), Long, Int)] =
        if (!largeSessionsNonRobotic.isEmpty)
          CustomPartitionerUtils.getPartitioningDetails(
            largeSessionsNonRobotic.collect.toList, partitionConfig.recordsPerPartition
          )
        else
          List.empty[((String, Long), Long, Int)]

      val partitioningSizes: List[(Int, Long)] =
        if (partitioningDetails.isEmpty)
          List.empty[(Int, Long)]
        else
          partitionDetails
            .groupBy(_._3)
            .map(x => (x._1, x._2.maxBy(_._2)))
            .map(x => (x._1, x._2._2))
            .toList
            .sortBy(_._1)

      val rPP: Long = Math.max(300, 1 + totalInputEventCount / partitionConfig.recordsPerPartition)
      val numLargeSessionPartitions: Int = partitioningSizes.length
      val numSmallSessionPartitions: Int = Math.abs(rPP.toInt - numLargeSessionPartitions)
      val largeSessionPartitioningMap: Map[String, Int] = partitioningDetails
        .map(x => (x._1._1, x._3))
        .toMap

      val numPartitions = numLargeSessionPartitions + numSmallSessionPartitions

      // Filter for non-robotic events
      val botSessionsBroadcast = spark.sparkContext.broadcast(
        largeSessions
          .filter(_._2 >= partitionConfig.roboticSessionSize)
          .map(_._1)
          .distinct
          .collect
          .toSet
      )
        .value

      val nonRoboticNormalizedWithUser = normalizedWithUser
        .filter(x => !botSessionsBroadcast(x.norm.sessionization_key))
      val nonRoboticSessionizationCache = sessionCache
        .filter(x => !botSessionsBroadcast(x.sessionization_key))

      // Create custom partitioning
      val partitioner = new CustomPartitionerUtils.CustomPartitioner(
        numPartitions,
        numSmallSessionPartitions,
        numLargeSessionPartitions,
        spark.sparkContext.broadcast(largeSessionPartitioningMap)
      )
      NonRobotEventsWithPartitionerDS(
        nonRoboticNormalizedWithUser,
        nonRoboticSessionizationCache,
        totalInputEventCount,
        partitioner
      )
    }
  }
}
package com.orinoco.commons

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.broadcast.Broadcast

object CustomPartitionerUtils {
  def getPartitioningDetails(
                              sessionSizes: List[(String, Long)],
                              targetPartitionSize: Long
                            ): List[((String, Long), Long, Int)] = {
    if (sessionSizes.isEmpty) throw new IllegalArgumentException("Non-empty list of session sizes is expected.")

    for ((sessionKey, sessionSize) <- sessionSizes) {
      if (sessionSize < 1){
        throw new IllegalArgumentException(
          s"Session sizes smaller than 1 prohibited: Key $sessionKey has size $sessionSize."
        )
      }
    }

    for ((sessionKey, sessionKeyOccurrences) <- sessionSizes.groupBy(_._1).mapValues(_.length)) {
      if (sessionKeyOccurrences > 1) {
        throw new IllegalArgumentException(
          s"Duplicate sessionization keys not allowed: Key $sessionKey appears $sessionKeyOccurrences times."
        )
      }
    }

    // Sorted, numeric values to be used as partition number for custom partitioner
    val sorted = sessionSizes.sortBy(x => (x._2, x._1)).reverse
    sorted.tail.foldLeft(List((sorted.head, sorted.head._2, 0))) {
      (r: List[((String, Long), Long, Int)], c: (String, Long)) => {
        if (r.head._2 + c._2 <= targetPartitionSize) {
          // Continue accumulating
          ((c._1, c._2), r.head._2 + c._2, r.head._3) +: r
        } else {
          // Start new accumulation
          ((c._1, c._2), c._2, r.head._3 + 1) +: r
        }
      }
    }.reverse
  }

  /*
   * Fills up first set of partitions (0 to N) with large sessions.
   * Custom partitioner knows which sessions are large and their assigned partitions.
   * Small sessions' start of their index  is supplied and a hash partitioner is used to evenly split small sessions.
   *
   * Rules:
   *    Large sessions will be distributed across partition
   *    Small sessions will use default partitioner: HashPartitioner
   */

  class CustomPartitioner(
                         override val numPartitions: Int,
                         numSmallSessionPartitions: Int,
                         numLargeSessionPartitions: Int,
                         largeSessionPartitioningMapBroadcast: Broadcast[Map[String, Int]]
                         ) extends Partitioner {
    val hashPartitioner = new HashPartitioner(numSmallSessionPartitions)

    /*
     * @param key sessionization_key
     * @return partition number
     */
     override def getPartition(key: Any): Int = {
       val k = key.toString
       largeSessionPartitioningMapBroadcast.value.getOrElse(
         k, numLargeSessionPartitions + hashPartitioner.getPartition(k))
     }
  }
}
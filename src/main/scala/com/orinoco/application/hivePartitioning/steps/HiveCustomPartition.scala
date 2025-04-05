package com.orinoco.application.hivePartitioning.steps

import org.apache.spark.sql.functions.{broadcast, ceil, hash, pmod}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * We partition by service group + year + month + day + hour. The size of the partitions vary wildly, from
 * millions to just a couple. We must therefore dynamically-size the output orc part files, according to the
 * number of records in the given partition.
 *
 * Overall approach inspired by: http://labs.criteo.com/2018/06/spark-custom-partitioner/
 */

object HiveCustomPartition {
  /**
   * Objective:
   *    This script has two main purposes:
   *      1. Calculate partitions based on number of records for each service group
   *      2. Apply repartitioning calculation from step 1
   *
   * @param records             Payload of users
   * @param recordsPerCutFile   Pathing of output
   * @param spark               Necessary for spark usage
   **/
  def apply(
           records: DataFrame,
           recordsPerCutFile: Int
           )(implicit spark: SparkSession): DataFrame = {
    val groupPartitionCount = countPartitions(records, recordsPerCutFile)
    repartitionRecords(records, groupPartitionCount)
  }
  /**
   * Calculate number of partitions for each service group
   *  according to recordsPerCutFile
   **/
  private object countPartitions {
    def apply(
               recordsDateFiltered: DataFrame,
               recordsPerCutFile: Int
             )(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      recordsDateFiltered
        .groupBy($"service_group")
        .count
        .withColumn("partition_count", ceil($"count")/ recordsPerCutFile)
    }
  }

  /**
   * Applies repartitioning for each service groups following their respective partition count.
   * Records are distributed evenly according to UUID values.
   **/
  private object repartitionRecords {
    def apply(
               recordsDateFiltered: DataFrame,
               groupPartitionCount: DataFrame
             )(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._

      recordsDateFiltered
        .joinWith(
          broadcast(groupPartitionCount),
          recordsDateFiltered("service_group") === groupPartitionCount("service_group"))
        .withColumn("custom_partition_idx", pmod(hash($"_1.uuid"), $"_2.partition_count"))
        .repartition($"_1.service_group", $"custom_partition_idx")
        .select($"_1.*")
    }
  }
}
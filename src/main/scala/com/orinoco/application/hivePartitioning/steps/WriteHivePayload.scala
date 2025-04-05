package com.orinoco.application.hivePartitioning.steps

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.schema.hivePartitioning.PublicRatSessionized
import org.slf4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteHivePayload {
  /**
   * Objective:
   *    This script has two main purposes:
   *      1. Retrieve payload depending on table type
   *      2. Save data via partitioning onto HDFS
   *
   * @param customPartitioned Data for output with custom partitions
   * @param tableType Table type between "public" and "private"
   * @param outputPath Pathing of output
   * @param spark Necessary for spark usage
   **/
  def apply(
             logger: Logger,
             customPartitioned: DataFrame,
             tableType: String,
             outputPath: String,
             excludeFields: List[String]
           )(implicit spark: SparkSession): Unit = {
    val payload = getHiveTablePayload(customPartitioned, tableType, excludeFields)
    writePartitionedData(logger, payload, tableType, outputPath)
  }

  // Selects certain fields within the payload; some fields are also dropped / renamed depending on business needs.
  object getHiveTablePayload {
    def apply(
               customPartitioned: DataFrame,
               tableType: String,
               excludeFields: List[String]
             ): DataFrame = {
      if (tableType == "public")
        customPartitioned
          .toDF(PublicRatSessionized.getFlattenedSchema.fields.map(_.name): _*)
          .drop(excludeFields: _*)
      else if (tableType == "private")
        customPartitioned
          .toDF(SessionizePayload.getFlattenedSchema.fields.map(_.name): _*)
          .drop(excludeFields: _*)
      else
        throw new Exception("Table type must be either 'public' or 'private'.")
    }
  }

  // Data is partitioned in order of specified fields onto HDFS.
  object writePartitionedData {
    def apply(
               logger: Logger,
               payload: DataFrame,
               tableType: String,
               outputPath: String
             ): Unit = {

      if (tableType == "public")
        payload
          .sortWithinPartitions("event_type")
          .write
          .mode("overwrite")
          .partitionBy("service_group", "year", "month", "day", "hour", "phase")
          .option("orc.compress.size", "4096")
          .option("hive.exec.orc.default.stripe.size", "268435456")
          .orc(outputPath)
      else if (tableType == "private")
        payload
          .sortWithinPartitions("event_type")
          .write
          .mode("overwrite")
          .partitionBy("service_group", "year", "month", "day", "hour", "phase")
          .orc(outputPath)
      else throw new Exception("Table type must be either 'public' or 'private'.")
      logger.info(s"Hive partition move Output: $outputPath")
    }
  }
}
package com.orinoco.application.hivePartitioning

import com.orinoco.adapter.logging.JobReport
import com.orinoco.adapter.logging.JobReport._
import com.orinoco.commons.DateUtils.hourIsoFormatter
import com.orinoco.commons.DebugUtils.checkCorruptedParquet
import com.orinoco.commons.ElasticSearchUtils.applicationEndMessageHive
import com.orinoco.commons.PathUtils.{createHDFSFileWithContent, forcePathDeletion, logger, pathBucketDatePhase, pathBucketDatePhaseTable, pathDateHour}
import com.orinoco.config.base.AppConfig
import com.orinoco.config.task.HivePartitioningConfig
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

object hivePartitioning {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val taskName = this.getClass.getSimpleName.split("\\$").last

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(taskName)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    /* Load config and log details (prints) */
    val hivePartitioningConfig = HivePartitioningConfig.load(args, AppConfig.init())
    println(hivePartitioningConfig.dump())

    /* Value Setup */
    val broadcastConfig: Broadcast[HivePartitioningConfig] = spark.sparkContext.broadcast(hivePartitioningConfig)
    val tableType = broadcastConfig.value.options.tableType
    val processingBucket = broadcastConfig.value.options.outProcBucketName
    val phaseType = broadcastConfig.value.options.phase

    // Time
    val hourDT = broadcastConfig.value.options.hour
    val UTCFormatter = hourIsoFormatter.withZone(DateTimeZone.UTC)
    val inputDateHourUTC = UTCFormatter.print(hourDT)

    // Pathing
    val inputPathConfig =  // Path changes based on phase type
      if (phaseType == "traffic") broadcastConfig.value.inputSessionized
      else broadcastConfig.value.inputPostCalculate

    val inputPath = pathBucketDatePhase(inputPathConfig, processingBucket, inputDateHourUTC, phaseType)
    val serviceGroupExclusionFor2ndUpdatePath = pathDateHour(
      broadcastConfig.value.inputSnapshotServiceGroupExclusionFor2ndUpdatePath, hourDT
    )

    val errorMessageFile = pathBucketDatePhaseTable(
      hivePartitioningConfig.errorMessageFilePattern, processingBucket, inputDateHourUTC, phaseType, tableType
    )

    // Gets list of service groups to be excluded from 2nd update
    val serviceGroupExclusionFor2ndUpdateList = spark.read
      .textFile(serviceGroupExclusionFor2ndUpdatePath).collect.toList

    val outputPath = // Pathing changes based on table type
      if (tableType == "public") broadcastConfig.value.outputPublicRatSessionized
      else if (tableType == "private") broadcastConfig.value.outputPrivateRatSessionized
      else throw new Exception("Table type must be either 'public' or 'private'.")

    // Repartition calculation
    val recordsPerCutFile =  // Repartition changes based on table type due data size differentials
      if (tableType == "public") broadcastConfig.value.recordsPerCutFilePublic
      else if (tableType == "private") broadcastConfig.value.recordsPerCutFilePrivate
      else throw new Exception("Table type must be either public or private")

    val excludeFields =
      if (tableType == "public") broadcastConfig.value.excludeFieldsPublicTable
      else if (tableType == "private") broadcastConfig.value.excludeFieldsPrivateTable
      else throw new Exception("Table type must be either 'public' or 'private'.")

    try {
      // Step 1: Read selected input via SQL
      val recordsDateFiltered = steps.ReadFlattenedRecords.apply(
        logger, phaseType, tableType, inputPath, serviceGroupExclusionFor2ndUpdateList, hourDT
      )

      if ( // Instance of no records - Encountered on STG as well
        (recordsDateFiltered.isEmpty && env == "stg") ||
        (recordsDateFiltered.isEmpty && processingBucket == "beta")
      )
        logger.info("No records seen for this hour; no need to process")
      else {
        // Step 2
        val customPartitioned = steps.HiveCustomPartition.apply(recordsDateFiltered, recordsPerCutFile)
        // Step 3: Write out partitioning to designated output pathing
        steps.WriteHivePayload(logger, customPartitioned, tableType, outputPath, excludeFields)
        logger.info(s"Data saved into rat_sessionized path: $outputPath")
      }

    } catch {  // Catches any issue and proceeds to log for debugging purposes.
      case t: Throwable => // Throwables are caught for the purpose of logging all errors.
        checkCorruptedParquet(Option(t))
        .foreach(message => {
          logger.error("Input parquet is corrupted. Creating an error message file.")
          createHDFSFileWithContent(errorMessageFile, message)
        })
        val message = "Throwable occurred at: " + ExceptionUtils.getStackTrace(t)

        logger.error(message)
        throw t
    }
    logger.info(s"HivePartitioning for processing bucket: $processingBucket and phase: $phaseType completed.")
  }
}
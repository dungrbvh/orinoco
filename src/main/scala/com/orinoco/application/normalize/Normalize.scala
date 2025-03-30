package com.orinoco.application.normalize

import com.orinoco.adapter.logging.JobReport
import com.orinoco.adapter.logging.JobReport._
import com.orinoco.adapter.normalize.steps
import com.orinoco.adapter.normalize.steps.MapTablesToRecordAndBroadcast
import com.orinoco.commons.DateUtils.hourIsoFormatter
import com.orinoco.commons.ElasticSearchUtils.applicationEndMessage
import com.orinoco.commons.PathUtils._
import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import com.orinoco.config.task.NormalizeConfig
import com.orinoco.option.NormalizeOptions
import com.orinoco.schema.normalize.{NormalizedWithUser, Parsed}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory
import com.orinoco.config.base.AppConfig
import com.orinoco.commons.MetaDataSnapshotsUtils._
import com.orinoco.schema.cdna.CustomerDNA
import com.rakuten.rat.config.base.AppConfig
import org.apache.commons.lang3.exception.ExceptionUtils

object Normalize {

  def readRawJson(inputPaths: List[String])(implicit  spark: SparksSession): Dataset[Parsed] = {
    import spark.implicits._
    spark
      .read
      .schema(getSchemaAsStruct[Parsed])
      .json(inputPaths)
      .as[Parsed]
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val taskName = this.getClass.getSimpleName.split("\\$").last

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(taskName)
      .getOrCreate()

    spark.sql("set spark.sql.files.ignoreMissingFiles=true")

    /* Load config and log details (prints) */
    val options = NormalizeOptions(args)
    val normalizeConfig = NormalizeConfig.load(AppConfig.init())
    println(normalizeConfig.dump())

    /* Value Setup */
    import spark.implicits._
    val broadcastConfig: Broadcast[NormalizeConfig] = spark.sparkContext.broadcast(normalizeConfig)
    val processingBucket = options.outputProcBucketName

    val logger = LoggerFactory.getLogger(this.getClass)
    logger.info(s"Normalization $processingBucket start ...")

    // Time
    val hourDT = options.hour
    val inputDateHourUTC = hourIsoFormatter.withZone(DateTimeZone.UTC).print(hourDT)

    // Load metadata snapshots
    val inputSnapshotCompanyMapping = getTuplesFromHourlyTsvSnapshot(normalizeConfig.inputSnapshotCompanyPath, hourDT).toMap
    val inputSnapshotServiceGroupTimezoneList = timezoneFromTableNameMap(getTuplesFromHourlyTsvSnapshot(normalizeConfig.inputSnapshotHiveNamePath, hourDT))
    val inputSnapshotServiceCategoryMapping = toMapKeyInt(getTuplesFromHourlyTsvSnapshot(normalizeConfig.inputSnapshotServiceCategoryPath, hourDT))
    val inputSnapshotServiceGroupMapping = toMapKeyInt(getTuplesFromHourlyTsvSnapshot(normalizeConfig.inputSnapshotServiceGroupPath, hourDT))
    val inputSnapshotProcessingBucketList = getTuplesFromHourlyTsvSnapshot(normalizeConfig.inputSnapshotProcessingBucketPath, hourDT)

    // Pathing
    val inputPaths = pathListRawFilteredByProcessingBucket(
      normalizeConfig.inputPatternJson,
      hourDT,
      options.outputProcBucketName,
      inputSnapshotProcessingBucketList
    )
    val outputPath = pathBucketDate(normalizeConfig.outputPatternNormalize, processingBucket, inputDateHourUTC)
    val exceptionsOutputPath = pathBucketDate(
      normalizeConfig.outputPatternExceptions, processingBucket, inputDateHourUTC
    )

    try {
      // Delete existing file(s)
      forcePathDeletion(options.force, outputPath, getFs(outputPath))
      forcePathDeletion(options.force, exceptionsOutputPath, getFs(exceptionsOutputPath))

      /* Data Load */
      val parsed: Dataset[Parsed] = readRawJson(inputPaths)
      val parsedCount = parsed.count()

      // Used for repartitioning
      val numPartitions = {
        val countPer30Mb: Long = 62491L // Count of records per 30Mb file size
        val countPer150Mb: Double = countPer30Mb * 2.0 // Multiplied to make it near hdfs block-size (128Mb)
        val numberOfInputRecords = parsedCount
        (Math.ceil(numberOfInputRecords / countPer150Mb) + 1).toInt // Put + 1 in case numberOfInputRecords is 0
      }

      /* Filter */
      // Filtering on general exclusion rules
      val parsedFiltered: Dataset[Parsed] =
    }
  }
}
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

import java.util.concurrent.atomic.LongAccumulator

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
      val parsedFiltered: Dataset[Parsed] = steps.RepartitionAndFilterRecord(
        parsed = parsed, numPartitions = numPartitions, broadcastConfig = broadcastConfig
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      /* Broadcast For Spark Executors */
      val broadcastInfoToNormalizeWithUser: Broadcast[MapTablesToRecordAndBroadcast] =
        steps.MapTablesToRecordAndBroadcast(
          parsedFiltered,
          normalizeConfig,
          inputSnapshotCompanyMapping,
          inputSnapshotServiceGroupTimezoneList,
          inputSnapshotServiceCategoryMapping,
          inputSnapshotServiceGroupMapping,
          env,
          hourDT
        )(spark)

      // Build up accumulators
      val normalizeFatalCounter: LongAccumulator = spark.sparkContext.longAccumulator("normalize_fatal_counter")

      /* Normalization */
      val normalizedWithUserEither: Dataset[Either[String, NormalizedWithUser]] =
        steps.NormalizeWithUserRecord(parsedFiltered = parsedFiltered, broadcast = broadcastInfoToNormalizeWithUser, normalizeFatalCounter = normalizeFatalCounter)

      /* Separate Output: Exceptions <-> Results */
      val normalizedWithUser = normalizedWithUserEither.flatMap(_.right.toOption)
      val normalizedWithUserExceptions = normalizedWithUserEither.flatMap(_.left.toOption)

      /* Combine with CDNA */
      val idList = normalizedWithUserEither.flatMap(_.norm.easyid).distinct
      val cdnaDS = broadcastInfoToNormalizeWithUser.value.cdnaMapping

      val normWithEasyID: Dataset[NormalizedWithUser] = normalizedWithUser.filter(_.norm.easyid.nonEmpty)
      val normWithoutEasyID: Dataset[NormalizedWithUser] = normalizedWithUser.filter(_.norm.easyid.isEmpty)

      val filteredCDNA = cdnaDS
        .joinWith(idList, idList("value") === cdnaDS("cdna_easy_id"), "inner")
        .map(_._1)

      val normalizedWithCDNA: Dataset[NormalizedWithUser] = normWithEasyID
        .joinWith(filterCDNA, normWithEasyID("norm.easy_id") === filteredCDNA("cdna_easy_id"), "left")
        .map(x => {
          val normalizedWithUser: NormalizedWithUser = x._1
          val cdna: CustomerDNA = x._2
          if (cdna != null)
            normalizedWithUser.copy(
              cdna = cdna
            )
          else
            NormalizedWithUser
        })
      val normWithCDNA = normalizedWithCDNA.unionAll(normWithoutEasyID)

      // Output exceptions
      if (true) {
        normalizedWithUserExceptions
          .repartition(10)
          .toDF.write
          .text(exceptionsOutputPath)
      }

      /* Write result */
      val normalizeOutputCounter: LongAccumulator = spark.sparkContext.longAccumulator("normalize_output_counter")

      steps.WriteNormalizedWithUser(
        normWithCDNA,
        broadcastConfig,
        numPartitions,
        outputPath,
        normalizeOutputCounter
      )

      val firstInputJSONCount = parsedCount
      val lastInputJSONCount = parsed.count()
      // JSON Preflight Check may pass with records still being written in input JSON HDFS directory.
      // Goal of validation is to compare firstInputJSONCount with lastInputJSONCount value of Normalization.
      // If firstInputJSONCount is not equal to lastInputJSONCount, Normalize job should fail.
      if (firstInputJSONCount != lastInputJSONCount) {
        val message = s"Input count has been changed. " +
          s"First input count: [$firstInputJSONCount], Final input count: [$lastInputJSONCount]."

        logger.info(message)
        throw new Exception(message)
      }
      // Elastic log for success

    } catch { // Catches any issue and proceeds to log for debugging purposes.
      case e: Exception => // Indicates application issue (I.E. FileNotFoundException)
        val message = "Exception occured at: " + ExceptionUtils.getStackTrace(e)
        logger.info(message)
        throw e
      case e: Error => // Indicates non application issue (I.E. OutOfMemoryError)
        val message = "Error occurred at: " + ExceptionUtils.getStackTrace(e)
        logger.info(message)
        throw e
      case t: Throwable => // Throwables are caught for the purpose of logging all errors.
        val message = "Throwable occurred at: " + ExceptionUtils.getStackTrace(t)
        logger.info(message)
        throw t
    }
    finally logger.info("Application finished") // Used to mark true end of application run.
  }
}
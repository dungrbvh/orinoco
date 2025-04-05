package com.orinoco.application.sessionize

import com.orinoco.adapter.logging.JobReport
import com.orinoco.adapter.logging.JobReport._
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.commons.DateUtils.hourIsoFormatter
import com.orinoco.commons.DebugUtils.checkCorruptedParquet
import com.orinoco.commons.ElasticSearchUtils.applicationEndMessage
import com.orinoco.commons.PathUtils.{createHDFSFileWithContent, forcePathDeletion, getFs, pathBucketDate}
import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import com.orinoco.config.base.AppConfig
import com.orinoco.config.task.SessionizeConfig
import com.orinoco.schema.normalize.NormalizedWithUser
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTimeZone
import org.slf4j.LoggerFactory

object Sessionize {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val taskName = this.getClass.getSimpleName.split("\\$").last

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(taskName)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    /* Load config and log details (prints) */
    val options = SessionizeOptions(args)
    val sessionizeConfig = SessionizeConfig.load(AppConfig.init())
    println(sessionizeConfig.dump())

    val broadcastConfig: Broadcast[SessionizeConfig] = spark.sparkContext.broadcast(sessionizeConfig)
    val lookBackWindowHours = broadcastConfig.value.config.lookBackWindowHours
    val sessionLifetimeMapping = sessionizeConfig.config.sessionLifetimeMapping
    val partitionConfig = sessionizeConfig.config.partitionConfig

    val evarDimensions = broadcastConfig.value.evar.dimensionsSessionize
    val processingBucket = options.outputProcBucketName
    val force = options.force
    val skipInputCache = options.skipInputCache

    val logger = LoggerFactory.getLogger(this.getClass)
    logger.info(s"Sessionization $processingBucket start ...")

    // Time
    val hourDT = options.hour
    val UTCFormatter = hourIsoFormatter.withZone(DateTimeZone.UTC)
    val inputDateHourUTC = UTCFormatter.print(hourDT)
    val hourDTCacheUTC = UTCFormatter.print(hourDT.plusHours(1))
    val hourEndTimeStamp = (hourDT.withMinuteOfHour(59).withSecondOfMinute(59).getMillis / 1000L).toInt

    // Pathing
    val normalizedInput = pathBucketDate(sessionizeConfig.config.inputNormalized, processingBucket = processingBucket, dateHour = inputDateHourUTC)
    val inputCache = pathBucketDate(sessionizeConfig.config.inputCache, processingBucket, inputDateHourUTC)
    val outputPath = pathBucketDate(sessionizeConfig.config.outputSessionized, processingBucket, inputDateHourUTC)
    val outputCache = pathBucketDate(sessionizeConfig.config.outputCache, processingBucket, hourDTCacheUTC)

    val errorMessageFile = pathBucketDate(
      sessionizeConfig.config.errorMessageFilePattern, processingBucket, inputDateHourUTC)

    try {
      // Delete existing files
      forcePathDeletion(force, outputPath, getFs(outputPath))
      forcePathDeletion(force, outputCache, getFs(outputCache))

      spark.sparkContext.setJobDescription(s"$inputDateHourUTC UTC ($processingBucket)")

      /* Data Load */
      val normalizedWithUser = spark.read
        .schema(getSchemaAsStruct[NormalizedWithUser])
        .parquet(normalizedInput)
        .as[NormalizedWithUser]

      val normalizedWithUserCount = normalizedWithUser.count

      val sessionizationCache = if (skipInputCache)
          List(SessionizationCache.empty).toDS
        else
          spark.read
            .schema(getSchemaAsStruct[SessionizationCache])
            .parquet(inputCache)
            .as[SessionizationCache]

      /* Filter Cache */
      val filteredCaches = steps.FilterCaches.asDataset(
        sessionizationCache,
        hourEndTimeStamp,
        lookBackWindowHours
      )(spark).persist(StorageLevel.MEMORY_AND_DISK)

      val cacheCount = sessionizationCache.count

      /* Filter Out Robotic Events */
      val nonRoboticEvents = steps.FilterRoboticEventsWithPartitioner.asDataset(
        normalizedWithUser = normalizedWithUser, sessionCache = sessionizationCache, useCustomPartitioner = true, partitionConfig = sessionizeConfig.config.partitionConfig, normalizedWithUserCount = normalizedWithUserCount, sessionCacheCount = cacheCount
      )(spark)

      /* Join Data */
      val initialized = steps.InitializeWithCaches.asDataset(
        nonRoboticEvents.nonRoboticNormalizedWithUser, nonRoboticEvents.nonRoboticSessionCache, hourDT
      )(spark)

      /* Remove Duplications */
      val deduplicated = steps.DedupAndOrderPerSG.asDataset(initialized)(spark)

      /* Calculate Previous Data Via Company Grouping */
      val withPreviousPage = steps.PreviousDataPerCompany.asDataset(deduplicated)(spark)

      /* Section Data Into Sessions */
      val cutIntoSessions = steps.CutIntoSessions.asDataset(withPreviousPage, hourEndTimeStamp, sessionLifetimeMapping)(spark)

      /* Calculate Data Via Service Group Groupings For Post Calculation */
      val analyzed = steps.PostSessionCut.asDataset(cutIntoSessions, evarDimensions)(spark)
        .persist(StorageLevel.MEMORY_AND_DISK)

      /* Write results */
      steps.WriteSessionCachePayload.asDataset(
        analyzed, cacheCount, partitionConfig.cachedRecordsPerCutFile, outputCache, filteredCaches
      )(spark)

      steps.WriteSessionizePayload.asDataset(
        analyzed, nonRoboticEvents.totalInputCount, partitionConfig.recordsPerCutFile, outputPath
      )(spark)

      analyzed.unpersist()

    } catch { // Catches any issue and proceeds to log for debugging purposes.
      case
    }
  }
}

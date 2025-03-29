package com.orinoco.commons

import com.orinoco.adapter.logging.JobReport

import java.util.concurrent.TimeUnit

object ElasticSearchUtils {
  /**
   * This creates logs for Elasticsearch to mark reason for application ending.
   * Output can be found in the respective Kibana environment.
   **/
  def applicationEndMessage(
                             taskName: String, status: String, envDC: String, dataCenter: String,
                             hostName: String, inputDateHourUTC: String, startTime: Long,
                             processingBucket: Option[String], phaseType: Option[String],
                             period: Option[String], message: String,
                             skipInputCache:Option[String] = None
                           ): JobReport = {
    val endTime = System.currentTimeMillis()
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - startTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

    JobReport(
      task = taskName,
      status = status,
      env = envDC,
      dataCenter = dataCenter,
      hostName = hostName,
      inputDateHourUTC = inputDateHourUTC,
      runStartTime = startTime,
      runEndTime = Some(endTime),
      runDuration = runDuration,
      executionDurationInSeconds = executionDurationInSeconds,
      specific = Map(
        "message" -> message,
        "processingBucket" -> processingBucket.getOrElse("None"),
        "phaseType" -> phaseType.getOrElse("None"),
        "period" -> period.getOrElse("None"),
        "skipInputCache" -> skipInputCache.getOrElse("None")
      )
    )
  }

  // Same as applicationEndMessage but for cross-check and validation checks.
  def applicationEndMessageCrosscheck(
                                       taskName: String, status: String, envDC: String, dataCenter: String,
                                       hostName: String, inputDateHourUTC: String, startTime: Long,
                                       processingBucket: String, phaseType: String,
                                       basin: String, tableType: String, message: String
                                     ): JobReport = {
    val endTime = System.currentTimeMillis()
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - startTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

    JobReport(
      task = taskName,
      status = status,
      env = envDC,
      dataCenter = dataCenter,
      hostName = hostName,
      inputDateHourUTC = inputDateHourUTC,
      runStartTime = startTime,
      runEndTime = Some(endTime),
      runDuration = runDuration,
      executionDurationInSeconds = executionDurationInSeconds,
      specific = Map(
        "message" -> message,
        "processingBucket" -> processingBucket,
        "phaseType" -> phaseType,
        "basin" -> basin,
        "tableType" -> tableType
      )
    )
  }

  /**
   * Same as applicationEndMessage but for validation checks.
   **/
  def applicationEndMessageValidation(
                                       taskName: String, status: String, envDC: String, dataCenter: String,
                                       hostName: String, inputDateHourUTC: String, startTime: Long,
                                       processingBucket: String, phaseType: String,
                                       basin: String, tableType: String, message: String
                                     ): JobReport = {
    val endTime = System.currentTimeMillis()
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - startTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

    JobReport(
      task = taskName,
      status = status,
      env = envDC,
      dataCenter = dataCenter,
      hostName = hostName,
      inputDateHourUTC = inputDateHourUTC,
      runStartTime = startTime,
      runEndTime = Some(endTime),
      runDuration = runDuration,
      executionDurationInSeconds = executionDurationInSeconds,
      specific = Map(
        "message" -> message,
        "processingBucket" -> processingBucket,
        "phaseType" -> phaseType,
        "basin" -> basin,
        "tableType" -> tableType
      )
    )
  }

  /**
   * Same as applicationEndMessage but for hive partitioning.
   **/
  def applicationEndMessageHive(
                                 taskName: String, status: String, envDC: String, dataCenter: String, hostName: String, inputDateHourUTC: String,
                                 startTime: Long, processingBucket: String, phaseType: String, tableType: String, message: String
                               ): JobReport = {
    val endTime = System.currentTimeMillis()
    val startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTime)
    val endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(endTime)
    val runDuration = Some(endTime - startTime)
    val executionDurationInSeconds = Some(endTimeInSeconds - startTimeInSeconds)

    JobReport(
      task = taskName,
      status = status,
      env = envDC,
      dataCenter = dataCenter,
      hostName = hostName,
      inputDateHourUTC = inputDateHourUTC,
      runStartTime = startTime,
      runEndTime = Some(endTime),
      runDuration = runDuration,
      executionDurationInSeconds = executionDurationInSeconds,
      specific = Map(
        "message" -> message,
        "processingBucket" -> processingBucket,
        "phaseType" -> phaseType,
        "tableType" -> tableType
      )
    )
  }
}

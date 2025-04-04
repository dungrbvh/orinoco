package com.orinoco.config.base

import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class PartitionConfig(
                            recordsPerPartition: Int,
                            recordsPerCutFile: Int,
                            cachedRecordsPerCutFile: Int,
                            roboticSessionSize: Int,
                            largeSessionSize: Int
                          )

case class SessionizationConfig(
                                 lookBackWindowHours: Int,
                                 partitionConfig: PartitionConfig,
                                 sessionLifetimeMapping: Map[Int, Int],
                                 inputNormalized: String,
                                 inputCache: String,
                                 outputSessionized: String,
                                 outputCache: String,
                                 errorMessageFilePattern: String
                               )

object SessionizationConfig {
  def load(config: Config): SessionizationConfig = {
    SessionizationConfig(
      lookBackWindowHours = config.getInt("look-back-window-hours"),
      partitionConfig = PartitionConfig(
        recordsPerPartition = config.getInt("partition-info.records-per-partition"),
        recordsPerCutFile = config.getInt("partition-info.records-per-cut-file"),
        cachedRecordsPerCutFile = config.getInt("partition-info.cached-records-per-cut-file"),
        roboticSessionSize = config.getInt("partition-info.robotic-session-size"),
        largeSessionSize = config.getInt("partition-info.large-session-size")
      ),
      sessionLifetimeMapping = config.getAnyRefList("session-lifetime-mapping").asScala.toList.collect {
        case array: java.util.ArrayList[Int] @unchecked => array.get(0) -> array.get(1)
      }.toMap,
      inputNormalized = config.getString("input-pattern.normalized"),
      inputCache = config.getString("input-pattern.sessionize-cache"),
      outputSessionized = config.getString("output-pattern.sessionized"),
      outputCache = config.getString("output-pattern.sessionize-cache"),
      errorMessageFilePattern = config.getString("error-message-file-pattern")
    )
  }
}
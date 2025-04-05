package com.orinoco.config.task

import com.orinoco.option.HivePartitioningOptions
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write
import scala.collection.JavaConverters._


case class HivePartitioningConfig (
  env: String,
  dataCenter: String,
  hostName: String,
  parallelJobs: Int,
  recordsPerCutFilePublic: Int,
  recordsPerCutFilePrivate: Int,
  excludeFieldsPublicTable: List[String],
  excludeFieldsPrivateTable: List[String],
  inputSessionized: String,
  inputPostCalculate: String,
  inputSnapshotServiceGroupExclusionFor2ndUpdatePath: String,
  outputPublicRatSessionized: String,
  outputPrivateRatSessionized: String,
  options: HivePartitioningOptions,
  elasticStatusUpdaterConfig: ElasticStatusUpdaterConfig,
  errorMessageFilePattern: String
                                  ) {
  def dump(): String = {
    write(this)(DefaultFormats)
  }
}

object HivePartitioningConfig {

  def load(args: Array[String], mainConfig: Config): HivePartitioningConfig = {
    val config = mainConfig.getConfig("hive-partitioning")

    HivePartitioningConfig(
      env = mainConfig.getString("env"),
      dataCenter = mainConfig.getString("data-center"),
      hostName = mainConfig.getString("host-name"),

      parallelJobs = config.getInt("parallel-jobs"),
      recordsPerCutFilePublic = config.getInt("partition-info.records-per-cut-file.public"),
      recordsPerCutFilePrivate = config.getInt("partition-info.records-per-cut-file.private"),

      excludeFieldsPublicTable = config.getStringList("exclude-fields.public-rat-sessionized").asScala.toList,
      excludeFieldsPrivateTable = config.getStringList("exclude-fields.private-rat-sessionized").asScala.toList,

      inputSessionized = config.getString("input-pattern.sessionized"),
      inputPostCalculate = config.getString("input-pattern.post-calculate"),
      inputSnapshotServiceGroupExclusionFor2ndUpdatePath = config.getString("input-pattern.snapshot.service-group-exclusion-for-2nd-update"),
      outputPublicRatSessionized = config.getString("output-pattern.public-rat-sessionized"),
      outputPrivateRatSessionized = config.getString("output-pattern.private-rat-sessionized"),
      options = HivePartitioningOptions(args),
      errorMessageFilePattern = config.getString("error-message-file-pattern")
    )
  }
}
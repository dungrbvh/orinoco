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

  }
}

package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.application.sessionize.RecordsPerServiceGroup2
import org.apache.spark.sql.{Dataset, SparkSession}

object WriteSessionizePayload {

  def asDataset(
                 payload: Dataset[(String, List[RecordsPerServiceGroup2])],
                 totalInputCount: Long,
                 recordsPerCutFile: Int,
                 outputPath: String
               )(implicit spark: SparkSession): Unit = {
    import  spark.implicits._

    val numPartitions: Int = Math.max(300, totalInputCount / recordsPerCutFile).toInt
    payload
      .mapPartitions(_.flatMap(_._2.flatMap(_.payloadSession.flatMap(_._2)))) // Grab base payload
      .mapPartitions(_.map(_.copy(sc = Option(SessionizationCache.empty))))
      .toDF
      .repartition(numPartitions) // Apply repartition to entire payload
      .write
      .parquet(outputPath)
    println(s"Sessionization Output: $outputPath")
  }
}
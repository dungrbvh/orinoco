package com.orinoco.adapter.normalize.steps

import com.orinoco.config.task.NormalizeConfig
import com.orinoco.schema.normalize.NormalizedWithUser
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator




object WriteNormalizedWithUser {

  def apply(
           normalizedWithUser: Dataset[NormalizedWithUser],
           broadcastConfig: Broadcast[NormalizeConfig],
           numPartitions: Int,
           outputPathNormalize: String,
           normalizeOutputCounter: LongAccumulator
           )(implicit spark: SparkSession): Unit = {

    normalizedWithUser.filter(x =>
      {
        val ret = !broadcastConfig.value.filterServiceGroupList.contains(x.norm.service_group)
        if (ret) normalizeOutputCounter.add(1L)
        ret
      }
        .repartition(numPartitions)
        .toDF()
        .write
        .parquet(outputPathNormalize)
    )
  }
}
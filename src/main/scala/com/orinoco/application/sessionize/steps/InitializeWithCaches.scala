package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.commons.DateUtils.DateParts
import com.orinoco.schema.normalize.NormalizedWithUser
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.DateTime

object InitializeWithCaches {
  /**
   * Combines normalized data with previous sessionized caching data.
   *
   * @param normalizedWithUser  Data of NormalizedWithUser
   * @param sessionizationCache Data of SessionizationCache
   * @param spark               Necessary for spark usage
   * @return
   */
  def asDataset(
               normalizedWithUser: Dataset[NormalizedWithUser],
               sessionizationCache: Dataset[SessionizationCache],
               hourDT: DateTime
               )(implicit spark: SparkSession): Dataset[(String, List[SessionizePayload])] = {
    normalizedWithUser.joinWith(sessionizationCache,
        normalizedWithUser("norm.sessionization_key") === sessionizationCache("sessionization_key")
          && normalizedWithUser("norm.company") === sessionizationCache("company")
          && normalizedWithUser("norm.service_group") === sessionizationCache("service_group"),
        "leftouter")
      .transform(createSessionizePayload(spark, hourDT)) // Step 1
      .transform(payloadSessionKeyGrouping(spark)) // Step 2

  }

  /**
   * Step 1: Initialize Datasets by Key; Co-group all Datasets and return OneKeyWithAllInput
   *
   */
  def createSessionizePayload(spark: SparkSession, hourDT: DateTime): Dataset[(NormalizedWithUser, SessionizationCache)] => Dataset[SessionizePayload] = {
    import spark.implicits._
    ds =>
      ds.map(x => {
        val normalizedWithUser: NormalizedWithUser = x._1
        val sessionizationCache: SessionizationCache = x._2
        joinRecords(
          normalizedWithUser,
          Option(sessionizationCache),
          hourDT
        )
      })
  }

  /**
   * Step 2: Join all Datasets and return SessionizePayload
   *
   */
  def payloadSessionKeyGrouping(spark: SparkSession):
    Dataset[SessionizePayload] => Dataset[(String, List[SessionizePayload])] = {

    import spark.implicits._

    ds =>
      ds.groupByKey(_.sessionization_key)
        .mapGroups((k,v) => (k, v.toList))
  }

  /**
   * Returns an object of SessionizePayload
   *
   * Skip sessionization logic if not found in nwu
   */
  def joinRecords(nwu: NormalizedWithUser, sc: Option[SessionizationCache], hourDT: DateTime): SessionizePayload = {
    SessionizePayload(
      time_stamp_epoch = nwu.norm.time_stamp_epoch,
      year = hourDT.utcyyyy,
      month = hourDT.utcMM,
      day = hourDT.utcdd,
      hour = hourDT.utcHH,
      sessionization_key = nwu.norm.sessionization_key,
      company = nwu.norm.company,
      service_group = nwu.norm.service_group,
      norm = nwu.norm,
      cdna = nwu.cdna,
      uuid = nwu.norm.uuid,

      sc = sc
    )
  }
}
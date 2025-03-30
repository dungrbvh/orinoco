package com.orinoco.adapter.normalize.steps

import com.orinoco.config.task.NormalizeConfig
import com.orinoco.schema.normalize.Parsed
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{coalesce, lit}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

object RepartitionAndFilterRecord {
  case class RepartitionAndFilterRecord(dataset: Dataset[Parsed], numPartitions: Long)

  def apply(parsed: Data[Parsed],
            numPartitions: Long,
            broadcastConfig:Broadcast[NormalizeConfig])
           (implicit spark: SparkSession): Dataset[Parsed] = {
    import spark.implicits._

    parsed
      .repartition(numPartitions)
      .filter {
        val accsExemptFromScrollExclusion = broadcastConfig.value.accsExemptFromScrollExclusionSet
        val accsExemptFromIframeExclusion = broadcastConfig.value.accsExemptFromIframeExclusionSet
        val accAidsExemptFromIframeExclusion = broadcastConfig.value.accAidsExemptFromIframeExclusionSeq

        !generalExclusion(
          accsExemptFromScrollExclusion,
          accsExemptFromIframeExclusion,
          accAidsExemptFromIframeExclusion,
          $"etype",
          $"aid",
          $"ifr",
          $"acc",
          $"acc_aid",
          $"url",
          $"app_type",
          $"cp"
        ) &&
          rejectUnwantedCheckoutConfirmedEvents($"pgt", $"order_id", $"navtype")
      }
  }

  def generalExclusion(
                        accsExemptFromScrollExclusion: Set[Int],
                        accsExemptFromIframeExclusion: Set[Int],
                        accAidsExemptFromIframeExclusion: Seq[String],
                        etype: Column,
                        aid: Column,
                        ifr: Column,
                        acc: Column,
                        accAid: Column,
                        url: Column,
                        appType: Column,
                        cp: Column
                      ): Column = {
    val isEtypeScroll = etype.eqNullSafe("scroll")
    val isAidGreaterThan1000 = aid.gt(1000)
    val isIfrEqualTo1 = ifr.eqNullSafe(1)

    // CONRAT-26852  List of acc to have Scroll event_type accepted
    val isAccsExemptFromScrollExclusion = coalesce(acc, lit(0)).isInCollection(accsExemptFromScrollExclusion)

    // CONRAT-11472  List of IFR to be excluded
    val isAccExemptFromIFrameExclusion = coalesce(acc, lit(0)).isInCollection(accsExemptFromIframeExclusion)
    val isAccAidExemptFromIFrameExclusion = coalesce(accAid, lit("")).isInCollection(accAidsExemptFromIframeExclusion)

    val isURLContains = coalesce(url, lit("")).contains("sc_off=true")
    val isNative = appType.eqNullSafe("native")
    val isNativeAndPVAndCPContainsRefType =
      isNative &&
        etype.eqNullSafe("pv") &&
        cp.getField("ref_type").eqNullSafe("internal") &&
        acc.eqNullSafe(101)
    val isNullAppType = appType.isNull

    (isEtypeScroll && !isAccsExemptFromScrollExclusion) ||
      isAidGreaterThan1000 ||
      (isIfrEqualTo1 && !(isAccExemptFromIFrameExclusion || isAccAidExemptFromIFrameExclusion)) ||
      isURLContains ||
      isNativeAndPVAndCPContainsRefType ||
      isNullAppType
  }

  /**
   * Reject Record if:
   *   pgt is "cart_checkout" &&
   *   order_id is not null &&
   *   navtype is not 0
   **/
  def rejectUnwantedCheckoutConfirmedEvents(pgt: Column, orderId: Column, navType: Column): Column = {
    !(pgt.eqNullSafe("cart_checkout")  &&
      orderId.isNotNull &&
      coalesce(navType, lit(0)).notEqual(0))
  }
}
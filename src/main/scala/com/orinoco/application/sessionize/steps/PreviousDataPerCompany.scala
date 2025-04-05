package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.cache.{PrevPlaceholder, SessionizationCache}
import com.orinoco.adapter.sessionize.sections.PreSessionDataSection
import com.orinoco.commons.SessionizeRowUtils._
import org.apache.spark.sql.{Dataset, SparkSession}

object PreviousDataPerCompany {
  case class Placeholder(record: List[SessionizePayload], prevPlaceholder: Option[PrevPlaceholder])

  // This class assumes that the payload is already deduplicated and sorted by timestamp, uuid and hashcode
  def asDataset(
               payload: Dataset[(String,List[SessionizePayload])]
               )(implicit spark: SparkSession): Dataset[(String, List[SessionizePayload])] = {
    import spark.implicits._

    payload.mapPartitions(itr => {
      itr.flatMap(kv => {
        val key = kv._1
        val recordsPerKey = kv._2
        val companyList = recordsPerKey.map(_.company).distinct

        companyList.map {
          company => (key, previousDataPerCompany(recordsPerKey, company))
        }
      })
    })
  }

  def previousDataPerCompany(list: List[SessionizePayload], company: String): List[SessionizePayload] = {

    val filteredList = list
      .filter(_.company.equals(company))
      .sortBy(x => (x.time_stamp_epoch, x.uuid, x.hashCode()))

    val cachedPrevPlaceholder = filteredList
      .map(x => x.sc.getOrElse(SessionizationCache.empty).prev_place_holder.getOrElse(PrevPlaceholder.empty)).head

    filteredList
      .filter(_.company.equals(company))
      .sortBy(x => (x.time_stamp_epoch, x.uuid, x.hashCode()))
      .foldLeft(Placeholder(List.empty[SessionizePayload], Option(cachedPrevPlaceholder))) {
        (r: Placeholder, c: SessionizePayload) => {
          val isPageView = pageViewFinder(c)
          val isWebPageView = webFinder(c) && isPageView
          val isParts = serviceCategoryPartsFinder(c)

          val _inflowChannel: String = null

          val inflowChannel: Option[String] = Option(if (_inflowChannel != "Internal") _inflowChannel else null)

          val placeholder: PreSessionDataSection = c.pre_session_data_section.getOrElse(PreSessionDataSection.empty)

          val pre: Option[PreSessionDataSection] = {
            if (placeholder.previous_service_group.isEmpty) {
              Option(
                PreSessionDataSection(
                  previous_page_name = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevPageName else None,
                  previous_path = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevPath else None,
                  previous_service_group = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevServiceGroup else None,
                  previous_site_section = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevSiteSection else None,
                  previous_domain = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevDomain else None,
                  previous_acc_aid = if (isPageView) r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevAccAid else None,
                  inflow_channel = inflowChannel,
                  inflow_channel_acc_aid = if (inflowChannel.isEmpty) None else c.norm.acc_aid,
                  inflow_channel_domain = if (inflowChannel.isEmpty) None else c.norm.domain,
                  inflow_channel_path = if (inflowChannel.isEmpty) None else c.norm.path,
                  inflow_channel_search_word = if (inflowChannel.isEmpty) None else c.norm.sq,
                  inflow_channel_site_section = if (inflowChannel.isEmpty) None else c.norm.ssc,
                  inflow_channel_page_name = if (inflowChannel.isEmpty) None else c.norm.pgn
                )
              )
            }
            else Option(placeholder.copy())
          }
          // Inherit from previous record if isPrevious(Web)Record; otherwise, inherit from previous previous record
          val prev = PrevPlaceholder(
            prevPath = if (isPreviousRecord(isPageView, isParts)) c.norm.path else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevPath,
            prevPageName = if (isPreviousRecord(isPageView, isParts)) c.norm.pgn else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevPageName,
            prevServiceGroup = if (isPreviousRecord(isPageView, isParts)) Some(c.norm.service_group) else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevServiceGroup,
            prevSiteSection = if (isPreviousRecord(isPageView, isParts)) c.norm.ssc else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevSiteSection,
            prevDomain = if (isPreviousRecord(isPageView, isParts)) c.norm.domain else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevDomain,
            prevAccAid = if (isPreviousRecord(isPageView, isParts)) c.norm.acc_aid else r.prevPlaceholder.getOrElse(PrevPlaceholder.empty).prevAccAid,
            prevServiceGroupForWeb = if (isPreviousWebRecord(isParts, isWebPageView)) Option(c.norm.service_group) else
              r.prevPlaceholder.get.prevServiceGroupForWeb
          )

          val norm = c.copy(
            sc = Option(
              c.sc.getOrElse(SessionizationCache.empty).copy(
                prev_place_holder = Option(prev)
              )
            ),
            pre_session_data_section =  pre
          )
          Placeholder(r.record :+ norm, Option(prev))
        }
      }.record
  }

  def isPreviousRecord(is_page_view: Boolean, is_parts: Boolean): Boolean = is_page_view && !is_parts

  def isPreviousWebRecord(is_parts: Boolean, is_web_page_view: Boolean): Boolean = !is_parts && is_web_page_view
}
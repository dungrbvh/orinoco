package com.orinoco.adapter.sessionize.cache

case class PrevPlaceholder (
  prevPath: Option[String] = None,
  prevPageName: Option[String] = None,
  prevServiceGroup: Option[String] = None,
  prevSiteSection: Option[String] = None,
  prevDomain: Option[String] = None,
  prevAccAid: Option[String] = None,
  prevServiceGroupForWeb: Option[String] = None
                           )

object PrevPlaceholder {
  def empty: PrevPlaceholder = PrevPlaceholder()
}
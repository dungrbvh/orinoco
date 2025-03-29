package com.orinoco.commons

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}

object DateUtils {
  val hourIsoFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH")
  val yyyyMMddHHFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")
  val yyyyMMddFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val yyyyMMddFormatterDash: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  val utcDateFormatterDash: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MMddHH")
      .withZone(DateTimeZone.UTC)

  val jstDateFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyyMMddHH")
      .withZone(DateTimeZone.forID("Asia/Tokyo"))

  val utcHourDateFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd'T'HH")
      .withZone(DateTimeZone.UTC)

  val utcDateFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyyMMddHH")
      .withZone(DateTimeZone.UTC)

  val utcTimestampISOFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd'T'HH:mm:ss")
      .withZone(DateTimeZone.UTC)

  val jstTimestampISOFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd'T'HH:mm:ss")
      .withZone(DateTimeZone.forID("Asia/Tokyo"))

  val hiveDatetimeFormatter: DateTimeFormatter =
    DateTimeFormat
      .forPattern("yyyy-MM-dd HH:mm:ss")
      .withZone(DateTimeZone.forID("Asia/Tokyo"))

  val yyyyFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy")
  val MMFormatter: DateTimeFormatter = DateTimeFormat.forPattern("MM")
  val ddFormatter: DateTimeFormatter = DateTimeFormat.forPattern("dd")
  val hhFormatter: DateTimeFormatter = DateTimeFormat.forPattern("hh")
  val HHFormatter: DateTimeFormatter = DateTimeFormat.forPattern("HH")
  val mmFormatter: DateTimeFormatter = DateTimeFormat.forPattern("mm")
  val ssFormatter: DateTimeFormatter = DateTimeFormat.forPattern("ss")

  implicit class DateParts(dt: DateTime) {
    def yyyy(dtz: DateTimeZone): String = yyyyFormatter.withZone(dtz).print(dt)

    def MM(dtz: DateTimeZone): String = MMFormatter.withZone(dtz).print(dt)

    def dd(dtz: DateTimeZone): String = ddFormatter.withZone(dtz).print(dt)

    def HH(dtz: DateTimeZone): String = HHFormatter.withZone(dtz).print(dt)

    def hhAMPM(dtz: DateTimeZone): String = hhFormatter.withZone(dtz).print(dt)

    def mm(dtz: DateTimeZone): String = mmFormatter.withZone(dtz).print(dt)

    def ss(dtz: DateTimeZone): String = ssFormatter.withZone(dtz).print(dt)

    def yyyyMMdd(dtz: DateTimeZone): String = yyyyMMddFormatter.withZone(dtz).print(dt)

    def yyyyMMddHH(dtz: DateTimeZone): String = yyyyMMddHHFormatter.withZone(dtz).print(dt)

    def utcyyyy: String = yyyy(DateTimeZone.UTC)

    def utcMM: String = MM(DateTimeZone.UTC)

    def utcdd: String = dd(DateTimeZone.UTC)

    def utcHH: String = HH(DateTimeZone.UTC)

    def utchhAMPM: String = hhAMPM(DateTimeZone.UTC)

    def utcmm: String = mm(DateTimeZone.UTC)

    def utcss: String = ss(DateTimeZone.UTC)

    def utcyyyyMMdd: String = yyyyMMdd(DateTimeZone.UTC)

    def utcyyyyMMddHH: String = yyyyMMddHH(DateTimeZone.UTC)

    def jstyyyy: String = yyyy(DateTimeZone.forID("Asia/Tokyo"))

    def jstMM: String = MM(DateTimeZone.forID("Asia/Tokyo"))

    def jstdd: String = dd(DateTimeZone.forID("Asia/Tokyo"))

    def jstHH: String = HH(DateTimeZone.forID("Asia/Tokyo"))

    def jsthhAMPM: String = hhAMPM(DateTimeZone.forID("Asia/Tokyo"))

    def jstmm: String = mm(DateTimeZone.forID("Asia/Tokyo"))

    def jstss: String = ss(DateTimeZone.forID("Asia/Tokyo"))

    def jstyyyyMMdd: String = yyyyMMdd(DateTimeZone.forID("Asia/Tokyo"))

    def jstyyyyMMddHH: String = yyyyMMddHH(DateTimeZone.forID("Asia/Tokyo"))
  }

  /**
   * Convert epoch time to 20XXXXXXX as integer
   * @param epoch
   * @return
   */
  def printEpochToUTC(epoch: Int): Int = {
    utcDateFormatter.print(epoch * 1000L).toInt
  }
}
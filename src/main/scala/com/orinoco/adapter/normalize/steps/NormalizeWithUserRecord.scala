package com.orinoco.adapter.normalize.steps

import com.google.common.net.InetAddresses
import com.maxmind.geoip2.model.CityResponse
import com.orinoco.adapter.normalize.GeoIpLookup
import com.orinoco.commons.DateUtils.DateParts
import com.orinoco.hive.ql.udf.{UDFComplementCV, UDFReferrerSummary, UDFReferrerType}
import com.orinoco.schema.cdna.CustomerDNA
import com.orinoco.schema.normalize.extended.{GroupedCarrierIdentification, GroupedMediaVideo}
import com.orinoco.schema.normalize.{JapanGeoLookup, Normalized, NormalizedWithUser, Parsed}
import com.orinoco.utils.ReferrerTypeUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.util.LongAccumulator
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import java.io.{ByteArrayInputStream, FileNotFoundException, IOException}
import java.net.{Inet4Address, Inet6Address}
import java.util
import java.util.concurrent.atomic.LongAccumulator
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object NormalizeWithUserRecord {
  /**
   * Objective:
   *    This script has two main purposes:
   *      1. Calculate normalized records
   *      2. Match said records with a user's CDNA
   *
   *    What are normalized records?
   *      This refers to a users general behavior during their session.
   *      Refer to C6000 documentation for more information.
   *
   *    What is CDNA?
   *      Otherwise known as Customer DNA, this is essentially information that describes the user.
   *      More information can be found here: https://customer-dna.dsd.intra.rakuten-it.com/login
   *
   * Notes:
   *    All calculations are derived from hive. See hive for details.
   *    Records are obtained from raw JSON data split via a users ACC AID combination.
   **/

  /* Inner value set */
  val referrerSummaryEvaluator = new UDFReferrerSummary()
  val referrerTypeEvaluator = new UDFReferrerType()
  val complementedCvEvaluator = new UDFComplementCV()

  // Regex logic from hive, "iOS|iPhone|iPod|iPad", not compatible
  // Mobile / UA regex patterns:
  val MOS_PATTERN: Pattern = """.*(iOS|iPhone|iPod|iPad).*""".r.pattern
  val UA_PATTERN_FOR_APP_NAME: Pattern = """.*-(.*rakuten.*)/.*""".r.pattern
  val UA_PATTERN_FOR_APP_VER: Pattern = """.*-(.*rakuten.*)/([0-9.]+).*""".r.pattern
  val MOS_PATTERN_FOR_OS: Pattern = """.* ([0-9.]+)""".r.pattern
  // Bank bot regex patterns:
  val BANK_MONEY_FORWARD_BOT_PATTERN: String = ".*MoneyForward.*"
  val BANK_MONEYTREE_BOT_PATTERN: String = "^Moneytree$"
  val BANK_ZAIM_BOT_SUBSTRING: String = "(https://zaim.net)"
  // Customer Parameter:
  val BRACKET_PATTERN: Pattern = """^\\[.*\\]$""".r.pattern

  val LITERAL_ARRAY_SEPARATOR = ","

  private val securitiesACCs = List(1003, 1296, 1297)

  private val mnoACCs = List(1312, 1313, 1314, 1316, 1528)

  def apply(
             parsedFiltered: Dataset[Parsed],
             broadcast: Broadcast[MapTablesToRecordAndBroadcast],
             normalizeFatalCounter: LongAccumulator
           ): Dataset[Either[String, NormalizedWithUser]] = {
    parsedFiltered.mapPartitions{partition =>
    val geoIpLookup = GeoIpLookup {
      if (broadcast.value.geoIpLookupByteArray.nonEmpty)
        Some(new ByteArrayInputStream(broadcast.value.geoIpLookupByteArray))
      else None
    }

    partion.map {
      rec =>
        NormalizeWithUserRecord.fromParsed(broadcast.value, geoIpLookup)(rec) match {
          case Success(i) => Right(i)
          case Failure(t) =>
            normalizeFatalCounter.add(1L)
            Left(s"Failed to normalize record: ${t.getStackTrace.mkString("Array(", ", ", ")")}, [$re
        }
      }
    } (Encoders.kryo[Either[String, NormalizedWithUser]])
  }

  def fromParsed(infoTN: MapTablesToRecordAndBroadcast, geoIpLookup: GeoIpLookup)(rec: Parsed): Try[NormalizedWithUser] = {
    Try {
      /* Setup */
      val eventType = rec.etype.getOrElse("")
      val appType = rec.app_type.getOrElse("")
      val ua = rec.ua.getOrElse("")
      val pgn = rec.pgn.getOrElse("")
      val mos = rec.mos.getOrElse("")
      val pgt = rec.pgt.getOrElse("")
      val cartTypeExists = rec.cp.exists(_.contains("cart_type"))

      // Interface with Hive's ComplementedCV UDF
      val javaOrderId = rec.order_id.getOrElse("")

      val javaMapCv =
        if (rec.cv.isDefined) {
          val cvMap = new java.util.HashMap[java.lang.String, java.lang.Double]()
          rec.cv.get.foreach(cv => cvMap.put(cv._1, cv._2.toDouble))
          cvMap
        } else null

      val javaTotalPrice = new util.ArrayList[java.lang.Double]()
      if (rec.total_price.isDefined)
        rec.total_price.get.flatten.foreach(d => javaTotalPrice.add(d))

      val javaNiList = new util.ArrayList[java.lang.Integer]()
      if (rec.ni.isDefined)
        rec.ni.get.flatten.foreach(ni => javaNiList.add(ni))

      val javaPrice: util.ArrayList[java.lang.Double] = new util.ArrayList[java.lang.Double]()
      if (rec.price.isDefined)
        rec.price.get.flatten.foreach(d => javaPrice.add(d))

      val complementedCV: Option[util.Map[String, java.lang.Double]] = rec.acc_aid match {
        case Some(acc_aid) if infoTN.complementedCvAccAidExemptions.contains(acc_aid) => Option(javaMapCv)
        case _ => Option(
          complementedCvEvaluator.evaluate(javaOrderId, javaMapCv, javaTotalPrice, javaNiList, javaPrice))
      }

      val shopIdPurchase: Option[Int] = getShopIdPurchase(rec.cv, rec.shopid_itemid)

      // Made into variable due to long condition and repeated check
      val isComplementedCVCard1 = complementedCV.exists(cv => cv.containsKey("cart_add") && cv.get("cart_add") == 1)

      /* Hive Logic */
      val serviceGroup = rec.acc_aid.flatMap {
        sgKey =>
          val line = sgKey.split("_")
          for {
            key <- Try(line(0).toInt, line(1).toInt).toOption
            result <- infoTN.serviceGroupRemapping.get(key)
          } yield result
      }
        .orElse(rec.acc.flatMap(sgKey => infoTN.serviceGroupMapping.get(sgKey)))
        .getOrElse("other")

      // TODO: Make this not a value
      val timeStamp = rec.ts1.map(_.toInt).orElse(rec.ts).getOrElse(
        throw new Exception("Record did not have ts1 or ts.")
      )

      val dtYYYYMMDDhh = new DateTime(rec.ts.getOrElse(-1) * 1000L)

      val jstYYYYMMDDhh = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH")
        .print(dtYYYYMMDDhh.withZone(DateTimeZone.forID("Asia/Tokyo")))

      val timeStampTZConverter: String =
        if (infoTN.serviceGroupTimezoneExceptions.contains(serviceGroup))
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            .print(dtYYYYMMDDhh.withZone(DateTimeZone.UTC))
        else
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            .print(dtYYYYMMDDhh.withZone(DateTimeZone.forID("Asia/Tokyo")))

      val pageType = if (rec.acc_aid.contains("7_12") && rec.path_level1.contains("buy")) Option("cart_checkout") else rec.pgt

      val visitorID: Option[String] = {
        if (
          rec.use_cks.contains(false) &&
            rec.amp.contains(false) &&
            serviceGroup == "ichiba"
        ) rec.cks
        else if ( // recipe
          (rec.browser_cookie.isEmpty || rec.browser_cookie.get == null) &&
            rec.use_cks.contains(false) &&
            rec.amp.contains(true) &&
            serviceGroup == "recipe"
        ) Option(rec.ua.getOrElse("") + rec.ip.getOrElse(""))
        else if ( // plaza
          (rec.browser_cookie.isEmpty || rec.browser_cookie.get == null) &&
            rec.use_cks.contains(false) &&
            rec.amp.contains(true) &&
            serviceGroup == "plaza"
        ) Option(rec.ua.getOrElse("") + rec.ip.getOrElse(""))
        else if ( // securities
          (rec.browser_cookie.isEmpty || rec.browser_cookie.get == null) &&
            securitiesACCs.contains(rec.acc.get) &&
            rec.amp.contains(true)
        ) Option(rec.ua.getOrElse("") + rec.ip.getOrElse(""))
        else if (rec.browser_cookie.contains("null")) None // Cases of value being literal 'null' value
        else rec.browser_cookie
      }

      val sessionizationKey =
        if (visitorID.isEmpty || visitorID.get == null) rec.ua.getOrElse("") + rec.ip.getOrElse("")
        else visitorID.get

      val cartAdd: Option[Int] =
        if (eventType.contains("pv"))
          if (isComplementedCVCard1) Option(1) else Option(0)
        else None

      // TODO: cartAddFirst and cartAddMore can be 1 function
      val cartAddFirst: Option[Int] = if (eventType.contains("pv"))
        if (rec.cp.nonEmpty && isComplementedCVCard1 && cartTypeExists)
          if (rec.cp.get("cart_type").startsWith("add_first")) Option(1) else Option(0)
        else Option(0)
      else None

      val cartAddMore: Option[Int] = if (eventType.contains("pv"))
        if (rec.cp.nonEmpty && isComplementedCVCard1 && cartTypeExists)
          if (rec.cp.get("cart_type").startsWith("add_more")) Option(1) else Option(0)
        else Option(0)
      else None

      val cartView: Option[Int] =
        if (eventType.contains("pv"))
          if (
            complementedCV.exists(_.containsKey("cart_go_checkout")) && rec.cp.nonEmpty && cartTypeExists
          ) if (
            rec.cp.get("cart_type").startsWith("cart_view") && complementedCV.exists(_.get("cart_go_checkout") == 1)
          ) Option(1) else Option(0)
          else Option(0)
        else None

      val reload: Int = rec.navtype match {
        case None => 0
        case Some(1) => 1
        case Some(_) => 0
      }

      val postReview: Option[Int] =
        if (eventType.contains("pv"))
          if (complementedCV.exists(_.containsKey("review_post"))) Option(1) else Option(0)
        else None

      val bookmarkAdd: Option[Int] =
        if (eventType.contains("pv"))
          if (complementedCV.exists(_.containsKey("bookmark_add"))) Option(1) else Option(0)
        else None

      val bookmarkAddShop: Option[Int] =
        if (eventType.contains("pv"))
          if (complementedCV.exists(_.containsKey("bookmark_add_shop"))) Option(1) else Option(0)
        else None

      val purchaseShop: Option[Int] =
        if (eventType.contains("pv"))
          if (complementedCV.exists(_.containsKey("purchase_shop"))) Option(1) else Option(0)
        else None

      val application: Int = if (complementedCV.exists(_.containsKey("application"))) 1 else 0

      val expandedBot: Option[Int] =
        if (
          rec.easyid.isEmpty
            && rec.acc.contains(3) // service_group=books
            && Array("Mozilla Firefox 48.0", "Google Chrome 52.0").contains(rec.browser.getOrElse(""))
        ) Option(1)
        else rec.bot

      /**
       * Hive logic:
       * IF (bot = 1, 1, 0)
       * As per hive doc, 1 = true_int, 0 = false_int
       **/
      val isBot: Option[Boolean] = expandedBot match {
        case None =>
          // ACC bank special bot judgement
          if (List[Int](1004, 1302, 1450, 1451).contains(rec.acc.get))
            rec.ua match {
              case None => Some(false)
              case Some(userAgent) =>
                Some(
                  userAgent.matches(BANK_MONEY_FORWARD_BOT_PATTERN) ||
                    userAgent.matches(BANK_MONEYTREE_BOT_PATTERN) ||
                    userAgent.contains(BANK_ZAIM_BOT_SUBSTRING)
                )
            }
          else Option(false)
        case Some(value) => if (value == 1) Option(true) else Option(false)
      }

      val isInternal: Option[Boolean] = expandedBot match {
        case None => Option(false)
        case Some(value) => if (value == 0) Option(true) else Option(false)
      }

      /** books sends data as:
       *    1. etype as click
       *       2. "cart_add" in cv and equals 1
       *       3. "cart_status" in cp not found
       *       Need to treat events as cart_add_click = 1
       **/
      val cartAddClick: Option[Int] =
        if (eventType.contains("click"))
          if (
            isComplementedCVCard1 && (
              rec.cp.isEmpty ||
                !rec.cp.get.contains("cart_status") ||
                !rec.cp.get("cart_status").toLowerCase().equals("error")
              )
          ) Option(1)
          else Option(0)
        else None

      val clickValue: Option[Int] = if (eventType.contains("click")) Option(1) else None

      /* Media Values */
      val media = rec.media.getOrElse(Map.empty[String, String])

      // TODO: Make media into function
      val mediaSegment: Option[String] =
        if (media.keySet.contains("seg")) Option(rec.media.get("seg"))
        else None

      val mediaAutoPlay: Option[Int] =
        if (media.keySet.contains("autoplay")) Try(rec.media.get("autoplay").toInt).toOption
        else None

      val mediaEvent: Option[String] =
        if (media.keySet.contains("event")) Option(rec.media.get("event").replaceAll("\"", ""))
        else None

      val mediaIframeUrl: Option[String] =
        if (media.keySet.contains("iframeurl")) Option(rec.media.get("iframeurl").replaceAll("\"", ""))
        else None

      val mediaName: Option[String] =
        if (media.keySet.contains("name")) Option(rec.media.get("name").replaceAll("\"", ""))
        else None

      val videoAutoPlay: Option[Int] =
        if (eventType.contains("video"))
          if (mediaAutoPlay.getOrElse(0) == 1) Option(1)
          else Option(0)
        else None

      val videoComplete: Option[Int] =
        if (eventType.contains("video"))
          if (mediaEvent.getOrElse("").toLowerCase().equals("complete")) Option(1) else Option(0)
        else None

      val videoLoad: Option[Int] =
        if (eventType.contains("video"))
          if (mediaEvent.getOrElse("").toLowerCase().equals("exist")) Option(1) else Option(0)
        else None

      val videoMilestone: Option[Int] =
        if (eventType.contains("video"))
          if (mediaEvent.getOrElse("").toLowerCase().equals("milestone")) Option(1) else Option(0)
        else None

      val videoPlay: Option[Int] =
        if (eventType.contains("video"))
          if (mediaEvent.getOrElse("").toLowerCase().equals("play")) Option(1) else Option(0)
        else None

      val videoSegmentComplete: Option[Int] =
        if (eventType.contains("video"))
          if (mediaEvent.getOrElse("").toLowerCase().equals("segment_complete")) Option(1) else Option(0)
        else None

      val ratDeviceCode: Option[String] = if (appType.equals("native")) None else rec.rat_device_code

      val appName: Option[String] =
        if (appType.equals("native") && rec.app_name.getOrElse("").equals("jp.co.rakuten.ichiba.ent"))
          Option("jp.co.rakuten.ichiba")
        else if (appType.equals("web") && ua.contains("IchibaApp-jp.co.rakuten.ichiba.ent"))
          Option("jp.co.rakuten.ichiba") // -- for others
        else if (appType.equals("web") && ua.contains("rakuten")) {
          val m = UA_PATTERN_FOR_APP_NAME.matcher(ua)
          if (m.matches) Option(m.group(1))
          else rec.app_name
        } else rec.app_name

      val appVer: Option[String] =
        if (appType.equals("web") && ua.contains("rakuten")) {
          val m = UA_PATTERN_FOR_APP_VER.matcher(ua)
          if (m.matches && m.groupCount > 1) Option(m.group(2))
          else rec.app_ver
        } else rec.app_ver

      val ssc: Option[String] =
        if (appType.equals("native") && pgn.equals("searchtab")) Option("search")
        else rec.ssc

      val deviceType: Option[String] = if (appType.equals("native")) None else rec.device_type

      val operatingSystemType: Option[String] =
        if (appType.equals("native") && mos.contains("Android")) Option("Google Android")
        else if (appType.equals("native") && MOS_PATTERN.matcher(mos).matches()) Option("Apple iOS")
        else rec.os_type

      val operatingSystem: Option[String] =
        if (appType.equals("native") && mos.contains("Android")) rec.mos
        else if (appType.equals("native") && MOS_PATTERN.matcher(mos).matches) {
          val mobileIos = "Mobile iOS "
          val m = MOS_PATTERN_FOR_OS.matcher(mos)
          if (m.matches()) Option(mobileIos + m.group(1)) else rec.os
        } else rec.os

      val sid =
        if (appType.equals("native") && infoTN.pgtList.contains(pgt) && rec.target_ele.isDefined)
          if (rec.ref.isDefined) Option(rec.ref.get + "_" + rec.target_ele.get)
          else rec.target_ele
        else rec.sid

      val referrerSummary =
        if (appType.equals("web") && eventType.contains("pv"))
          Option(referrerSummaryEvaluator.evaluate(rec.ref.getOrElse(""), rec.url.getOrElse("")))
        else None

      val referrerType =
        if (appType.equals("web") && eventType.contains("pv")) {
          val socialNetworkList = readSocialNetworkList(infoTN.socialNetworkPath)
          Option(ReferrerTypeUtil.getType(rec.ref.getOrElse(""), socialNetworkList))
        } else None

      val scalaComplementedCv: Option[Map[String, Double]] =
        complementedCV.map(_.asScala.map(kv => (kv._1, kv._2.toDouble)).toMap)

      val purchaseGMS =
        if (eventType.contains("pv") && complementedCV.exists(_.containsKey("purchase_gms")))
          complementedCV.flatMap(cv => Option(cv.get("purchase_gms").toDouble))
        else None

      val purchaseOrder =
        if (eventType.contains("pv") && complementedCV.exists(_.containsKey("purchase_order")))
          complementedCV.flatMap(cv => Option(cv.get("purchase_order").toDouble))
        else None

      val purchaseItem =
        if (eventType.contains("pv") && complementedCV.exists(_.containsKey("purchase_item")))
          complementedCV.flatMap(cv => Option(cv.get("purchase_item").toDouble))
        else None

      val purchaseUnit =
        if (eventType.contains("pv") && complementedCV.exists(_.containsKey("purchase_unit")))
          complementedCV.flatMap(cv => Option(cv.get("purchase_unit").toDouble))
        else None

      val purchaseRegular =
        if (eventType.contains("pv") && complementedCV.exists(_.containsKey("purchase_regular")))
          complementedCV.flatMap(cv => Option(cv.get("purchase_regular").toDouble))
        else None

      // Updates cv and cp when multiple checkout or point_price/coupon_price/shipping_fee exist
      val checkoutCount =
        if (purchaseOrder.getOrElse(0.0) > 0)
          rec.order_list.map(_.length).getOrElse(0) match {
            case n: Int if n > 1 => n
            case _ => 1
          }
        else 0

      val customParameter: Option[Map[String, String]] =
        if (checkoutCount > 0)
          Option(rec.cp.getOrElse(Map.empty[String, String]) ++ Map("checkout_count" -> checkoutCount.toString))
        else rec.cp

      def emptyCPCheck(cp: Option[Map[String, String]]): Option[Map[String, String]] = cp match {
        case Some(value) => Option(value.filterNot(t => t._1.isEmpty))
        case None => None
      }

      val additionalCV: Option[Map[String, Double]] = if (checkoutCount == 1)
        Option(
          getAdditionalCV("point_price", rec.point_price, scalaComplementedCv) ++
            getAdditionalCV("coupon_price", rec.coupon_price, scalaComplementedCv) ++
            getAdditionalCV("shipping_fee", rec.shipping_fee, scalaComplementedCv)
        )
      else None

      val multiCheckoutCV: Option[Map[String, Double]] = checkoutCount match {
        case n: Int if n > 1 => Option(Map("multiple_checkout" -> 1.0))
        case 1 => Option(Map("multiple_checkout" -> 0.0))
        case _ => None
      }

      val conversionParameter: Option[Map[String, Double]] =
        if (additionalCV.isEmpty && multiCheckoutCV.isEmpty) scalaComplementedCv
        else Option(
          scalaComplementedCv.getOrElse(Map.empty[String, Double]) ++
            additionalCV.getOrElse(Map.empty[String, Double]) ++
            multiCheckoutCV.getOrElse(Map.empty[String, Double])
        )

      val eventCV = getEventCV(rec)
      val conversionParameterWithEvents: Option[Map[String, Double]] = conversionParameter match {
        case None => if (eventCV.isEmpty) None else Some(eventCV)
        case Some(cvMap) => Some(cvMap ++ eventCV)
      }

      val purchasedEasyId =
        if (scalaComplementedCv.isDefined && scalaComplementedCv.get.getOrElse("purchase_order", 0.0) > 0.0)
          if (rec.easyid.isDefined) rec.easyid else visitorID
        else None

      val deviceSummary =
        if (appName.nonEmpty) appName
        else deviceType.getOrElse("") match {
          case "PC" => Some("PC")
          case "iPhone" | "Android Mobile" => Some("SP")
          case "iPad" | "Android Tablet" => Some("TB")
          case _ => Some("Other")
        }

      /* Geo IP lookup */
      val validatedIp: Option[String] = rec.ip.filter(
        v => Try(
          InetAddresses.forString(v) match {
            case _: Inet4Address => true
            case _: Inet6Address => true
            case _ => false
          }
        ).getOrElse(false)
      )

      val geoIpCityResponse: Option[CityResponse] =
        if (validatedIp.nonEmpty) geoIpLookup.cityResponse(validatedIp.get) else None
      val geoIpCountry: Option[String] = geoIpLookup.country(geoIpCityResponse)
      val geoIpCity: Option[String] = geoIpLookup.city(geoIpCityResponse)
      val geoIpPostalCode: Option[String] = geoIpLookup.postalCode(geoIpCityResponse)

      val japanGeo: Option[JapanGeoLookup] =
        if (geoIpCountry.getOrElse("-1") == "Japan" && infoTN.japanGeoLookup.contains(geoIpPostalCode.getOrElse("-1")))
          Option(infoTN.japanGeoLookup(geoIpPostalCode.getOrElse("-1"))) else None

      val Cookies = {
        val cookieJar = Map(
          "Rp" -> rec.Rp.orNull,
          "_ra" -> rec._ra.orNull,
          "rat_v" -> rec.rat_v.orNull,
          "Rz" -> rec.Rz.orNull,
          "grm" -> rec.grm.orNull,
          "_mall_uuid" -> rec._mall_uuid.orNull,
          "Rt" -> rec.Rt.orNull,
          "m3_r" -> rec.m3_r.orNull,
          "BTA001" -> rec.BTA001.orNull,
          "tg_af_histid" -> rec.tg_af_histid.orNull,
          "ss_r" -> rec.ss_r.orNull,
          "Rq" -> rec.Rq.orNull
        ).filter(_._2 != null)

        if (cookieJar.nonEmpty) Option(cookieJar) else None
      }

      val itemGenrePath: Option[String] =
        if (rec.igenre.nonEmpty && rec.igenre.get.length > 0
          && infoTN.itemGenreMapping.contains(rec.igenre.get.toList.head.getOrElse("")))
          Option(infoTN.itemGenreMapping(rec.igenre.get.toList.head.getOrElse("")))
        else None

      val itemID: Option[Array[Option[String]]] =
        if (rec.browsing_itemid.isDefined) rec.browsing_itemid else rec.shopid_itemid

      /* Normalized Output */
      val normalized =
        Normalized(
          uuid = rec.uuid.getOrElse(""),
          company = infoTN.companyMapping.getOrElse(rec.acc.getOrElse(-1), "other"),
          service_group = serviceGroup,
          event_type = bucketedEventType(eventType),
          sessionization_key = sessionizationKey,
          // Timezone depends on service group
          time_stamp = timeStampTZConverter,
          // No timezone
          time_stamp_epoch = rec.ts.getOrElse(-1),

          // UTC
          year = dtYYYYMMDDhh.utcyyyy,
          month = dtYYYYMMDDhh.utcMM,
          day = dtYYYYMMDDhh.utcdd,
          hour = dtYYYYMMDDhh.utcHH,
          yyyymmddhh = dtYYYYMMDDhh.utcyyyyMMddHH.toInt,

          // JST
          dt = jstYYYYMMDDhh,
          easyid = rec.easyid,
          memberid = rec.memberid,
          ip = validatedIp,
          ua = rec.ua,
          mos = rec.mos,
          device_type = deviceType,
          device = if (appType.equals("native")) None else rec.device,
          browser_type = if (appType.equals("native")) None else rec.browser_type,
          browser_app_type = rec.browser_app_type,
          browser = if (appType.equals("native")) None else rec.browser,
          operating_system_type = operatingSystemType,
          operating_system = operatingSystem,
          bot_flag = expandedBot,
          bot_name = rec.bot_name,
          app_name = appName,
          app_type = rec.app_type,
          app_ver = appVer,
          lang = rec.lang,
          navtype = rec.navtype,
          loc = rec.loc.flatMap( // Grabs location value
            locationParam => Option(
              locationParam.mapValues(
                value => Try(value.toDouble) match {
                  case Success(value) => value
                  case Failure(_) => null
                }
              )
            )
          ).map(_.filter(_._2 != null).mapValues(_.toString.toDouble)), // Removes any key that has an empty value
          action_params = rec.action_params,
          res = rec.res,
          acc_aid = rec.acc_aid,
          service_category = Option(infoTN.serviceCategoryMapping.getOrElse(rec.acc.getOrElse(-1), "other")),
          service_type = rec.acc_aid,
          url = rec.url,
          domain = rec.domain,
          path = rec.path,
          path_level1 = rec.path_level1,
          path_level2 = rec.path_level2,
          path_level3 = rec.path_level3,
          path_level4 = rec.path_level4,
          path_level5 = rec.path_level5,
          igenre = arrayFlatten(rec.igenre),
          itemid = arrayFlatten(itemID),
          compid = arrayFlatten(rec.compid),
          ssc = ssc,
          pgl = rec.pgl,
          pgn = rec.pgn,
          pgt = pageType,
          rancode = rec.product_code,
          rat_device_code = ratDeviceCode,
          ver = rec.ver,
          tag = arrayFlatten(rec.tag),
          ref = rec.ref,
          referrer_domain = rec.referrer_domain,
          referrer_summary = referrerSummary,
          referrer_type = referrerType,
          scid =
            if (rec.scid.getOrElse("") != "") Option(rec.scid.get)
            else if (rec.sclid.getOrElse("") != "") Option(rec.sclid.get)
            else None,
          l_id = rec.lid,
          l2id = rec.l2id,
          sc2id = rec.sc2id,
          s_id = sid,
          lsid = rec.lsid,
          search_word_external = rec.search_word_external,
          sq = rec.sq,
          esq = rec.esq,
          zero_hit_search_word = rec.zero_hit_search_word,
          genre = rec.genre,
          navigation_genre_path =
            if (rec.genre.nonEmpty && infoTN.itemGenreMapping.contains(rec.genre.get))
              Option(infoTN.itemGenreMapping(rec.genre.get))
            else None,
          sgenre = rec.sgenre,
          shopid_itemid = arrayFlatten(rec.shopid_itemid),
          shopurl = rec.shopurl,
          itag = arrayFlatten(rec.itag),
          abtest = rec.abtest,
          phoenix_pattern = rec.phoenix_pattern,
          errors = rec.errors,
          order_list = arrayFlatten(rec.order_list),
          order_id = rec.order_id,
          cp = emptyCPCheck(customParameter),
          abtest_target = rec.abtest_target,
          cv = conversionParameterWithEvents,
          shop_url_purchase =
            if (shopIdPurchase.nonEmpty && infoTN.shopUrlMapping.contains(shopIdPurchase.get))
              infoTN.shopUrlMapping.get(shopIdPurchase.get)
            else None,
          page_view = if (eventType.contains("pv")) Option(1) else None,
          cart_add = cartAdd,
          cart_add_first = cartAddFirst,
          cart_add_more = cartAddMore,
          cart_view = cartView,
          reload = Option(reload),
          post_review = postReview,
          bookmark_add = bookmarkAdd,
          bookmark_add_shop = bookmarkAddShop,
          purchase_shop = purchaseShop,
          application = Option(application),
          target_ele = rec.target_ele,
          actype = rec.actype,
          mnetw = rec.mnetw,
          model = rec.model,
          mori = Option(rec.mori.getOrElse(0)),
          cart_add_click = cartAddClick,
          click = clickValue,
          deviceid_list = rec.deviceid_list,
          is_bot = isBot,
          is_internal = isInternal,
          isaction = rec.isaction,
          js_user_agent = rec.js_ua,
          async = if (eventType.contains("async")) Option(1) else None, // TODO: Make function
          appear = if (eventType.contains("appear")) Option(1) else None,
          member = if (rec.easyid.isDefined || rec.memberid.isDefined) Option("member") else Option("non-member"),
          contents_pos = rec.contents_pos,
          purchased_easyid = purchasedEasyId,
          cc = rec.campaign_code,
          itemurl = rec.itemurl,
          item_name = rec.item_name,
          amp = rec.amp,
          device_summary = deviceSummary,
          bgenre = convertArrayOfStringToString(rec.bgenre),
          prdctcd = convertArrayOfStringToString(rec.prdctcd),
          aflg = rec.aflg,
          srt = rec.srt,
          publisher = convertArrayOfStringToString(rec.publisher),
          maker = convertArrayOfStringToString(rec.maker),
          igenrepath = rec.igenrepath,
          igenrenamepath = rec.igenrenamepath,
          cycode = rec.cycode,
          cycodelist = convertArrayOfStringToString(rec.cycodelist),
          country = geoIpCountry,
          city = geoIpCity,
          reservation_id = rec.reservation_id,
          postal_code = geoIpPostalCode,

          // Geo
          region_1 = japanGeo.flatMap(_.region_1),
          region_2 = japanGeo.flatMap(_.region_2),
          region_3 = japanGeo.flatMap(_.region_3),
          region_4 = japanGeo.flatMap(_.region_4),
          latitude = japanGeo.flatMap(_.latitude),
          longitude = japanGeo.flatMap(_.longitude),
          elevation = japanGeo.flatMap(_.elevation),

          area = arrayToString(rec.area),
          payment = rec.payment,
          price = arrayFlatten(rec.price),
          shopid = rec.shopid,
          shopurllist = arrayToString(rec.shopurllist),
          mouseover = if (eventType.contains("mouseover")) Some(1) else None,
          ino = arrayToString(rec.ino),
          rg_books = rec.rg_books,
          rg_kobo = rec.rg_kobo,
          item_genre_path = itemGenrePath,
          ra = rec.ra,
          rp = rec.Rp,
          customerid = rec.customerid,
          etype = rec.etype.getOrElse(""),
          cka = rec.cka,
          ck = rec.ck,
          ck_tbs = rec.ck_tbs,
          ck_tc = rec.ck_tc,
          rat_v = rec.rat_v,
          mcn = rec.mcn,
          mcnd = rec.mcnd,
          mbat = rec.mbat,
          navtime = rec.navtime,
          shopidlist = arrayFlatten(rec.shopidlist),
          rqtime = rec.rqtime,
          ldtime = rec.ldtime,
          astime = rec.astime,
          ni = arrayFlatten(rec.ni),
          ni_order = arrayFlatten(rec.ni_order),
          oa = rec.oa,
          dln = rec.dln,
          bln = rec.bln,
          search_entity = arrayFlatten(rec.search_entity),
          tid = rec.tid,
          pgid = rec.pgid,
          issc = rec.issc,
          ifr = rec.ifr,
          ltm = rec.ltm,
          tzo = rec.tzo,
          target_pos = arrayFlatten(rec.target_pos),
          reslayout = rec.reslayout,
          cookies = Cookies,
          acc = rec.acc,
          aid = rec.aid,
          afid = rec.afid,
          bid = rec.bid,
          brand = arrayFlatten(rec.brand),
          chkout = rec.chkout,
          chkpt = rec.chkpt,
          ckp = rec.ckp,
          cks = rec.cks,
          cntln = rec.cntln,
          comptop = arrayFlatten(rec.comptop),
          coupon_price = arrayFlatten(rec.coupon_price),
          couponid = arrayFlatten(rec.couponid),
          errorlist = rec.errorlist,
          gol = rec.gol,
          hurl = rec.hurl,
          jav = rec.jav,
          mnavtime = rec.mnavtime,
          online = rec.online,
          point_price = arrayFlatten(rec.point_price),
          powerstatus = rec.powerstatus,
          reqc = rec.reqc,
          rescreadate = rec.rescreadate,
          resdate = rec.resdate,
          scond = arrayFlatten(rec.scond),
          shipping = rec.shipping,
          shipping_fee = arrayFlatten(rec.shipping_fee),
          sresv = arrayFlatten(rec.sresv),
          tis = rec.tis,
          total_price = arrayFlatten(rec.total_price),
          ts = rec.ts,
          ts1 = rec.ts1,
          userid = rec.userid,
          variation = arrayFlatten(rec.variation),
          purchase_gms = purchaseGMS,
          purchase_order = purchaseOrder,
          purchase_item = purchaseItem,
          purchase_unit = purchaseUnit,
          purchase_regular = purchaseRegular,
          variantid = arrayFlatten(rec.variantid),
          click_coord = arrayFlatten(rec.click_coord),
          tpgldtime = rec.tpgldtime,
          wv_fcp = rec.wv_fcp,
          wv_lcp = rec.wv_lcp,
          wv_fid = rec.wv_fid,
          wv_cls = rec.wv_cls,
          wv_ttfb = rec.wv_ttfb,
          wv_ver = rec.wv_ver,
          device_per = rec.device_per,
          wv_inp = rec.wv_inp,
          grouped_media_video = Option( // Grouped media and video related fields
            GroupedMediaVideo(
              media = rec.media,
              media_autoplay = mediaAutoPlay,
              media_event = mediaEvent,
              media_iframeurl = mediaIframeUrl,
              media_name = mediaName,
              media_segment = mediaSegment,
              video_auto_play = videoAutoPlay,
              video_complete = videoComplete,
              video_load = videoLoad,
              video_milestone = videoMilestone,
              video_play = videoPlay,
              video_segment_complete = videoSegmentComplete
            )
          ),
          grouped_carrier_id = Option( // Grouped carrier identifier fields
            GroupedCarrierIdentification(
              isroam = rec.isroam,
              netop = rec.netop,
              netopn = rec.netopn,
              simcid = rec.simcid,
              simcn = rec.simcn,
              simop = rec.simop,
              simopn = rec.simopn
            )
          )
        )
        NormalizedWithUser(norm = normalized, cdna = CustomerCDNA.empty)

    }
  }

  def arrayToString(x: Option[Array[Option[String]]]): Option[String] =
    if (x.isDefined) Some(x.get.flatten.mkString(LITERAL_ARRAY_SEPARATOR)) else None

  def arrayFlatten[T: ClassTag](x: Option[Array[Option[T]]]): Option[Array[T]] = x.map(_.flatten)

  def mapParser(
                 map: Option[Map[String, String]], keyName: String,
                 stripBrackets: Boolean = false, // optionally remove brackets from lists
                 stripNulls: Boolean = false // optionally remove nulls embedded in lists
               ): Option[String] = {

    def emptyMapStringCatcher(x: String): Boolean = x == null || x.isEmpty

    def hasBrackets(s: String): Boolean = s.matches("^\\[.*\\]$")

    def hasNulls(s: String): Boolean = s.matches("^.*null.*$")

    def removeBrackets(s: String): String = s.stripPrefix("[").stripSuffix("]")

    def removeNullsFromList(s: String): String = s.split(",").filterNot(x => x == "null").mkString(",")

    def stripBracketsFunc(s: String, stripBrackets: Boolean): String =
      if (stripBrackets && hasBrackets(s)) removeBrackets(s) else s

    def stripNullsFunc(s: String, stripNulls: Boolean): String =
      if (stripNulls && hasNulls(s)) {
        if (hasBrackets(s)) "[" + removeNullsFromList(removeBrackets(s)) + "]" else removeNullsFromList(s)
      } else s

    if (map.isEmpty) None
    else {
      if (map.get.contains(keyName) && map.get.contains(keyName)) {
        if (emptyMapStringCatcher(map.get(keyName))) None
        else {
          val v = stripBracketsFunc(stripNullsFunc(map.get(keyName), stripNulls), stripBrackets)
          if (v != "" && v != "null") Some(v) else None
        }
      }
      else None
    }
  }

  def bucketedEventType(rawEventType: String): String = {
    val knownEventTypes = List("appear", "async", "click", "deeplink", "error", "mouseover", "pv", "scroll", "video")
    if (rawEventType.startsWith("_rem")) "rem"
    else knownEventTypes.find(_ == rawEventType).getOrElse("other")
  }

  def getShopIdPurchase(cv: Option[Map[String, Int]], shopid_itemid: Option[Array[Option[String]]]): Option[Int] = {
    if (cv.nonEmpty &&
      cv.get.contains("purchase_gms") &&
      cv.get("purchase_gms") > 0 &&
      shopid_itemid.nonEmpty)
      Try(shopid_itemid.get.head.get.split("/").head.toInt).toOption else None
  }

  def getField[T](map: Option[Map[String, T]], fieldName: String): Option[T] =
    map.flatMap(_.get(fieldName))

  // Used for point_price, coupon_price, shipping_fee
  def getAdditionalCV(
                       key: String,
                       arr: Option[Array[Option[Double]]],
                       cv: Option[Map[String, Double]]
                     ): Map[String, Double] =
    if (arr.isEmpty || arr.getOrElse(Array()).length == 0)
      Map.empty[String, Double]
    else
      cv.getOrElse(Map.empty[String, Double]).get(key) match {
        case None => Map(key -> arr.get.apply(0).getOrElse(0))
        case Some(_) => Map.empty[String, Double]
      }

  def getEventCV(rec: Parsed): Map[String, Double] = {
    val event001Opt =
      if (rec.url.isDefined &&
        (
          rec.url.get == "https://network.mobile.rakuten.co.jp/area/" ||
            rec.url.get.startsWith("https://network.mobile.rakuten.co.jp/area/?")
          ) && rec.etype.contains("pv") && mnoACCs.contains(rec.acc.getOrElse(""))
      ) Some("event_001" -> 1.toDouble) else None

    val event002Opt =
      if (rec.url.isDefined &&
        (
          rec.url.get == "https://portal.mobile.rakuten.co.jp/" ||
            rec.url.get.startsWith("https://portal.mobile.rakuten.co.jp/?")
          ) && rec.etype.contains("pv") && mnoACCs.contains(rec.acc.getOrElse(""))
      ) Some("event_002" -> 1.toDouble) else None

    val event003Opt =
      if (rec.url.isDefined &&
        (
          rec.url.get == "https://network.mobile.rakuten.co.jp/guide/application/" ||
            rec.url.get.startsWith("https://network.mobile.rakuten.co.jp/guide/application/?")
          ) && rec.etype.contains("pv") && mnoACCs.contains(rec.acc.getOrElse(""))
      ) Some("event_003" -> 1.toDouble) else None

    Seq(event001Opt, event002Opt, event003Opt).flatten.toMap
  }

  @throws[Exception]
  def readSocialNetworkList(filePath: String): util.List[String] = {
    val source = scala.io.Source.fromFile(filePath)
    try {
      val socialNetworkList = source.getLines.toList.asJava
      if (socialNetworkList.isEmpty) throw new IOException(filePath + " has no contents")
      else socialNetworkList
    } catch {
      case e: FileNotFoundException =>
        throw new Exception(filePath + " doesn't exist")
      case e: IOException =>
        throw new Exception("Failed to read " + filePath + " file, please check format")
    } finally source.close
  }

}
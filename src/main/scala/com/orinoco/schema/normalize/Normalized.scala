package com.orinoco.schema.normalize

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import com.orinoco.schema.normalize.extended.{GroupedCarrierIdentification, _}
import org.apache.spark.sql.types.StructType

/**
 * Objective:
 *    This script has one main purpose:
 *      1. To map normalized data that has been transformed through the Normalized job.
 *
 * Notes:
 *    For a review of all transformations, please see the business documentation of Sessionization C6000.
 *    Fields listed here is what is shown for end-users.
 *    For the business field names, please either refer to the above mentioned documentation or Mongo DB.
 *    Field names go through name changes, the attached comment to the field is the original name.
 **/

case class Normalized(
                       uuid: String = "-1",
                       company: String = "-1",
                       service_group: String = "-1",
                       event_type: String = "-1",
                       sessionization_key: String = "-1", // Need for calculations

                       // Timezone depends on service group
                       time_stamp: String = "-1",
                       // No timezone
                       time_stamp_epoch: Int = -1,

                       // UTC
                       year: String = "-1",
                       month: String = "-1",
                       day: String = "-1",
                       hour: String = "-1",
                       yyyymmddhh: Int = -1,

                       // JST
                       dt: String = "-1",

                       // All other normalized fields
                       abtest: Option[String] = None,
                       abtest_target: Option[Map[String, String]] = None,
                       acc: Option[Int] = None,
                       acc_aid: Option[String] = None,
                       action_params: Option[Map[String, String]] = None,
                       actype: Option[String] = None, // action_type
                       afid: Option[Int] = None, // affiliate_id
                       aflg: Option[Int] = None, // adult_flag
                       aid: Option[Int] = None,
                       amp: Option[Boolean] = None,
                       app_name: Option[String] = None,
                       app_type: Option[String] = None,
                       app_ver: Option[String] = None,
                       appear: Option[Int] = None,
                       application: Option[Int] = None,
                       area: Option[String] = None,
                       astime: Option[Int] = None, // asynchronous_element_load_time
                       async: Option[Int] = None,
                       bgenre: Option[String] = None, // books_genre
                       bid: Option[String] = None,
                       bln: Option[String] = None, // browser_language
                       bookmark_add: Option[Int] = None,
                       bookmark_add_shop: Option[Int] = None,
                       bot_flag: Option[Int] = None,
                       bot_name: Option[String] = None,
                       brand: Option[Array[String]] = None,
                       browser: Option[String] = None,
                       browser_app_type: Option[String] = None,
                       browser_type: Option[String] = None,
                       cart_add: Option[Int] = None,
                       cart_add_click: Option[Int] = None,
                       cart_add_first: Option[Int] = None,
                       cart_add_more: Option[Int] = None,
                       cart_view: Option[Int] = None,
                       cc: Option[String] = None, // campaign_code
                       chkout: Option[Int] = None, // checkout_step
                       chkpt: Option[Int] = None, // checkpoint_step
                       city: Option[String] = None,
                       ck: Option[Map[String, Int]] = None,
                       ck_tbs: Option[Int] = None,
                       ck_tc: Option[Int] = None,
                       cka: Option[String] = None,
                       ckp: Option[String] = None,
                       cks: Option[String] = None,
                       click: Option[Int] = None,
                       click_coord: Option[Array[Int]] = None,
                       cntln: Option[String] = None, // language_used_for_page_content
                       compid: Option[Array[String]] = None, // component_id
                       comptop: Option[Array[Double]] = None, // compid_vertical_position
                       contents_pos: Option[Int] = None,
                       cookies: Option[Map[String, String]] = None,
                       country: Option[String] = None,
                       coupon_price: Option[Array[Double]] = None,
                       couponid: Option[Array[String]] = None,
                       cp: Option[Map[String, String]] = None, // custom_parameter
                       customerid: Option[String] = None,
                       cv: Option[Map[String, Double]] = None, // conversion_parameter
                       cycode: Option[String] = None, // currency_code
                       cycodelist: Option[String] = None, // currency_code_list
                       device: Option[String] = None,
                       device_summary: Option[String] = None,
                       device_type: Option[String] = None,
                       deviceid_list: Option[Map[String, String]] = None,
                       dln: Option[String] = None, // device_language
                       domain: Option[String] = None,
                       easyid: Option[String] = None, // easy_id
                       elevation: Option[String] = None,
                       errorlist: Option[Map[String, String]] = None,
                       errors: Option[Map[String, String]] = None,
                       esq: Option[String] = None, // exclude_search_word
                       etype: String = "-1", // event_type_value
                       genre: Option[String] = None, // navigation_genre
                       gol: Option[String] = None, // goal_id
                       grouped_carrier_id: Option[GroupedCarrierIdentification] = None, // Grouped carrier id fields
                       grouped_media_video: Option[GroupedMediaVideo] = None, // Grouped media and video related fields
                       hurl: Option[String] = None, // referrer_header
                       ifr: Option[Int] = None, // iframe_flag
                       igenre: Option[Array[String]] = None, // browsing_item_genre
                       igenrenamepath: Option[String] = None, // genre_name_hierarchy
                       igenrepath: Option[String] = None, // genre_id_hierarchy
                       ino: Option[String] = None,
                       ip: Option[String] = None,
                       is_bot: Option[Boolean] = None,
                       is_internal: Option[Boolean] = None,
                       isaction: Option[Boolean] = None,
                       issc: Option[Int] = None, // is_scroll
                       itag: Option[Array[String]] = None, // item_tag
                       item_genre_path: Option[String] = None,
                       item_name: Option[String] = None,
                       itemid: Option[Array[String]] = None, // browsing_itemid
                       itemurl: Option[String] = None,
                       jav: Option[Boolean] = None, // java_plugin_presence_flag
                       js_user_agent: Option[String] = None,
                       l_id: Option[String] = None, // link_internal
                       l2id: Option[String] = None, // link_l2id
                       lang: Option[String] = None, // language
                       latitude: Option[String] = None,
                       ldtime: Option[Int] = None, // rendering_time
                       loc: Option[Map[String, Double]] = None, // location
                       longitude: Option[String] = None,
                       lsid: Option[String] = None, // listing_id
                       ltm: Option[String] = None, // client_time_stamp
                       maker: Option[String] = None,
                       mbat: Option[String] = None,
                       mcn: Option[String] = None,
                       mcnd: Option[String] = None,
                       member: Option[String] = None,
                       memberid: Option[String] = None, // member_id
                       mnavtime: Option[Int] = None, // mobile_navigation_time
                       mnetw: Option[Int] = None, // mobile_network_type
                       model: Option[String] = None, // mobile_device_model
                       mori: Option[Int] = None, // mobile_orientation
                       mos: Option[String] = None, // mobile_os
                       mouseover: Option[Int] = None,
                       navigation_genre_path: Option[String] = None,
                       navtime: Option[Int] = None,
                       navtype: Option[Int] = None,
                       ni: Option[Array[Int]] = None, // item_count
                       ni_order: Option[Array[Int]] = None, // item_count_order
                       oa: Option[String] = None, // search_result_relationship
                       online: Option[Boolean] = None,
                       operating_system: Option[String] = None,
                       operating_system_type: Option[String] = None,
                       order_id: Option[String] = None, // order_number
                       order_list: Option[Array[String]] = None,
                       page_view: Option[Int] = None,
                       path: Option[String] = None,
                       path_level1: Option[String] = None,
                       path_level2: Option[String] = None,
                       path_level3: Option[String] = None,
                       path_level4: Option[String] = None,
                       path_level5: Option[String] = None,
                       payment: Option[String] = None,
                       pgid: Option[String] = None, // page_id
                       pgl: Option[String] = None, // page_layout
                       pgn: Option[String] = None, // page_name
                       pgt: Option[String] = None, // page_type
                       phoenix_pattern: Option[String] = None,
                       point_price: Option[Array[Double]] = None,
                       post_review: Option[Int] = None,
                       postal_code: Option[String] = None,
                       powerstatus: Option[Int] = None,
                       prdctcd: Option[String] = None,
                       price: Option[Array[Double]] = None,
                       publisher: Option[String] = None,
                       purchase_gms: Option[Double] = None,
                       purchase_item: Option[Double] = None,
                       purchase_order: Option[Double] = None,
                       purchase_regular: Option[Double] = None,
                       purchase_shop: Option[Int] = None,
                       purchase_unit: Option[Double] = None,
                       purchased_easyid: Option[String] = None, // purchased_easy_id
                       ra: Option[String] = None,
                       rancode: Option[String] = None, // product_code
                       rat_device_code: Option[String] = None,
                       rat_v: Option[String] = None,
                       ref: Option[String] = None, // referrer
                       referrer_domain: Option[String] = None,
                       referrer_summary: Option[String] = None,
                       referrer_type: Option[String] = None,
                       region_1: Option[String] = None,
                       region_2: Option[String] = None,
                       region_3: Option[String] = None,
                       region_4: Option[String] = None,
                       reload: Option[Int] = None,
                       reqc: Option[String] = None, // result_code
                       res: Option[String] = None, // monitor_resolution
                       rescreadate: Option[String] = None, // reservation_creation_date
                       resdate: Option[String] = None, // reservation_date
                       reservation_id: Option[String] = None,
                       reslayout: Option[String] = None, // result_layout
                       rg_books: Option[Int] = None,
                       rg_kobo: Option[Int] = None,
                       rp: Option[String] = None,
                       rqtime: Option[Int] = None, // network_response_time
                       s_id: Option[String] = None, // s_id
                       sc2id: Option[String] = None, // link_sc2id
                       scid: Option[String] = None, // link_external
                       scond: Option[Array[String]] = None, // search_criteria_id
                       search_entity: Option[Array[Int]] = None,
                       search_word_external: Option[String] = None,
                       service_category: Option[String] = None,
                       service_type: Option[String] = None,
                       sgenre: Option[String] = None, // shop_genre
                       shipping: Option[String] = None,
                       shipping_fee: Option[Array[Double]] = None,
                       shop_url_purchase: Option[String] = None,
                       shopid: Option[String] = None,
                       shopid_itemid: Option[Array[String]] = None,
                       shopidlist: Option[Array[String]] = None,
                       shopurl: Option[String] = None,
                       shopurllist: Option[String] = None,
                       sq: Option[String] = None, // search_word
                       sresv: Option[Array[String]] = None,
                       srt: Option[String] = None, // alternate_search
                       ssc: Option[String] = None, // site_section
                       tag: Option[Array[String]] = None, // search_tag
                       target_ele: Option[String] = None, // target_element
                       target_pos: Option[Array[Int]] = None, // target_position
                       tid: Option[String] = None, // tab_id
                       tis: Option[String] = None, // character_set_of_page_content
                       total_price: Option[Array[Double]] = None,
                       tpgldtime: Option[Int] = None,
                       ts: Option[Int] = None,
                       ts1: Option[Int] = None,
                       tzo: Option[Double] = None, // time_zone
                       ua: Option[String] = None, // user_agent
                       url: Option[String] = None,
                       userid: Option[String] = None,
                       variantid: Option[Array[String]] = None,
                       variation: Option[Array[Map[String, String]]] = None,
                       ver: Option[String] = None, // rat_version
                       visitor_id: Option[String] = None, // browser_cookie
                       wv_cls: Option[Double] = None,
                       wv_fcp: Option[Int] = None,
                       wv_fid: Option[Int] = None,
                       wv_lcp: Option[Int] = None,
                       wv_ttfb: Option[Int] = None,
                       wv_ver: Option[String] = None,
                       zero_hit_search_word: Option[String] = None,
                       device_per: Option[String] = None,
                       wv_inp: Option[Int] = None
                     )

object Normalized {
  val getFlattenedSchema: StuctType = StructType(getSchemaAsStruct[Normalized].fields
    .filter(x =>
      !(x.name.equals("flat_abtest_target") || x.name.equals("flat_cp") ||
        x.name.equals("grouped_carrier_id") || x.name.equals("grouped_media_video"))
    ).toList :::
    GroupedCarrierIdentification.getFlattenedSchema.fields.toList :::
    GroupedMediaVideo.getFlattenedSchema.fields.toList
  )

  val empty = Normalized()
  val nestedFieldsList: List[String] = getSchemaAsStruct[Normalized].map(_.name).toList
    .filter(_ != ("flat_abtest_target", "flat_cp", "grouped_carrier_id", "grouped_media_video")) :::
    GroupedCarrierIdentification.nestedFieldList.map(x => s"grouped_carrier_id.$x") :::
    GroupedMediaVideo.nestedFieldList.map(x => s"grouped_media_video.$x")

}
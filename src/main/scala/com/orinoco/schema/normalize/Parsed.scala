package com.orinoco.schema.normalize

/**
 * Objective:
 *    This script has one main purpose:
 *      1. Map data from upstream into a set format.
 *
 * Notes:
 *    To review the original data source, please see the rt_norm_st project, and other previous upstream sources.
 *    Some of these fields are mapped to different names, please refer to NormalizeWithUserRecord.scala for mapping.
 **/

case class Parsed(
                   _mall_uuid: Option[String] = None,
                   _ra: Option[String] = None,
                   abtest: Option[String] = None,
                   abtest_target: Option[Map[String, String]] = None,
                   acc: Option[Int] = None,
                   acc_aid: Option[String] = None,
                   action_params: Option[Map[String, String]] = None,
                   actype: Option[String] = None,
                   afid: Option[Int] = None,
                   aflg: Option[Int] = None,
                   aid: Option[Int] = None,
                   amp: Option[Boolean] = None,
                   app_name: Option[String] = None,
                   app_type: Option[String] = None,
                   app_ver: Option[String] = None,
                   area: Option[Array[Option[String]]] = None,
                   assc: Option[String] = None,
                   astime: Option[Int] = None,
                   bgenre: Option[Array[Option[String]]] = None,
                   bid: Option[String] = None,
                   bln: Option[String] = None,
                   bot: Option[Int] = None,
                   bot_name: Option[String] = None,
                   brand: Option[Array[Option[String]]] = None,
                   browser: Option[String] = None,
                   browser_app_type: Option[String] = None,
                   browser_cookie: Option[String] = None,
                   browser_type: Option[String] = None,
                   browsing_itemid: Option[Array[Option[String]]] = None,
                   BTA001: Option[String] = None,
                   campaign_code: Option[String] = None,
                   chkout: Option[Int] = None,
                   chkpt: Option[Int] = None,
                   ck: Option[Map[String, Int]] = None,
                   ck_tbs: Option[Int] = None,
                   ck_tc: Option[Int] = None,
                   cka: Option[String] = None,
                   ckp: Option[String] = None,
                   cks: Option[String] = None,
                   click_coord: Option[Array[Option[Int]]] = None,
                   cntln: Option[String] = None,
                   compid: Option[Array[Option[String]]] = None,
                   comptop: Option[Array[Option[Double]]] = None,
                   contents_pos: Option[Int] = None,
                   country: Option[String] = None,
                   coupon_price: Option[Array[Option[Double]]] = None,
                   couponid: Option[Array[Option[String]]] = None,
                   cp: Option[Map[String, String]] = None,
                   customerid: Option[String] = None,
                   cv: Option[Map[String, Int]] = None,
                   cycode: Option[String] = None,
                   cycodelist: Option[Array[Option[String]]] = None,
                   device: Option[String] = None,
                   device_type: Option[String] = None,
                   deviceid_list: Option[Map[String, String]] = None,
                   dln: Option[String] = None,
                   domain: Option[String] = None,
                   easyid: Option[String] = None,
                   errorlist: Option[Map[String, String]] = None,
                   errors: Option[Map[String, String]] = None,
                   esq: Option[String] = None,
                   etype: Option[String] = None,
                   genre: Option[String] = None,
                   gol: Option[String] = None,
                   grm: Option[String] = None,
                   hurl: Option[String] = None,
                   ifr: Option[Int] = None,
                   igenre: Option[Array[Option[String]]] = None,
                   igenrenamepath: Option[String] = None,
                   igenrepath: Option[String] = None,
                   ino: Option[Array[Option[String]]] = None,
                   ip: Option[String] = None,
                   is_bot: Option[Boolean] = None,
                   is_internal: Option[Boolean] = None,
                   isaction: Option[Boolean] = None,
                   isroam: Option[Integer] = None,
                   issc: Option[Int] = None,
                   itag: Option[Array[Option[String]]] = None,
                   item_name: Option[String] = None,
                   itemurl: Option[String] = None,
                   jav: Option[Boolean] = None,
                   js_ua: Option[String] = None,
                   l2id: Option[String] = None,
                   lang: Option[String] = None,
                   ldtime: Option[Int] = None,
                   lid: Option[String] = None,
                   loc: Option[Map[String, String]] = None,
                   lsid: Option[String] = None,
                   ltm: Option[String] = None,
                   m3_r: Option[String] = None,
                   maker: Option[Array[Option[String]]] = None,
                   mbat: Option[String] = None,
                   mcn: Option[String] = None,
                   mcnd: Option[String] = None,
                   media: Option[Map[String, String]] = None,
                   member: Option[String] = None,
                   memberid: Option[String] = None,
                   mnavtime: Option[Int] = None,
                   mnetw: Option[Int] = None,
                   model: Option[String] = None,
                   mori: Option[Int] = None,
                   mos: Option[String] = None,
                   navtime: Option[Int] = None,
                   navtype: Option[Int] = None,
                   netop: Option[String] = None,
                   netopn: Option[String] = None,
                   ni: Option[Array[Option[Int]]] = None,
                   ni_order: Option[Array[Option[Int]]] = None,
                   oa: Option[String] = None,
                   online: Option[Boolean] = None,
                   order_id: Option[String] = None,
                   order_list: Option[Array[Option[String]]] = None,
                   os: Option[String] = None,
                   os_type: Option[String] = None,
                   path: Option[String] = None,
                   path_level1: Option[String] = None,
                   path_level2: Option[String] = None,
                   path_level3: Option[String] = None,
                   path_level4: Option[String] = None,
                   path_level5: Option[String] = None,
                   payment: Option[String] = None,
                   pgid: Option[String] = None,
                   pgl: Option[String] = None,
                   pgn: Option[String] = None,
                   pgt: Option[String] = None,
                   phoenix_pattern: Option[String] = None,
                   point_price: Option[Array[Option[Double]]] = None,
                   powerstatus: Option[Int] = None,
                   prdctcd: Option[Array[Option[String]]] = None,
                   price: Option[Array[Option[Double]]] = None,
                   product_code: Option[String] = None,
                   publisher: Option[Array[Option[String]]] = None,
                   ra: Option[String] = None,
                   rat_device_code: Option[String] = None,
                   rat_v: Option[String] = None,
                   ref: Option[String] = None,
                   referrer_domain: Option[String] = None,
                   reqc: Option[String] = None,
                   res: Option[String] = None,
                   rescreadate: Option[String] = None,
                   resdate: Option[String] = None,
                   reservation_id: Option[String] = None,
                   reslayout: Option[String] = None,
                   rg_books: Option[Int] = None,
                   rg_kobo: Option[Int] = None,
                   Rp: Option[String] = None,
                   Rq: Option[String] = None,
                   rqtime: Option[Int] = None,
                   Rt: Option[String] = None,
                   Rz: Option[String] = None,
                   sc2id: Option[String] = None,
                   scid: Option[String] = None,
                   sclid: Option[String] = None,
                   scond: Option[Array[Option[String]]] = None,
                   search_entity: Option[Array[Option[Int]]] = None,
                   search_word_external: Option[String] = None,
                   sgenre: Option[String] = None,
                   shipping: Option[String] = None,
                   shipping_fee: Option[Array[Option[Double]]] = None,
                   shopid: Option[String] = None,
                   shopid_itemid: Option[Array[Option[String]]] = None,
                   shopidlist: Option[Array[Option[String]]] = None,
                   shopurl: Option[String] = None,
                   shopurllist: Option[Array[Option[String]]] = None,
                   sid: Option[String] = None,
                   simcid: Option[Integer] = None,
                   simcn: Option[String] = None,
                   simop: Option[String] = None,
                   simopn: Option[String] = None,
                   sq: Option[String] = None,
                   sresv: Option[Array[Option[String]]] = None,
                   srt: Option[String] = None,
                   ss_r: Option[String] = None,
                   ssc: Option[String] = None,
                   tag: Option[Array[Option[String]]] = None,
                   target_ele: Option[String] = None,
                   target_pos: Option[Array[Option[Int]]] = None,
                   tg_af_histid: Option[String] = None,
                   tid: Option[String] = None,
                   tis: Option[String] = None,
                   total_price: Option[Array[Option[Double]]] = None,
                   tpgldtime: Option[Int] = None,
                   ts: Option[Int] = None,
                   ts1: Option[Int] = None,
                   tzo: Option[Double] = None,
                   ua: Option[String] = None,
                   url: Option[String] = None,
                   use_cks: Option[Boolean] = None,
                   userid: Option[String] = None,
                   uuid: Option[String] = None,
                   variantid: Option[Array[Option[String]]] = None,
                   variation: Option[Array[Option[Map[String, String]]]] = None,
                   ver: Option[String] = None,
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
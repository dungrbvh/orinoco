package com.orinoco.commons.EvarSections

import com.orinoco.adapter.sessionize.cache.evar.EvarLast
import com.orinoco.commons.EvarUtils.getEvarized
import com.orinoco.schema.cdna.CustomerDNA

object EvarLastUtils {
  /**
   * Objective:
   *    This script has one main purpose:
   *      1. Grab data to be evarized and apply them appropriately to the EvarLast case class.
   **/

  def evarLastGrabber(evarized: Map[String, Option[Any]], cdna: CustomerDNA): EvarLast = EvarLast(
    abtest_l = getEvarized[String]("abtest_l", evarized),
    abtest_target_l = getEvarized[Map[String, String]]("abtest_target_l", evarized),
    acc_aid_l = getEvarized[String]("acc_aid_l", evarized),
    action_params_l = getEvarized[Map[String, String]]("action_params_l", evarized),
    actype_l = getEvarized[String]("actype_l", evarized),
    aflg_l = getEvarized[Int]("aflg_l", evarized),
    amp_l = getEvarized[Boolean]("amp_l", evarized),
    app_name_l = getEvarized[String]("app_name_l", evarized),
    app_type_l = getEvarized[String]("app_type_l", evarized),
    app_ver_l = getEvarized[String]("app_ver_l", evarized),
    area_l = getEvarized[String]("area_l", evarized),
    bgenre_l = getEvarized[String]("bgenre_l", evarized),
    bot_flag_l = getEvarized[Int]("bot_flag_l", evarized),
    bot_name_l = getEvarized[String]("bot_name_l", evarized),
    browser_app_type_l = getEvarized[String]("browser_app_type_l", evarized),
    browser_l = getEvarized[String]("browser_l", evarized),
    browser_type_l = getEvarized[String]("browser_type_l", evarized),
    cc_l = getEvarized[String]("cc_l", evarized),
    chkout_l = getEvarized[Int]("chkout_l", evarized),
    city_l = getEvarized[String]("city_l", evarized),
    cka_l = getEvarized[String]("cka_l", evarized),
    compid_l = getEvarized[Array[String]]("compid_l", evarized),
    complemented_easyid_l = getEvarized[String]("complemented_easyid_l", evarized),
    contents_pos_l = getEvarized[Int]("contents_pos_l", evarized),
    country_l = getEvarized[String]("country_l", evarized),
    cp_l = getEvarized[Map[String, String]]("cp_l", evarized),
    custom_dimension_l = getEvarized[Map[String, String]]("custom_dimension_l", evarized),
    cycode_l = getEvarized[String]("cycode_l", evarized),
    cycodelist_l = getEvarized[String]("cycodelist_l", evarized),
    device_l = getEvarized[String]("device_l", evarized),
    device_summary_l = getEvarized[String]("device_summary_l", evarized),
    device_type_l = getEvarized[String]("device_type_l", evarized),
    domain_l = getEvarized[String]("domain_l", evarized),
    easyid_l = getEvarized[String]("easyid_l", evarized),
    errors_l = getEvarized[Map[String, String]]("errors_l", evarized),
    esq_l = getEvarized[String]("esq_l", evarized),
    genre_l = getEvarized[String]("genre_l", evarized),
    igenre_l = getEvarized[Array[String]]("igenre_l", evarized),
    igenrenamepath_l = getEvarized[String]("igenrenamepath_l", evarized),
    igenrepath_l = getEvarized[String]("igenrepath_l", evarized),
    inflow_channel_acc_aid_l = getEvarized[String]("inflow_channel_acc_aid_l", evarized),
    inflow_channel_domain_l = getEvarized[String]("inflow_channel_domain_l", evarized),
    inflow_channel_entry_l = getEvarized[String]("inflow_channel_entry_l", evarized),
    inflow_channel_l = getEvarized[String]("inflow_channel_l", evarized),
    inflow_channel_page_name_l = getEvarized[String]("inflow_channel_page_name_l", evarized),
    inflow_channel_path_l = getEvarized[String]("inflow_channel_path_l", evarized),
    inflow_channel_search_word_l = getEvarized[String]("inflow_channel_search_word_l", evarized),
    inflow_channel_site_section_l = getEvarized[String]("inflow_channel_site_section_l", evarized),
    ino_l = getEvarized[String]("ino_l", evarized),
    ip_l = getEvarized[String]("ip_l", evarized),
    isaction_l = getEvarized[Boolean]("isaction_l", evarized),
    is_entry_l = getEvarized[Boolean]("is_entry_l", evarized),
    is_exit_l = getEvarized[Boolean]("is_exit_l", evarized),
    itag_l = getEvarized[Array[String]]("itag_l", evarized),
    item_genre_path_l = getEvarized[String]("item_genre_path_l", evarized),
    item_name_l = getEvarized[String]("item_name_l", evarized),
    itemid_l = getEvarized[Array[String]]("itemid_l", evarized),
    itemurl_l = getEvarized[String]("itemurl_l", evarized),
    js_user_agent_l = getEvarized[String]("js_user_agent_l", evarized),
    l_id_l = getEvarized[String]("l_id_l", evarized),
    l2id_l = getEvarized[String]("l2id_l", evarized),
    lang_l = getEvarized[String]("lang_l", evarized),
    loc_l = getEvarized[Map[String, Double]]("loc_l", evarized),
    lsid_l = getEvarized[String]("lsid_l", evarized),
    maker_l = getEvarized[String]("maker_l", evarized),
    media_autoplay_l = getEvarized[Int]("media_autoplay_l", evarized),
    media_event_l = getEvarized[String]("media_event_l", evarized),
    media_iframeurl_l = getEvarized[String]("media_iframeurl_l", evarized),
    media_name_l = getEvarized[String]("media_name_l", evarized),
    media_segment_l = getEvarized[String]("media_segment_l", evarized),
    member_l = getEvarized[String]("member_l", evarized),
    memberid_l = getEvarized[String]("memberid_l", evarized),
    mnetw_l = getEvarized[Int]("mnetw_l", evarized),
    model_l = getEvarized[String]("model_l", evarized),
    mori_l = getEvarized[Int]("mori_l", evarized),
    mos_l = getEvarized[String]("mos_l", evarized),
    navigation_genre_path_l = getEvarized[String]("navigation_genre_path_l", evarized),
    navtype_l = getEvarized[Int]("navtype_l", evarized),
    oa_l = getEvarized[String]("oa_l", evarized),
    operating_system_l = getEvarized[String]("operating_system_l", evarized),
    operating_system_type_l = getEvarized[String]("operating_system_type_l", evarized),
    order_id_l = getEvarized[String]("order_id_l", evarized),
    order_list_l = getEvarized[Array[String]]("order_list_l", evarized),
    path_l = getEvarized[String]("path_l", evarized),
    path_level1_l = getEvarized[String]("path_level1_l", evarized),
    path_level2_l = getEvarized[String]("path_level2_l", evarized),
    path_level3_l = getEvarized[String]("path_level3_l", evarized),
    path_level4_l = getEvarized[String]("path_level4_l", evarized),
    path_level5_l = getEvarized[String]("path_level5_l", evarized),
    payment_l = getEvarized[String]("payment_l", evarized),
    pgl_l = getEvarized[String]("pgl_l", evarized),
    pgn_l = getEvarized[String]("pgn_l", evarized),
    pgt_l = getEvarized[String]("pgt_l", evarized),
    prdctcd_l = getEvarized[String]("prdctcd_l", evarized),
    previous_acc_aid_l = getEvarized[String]("previous_acc_aid_l", evarized),
    previous_domain_l = getEvarized[String]("previous_domain_l", evarized),
    previous_page_name_l = getEvarized[String]("previous_page_name_l", evarized),
    previous_path_l = getEvarized[String]("previous_path_l", evarized),
    previous_service_group_l = getEvarized[String]("previous_service_group_l", evarized),
    previous_site_section_l = getEvarized[String]("previous_site_section_l", evarized),
    price_l = getEvarized[Array[Double]]("price_l", evarized),
    publisher_l = getEvarized[String]("publisher_l", evarized),
    rancode_l = getEvarized[String]("rancode_l", evarized),
    rat_device_code_l = getEvarized[String]("rat_device_code_l", evarized),
    recent_prchs_code_l = cdna.recent_prchs_code,
    recent_prchs_date_l = cdna.recent_prchs_date,
    ref_l = getEvarized[String]("ref_l", evarized),
    referrer_domain_l = getEvarized[String]("referrer_domain_l", evarized),
    referrer_summary_l = getEvarized[String]("referrer_summary_l", evarized),
    referrer_type_l = getEvarized[String]("referrer_type_l", evarized),
    res_l = getEvarized[String]("res_l", evarized),
    resdate_l = getEvarized[String]("resdate_l", evarized),
    reservation_id_l = getEvarized[String]("reservation_id_l", evarized),
    reslayout_l = getEvarized[String]("reslayout_l", evarized),
    rg_books_l = getEvarized[Int]("rg_books_l", evarized),
    rg_kobo_l = getEvarized[Int]("rg_kobo_l", evarized),
    s_id_l = getEvarized[String]("s_id_l", evarized),
    sc2id_l = getEvarized[String]("sc2id_l", evarized),
    scid_l = getEvarized[String]("scid_l", evarized),
    search_word_external_l = getEvarized[String]("search_word_external_l", evarized),
    service_category_l = getEvarized[String]("service_category_l", evarized),
    service_group_l = getEvarized[String]("service_group_l", evarized),
    service_type_l = getEvarized[String]("service_type_l", evarized),
    sgenre_l = getEvarized[String]("sgenre_l", evarized),
    shopid_l = getEvarized[String]("shopid_l", evarized),
    shopid_itemid_l = getEvarized[Array[String]]("shopid_itemid_l", evarized),
    shopidlist_l = getEvarized[Array[String]]("shopidlist_l", evarized),
    shopurl_l = getEvarized[String]("shopurl_l", evarized),
    shopurllist_l = getEvarized[String]("shopurllist_l", evarized),
    sq_l = getEvarized[String]("sq_l", evarized),
    srt_l = getEvarized[String]("srt_l", evarized),
    ssc_l = getEvarized[String]("ssc_l", evarized),
    tag_l = getEvarized[Array[String]]("tag_l", evarized),
    target_ele_l = getEvarized[String]("target_ele_l", evarized),
    ua_l = getEvarized[String]("ua_l", evarized),
    url_l = getEvarized[String]("url_l", evarized),
    user_age_l = cdna.user_age,
    user_category_by_recent_prchs_l = cdna.user_category_by_recent_prchs,
    user_city_l = cdna.user_city,
    user_gender_l = cdna.user_gender,
    user_rank_l = cdna.user_rank,
    ver_l = getEvarized[String]("ver_l", evarized),
    zero_hit_search_word_l = getEvarized[String]("zero_hit_search_word_l", evarized)
  )
}
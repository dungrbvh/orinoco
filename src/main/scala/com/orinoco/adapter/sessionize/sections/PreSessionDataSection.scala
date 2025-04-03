package com.orinoco.adapter.sessionize.sections

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class PreSessionDataSection(
                                  previous_page_name: Option[String] = None,
                                  previous_path: Option[String] = None,
                                  previous_service_group: Option[String] = None,
                                  previous_site_section: Option[String] = None,
                                  previous_domain: Option[String] = None,
                                  previous_acc_aid: Option[String] = None,
                                  inflow_channel: Option[String] = None,
                                  inflow_channel_acc_aid: Option[String] = None,
                                  inflow_channel_domain: Option[String] = None,
                                  inflow_channel_path: Option[String] = None,
                                  inflow_channel_search_word: Option[String] = None,
                                  inflow_channel_site_section: Option[String] = None,
                                  inflow_channel_page_name: Option[String] = None
                                )

object PreSessionDataSection {
  val getFlattenedSchema: StructType = getSchemaAsStruct[PreSessionDataSection]
  val empty: PreSessionDataSection = PreSessionDataSection()
  val nestedFieldList: List[String] = getSchemaAsStruct[PreSessionDataSection].map(_.name).toList
}
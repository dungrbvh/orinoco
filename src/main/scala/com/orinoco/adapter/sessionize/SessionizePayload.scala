package com.orinoco.adapter.sessionize

import com.orinoco.adapter.postcalculate.cache.PostCalculateCache
import com.orinoco.adapter.postcalculate.nextdimension.DimensionalDataSection
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.sections.{PreSessionDataSection, SessionDataSection}
import com.orinoco.adapter.sessionize.sections.PostCuttingSection
import com.orinoco.schema.normalize.Normalized
import com.orinoco.schema.cdna.CustomerDNA
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class SessionizePayload(
                              time_stamp_epoch: Int = -1,
                              year: String = "-1",
                              month: String = "-1",
                              day: String = "-1",
                              hour: String = "-1",
                              phase: String = "traffic",
                              sessionization_key: String = "-1",
                              company: String = "-1",
                              service_group: String = "-1",
                              norm: Normalized = Normalized.empty,
                              cdna: CustomerDNA = CustomerDNA.empty,
                              uuid: String = "-1",

                              sc: Option[SessionizationCache] = None,

                              pre_session_data_section: Option[PreSessionDataSection] = None,
                              session_data_section: Option[SessionDataSection] = None,
                              evar_section: Option[EvarSection] = None,

                              // post-compute flattened fields
                              dimensional_data_section: Option[DimensionalDataSection] = None,
                              post_cutting_section: Option[PostCuttingSection] = None,
                              post_calculate_cache: Option[PostCalculateCache] = None
                            )
object SessionizePayload {
  // Flatten schema with only private table fields
  val getFlattenedSchema: StructType = StructType(
    List(StructField("phase", StringType)) :::
      Normalized.getFlattenedSchema.fields.toList :::
      CustomerDNA.getFlattenedSchema.fields.toList :::
      PreSessionDataSection.getFlattenedSchema.fields.toList :::
      SessionDataSection.getFlattenedSchema.fields.toList :::
      EvarSection.getFlattenedSchema.fields.toList :::
      DimensionalDataSection.getFlattenedSchema.fields.toList :::
      PostCuttingSection.getFlattenedSchema.fields.toList
  )
  val empty: SessionizePayload = SessionizePayload()
  // Only private table fields
  val nestedFieldList: List[String] =
    List("phase") :::
      Normalized.nestedFieldList.map(x => s"norm.$x") :::
      CustomerDNA.nestedFieldList.map(x => s"cdna.$x") :::
      PreSessionDataSection.nestedFieldList.map(x => s"pre_session_data_section.$x") :::
      SessionDataSection.nestedFieldList.map(x => s"session_data_section.$x") :::
      EvarSection.nestedFieldList.map(x => s"evar_section.$x") :::
      DimensionalDataSection.nestedFieldList.map(x => s"dimensional_data_section.$x") :::
      PostCuttingSection.nestedFieldList.map(x => s"post_cutting_section.$x")
}
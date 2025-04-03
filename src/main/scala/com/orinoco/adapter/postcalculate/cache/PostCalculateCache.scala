package com.orinoco.adapter.postcalculate.cache

import com.orinoco.adapter.sessionize.cache.evar.EvarSection
import com.orinoco.commons.ReflectionUtls._
import com.orinoco.schema.cdna.CustomerDNA
import org.apache.spark.sql.types.StructType

/**
 * Objective:
 *    This data is used internally and between phase preview / final hour runs.
 *    The primary purpose is to connect relevant session IDs so that information can cross over if needed.
 *
*/
case class PostCalculateCache (
                                // For joining back to original session
                                service_group: String = "-1",
                                session_id: String = "-1",

                                // Values of prior hour
                                complemented_easyid_cache: Option[String] = None,
                                complemented_user_cache: Option[CustomerDNA] = None,
                                dimension_holders: Option[DimensionHolder] = None,
                                evar_cache: Option[EvarSection] = None,
                                inflow_channel_entry_cache: Option[String] = None,
                                session_id_for_easy_id_cache: Option[String] = None
                              )

object PostCalculateCache {
  val getFlattenedSchema: StructType = getSchemaAsStruct[PostCalculateCache]
  val empty: PostCalculateCache = PostCalculateCache()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
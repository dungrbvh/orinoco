package com.orinoco.adapter.sessionize.cache

import com.orinoco.adapter.sessionize.cache.evar.EvarSection
import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class SessionizationCache(
                                sessionization_key: String = "-1",
                                company: String = "-1",
                                service_group: String = "-1",

                                // SessionCacheByServiceGroup
                                evar_cache: Option[EvarSection] = None,

                                // SessionCuttingCache
                                first_event_time: Option[Int] = None,
                                last_event_time: Option[Int] = None,
                                event_count: Option[Int] = None,

                                // SessionStatsCache
                                session_id: Option[String] = None,
                                sessionEndReason: Option[String] = None,
                                session_start_time: Option[Int] = None,

                                prev_place_holder: Option[PrevPlaceholder] = None,
                                last_time_stamp: Option[Int] = None
                              )

object SessionizationCache {
  val getFlattenedSchema: StructType = getSchemaAsStruct[SessionizationCache]
  val empty: SessionizationCache = SessionizationCache()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
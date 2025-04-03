package com.orinoco.adapter.sessionize.sections

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class SessionDataSection(
                               session_id: String,
                               sessionStartDateHourUTC: Int,
                               sessionEndReason: Option[String] = None,
                               sessionReset: Option[Int] = None
                             )

object SessionDataSection {
  val getFlattenedSchema: StructType = getSchemaAsStruct[SessionDataSection]
  val empty: SessionDataSection = SessionDataSection(null, -1)
  val nestedFieldList: List[String] = getSchemaAsStruct[SessionDataSection].map(_.name).toList

  object SessionEndReason {
    val PENDING = "PENDING"
    val END_30_MINUTES_INACTIVITY = "END_30_MINUTES_INACTIVITY"
    val END_100_EVENTS_WITHIN_100_SECS = "END_100_EVENTS_WITHIN_100_SECS"
    val END_AFTER_2500_EVENTS = "END_AFTER_2500_EVENTS"
    val END_AFTER_MAX_SESSION_LIFETIME = "END_AFTER_MAX_SESSION_LIFETIME"
  }

  object SessionCutRule {
    case class Folding(
                        sessionId: String,
                        sessionStartDateHourUTC: Int, // This is Int of YYYYMMDDHH, not of epoch
                        event: SessionizePayload,
                        sessionEndReason: Option[String],
                        sessionReset: Option[Int] // Used to identify if session will be split due to size
                      )
    case class Counts(
                       lastRecordTime: Int,
                       aggSize: Int,
                       sessStartTime: Int
                     )
  }
}
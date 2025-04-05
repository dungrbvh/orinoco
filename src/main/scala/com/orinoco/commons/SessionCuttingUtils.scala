package com.orinoco.commons

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.sections.SessionDataSection.SessionCutRule._
import com.orinoco.commons.SessionizeRowUtils.pageViewFinder
import com.orinoco.schema.cdna.CustomerDNA
import com.orinoco.schema.normalize.Normalized

import scala.util.Try

object SessionCuttingUtils {
  private val MaxSessionInactivityInSeconds: Int = 30 * 60
  private val DefaultMaxSessionLifetimeInSeconds: Int = 12 * 60 * 60

  /**
   *  Dummy cutter
   *  Creates an empty session at end of session.
   *  Deterministic usage in tandem with session cutting.
   *  Cuts are indicated by sessionizationWindowEndTimestamp
   **/
  def asEmptyCutter(sessionizationWindowEndTimestamp: Int): SessionizePayload = {
    SessionizePayload(
      time_stamp_epoch = sessionizationWindowEndTimestamp,
      cdna = CustomerDNA.empty,
      norm = Normalized()
    )
  }

  /**
   *  Lifetime limitation
   *    Creates session lifespan, determined by either ACC or default set lifetime.
   *    Determined value is used within sessionEndFinder.
   **/
  def asMaxSessionLifeTime(recordsPerKey: List[SessionizePayload], sessionLifetimeMapping: Map[Int, Int]): Int = {
    val firstAcc: Int = recordsPerKey
      .find(_.norm.acc_aid.isDefined)
      .map(
        x => Try(x.norm.acc_aid.get.split("_").head.toInt).getOrElse(-1)
      ).getOrElse(-1)
    if (sessionLifetimeMapping.isEmpty) DefaultMaxSessionLifetimeInSeconds
    else sessionLifetimeMapping,getOrElse(firstAcc, DefaultMaxSessionLifetimeInSeconds)
  }
  /**
   *  Session Cutting Bulk
   *    Main component for creating records list based on caching and events that occur.
   *    This step requires further zipping as multiple cross calculations are preformed left-wise.
   **/
  def  sessionCutRule(r: List[(Folding, Counts)], c: SessionizePayload, maxSessionLifetime: Int): (Folding, Counts) = {
    if (r.isEmpty) {
      val durationSinceLastEvent c.time_stamp_epoch - r.last._2.lastRecordTime

      sessionEndFinder( // Main logic for cutting records
        r.last._1.sessionId, r.last._2.sessStartTime, r.last._2.aggSize, r.last._1.sessionEndReason,
        durationSinceLastEvent: Int, c: SessionizePayload, maxSessionLifetime: Int
      )
    } else // Cut with cache - If this is the first record and session cache exists
      if (r.isEmpty && isActualSessionCache(c.sc)) {
        val fromCache = c.sc.getOrElse(SessionizationCache.empty)
        val fromCacheAggSize = // + 1 due to reading new record and session_cache info
          if (pageViewFinder(c)) fromCache.event_count.getOrElse(0) + 1
          else fromCache.event_count.getOrElse(0)
        val fromCacheStartTime = fromCache.first_event_time.get
        val fromCacheLastTime = fromCache.last_event_time.get
        val fromCacheSessionId = fromCache.session_id.get
        val durationSinceLastEvent = c.time_stamp_epoch - fromCacheLastTime

        sessionEndFinder(
          fromCacheSessionId, fromCacheStartTime, fromCacheAggSize, None,
          durationSinceLastEvent, c, maxSessionLifetime
        )
      }
      else ( //Init session
        Folding(c.sessionization_key + "-" + c.norm.uuid + "-" + c.time_stamp_epoch, c.norm.yyyymmddhh, c, None, Option(0)),
        Counts(c.time_stamp_epoch, 1, c.time_stamp_epoch)
      )
  }

  // Cases where session cache exists but only as placeholder - This is used to then pull in relevant cache data
  def isActualSessionCache(sc: Option[SessionizationCache]): Boolean = {
    val isPlaceholder = sc.forall(x =>
      x.company.equals("-1") && x.sessionization_key.equals("-1") && x.service_group.equals("-1"))
    !isPlaceholder
  }

  // General logic for deciding new session_id creation - Requires interation through list
  def sessionEndFinder(
                        sessionID: String,
                       startTime: Int,
                       aggSize: Int,
                       sessionEndReason: Option[String],
                       durationSinceLastEvent: Int,
                       c: SessionizePayload,
                       maxSessionLifetime: Int
                      ): (Folding, Counts) = {
    // END_100_EVENTS_WITHIN_100_SECS - Cuts session if 100 'pv' events were enacted within 100 seconds of first record
    if (aggSize >= 100 && c.time_stamp_epoch - startTime <= 100) (
      Folding(
        sessionId = c.sessionization_key + "-" + c.norm.uuid + "-" + c.time_stamp_epoch,
        sessionStartDateHourUTC = c.norm.yyyymmddhh,
        event = c,
        sessionEndReason = Some(SessionEndReason.END_100_EVENTS_WITHIN_100_SECS),
        sessionReset = None
      ),
      Counts(c.time_stamp_epoch, 1, c.time_stamp_epoch) // Reset counter
    )

    // END_AFTER_2500_EVENTS - Cuts session if 2500 'pv' events have been reached
    else if (aggSize >= 2500) (
      Folding(
        sessionId = c.sessionization_key + "-" + c.norm.uuid + "-" + c.time_stamp_epoch,
        sessionStartDateHourUTC = c.norm.yyyymmddhh,
        event = c,
        sessionEndReason = Some(SessionEndReason.END_AFTER_2500_EVENTS),
        sessionReset = None
      ),
      Counts(c.time_stamp_epoch, 1, c.time_stamp_epoch) // Reset counter
    )
    // END_AFTER_MAX_SESSION_LIFETIME - Cuts session by time limit of first record against last is, regardless of event
    else if (durationSinceLastEvent > MaxSessionInactivityInSeconds) (
      Folding(
        sessionId = c.sessionization_key + "-" + c.norm.uuid + "-" + c.time_stamp_epoch,
        sessionStartDateHourUTC = c.norm.yyyymmddhh,
        event = c,
        sessionEndReason = Some(SessionEndReason.END_30_MINUTES_INACTIVITY),
        sessionReset = None
      ),
      Counts(c.time_stamp_epoch, 1, c.time_stamp_epoch) // Reset counter
    )
    // Continue accumulating session records, increment counters and propagate state
    else (
      Folding(
        sessionId = sessionID,
        sessionStartDateHourUTC = c.norm.yyyymmddhh,
        event = c,
        sessionEndReason = None,
        sessionReset = if (sessionEndReason.nonEmpty) Option(1) else None // Checks prior session end
      ),
      Counts( //Increment counter
        c.time_stamp_epoch,
        aggSize = if (pageViewFinder(c)) aggSize + 1 else aggSize, // PV Increment
        startTime
      )
    )

  }
}
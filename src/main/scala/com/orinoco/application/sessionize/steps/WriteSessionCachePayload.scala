package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.sections.SessionDataSection
import com.orinoco.application.sessionize.RecordsPerServiceGroup2
import org.apache.spark.sql.{Dataset, SparkSession}

object WriteSessionCachePayload {

  /**
   * Objective: Creates cache, combines passing cache, and outputs into cache path.
   *
   * Step 1: Creates cache from current hour's sessionized data
   * Step 2: Combines previous hours' session with current hours; retrieves most recent
   * Step 3: Output data to designated path
   *
   * @param payload                 Data to output
   * @param filteredCachesCount     Prior hours' cache filtered by unexpired sessions
   * @param cachedRecordsPerCutFile Used for partitioning
   * @param outputCache             Output path
   * @param prevCache               Prior hours' cache
   * @param spark                   Necessary for spark usage
   * @return
   * */
  def asDataset(
               payload: Dataset[(String, List[RecordsPerServiceGroup2])],
               filteredCachesCount: Long,
               cachedRecordsPerCutFile: Int,
               outputCache: String,
               prevCache: Dataset[SessionizationCache]
               )(implicit spark: SparkSession): Unit = {
    val numPartitions: Int = Math.max(48, filteredCachesCount / cachedRecordsPerCutFile).toInt

    payload
      .transform(currentHourCache()(spark)) // Step 1
      .transform(cacheUpdate(prevCache)(spark)) //Step 2
      .toDF // Step 3
      .repartition(numPartitions)
      .write
      .parquet(outputCache)

    println(s"Sessionization Cache Output: $outputCache")
  }

  /* Step 1: Creates cache from current hour's sessionized data */
  def currentHourCache()(spark: SparkSession):
    Dataset[(String, List[RecordsPerServiceGroup2])] => Dataset[SessionizationCache] = {

    import spark.implicits._

    ds => ds.mapPartitions(
      _.flatMap {
        kv =>
          val sessionizationKey = kv._1

          kv._2.map {
            case RecordsPerServiceGroup2(serviceGroup, payloadPerSessionMap) =>
            /**
             *  The following two values grabs the session Id of the mapped payload.
             *  Then, either the most recent or the oldest session Id is obtained via the time stamp of said Id.
             *
             **/

              val latestSessionID = payloadPerSessionMap.keys
                .map(k => (k.substring(k.lastIndexOf('-') + 1).toInt, k)) // Gets time stamp of session ID
                .toList
                .maxBy(_._1)
                ._2

              val earliestSessionID = payloadPerSessionMap.keys
                .map(k => (k.substring(k.lastIndexOf('-') + 1).toInt, k)) // Gets time stamp of session ID
                .toList
                .minBy(_._1)
                ._2
              val firstSessionList = payloadPerSessionMap(earliestSessionID)
              val lastSessionList = payloadPerSessionMap(latestSessionID)

              val cacheSessionId = firstSessionList.flatMap(_.sc.getOrElse(SessionizationCache.empty).session_id)
              val firstRecord = lastSessionList.head
              val lastRecord = lastSessionList.last
              val lastRecordSC = lastRecord.sc.getOrElse(SessionizationCache.empty)
              val lastSessionPVOnly = lastSessionList.filter(_.norm.event_type.equals("pv"))

              val newSession = lastSessionList.filter(
                _.session_data_section.getOrElse(SessionDataSection.empty).sessionReset.contains(1)
              )

              // Computed with event_type pv
              val (evarCache, prevPlaceHolder, lastPageSequence) =
                if (lastSessionPVOnly.isEmpty) {
                  if (lastRecordSC.event_count.isDefined)
                    if (newSession.isEmpty)
                      (lastRecordSC.evar_cache, lastRecordSC.prev_place_holder, lastRecordSC.event_count)
                    else (None, None, None)
                  else (None, None, None)
                } else
                  (
                    lastSessionPVOnly.last.evar_section,
                    lastSessionPVOnly.last.sc.get.prev_place_holder,
                    lastSessionPVOnly.last.post_cutting_section.get.page_sequence
                  )

              val firstEventTime = // Determines first event time of session Id
                if ( // Gets cache value - Session Id still persisting
                  cacheSessionId.contains(earliestSessionID) && // Session Id not cut in cache
                    latestSessionID.contains(earliestSessionID) // Session Id continues to persist
                )
                  lastRecordSC.first_event_time
                else // Gets current records value - New session Id created regardless of cache instance
                  Option(firstRecord.time_stamp_epoch)

              SessionizationCache(
                sessionization_key = sessionizationKey,
                company = lastRecord.company,
                service_group = serviceGroup,
                evar_cache = evarCache,
                first_event_time = firstEventTime,
                last_event_time = Option(lastRecord.time_stamp_epoch),
                event_count = lastPageSequence, // Last session (uncut/cut) event_count

                session_id = Option(latestSessionID),
                sessionEndReason = lastRecord.session_data_section.get.sessionEndReason, // Value of session cut
                session_start_time = Option(firstRecord.time_stamp_epoch),

                prev_place_holder = prevPlaceHolder,
                last_time_stamp = Option(lastRecord.time_stamp_epoch)
              )
          }
      }
    )
  }

  /* Step 2: Combines previous hours' session with current hours; retrieves most recent */
  def cacheUpdate(filteredCaches: Dataset[SessionizationCache])(implicit spark: SparkSession):
  Dataset[SessionizationCache] => Dataset[SessionizationCache] = {
    import spark.implicits._

    records => records.union(filteredCaches)
      .groupByKey(y => (y.sessionization_key, y.company, y.service_group))
      .reduceGroups(
        (currentHourCache, prevCache) => {
          val evarUpdate =
            if (currentHourCache.evar_cache.isDefined) currentHourCache.evar_cache
            else if (prevCache.evar_cache.isDefined) prevCache.evar_cache
            else None

          val eventCountUpdate =
            if (currentHourCache.event_count.isDefined) currentHourCache.event_count
            else if (prevCache.event_count.isDefined) prevCache.event_count
            else None

          val pphUpdate =
            if (currentHourCache.prev_place_holder.isDefined) currentHourCache.prev_place_holder
            else if (prevCache.prev_place_holder.isDefined) prevCache.prev_place_holder
            else None

          val updatedCache = currentHourCache.copy(
            evar_cache = evarUpdate,
            event_count = eventCountUpdate,
            prev_place_holder = pphUpdate
          )
          // Get cache with latest last_time_stamp
          if (updatedCache.last_time_stamp.getOrElse(-1) >= prevCache.last_time_stamp.getOrElse(-1))
            updatedCache
          else prevCache
        }
      )
      .map(_._2)
  }
}
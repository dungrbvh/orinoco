package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.cache.SessionizationCache
import org.apache.spark.sql.{Dataset, SparkSession}

object FilterCaches {

  /**
   *  Objective:
   *    Filter sessions that are older than past x hours
   *
   *  Notes:
   *    While session cutting also happens in CutIntoSessions,
   *    this filter is also necessary as there are sessions that continue to persist between hours,
   *    but do not meet the requirements to be cut (I.E. 30 minute idle sessions)
   *    This filtered cache is passed, but excludes lifetime expired sessions, to indicate a new session next time.
   *
   * @param sessionizationCache Prior hours' cache
   * @param hourEndTimeStamp    Current hour's end time stamp
   * @param lookBackWindowHours Session expiration hour limit
   * @param spark               Necessary for spark usage
   * @return
   **/

  def asDataset(
               sessionizationCache: Dataset[SessionizationCache],
               hourEndTimeStamp: Int,
               lookBackWindowHours: Int
               )(implicit spark: Sparksession): Dataset[SessionizationCache] = {
    sessionizationCache.filter(recordCache => filterCondition(recordCache, hourEndTimeStamp, lookBackWindowHours))
  }

  def filterCondition(
                       sessionizationCache: SessionizationCache,
                       hourEndTimeStamp: Int,
                       lookBackWindowHours: Int): Boolean = {
    sessionizationCache.first_event_time.getOrElse(hourEndTimeStamp) > (hourEndTimeStamp - (lookBackWindowHours * 60 * 60))
  }

}
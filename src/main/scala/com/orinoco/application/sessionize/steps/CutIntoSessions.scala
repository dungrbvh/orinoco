package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.sections.SessionDataSection
import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.sections.SessionDataSection.SessionCutRule.Folding
import com.orinoco.application.sessionize.RecordsPerServiceGroup
import com.orinoco.commons.SessionCuttingUtils._
import org.apache.spark.sql.{Dataset, SparkSession}

object CutIntoSessions {
  /**
   * Objective is to cut user session based on varying criteria
   * If there are no 'pv' events for current hour, session will continue onto following hour if events continue
   *
   * Pre-requisite:
   *    Input from PreviousDataPerCompany <- Provides previous rows information per company
   *
   * Below is a brief breakdown of each steps' purpose. For more information, see relevant step.
   * Step 1: Divide user activity by service group usage
   * Step 2: Cut session of activity based on varying criteria
   *
   *
   * @param payload Data to output
   * @param sessionizationWindowEndTimestamp Used to cut session of user
   * @param sessionLifetimeMapping Used to mark maximum lifetime of session
   * @param spark Necessary for spark usage
   * @return
   **/
  def asDataset(
               payload: Dataset[(String, List[SessionizePayload])],
               sessionizationWindowEndTimestamp: Int,
               sessionLifetimeMapping: Map[Int, Int]
               )(implicit spark: SparkSession): Dataset[(String, List[RecordsPerServiceGroup])] = {
    import spark.implcits._
    payload.mapPartitions(_.map(kv => cutIntoSessions(kv._1, kv._2, sessionizationWindowEndTimestamp, sessionLifetimeMapping)))
  }

  def cutIntoSessions(
                       sessionizationKey: String,
                       recordsPerKey: List[SessionizePayload],
                       sessionizationWindowEndTimestamp: Int,
                       sessionLifetimeMapping: Map[Int, Int]): (String, List[RecordsPerServiceGroup]) = {
    /* Step 1: Divide user activity by service group usage */
    val serviceGroupList = recordsPerKey.map(_.service_group).distinct

    val payloadPerServiceGroupList = serviceGroupList.map(serviceGroup =>
      RecordsPerServiceGroup(
        serviceGroup,
        cutIntoSessions( // Step 2
          recordsPerKey.filter(_.service_group == serviceGroup),
          sessionizationWindowEndTimestamp, sessionLifetimeMapping
        )
      )
    )
    (sessionizationKey, payloadPerServiceGroupList)
  }

  /**
   *  Step 2: Cut session of activity based on varying criteria
   *     Sessions are determined by their relevant key and time stamp at time of first record.
   *     These are then grouped and split depending on the criteria set upon in sessionCutRule (see section).
   *     Based on the cut, a new session_id is formed and used for all subsequent calculations.
   **/
  def cutIntoSessions(
                       recordsPerKey: List[SessionizePayload],
                       sessionizationWindowEndTimestamp: Int,
                       sessionLifetimeMapping: Map[Int, Int]
                     ): List[SessionizePayload] = {
    val maxSessionLifeTime: Int = asMaxSessionLifeTime(recordsPerKey, sessionLifetimeMapping) //General lifespan
    val emptyCutter  = asEmptyCutter(sessionizationWindowEndTimestamp) //Apply to end of list

    // Use session window end time to cut/flush last session (or not)
    val _cutAndPending: List[(Folding, Counts)] = (recordsPerKey :+ emptyCutter)
      .foldLeft(List.empty[(Folding, Counts)]) {
        (r, c) => r :+ sessionCutRule(r,c, maxSessionLifeTime)
      }
    /**
     *   Zip is used to map session end reason from start of subsequent session back to end of prior session
     *
     * Example:
     *   scala> val l1 = List[A](A("a", "b"), A("a1", "b1"),A("a2", "b2"),A("a3", "b3"))
     *     l1: List[A] = List(A(a,b), A(a1,b1), A(a2,b2), A(a3,b3))
     *
     *   Dropping first element allows for mapping of start and back of session.
     *   scala> l1.zip(l1.drop(1))
     *     res0: List[(A, A)] = List((A(a,b),A(a1,b1)), (A(a1,b1),A(a2,b2)), (A(a2,b2),A(a3,b3)))
     *
     *   Not dropping first element will result in duplication of row.
     *   scala> l1.zip(l1)
     *     res1: List[(A, A)] = List((A(a,b),A(a,b)), (A(a1,b1),A(a1,b1)), (A(a2,b2),A(a2,b2)), (A(a3,b3),A(a3,b3)))
     **/
    val cutAndPending = _cutAndPending.zip(_cutAndPending.drop(1))
      .map(x =>
        Folding(
          x._1._1.sessionId,
          x._1._1.sessionStartDateHourUTC,
          x._1._1.event,
          x._2._1.sessionEndReason,
          x._2._1.sessionReset
        )
      )

    // Recompile SessionPayload list based on new session IDs
    cutAndPending.map(cp =>
      cp.event.copy(
        session_data_section =  Option(
          SessionDataSection(cp.sessionId, cp.sessionStartDateHourUTC, cp.sessionEndReason, cp.sessionReset)
        )
      )
    )
  }
}
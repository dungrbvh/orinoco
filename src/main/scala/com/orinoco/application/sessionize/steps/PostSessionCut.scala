package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.adapter.sessionize.cache.evar.EvarMapping.toEvarSection
import com.orinoco.adapter.sessionize.sections.{PostCuttingSection, SessionDataSection}
import com.orinoco.application.sessionize.post.evar
import com.orinoco.application.sessionize.{RecordsPerServiceGroup, RecordsPerServiceGroup2}
import com.orinoco.commons.EvarUtils.evarizeSessionizedPayload
import com.orinoco.commons.SessionizeRowUtils.pageViewFinder
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

object PostSessionCut {
  /**
   * Prelude of post calculation per service group and per session ID
   *
   * Pre-requisite:
   *    Input from CutIntoSessions
   *    Records grouped by sessionization_key
   *    Record sorted by time_stamp_epoch
   *    Record must have session_id
   *    Multiple sessions are expected per sessionization_key and service_group
   *
   * @param payload        Data to output
   * @param evarDimensions Used to set values for evar schema
   * @param spark          Necessary for spark usage
   * @return
   **/
  def asDataset(
                 payload: Dataset[(String, List[RecordsPerServiceGroup])], evarDimensions: List[String] = List.empty
               )(implicit spark: SparkSession): Dataset[(String, List[RecordsPerServiceGroup2])] = {
    import spark.implicits._

    val evarizationSchema = evar.setEvarSchema(evarDimensions)
    payload
      .mapPartitions(itr => itr
        .map(kv => postSessionCut(kv._1, kv._2, evarDimensions, evarizationSchema)))
  }

  def postSessionCut(
                      sessionizationKey: String,
                      recordsPerServiceGroup: List[RecordsPerServiceGroup],
                      evarDimensions: List[String],
                      evarizationSchema: StructType): (String, List[RecordsPerServiceGroup2]) = {

    val output = recordsPerServiceGroup.map {
      s =>
        val bySessionIdList: Map[String, List[SessionizePayload]] = s.payload
          .groupBy(_.session_data_section.getOrElse(SessionDataSection.empty).session_id)
          .map {
            bySessionId =>
              bySessionId._1 -> setEvar(
                setEntry(
                  setPageSequence(bySessionId._2)
                ), evarizationSchema, evarDimensions
              )
          }

        if (bySessionIdList.isEmpty) {
          throw new Exception(
            s"bySessionIdList is empty!! for sesh key $sessionizationKey >> ${
              recordsPerServiceGroup.toString()
            }"
          )
        }

        RecordsPerServiceGroup2(s.serviceGroup, bySessionIdList)
    }
    (sessionizationKey, output)
  }

  /**
   * Sets page sequence dependent on 'pv' event.
   * Cache for the hour is inferred if present; this is used to mark continuation of session ID.
   **/
  def setPageSequence(in: List[SessionizePayload]): List[SessionizePayload] = {
    in.foldLeft(List.empty[(SessionizePayload, Int)]) {
      (r: List[(SessionizePayload, Int)], x: SessionizePayload) => {
        val pvHit = pageViewFinder(x)

        // First row will check cache, all other rows will default to a 0 value
        val fromCache = if (r.isEmpty) {
          val lastTimeStamp = x.sc.getOrElse(SessionizationCache.empty).last_time_stamp.getOrElse(0)
          if ((x.time_stamp_epoch - lastTimeStamp).abs > 1800) 0
          else x.sc.getOrElse(SessionizationCache.empty).event_count.getOrElse(0)
        } else 0

        // Only increment across page views
        val runningPageSequence: Int =
          if (x.session_data_section.getOrElse(SessionDataSection.empty).sessionReset.nonEmpty)
            if (pvHit) 1 else 0
          else fromCache + { // Add onto determined cache value
            if (pvHit)
              if (r.nonEmpty) r.last._2 + 1 else 1
            else
              if (r.nonEmpty) r.last._2 else 0
          }

        val postCuttingSection = x.post_cutting_section
          .getOrElse(PostCuttingSection.empty)
          .copy(page_sequence = if (pvHit) Option(runningPageSequence) else None)

        val currentRecord = x.copy(
          post_cutting_section = Option(postCuttingSection)
        )

        r :+ (currentRecord, runningPageSequence)
      }
    }.map(_._1)
  }

  /**
   * Prerequisite: page_sequence
   * No need to put reference on cache since it refers to page_sequence's value
   *
   * entry / is_entry is used to mark user's first 'pv' event.
   * This is referenced by page_sequence of 1.
   **/
  def setEntry(in: List[SessionizePayload]): List[SessionizePayload] = {
    in.foldLeft(List.empty[SessionizePayload]) {
      (r: List[SessionizePayload], c: SessionizePayload) => {
        val currentPostRecord = c.post_cutting_section.getOrElse(PostCuttingSection.empty)
        val postCuttingSection = currentPostRecord
          .copy(
            entry =
              if (pageViewFinder(c))
                if (currentPostRecord.page_sequence.getOrElse(0) == 1) Option(1) else Option(0)
              else None,
            is_entry = if (currentPostRecord.page_sequence.getOrElse(0) == 1) Option(true) else Option(false)
          )
        val currentRecord = c.copy(
          post_cutting_section = Option(postCuttingSection)
        )

        r :+ currentRecord
      }
    }
  }

  /**
   * Prerequisite: All prior information calculated (except for those defined in post calculation)
   *
   * evar is used to define first, last, and last ex checkout of user.
   * For more information, refer to evar documentation / scripts.
   **/
  def setEvar(
             in: List[SessionizePayload], evarStruct: StructType, evarDimensions: List[String]
             ): List[SessionizePayload] = {
    val evarized: Map[String, Map[String, Option[Any]]] =
      evarizeSessionizedPayload(in, evarStruct, evarDimensions)

    in.foldLeft(List.empty[SessionizePayload]) {
      (r: List[SessionizePayload], c: SessionizePayload) => {
        val uuidEvarized: Option[Map[String, Option[Any]]] = evarized.get(c.uuid)
        val evarSection = if (uuidEvarized.nonEmpty) Option(toEvarSection(uuidEvarized.get, c.cdna)) else c.evar_section
        val currentRecord = c.copy(evar_section = evarSection)

        r :+ currentRecord
      }
    }
  }
}
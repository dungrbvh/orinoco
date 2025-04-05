package com.orinoco.application.sessionize.steps

import com.orinoco.adapter.sessionize.SessionizePayload
import org.apache.spark.sql.{Dataset, SparkSession}

/*
Send service groups' events into deduplication process for pre-computes to perform correct 'previous' and
'inflow_channel' computations on interleaved service group events. Proceeds to
filter on target service group before sending it into sessionize routine. Also need to include last_channel_container
for service groups (if exists) to ensure consistent state across sessions.
*/
object DedupAndOrderPerSG {

  def asDataset(payload: Dataset[(String, SessionizePayload])(implicit spark: SparkSession): Dataset[(String, List[SessionizePayload])] = {
    import spark.implicits._

    payload.mapPartitions(itr => {
      itr.flatMap(kv => dedup(kv._1, kv._2))
    })

  }

  def dedup(key: String, recordsPerKey: List[SessionizePayload]): List[(String, List[SessionizePayload])] = {
    val companyList = recordsPerKey.map(_.norm.company).distinct
    val serviceGroupList = recordsPerKey.map(_.service_group).distinct

    companyList.map {
      targetCompay =>
        val recordsPerCompany = recordsPerKey.filter(c => targetCompany == c.norm.company)
        val deduplicated = serviceGroupList.flatMap(serviceGroup => deduplicate(recordsPerCompany, serviceGroup))
        (key, deduplicated)
    }

  }

/*
Removes duplicates and orders session events in deterministic manner. To perform sessionization, first need to order
events in ascending time-order. However, two events might occur concurrently. In that case, ordering furthered  by
uuid of events so that behavior of application is deterministic. Upstream issues also encountered; may see multiple
events with same time_stamp_epoch and uuid combinations. To achieve deterministic behavior in such cases, ordering furthered
by incoming hashCode() of case class object, only receiving the one with minimum hashCode().

Deduplication function now allows all service groups to go through by partitioning on the service group + uuid
  combination. Required to be able to pass deduplicated result into pre-computes phase.
 */

  def deduplicate: (List[SessionizePayload], String) => List[SessionizePayload] = {
    (records, targetServiceGroup) => records
      .filter(c => targetServiceGroup == c.service_group)
      .sortBy(x => (x.time_stamp_epoch, x.uuid, x.hashCode()))
      .foldLeft(List.empty[SessionizePayload]) {
        (r: List[SessionizePayload], c: SessionizePayload) =>
          if (r.map(x => x.uuid).contains(c.uuid)) r else r :+ c
      }
      .foldLeft(List.empty[SessionizePayload]) {
        (r: List[SessionizePayload], c: SessionizePayload) =>
          if (c.norm.order_id.isEmpty) r :+ c
          else if (!r.map(x => x.norm.order_id.getOrElse(-1)).contains(c.norm.order_id.get)) r :+ c
          else r
      }
  }

}

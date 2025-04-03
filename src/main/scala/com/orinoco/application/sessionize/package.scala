package com.orinoco.application

import com.orinoco.adapter.sessionize.SessionizePayload
import com.orinoco.adapter.sessionize.cache.SessionizationCache
import com.orinoco.schema.normalize.NormalizedWithUser
import org.apache.spark.Partitioner
import org.apache.spark.sql.Dataset

package object sessionize {
  case class NonRobotEventsWithPartitionerDS(
                                              nonRoboticNormalizedWithUser: Dataset[NormalizedWithUser],
                                              nonRoboticSessionCache: Dataset[SessionizationCache],
                                              totalInputCount: Long,
                                              partitioner: Partitioner
                                            )
  case class RecordsPerServiceGroup(serviceGroup: String, payload: List[SessionizePayload])
  case class RecordsPerServiceGroup2(serviceGroup: String, payloadPerSession: Map[String, List[SessionizePayload]])

}

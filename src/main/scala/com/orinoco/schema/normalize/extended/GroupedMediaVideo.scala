package com.orinoco.schema.normalize.extended

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

/**
 * Objective:
 *    Groups fields used for media and video.
 *    The other purpose is to avoid the 254 field limit in Normalized.
 *
 **/

case class GroupedMediaVideo(
                              media: Option[Map[String, String]] = None,
                              media_autoplay: Option[Int] = None,
                              media_event: Option[String] = None,
                              media_iframeurl: Option[String] = None,
                              media_name: Option[String] = None,
                              media_segment: Option[String] = None,
                              video_auto_play: Option[Int] = None,
                              video_complete: Option[Int] = None,
                              video_load: Option[Int] = None,
                              video_milestone: Option[Int] = None,
                              video_play: Option[Int] = None,
                              video_segment_complete: Option[Int] = None
                            )
object GroupedMediaVideo {
  val getFlattenedSchema: StuctType = getSchemaAsStruct[GroupedMediaVideo]
  val empty = GroupedMediaVideo()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
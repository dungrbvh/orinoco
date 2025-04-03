package com.orinoco.adapter.sessionize.sections

import com.orinoco.adapter.sessionize.sections.post.GMS
import com.orinoco.commons.ReflectionUtils.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

/**
 * If availability is 4/12 hours = PostCalculation spark job
 * If availability is 1 hours = Sessionization spark job
 */
case class PostCuttingSection(
                               bounce: Option[Int] = None, // private - 4/12 hours
                               complemented_easyid: Option[String] = None, // public - 4/12 hours
                               custom_dimension: Option[Map[String, String]] = None, // private - 4/12 hours
                               duration: Option[Int] = None, // public - 4/12 hours
                               view_type: Option[String] = None,
                               entry: Option[Int] = None, // private - 1 hour
                               exit: Option[Int] = None, // private - 4/12 hours
                               gms: Option[GMS] = None, // private - 4/12 hours
                               inflow_channel_entry: Option[String] = None, // private 4/12 hours
                               is_entry: Option[Boolean] = None, // public - 1 hour
                               is_exit: Option[Boolean] = None, // private - 4/12 hours
                               is_participation_event: Option[Boolean] = None, // private - 4/12 hours
                               page_sequence: Option[Int] = None, // public - 1 hour
                               reserve_order_gms: Option[Double] = None, // not on specs and mongodb. private?
                               reserve_order: Option[Double] = None, // not on specs and mongodb. private?
                               session_id_for_easy_id: Option[String] = None // private - 4/12 hours
                             )

object PostCuttingSection {
  val getFlattenedSchema: StructType = StructType(
    getSchemaAsStruct[PostCuttingSection].fields.filter(x => !x.name.equals("gms")).toList :::
      GMS.getFlattenedSchema.fields.toList
  )
  val empty: PostCuttingSection = PostCuttingSection()
  val nestedFieldList: List[String] = getSchemaAsStruct[PostCuttingSection].map(_.name).toList.filter(x => x != "gms") :::
    GMS.nestedFieldList.map(x => s"gms.$x")
}
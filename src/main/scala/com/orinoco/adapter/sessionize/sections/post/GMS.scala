package com.orinoco.adapter.sessionize.sections.post

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class GMS (
                 cart_add_through: Option[Double] = None,
                 complete_through: Option[Double] = None,
                 complete1_through: Option[Double] = None,
                 complete2_through: Option[Double] = None,
                 complete3_through: Option[Double] = None,
                 complete4_through: Option[Double] = None,
                 complete5_through: Option[Double] = None,
                 complete6_through: Option[Double] = None,
                 complete7_through: Option[Double] = None,
                 complete8_through: Option[Double] = None,
                 complete9_through: Option[Double] = None,
                 complete10_through: Option[Double] = None,
                 complete11_through: Option[Double] = None,
                 complete12_through: Option[Double] = None,
                 complete13_through: Option[Double] = None,
                 complete14_through: Option[Double] = None,
                 complete15_through: Option[Double] = None,
                 complete16_through: Option[Double] = None,
                 complete17_through: Option[Double] = None,
                 complete18_through: Option[Double] = None,
                 complete19_through: Option[Double] = None,
                 complete20_through: Option[Double] = None,
                 entry_through: Option[Double] = None,
                 event_001_through: Option[Double] = None,
                 event_002_through: Option[Double] = None,
                 event_003_through: Option[Double] = None,
                 purchase_gms_through: Option[Double] = None,
                 purchase_item_through: Option[Double] = None,
                 purchase_order_through: Option[Double] = None,
                 purchase_regular_through: Option[Double] = None,
                 purchase_shop_through: Option[Double] = None,
                 purchase_unit_through: Option[Double] = None,
                 registration_through: Option[Double] = None
               )

object GMS {
  val getFlattenedSchema: StructType = getSchemaAsStruct[GMS]
  val empty: GMS = GMS()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
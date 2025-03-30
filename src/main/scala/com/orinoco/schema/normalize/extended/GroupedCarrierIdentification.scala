package com.orinoco.schema.normalize.extended

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class GroupedCarrierIdentification(
                                         isroam: Option[Integer] = None,
                                         netop: Option[String] = None,
                                         netopn: Option[String] = None,
                                         simcid: Option[Integer] = None,
                                         simcn: Option[String] = None,
                                         simop: Option[String] = None,
                                         simopn: Option[String] = None
                                       )

object GroupedCarrierIdentification {
  val getFlattenedSchema: StuctType = getSchemaAsStruct[GroupedCarrierIdentification]
  val empty: GroupedCarrierIdentification = GroupedCarrierIdentification()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
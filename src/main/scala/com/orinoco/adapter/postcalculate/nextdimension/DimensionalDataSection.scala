package com.orinoco.adapter.postcalculate.nextdimension

import org.apache.spark.sql.types.StructType

case class DimensionalDataSection(
                                   next_dimensions: Option[NextDimensions] = None,
                                   prev_dimensions: Option[PriorDimensions] = None
                                 )

object DimensionalDataSection {
  val getFlattenedSchema: StructType = StructType(
    NextDimensions.getFlattenedSchema.fields.toList :::
      PriorDimensions.getFlattenedSchema.fields.toList
  )
  val empty: DimensionalDataSection = DimensionalDataSection()
  val nestedFieldList: List[String] =
    NextDimensions.nestedFieldList.map(x => s"next_dimensions.$x") :::
      PriorDimensions.nestedFieldList.map(x => s"prev_dimensions.$x")
}


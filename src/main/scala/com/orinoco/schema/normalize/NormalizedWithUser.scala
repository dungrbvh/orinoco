package com.orinoco.schema.normalize

import com.orinoco.schema.cdna.CustomerCDNA
import org.apache.spark.sql.types.StructType

/**
 * Objective:
 *    This script combines both normalized and cdna data.
 *    This is mainly used for the Normalize job.
 *
 **/
case class NormalizedWithUser (
  norm: Normalized,
  cdna: CustomerCDNA
)

object NormalizedWithUser {
  val getFlattenedSchema: StructType = StructType(
    Normalized.getFlattenedSchema.fields.toList :::
      CustomerCDNA.getFlattenedSchema.fields.toList
  )

  val empty: NormalizedWithUser = NormalizedWithUser(
    Normalized.empty,
    CustomerDNA.empty
  )

  val nestedFieldList: List[String] =
    Normalized.nestedFieldList.map(x => s"norm.$x") :::
      CustomerDNA.nestedFieldList.map(x => s"cdna.$x")
}

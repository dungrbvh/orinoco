package com.orinoco.adapter.postcalculate.nextdimension

import com.orinoco.commons.ReflectionUtls.getSchemaAsStruct
import org.apache.spark.sql.types.StructType

case class PriorDimensions(
                            prior_domain_level_1: Option[String] = None,
                            prior_domain_level_2: Option[String] = None,
                            prior_domain_level_3: Option[String] = None,
                            prior_domain_level_4: Option[String] = None,
                            prior_domain_level_5: Option[String] = None,
                            prior_path_level_1: Option[String] = None,
                            prior_path_level_2: Option[String] = None,
                            prior_path_level_3: Option[String] = None,
                            prior_path_level_4: Option[String] = None,
                            prior_path_level_5: Option[String] = None,
                            prior_pgn_level_1: Option[String] = None,
                            prior_pgn_level_2: Option[String] = None,
                            prior_pgn_level_3: Option[String] = None,
                            prior_pgn_level_4: Option[String] = None,
                            prior_pgn_level_5: Option[String] = None,
                            prior_ssc_level_1: Option[String] = None,
                            prior_ssc_level_2: Option[String] = None,
                            prior_ssc_level_3: Option[String] = None,
                            prior_ssc_level_4: Option[String] = None,
                            prior_ssc_level_5: Option[String] = None
                          )

object PriorDimensions {
  val getFlattenedSchema: StructType = getSchemaAsStruct[PriorDimensions]
  val empty: PriorDimensions = PriorDimensions()
  val nestedFieldList: List[String] = getFlattenedSchema.map(_.name).toList
}
package com.orinoco.config.base

import com.typesafe.config.Config

import scala.collection.JavaConverters._

case class EvarConfig(
                       dimensionsSessionize: List[String],
                       dimensionsPostCalculation: List[String]
                     )

object EvarConfig {
  private val config = AppConfig.init().getConfig("evar")
  def load(config: Config = config): EvarConfig = {
    EvarConfig(
      dimensionsSessionize = config.getStringList("sessionize").asScala.toList,
      dimensionsPostCalculation = config.getStringList("post_calculate").asScala.toList
    )
  }
}
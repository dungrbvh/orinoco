package com.orinoco.config.base

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

object AppConfig {

  private val LOG = Logger.getLogger(this.getClass.getName)

  /**
   *
   * @param envConfPath
   */
  def init(): Config = {
    val envConfPath = ConfigFactory.systemProperties().getString("orinoco.conf")
    val baseConf = ConfigFactory.load("application.base.conf")
    val config = if (envConfPath == null) {
      LOG.info("-Dorinoco.conf is not given.")
      baseConf
    } else {
      ConfigFactory
        .parseFileAnySyntax(new java.io.File(envConfPath))
        .withFallback(baseConf)
        .resolve()
    }

    LOG.info("Config -start-")
    config.entrySet().asScala.foreach(entry => LOG.info(entry))
    LOG.info("Config -end-")

    config
  }

}
package com.orinoco.config.task

import com.orinoco.config.base._
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write

case class SessionizeConfig(
                             env: String,
                             dataCenter: String,
                             hostName: String,
                             config: SessionizeConfig,
                             evar: EvarConfig
                           )
object SessionizeConfig {
  def load(mainConfig: Config): SessionizeConfig = {
    SessionizeConfig(
      env = mainConfig.getString("env"),
      dataCenter = mainConfig.getString("data-center"),
      hostName = mainConfig.getString("host-name"),
      config = SessionizationConfig.load(mainConfig.getConfig("sessionize")),
      evar = EvarConfig.load())
  }
}
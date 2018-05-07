package com.sc.eni.common

import com.typesafe.config.{Config, ConfigFactory}


trait AppConfig {

  lazy val config: Config = ConfigFactory.load("appConfiguration.conf")

}

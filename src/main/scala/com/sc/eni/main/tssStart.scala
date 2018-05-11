package com.sc.eni.main

import com.sc.eni.common.AppConfig
import com.sc.eni.common.vo.factory.LoadConfigFactory._
import com.sc.eni.config.MetaConfiguration
import com.sc.eni.core.Ingest

object tssStart extends AppConfig {

  def main(args: Array[String]): Unit = {
/*
    if (args == null || args.length == 0 || args(0) == null) {
      throw new IllegalArgumentException("Required fully qualified file name.")
    }
*/


    //lazy val runpipelineValue = args(0).toString
    //lazy val runpipelineValue = "loadLayer2"

    // Loading One time configuration
    //lazy val configPath = getClass.getResource("/tableConfig.json").getPath
    val configPath ="test"
    lazy val configurations: MetaConfiguration = loadTableConfigs(configPath)

    println("Meta configuration Info :"+configurations.trades.layer1 )
    //println(configurations)

    //Ingest.runPipeline(runpipelineValue, configurations)

    Ingest.runPipeline

  }


}

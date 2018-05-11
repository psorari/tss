package com.sc.eni.common.vo.factory

import com.sc.eni.config.MetaConfiguration
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object LoadConfigFactory {

  def loadTableConfigs(configPath: String): MetaConfiguration = {

    //local joson
   // val json ="{\n  \"trades\": {\n    \"layer1\": {\n      \"tables\": [\n        {\n          \"tableName\": \"trade\",\n          \"tableLocation\": \"C:\\\\ENI\\\\Files\\\\DataFiles\\\\l1\",\n          \"fileType\": \"csv\"\n        }\n      ]\n    },\n    \"layer2\": {\n      \"tables\": [\n        {\n          \"tableName\": \"trade\",\n          \"tableLocation\": \"C:\\\\ENI\\\\Files\\\\DataFiles\\\\l2\",\n          \"fileType\": \"avro\"\n        }\n      ]\n    },\n    \"layer3\": {\n      \"tables\": [\n        {\n          \"tableName\": \"trade\",\n          \"tableLocation\": \"C:\\\\ENI\\\\Files\\\\DataFiles\\\\l3\",\n          \"fileType\": \"parquet\"\n        }\n      ]\n    }\n  }\n}"

    //Cluster json

    val json = "{\n  \"trades\": {\n    \"layer1\": {\n      \"tables\": [\n        {\n          \"tableName\": \"trade\",\n          \"tableLocation\": \"/data/l1/\",\n          \"fileType\": \"csv\"\n        }\n\n      ]\n    },\n    \"layer2\": {\n      \"tables\": [\n        {\n          \"tableName\": \"tradeAvro\",\n          \"tableLocation\": \"/data/l2/trade/\",\n          \"fileType\": \"avro\"\n        }\n\n      ]\n    },\n    \"layer3\": {\n      \"tables\": [\n        {\n          \"tableName\": \"tradeParquet\",\n          \"tableLocation\": \"/data/l3/\",\n          \"fileType\": \"parquet\"\n        }\n\n      ]\n    }\n  }\n}\n"

    val metaConfig: MetaConfiguration = {
      implicit val formats = DefaultFormats
      val parsedJson = parse(json)
      println("parsedJson::" + parsedJson)
      parsedJson.extract[MetaConfiguration]
    }

    metaConfig

  }
}

package com.sc.eni.config

/*
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
*/

case class MetaConfiguration(trades: Trades)

case class Trades(layer1: Layer, layer2: Layer, layer3: Layer)

case class Layer(tables: List[Table])

case class Table(tableName: String,
                 tableLocation: String,
                 schemaLocation: Option[String] = None,
                 fileType: Option[String] = Some("csv"))


//case class MetaConfiguration(name:String, age: Int)

/*object test extends App {
  val json = "{\n  \"Trades\": {\n    \"Layer1\": [\n      {\n        \"tableName\": \"Employee\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020/l1/Employee/\",\n        \"schemaLocation\": \"\",\n        \"fileType\": \"csv\"\n      },\n      {\n        \"tableName\": \"Dept\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020//l1/dept/\",\n        \"fileType\": \"csv\"\n      }\n    ],\n    \"Layer2\": [\n      {\n        \"tableName\": \"EmployeeAvro\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020/l2/Employee/\",\n        \"fileType\": \"avro\"\n      },\n      {\n        \"tableName\": \"DeptAvro\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020//l2/dept/\",\n        \"fileType\": \"avro\"\n      }\n    ],\n    \"Layer3\": [\n      {\n        \"tableName\": \"EmployeeParquet\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020/l3/Employee/\",\n        \"fileType\": \"parquet\"\n      },\n      {\n        \"tableName\": \"DeptParquet\",\n        \"tableLocation\": \"hdfs://quickstart.cloudera:8020//l3/dept/\",\n        \"fileType\": \"parquet\"\n      }\n    ]\n  }\n}"

  //val json = "{\"name\" : \"ABC\", \"age\" : 25}"

  lazy val metaConfig: MetaConfiguration = {
    implicit val formats = DefaultFormats

    val toWrite = MetaConfiguration(Trades(
      Layer(List(Table("a", "b", Some("c"), Some("d")), Table("a", "b", Some("c"), Some("d")))),
      Layer(List(Table("a", "b", Some("c"), Some("d")), Table("a", "b", Some("c"), Some("d")))),
      Layer(List(Table("a", "b", Some("c"), Some("d")), Table("a", "b", Some("c"), Some("d"))))))

    println(write(toWrite))

    val parsedJson = parse(json)
    println("parsedJson::" + parsedJson)
    println(parsedJson.extract[MetaConfiguration])
    parsedJson.extract[MetaConfiguration]

  }

  println(metaConfig)

}*/

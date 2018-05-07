package com.sc.eni.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

trait SparkContextProvider extends AppConfig {

  //implicit val normalKryoEncoder = Encoders.kryo[Row]

  lazy val spark = SparkSession.builder()
//    .master(config.getString("core.spark.master.url"))
    .master("local[*]")
    .appName("TSS-Spark-ETL")
    .getOrCreate()



/*
  spark.conf.set("spark.executor.memory", config.getString("core.spark.executor.memory"))
  spark.conf.set("spark.executor.cores", config.getString("core.spark.executor.cores"))
  spark.conf.set("spark.executor.instances", config.getString("core.spark.executor.instances"))
*/


}

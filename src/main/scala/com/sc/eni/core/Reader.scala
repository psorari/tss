package com.sc.eni.core

import com.sc.eni.common.{SchemaBuilder, SparkContextProvider}
import com.sc.eni.config.Table
import com.databricks.spark.avro._

object Reader extends SparkContextProvider {

  def readFromFile(tableInfo: Table) = {
    val filePath = tableInfo.tableLocation

    println(tableInfo)
    tableInfo.fileType.get.toLowerCase match {
      case "csv" => tableInfo.schemaLocation match {
        case Some(value) =>
          val x = spark.read.schema(SchemaBuilder.build(tableInfo.schemaLocation.get)).csv(filePath)
          x
        case None =>
          val x= spark.read.option("inferSchema", "true").csv(filePath)
          println("Dataframe from Readng ......")
          x.show(10)
          x

      }

      case "parquet" => spark.read.parquet(filePath)
      case "avro" => {
        println("File path is "+filePath)
        spark.read.avro(filePath) }
      case _ => throw new IllegalArgumentException("")

    }


  }

  def readFromTable(tableInfo: Table) = {
    spark.table(tableInfo.tableName)
  }
}

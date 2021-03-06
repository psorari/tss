package com.sc.eni.core


import org.apache.spark.sql.{DataFrame, SaveMode}
import com.databricks.spark.avro._

//extends hbaseContextprovider
object Writer {

  def writeToFile(tableName : String, df : DataFrame, location : String,ftype:String) = {
    val writelocation= location+"/"+tableName
    println("Avro write location is "+writelocation)

      df.write.format("com.databricks.spark.avro").save(writelocation)
      //df.write.format("com.databricks.spark.avro").save(writelocation)
    //df.write.format(ftype).save(writelocation)

  }



  def writeToParquet(tableName : String, df : DataFrame, location : String,ftype:String) = {
    val writelocation= location+"/"+tableName
    println("Parquet write location is "+writelocation)

df.write.mode(SaveMode.Append).format("Avro")
    df.write.format("parquet").save(writelocation)
    //df.write.format(ftype).save(writelocation)

  }





}

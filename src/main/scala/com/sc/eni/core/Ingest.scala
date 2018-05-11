package com.sc.eni.core

import com.sc.eni.common.SparkContextProvider
import com.sc.eni.config.{HbaseConfiguration, Layer, MetaConfiguration}
import com.databricks.spark.avro._
import org.apache.spark.sql.{DataFrame, SaveMode}


object Ingest extends SparkContextProvider{

  //def runPipeline(pipeline :String,tableConfig: MetaConfiguration) = {
    def runPipeline() = {
     /*pipeline match {
       case "loadLayer2" => loadLayer2(tableConfig.trades.layer1)
       case "loadLayer3" => loadLayer3(tableConfig.trades.layer2)
       case "loadLayer4" => loadLayer4(tableConfig.trades.layer3)
       case _ => throw new IllegalArgumentException("Invalid input specified")
     }*/

      val table_name = "trades"
      //local location
/*      val l1_location ="C:\\ENI\\Files\\DataFiles\\l1"
      val l2_location ="C:\\ENI\\Files\\DataFiles\\l2"
      val l3_location ="C:\\ENI\\Files\\DataFiles\\l3"*/

      // EMR locaton
      val l1_location ="/data/l1"
        val l2_location ="/data/l2"
       val l3_location ="/data/l3"

      loadLayer2(table_name,l1_location,l2_location,l3_location)
      loadLayer3(table_name, l1_location,l2_location,l3_location)

      println("Starting L4..Hbase Load.......")
      loadLayer4()


    }

  //TODO:  val tableDF = IngestHandler.ingest(rawTables)

  //def loadLayer2(layer1 : Layer) = {
/*    val layer2Location = "C:\\ENI\\Files\\DataFiles\\l2"
    //val layer2Location ="/data/l2/trade"
    val ftype="avro"
    layer1.tables.map {
     table => {
       println(table)
       val df = Reader.readFromFile(table)

       //TODO : Transform dataframes
       Writer.writeToFile(table.tableName, df, layer2Location,ftype)
     }
   }*/

  def loadLayer2(table_name:String,l1_location:String,l2_location:String,l3_location:String) = {
    val ftype="avro"
    val x= spark.read.option("inferSchema", "true").csv(l1_location)
    x.show()
    x.write.mode(SaveMode.Append).format("com.databricks.spark.avro").save(l2_location)
 }

/*  def loadLayer3(layer2 : Layer) = {
    // val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3"
    val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3\\trade"
   // val layer3Location = "/data/l3/trade"
    val tableName="tradeParquet"
    val ftype="parquet"
    layer2.tables.map {
      table => {
        val df = Reader.readFromFile(table)
        //TODO : Transform dataframes
         Writer.writeToParquet(tableName, df, layer3Location,ftype)
      }
    }
  }*/


  def loadLayer3(table_name:String, l1_location:String,l2_location:String,l3_location:String) = {
    val x= spark.read.avro(l2_location)
    x.show()
    x.write.mode(SaveMode.Append).format("parquet").save(l3_location)
  }

  //def loadLayer4(layer3 : Layer) = {
    def loadLayer4() = {
    // val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3"
    //val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3\\trade"
    val layer3Location = "/data/l3/"
    val tableName="trade"
    val ftype="parquet"
      /*   layer3.tables.map {
           table => {
             val df = Reader.readFromFile(table)

             val filterdf= Transform.filterColumns(df)
             filterdf.show()

             val traderdf= Transform.countTrader(df)
             println("Prinintf trade DF ")
             traderdf.show()

             val aggregatedf= Transform.aggregate(df)
             println("Printing aggregate DF")
             aggregatedf.show()*/

        val mydata= Seq(("key1","10","20","30","40"),("key2","20","676","33636","22"),("key3","22","128282","3101","23"),("key3","4545","233","121","234"),("key4","2222","999","1","22") )
        //val mydataRow= mydata.map( x => Row(x:_*))
        import spark.implicits._
        val mydataDF: DataFrame = mydata.toDF("col0","col1","col2","col3","col4")

        println("Printing Hbase Dataframe.....")
        mydataDF.show()
        //TODO : Transform dataframes
        //Writer.writeToFile(tableName, filterdf, layer3Location,ftype)
        HbaseConfiguration.hbaseload(mydataDF)
      }

}







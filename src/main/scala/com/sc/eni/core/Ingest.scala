package com.sc.eni.core

import com.sc.eni.config.{HbaseConfiguration, Layer, MetaConfiguration}

object Ingest {

  def runPipeline(pipeline :String,tableConfig: MetaConfiguration) = {

     pipeline match {
       case "loadLayer2" => loadLayer2(tableConfig.trades.layer1)
       case "loadLayer3" => loadLayer3(tableConfig.trades.layer2)
       case "loadLayer4" => loadLayer4(tableConfig.trades.layer3)
       case _ => throw new IllegalArgumentException("Invalid input specified")
     }
  }

  //TODO:  val tableDF = IngestHandler.ingest(rawTables)

  def loadLayer2(layer1 : Layer) = {
    val layer2Location = "C:\\ENI\\Files\\DataFiles\\l2"
    //val layer2Location ="/data/l2/trade"
    val ftype="avro"
    layer1.tables.map {
     table => {
       println(table)
       val df = Reader.readFromFile(table)

       //TODO : Transform dataframes
       Writer.writeToFile(table.tableName, df, layer2Location,ftype)
     }
   }
  }

  def loadLayer3(layer2 : Layer) = {
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
  }


  def loadLayer4(layer3 : Layer) = {
    // val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3"
    //val layer3Location = "C:\\ENI\\Files\\DataFiles\\l3\\trade"
    val layer3Location = "/data/l3/"
    val tableName="trade"
    val ftype="parquet"
    layer3.tables.map {
      table => {
        val df = Reader.readFromFile(table)

        val filterdf= Transform.filterColumns(df)
        filterdf.show()

        val traderdf= Transform.countTrader(df)
        println("Prinintf trade DF ")
        traderdf.show()

        val aggregatedf= Transform.aggregate(df)
        println("Printing aggregate DF")
        aggregatedf.show()


        //TODO : Transform dataframes
        //Writer.writeToFile(tableName, filterdf, layer3Location,ftype)
        HbaseConfiguration.hbaseload(filterdf)
      }
    }
  }
}







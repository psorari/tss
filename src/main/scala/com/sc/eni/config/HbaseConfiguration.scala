package com.sc.eni.config

//import com.cloudera.spark.hbase.HBaseContext
import com.sc.eni.common.HbaseProvider
import org.apache.hadoop.hbase.client.{Admin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HbaseConfiguration extends HbaseProvider{/*

  def deletetableifexists ( tableName :String, ad :Admin ): Unit ={

    if(ad.tableExists(TableName.valueOf(tableName))){
      if(!ad.isTableDisabled(TableName.valueOf(tableName))){
        ad.disableTable(TableName.valueOf(tableName))
      }
      ad.deleteTable(TableName.valueOf(tableName))
    }

  }

  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"trade"},
                   |"rowkey":"tradeid",
                   |"columns":{
                   |"temp":{"cf":"t1", "col":"temp", "type":"float"},
                   |"pressure":{"cf":"t2", "col":"pressure", "type":"float"}
                   |"pressure":{"cf":"t2", "col":"pressure", "type":"float"}
                   |}
                   |}""".stripMargin

  val df :DataFrame

  df.write.options(
    Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> “5”))
  .format(“org.apache.spark.sql.execution.datasources.hbase”)
  .save()

  def createtable(tableName :String,columnFamily1: String, columnFamily2: String, ad :Admin): Unit = {
    val tdescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    // Adding column families to table descriptor
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily1))
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily2))

    // Execute the table through admin
    ad.createTable(tdescriptor)
  }

  //Converting RDD[Row] into RDD[columns] of type: (column of Hbase)
  def rowtocolumn(arrays: Array[String], columnFamily1 :String , columnFamily2: String): Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    val result: Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = Array(
      (Bytes.toBytes(arrays(1)), Array(
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("empid"), Bytes.toBytes(arrays(1))),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("empname"), Bytes.toBytes(arrays(2))),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("emploc"), Bytes.toBytes(arrays(3))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empscore"), Bytes.toBytes(arrays(4))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empoldsal"), Bytes.toBytes(arrays(5))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empnewsal"), Bytes.toBytes(arrays(6))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("datalaketime"), Bytes.toBytes(arrays(0)))
      )))
    val fin =result
    println(result)
    fin
  }

  def bulkputcolumns(hBaseContext: HBaseContext, rdd:RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])], tableName: String): Unit = {
    println{"Loading Data into Hbase Started ....."}
    hBaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      },
      "true".toBoolean)
    println{"Loading Data into Hbase Completed ....."}
  }


  def loadHbase(df : DataFrame): Unit = {

    val tableName = "trades"
    val columnFamily1 = "personal"
    val columnFamily2 = "prof"

    val inputrdd= df.rdd
    val stringrdd: RDD[String] = inputrdd.map(line => line.toString())
    val toRemove = "[]".toSet
    val correctedrdd: RDD[String] = stringrdd.map(lines => lines.filterNot(toRemove))
    val splitrdd: RDD[Array[String]] = correctedrdd.map(line => line.split(","))

    // Input Data : in RDD with two cf1 and two rows :
    // (RowKey, columnFamily, columnQualifier, value)
    val rdd123: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = splitrdd.map(
      lines =>rowtocolumn(lines,columnFamily1, columnFamily2)
    ).flatMap(x=> x)

    //If table exists delete it
    //deletetableifexists(tableName,ad)

    //create table
    //createtable(tableName, columnFamily1, columnFamily2, ad)

    // BUlkPut the rdd into HBase table "tableName' -
    println("putting the value into hbase ************")
    bulkputcolumns(hbaseContext,rdd123,tableName)



}*/

  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"trade"},
                   |"rowkey":"tradeid",
                   |"columns":{
                   |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"officeAddress":{"cf":"Office", "col":"Address", "type":"string"},
                   |"officePhone":{"cf":"Office", "col":"Phone", "type":"string"},
                   |"personalName":{"cf":"Personal", "col":"Name", "type":"string"},
                   |"personalPhone":{"cf":"Personal", "col":"Phone", "type":"string"}
                   |}
                   |}""".stripMargin


/*  def withCatalog(cat: String): DataFrame = {
    spark.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }*/
def hbaseload(df : DataFrame) {
  df.write
    .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
    .format("org.apache.spark.sql.execution.datasources.hbase").save()
}

}

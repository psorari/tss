package com.sc.eni.config

import com.sc.eni.common.{HbaseProvider, SparkContextProvider}

import org.apache.spark.sql.execution.datasources.hbase._

//import org.apache.hadoop.hbase.client.{Admin, Put}
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
  import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.DataFrame


object HbaseConfiguration extends SparkContextProvider {





  def withCatalog(cat: String): DataFrame = {
    spark.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

def hbaseload(df : DataFrame) {
  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"test"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"testcf", "col":"id1", "type":"string"},
                   |"col2":{"cf":"testcf", "col":"id2", "type":"string"},
                   |"col3":{"cf":"testcf", "col":"id3", "type":"string"},
                   |"col4":{"cf":"testcf", "col":"id4", "type":"string"}
                   |}
                   |}""".stripMargin

  df.write
    .options(Map(HBaseTableCatalog.tableCatalog -> catalog,HBaseTableCatalog.newTable -> "5"))
    .format("org.apache.spark.sql.execution.datasources.hbase").save()
}

}

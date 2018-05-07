package com.sc.eni.common

import org.apache.spark.sql.types.StructType

package object config {

  case class ConfigurationVO(tableInfo : TableInfo)

  case class TableInfo(
                        layerL2 : Option[Seq[Table]],
                        layerL3 : Option[Seq[Table]],
                        layerL4 : Option[Seq[Table]]
                        )

//  case class TableInfo(tableName : String,tableSchema :Map[String, StructType])
  //, schemaPath : Option[Array[String]]

 // case class SchemaInfo(tableSchema : Map[String,StructType])

  case class Table(tablename:String, tablelocation:String ,schema : Option[StructType], fileType : Option[String] = Some("CSV"))

}



package com.sc.eni.common

import com.sc.eni.common.SparkSQLDataTypes._
import org.apache.hadoop.io.SequenceFile.Metadata
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

object SchemaBuilder {


  def build(schemPath: String): StructType = {
    var json: String = ""
    for (line <- Source.fromFile(schemPath).getLines()) json += line

    val schemaFields: Option[List[StructField]] = JSON.parseFull(json).map {
      case json: Map[String, List[Map[String, Any]]] =>
        json("fields").map(x => (x("name"), x("type"), Try(x("isNull")).getOrElse(true))).map { case (name, dataType, isnull) =>
          StructField(name.toString, getDataType(dataType.toString), isnull.toString.toBoolean)
        }
    }
    StructType(schemaFields.get.toArray)
  }

  private def getDataType(dataType: String) = {
    val dateTypeEnumValue: SparkSQLDataTypes.Value = Try(SparkSQLDataTypes.withName(dataType.toUpperCase))  match {
      case Success(dateTypeEnum) => dateTypeEnum
      case Failure(error) => throw new IllegalStateException("Json schema is invalid")
    }
    dateTypeEnumValue match {
      case STRING => DataTypes.StringType
      case INTEGER | INT => DataTypes.IntegerType
      case LONG => DataTypes.LongType
      case FLOAT => DataTypes.FloatType
      case DOUBLE => DataTypes.DoubleType
      case TIMESTAMP => DataTypes.TimestampType
      case DATE => DataTypes.DateType
    }
  }

}

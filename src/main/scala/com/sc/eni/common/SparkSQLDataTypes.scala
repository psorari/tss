package com.sc.eni.common

object SparkSQLDataTypes extends Enumeration{
  type Strategy = Value
  val STRING, INTEGER, INT, LONG, DOUBLE, FLOAT, TIMESTAMP, DATE = Value

}

package com.sc.eni.core

import com.sc.eni.common.SparkContextProvider
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

object Transform extends SparkContextProvider{

  def calSalary(dataframe:DataFrame):DataFrame ={

    dataframe.select(when(dataframe("empscore") >= 85 ,5)
      .when(dataframe("empscore") === "10", 10)
      .otherwise(2))

  }

  def Filterdata(dataframe:DataFrame, condition: Column) :DataFrame={
    dataframe.filter(condition)
  }


  def filterColumns(df: DataFrame) :DataFrame= {

    val filteredDF = spark.sql(
      """select dwh_trade_id, tld_source_system, exchange_or_otc, trader, buy_sell, trade_date, trade_volume,
        | trade_price, internal_acc_book, internal_acc_book_id, dwh_internal_portfolio_id, trading_period,
        | trade_currency, trade_status
        | from Trades_v_Obs""".stripMargin
    )
    filteredDF
  }

  def countTrader(df: DataFrame) :DataFrame= {

    /*val traderdf = sqlContext.sql(
      """select trader, count(trader) AS No_of_Trades
        |groupby trader
        |from Trades_v_Obs
      """.stripMargin
    )*/


    val traderdf = df.groupBy("trader").count().as("No_of_Trades_done")
    traderdf

  }

  def aggregate( df: DataFrame) :DataFrame= {

    val aggdf = spark.sql(
      """select trader, trade_volume, trade_price
        |group by
        | from Trades_v_Obs""".stripMargin
    )
    aggdf
  }



}


package com.sc.eni.common.handler

import com.sc.eni.common.config.Table
import com.sc.eni.core.Reader
import org.apache.spark.sql.DataFrame

object IngestHandler {

  def ingest(tables:Seq[Table])= ???


/*  def ingest(tables:Seq[Table]):Seq[DataFrame]= {

    //TODO : Just commenting for running
/*    tables.map{table =>
     table.SourceType match {
       case Some("file") => Reader.readFromFile(table)
       case Some("table")=> Reader.readFromTable(table)

      }

    }*/


  }*/

}

package com.sc.eni.common


/*import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory}*/

trait HbaseProvider extends SparkContextProvider{
  /*val hconf = HBaseConfiguration.create()
  hconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  hconf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
*/

  //val hbaseContext = new HBaseContext(spark.sparkContext, hconf)

/*  //create connection
  val conn = ConnectionFactory.createConnection(hconf)

  //create Admin
  val ad: Admin = conn.getAdmin*/

}

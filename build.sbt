name := "tss"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

//resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"


libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  % "provided"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"
libraryDependencies += "org.apache.avro" % "avro-mapred" % "1.8.1"
libraryDependencies += "org.apache.hbase" % "hbase" % "1.4.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.4.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.4.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.4.2" % "provided"
libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.3"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
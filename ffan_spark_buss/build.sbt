import sbt.Keys._

name := "ffan_spark_buss"

version := "1.0"

scalaVersion := "2.10.6"

resolvers ++= Seq(
 "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "nexus" at "http://maven.intra.ffan.com/nexus/content/groups/public",
  "nexus2" at "http://repo1.maven.org/maven2",
//  "maven" at "http://mvnrepository.com/",
  "maven2" at "http://repository.jboss.org/nexus/content/groups/public/"
)

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-catalyst" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",

//  "org.apache.spark" %% "spark-core" % "2.0.0",
//  "org.apache.spark" %% "spark-sql" % "2.0.0",
//  "org.apache.spark" %% "spark-catalyst" % "2.0.0",
//  "org.apache.spark" %% "spark-hive" % "2.0.0",
//  "org.apache.spark" % "spark-streaming_2.10" % "2.0.0",
//  "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.0.0",
//  "org.scala-lang" % "scala-reflect" % "2.10.6",
  "com.alibaba" % "fastjson" % "1.2.11",
  "mysql" % "mysql-connector-java" % "5.1.38"
)





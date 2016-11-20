import sbt.Keys._

name := "ffan_spark_etl"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
 "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "RethinkScala Repository" at "http://kclay.github.io/releases",
  "nexus" at "http://10.77.144.229:10081/nexus/content/groups/public/",
  "maven" at "http://mvnrepository.com/",
  "maven2" at "http://repository.jboss.org/nexus/content/groups/public/"
)

externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-catalyst" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "com.alibaba" % "fastjson" % "1.2.11",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

)





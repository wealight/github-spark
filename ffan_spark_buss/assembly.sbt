import AssemblyKeys._

assemblySettings

jarName in assembly := "ffan-spark-etl-snapshot.jar"

scalaVersion := "2.10.6"

test in assembly := {}

mainClass in assembly := Some( "Spark_Test")

assemblyOption in packageDependency ~= { _.copy(appendContentHash = true) }

mergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith  "pom.properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith  "pom.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith  "overview.html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith  "parquet.thrift" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith  "plugin.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith  "class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last contains  "org.scala-lang" => MergeStrategy.first

  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}


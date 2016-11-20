package cn.wanda.spark.etl.ffan_server_log_etl.main

import cn.wanda.spark.etl.ffan_server_log_etl.map.ServerLogMapObject
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import utils.Cmd
import scala.collection.JavaConversions._


/**
  * Created by weishuxiao on 16/6/28.
  **/

object ServerLogMain {
  def main(args: Array[String]): Unit = {

    val cmd: Cmd = new Cmd("")
    cmd.addParam("inputPath", "输入目录为")
    cmd.addParam("outputPath", "输出目录为")
    cmd.addParam("jobName", "任务名")
    cmd.addParam("schemaName", "schema")
    cmd.addParam("tableName", "表名称")
    cmd.addParam("date", "日期")
    cmd.parse(args)
    if (args.length == 0) {
      cmd.printHelp
    }

    val inputPath = cmd.getArgValue("inputPath")
    val outputPath = cmd.getArgValue("outputPath")
    val jobName = cmd.getArgValue("jobName")
    val schemaName = cmd.getArgValue("schemaName")
    val tableName = cmd.getArgValue("tableName")
    val date = cmd.getArgValue("date")

    //    val conf =new SparkConf().setMaster("local[*]").setAppName(jobName)
    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    val rawRDD = sc.textFile(inputPath, 2)
    //    rawRDD.foreach(println)
    val unionRdd = ServerLogMapObject.logparser(rawRDD, date)

    import sqlContext.implicits._
    //toDF()隐式转换 注意导入时确认之前已经导入了类似org.apache.spark.sql.SQLContext
    val logDF = unionRdd.toDF()
    //    logDF.foreach(println)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    logDF.repartition(new Column("dt"), new Column("source"), new Column("domain"))
      .write.mode(SaveMode.Overwrite)
      .partitionBy("dt", "source", "domain").insertInto(schemaName + "." + tableName)
  }
}

package cn.wanda.spark.etl.ffan_event_point_etl.main

/**
  * Created by weishuxiao on 16/5/19.
  */

import cn.wanda.spark.etl.ffan_event_point_etl.filter.FilterObject
import cn.wanda.spark.etl.ffan_event_point_etl.map.EventLogMapObject
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, Column, Row}
import utils.{HdfsOperator, InputKeys, Cmd}
import scala.collection.JavaConversions._

object EventLogMain {

  def main(args: Array[String]) {

    //读取输入参数
    val cmd: Cmd = new Cmd("")
    cmd.addParam("inputPath", "输入目录为")
    cmd.addParam("outputPath", "输出目录为")
    cmd.addParam("jobName", "任务名")
    cmd.addParam("schemaName", "schema")
    cmd.addParam("tableName", "表名称")
    cmd.addParam("keys", "解析字段")
    cmd.addParam("className", "解析类名")
    cmd.addParam("date", "日期")
    cmd.parse(args)
    if (args.length == 0) {
      cmd.printHelp
    }

    val inputPath  = cmd.getArgValue("inputPath")
    val outputPath = cmd.getArgValue("outputPath")
    val jobName    = cmd.getArgValue("jobName")
    val schemaName = cmd.getArgValue("schemaName")
    val tableName  = cmd.getArgValue("tableName")
    val keys       = cmd.getArgValue("keys")
    val className  = cmd.getArgValue("className")
    val date       = cmd.getArgValue("date")

    val inputKeys = new InputKeys()
    inputKeys.keysParser(keys)
    val keysString = inputKeys.getKeysString+",dt" //对解析字段字符串拼接上分区字段

    println("待解析的字段为: "+inputKeys.getKeysString)

//    val conf =new SparkConf().setMaster("local[*]").setAppName(jobName)
    val conf =new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val keysNum=keysString.trim.split(",").length
    val rawRDD = sc.textFile(inputPath,2)
    val parsedRDD = EventLogMapObject.logparser(inputKeys,rawRDD,className)

//    parsedRDD.foreach(println)

    //根据输入字段顺序指定schema 自动生成dafaframe
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    val schema = StructType(keysString.trim.split(",").map(fieldName => StructField(fieldName, StringType, true)))

//    println(schema)
    val rowRDD = parsedRDD.map(_+"\t"+date).filter(line=>FilterObject.wordFilter(line,keysNum)).map(_.split("\t",-1)).map(p =>Row(p:_*))
    val logDF = sqlContext.createDataFrame(rowRDD, schema)

//    logDF.registerTempTable("logDF")  //注册该表

    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    logDF.write.mode(SaveMode.Overwrite).partitionBy("dt").insertInto(schemaName+"."+tableName)

    //服务器日志数量写入到HDFS
    val hostCount = rawRDD.filter(line=>FilterObject.jsonFilter(line)).map(line=>(JSON.parseObject(line).getString("host"),1))
      .reduceByKey((a,b)=>a+b).map(line=>date.replaceAll("-","")+","+jobName+","+line._1+","+line._2)
    val hostCountStr=hostCount.collect().mkString("\n")
    val hadoopConf = new Configuration
    val ho = new HdfsOperator
    val path = "/user/sre/log_monitor/"+date.replaceAll("-","")+"/"+jobName+"_"+date+".log"
//    val path = "source/output/log_monitor/"+date.replaceAll("-","")+"/"+date+"_"+jobName+".log"
    ho.dirClear(hadoopConf,path)
    ho.write2HDFS(hadoopConf,path,hostCountStr)

    sc.stop()
  }
}
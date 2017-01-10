package cn.wanda.spark_streaming.main

/**
  * Created by weishuxiao on 17/1/4.
  */

import cn.wanda.ETLFactory.ParserTrait
import cn.wanda.ETLFactory.ParserTrait
import cn.wanda.target.MysqlConnection
import cn.wanda.target.MysqlConnectionPool
import cn.wanda.target.{MysqlConnection, MysqlConnectionPool}
import cn.wanda.util.FilterObject
import cn.wanda.util.HdfsUtil
import cn.wanda.util.JsonKeys
import cn.wanda.util.JvmIdGetter
import cn.wanda.util.KafkaManger
import cn.wanda.util.OffsetManager
import cn.wanda.util.PropertiesUtil
import cn.wanda.util.RddFunctions
import cn.wanda.util.ReflectUtil
import cn.wanda.util._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkConf, TaskContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

/**
  * Created by weishuxiao on 16/8/25.
  */
object streamingTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val propObject = PropertiesUtil.getPropObject("/Users/weishuxiao/Documents/spark/ffan_spark_buss/src/main/resources/Spark_streaming_double_dan_liuliang_stat.properties")
    val appName = propObject.getProperty("spark.streaming.appName")
    val zkConnect = propObject.getProperty("spark.streaming.zkConnect")
    val kafkaRoot = propObject.getProperty("spark.streaming.kafkaRoot")
    val topics    = propObject.getProperty("spark.streaming.topics")
    val className = propObject.getProperty("spark.streaming.parser.className")
    val checkpointPath = propObject.getProperty("spark.streaming.checkpointPath")
    val jdbcUrl = propObject.getProperty("spark.target.jdbc")
    val jdbcSql = propObject.getProperty("spark.target.jdbc.query")
    val keys      = propObject.getProperty("parser.keys")
    val appDataPath      = propObject.getProperty("spark.streaming.appDataPath")
    val date      = propObject.getProperty("load.date")

    val parserClass = ReflectUtil.getSingletonObject[ParserTrait](className+"$")


    val jsonKeys =JsonKeys(keys)
    val keysString = jsonKeys.getKeyString
    println("待解析的字段为: "+keysString)

    val keysNum = keysString.trim.split(",").length
    val schema = StructType(keysString.trim.split(",").map(fieldName => StructField(fieldName, StringType, true))) //埋点解析字段schema

    val brokerList = KafkaManger.getBrokerList(zkConnect, kafkaRoot)
    println("current brokerList:"+brokerList)


    val topicsSet = topics.split(",").toSet
    println(topicsSet.toArray.mkString("-"))

    val upstateDir = s"$appDataPath/data"
    val upstateDir_bak = s"$appDataPath/data_bak"
    val offsetCheckDir = s"$appDataPath/offset/offset.txt"
    val initDir = s"$appDataPath/init"

    val offset =HdfsUtil.readHdfs(offsetCheckDir,new Configuration())
    val offsetInfo = OffsetManager.getOffsetInfo(offset)
    //    println(s"zookeeper offset:$offset")
    val startOffset = OffsetManager.getStartOffset(brokerList,topics,offsetInfo)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset, mmd.message) //messageHandler 为获取到的日志处理函数

    HdfsUtil.rmDir(s"$appDataPath/checkpoint",new Configuration())
    HdfsUtil.copy(upstateDir,initDir,false,true,new Configuration())

    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    val interval=Seconds(2)
    val ssc = new StreamingContext(conf,interval) //spark streaming context
    val kafkaStream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long, String)](ssc, kafkaParams, startOffset, messageHandler)
    val kafkaStream2 = kafkaStream1.map(line=>line._3).map(line=>line.replace("\\t","\\\\t")).filter(line=>FilterObject.jsonFilter(line))
    val kafkaStream3 = kafkaStream2.transform{rdd=>parserClass.logparser(rdd,jsonKeys)}

    val df= kafkaStream3.map(_.split("\t")).transform{
      rdd=>
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val rowRDD = rdd.map(p =>Row(p:_*))
        val logDF = sqlContext.createDataFrame(rowRDD, schema)
        logDF.registerTempTable("event_log")
        val aa= sqlContext.sql("select os_version,event_id,count(*) pv,count(distinct device_id) uv from event_log group by os_version,event_id with rollup")
//        logDF.rollup("os_version","event_id").agg(Map("event_time"->"count")).show(1000)
//        import org.apache.spark.sql.functions._
//        logDF.rollup("os_version","event_id").agg(count("device_id"),countDistinct("device_id")).show(1000)
//
////        sqlContext.sql("select ")
//        println(logDF.select("device_id").distinct().count())
//        logDF.select("event_time","event_id").show()
        aa.show(1000)
        aa.rdd
    }
//    df.transform{
//      rdd=>rdd.
//    }.print()
    df.count().print()

//    val directory = s"$initDir/data"
//    val pathFilter=(path: Path)=>true
//    val checkDstream = ssc.fileStream[LongWritable, Text, TextInputFormat](directory,pathFilter(_),false)
//      .map(_._2.toString)



    ssc.start()
    ssc.awaitTermination()
  }

}
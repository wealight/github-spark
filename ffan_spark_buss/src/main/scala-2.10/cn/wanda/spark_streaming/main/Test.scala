package cn.wanda.spark_streaming.main

import cn.wanda.ETLFactory.ParserTrait
import cn.wanda.target.{MysqlConnection, MysqlConnectionPool}
import cn.wanda.util._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import utils.kafkaUtils.KafkaZookeeperOffsetManager
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import utils._
import scala.collection.JavaConversions._
import org.apache.log4j.{Level, Logger}

/**
  * Created by weishuxiao on 16/8/25.
  */
object Test {
  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    val propBean =new PropertiesBean("/Users/weishuxiao/Documents/spark/ffan_spark_buss/src/main/resources/Spark_streaming_double_dan_liuliang_stat.properties")
    val jsonKeys =JsonKeys(propBean.keys)
    val keysString = jsonKeys.getKeyString
    println("待解析的字段为: "+keysString)

    val schema =JsonKeys.getStructType(keysString,",")

    val brokerList = KafkaManger.getBrokerList(propBean.zkConnect, propBean.kafkaRoot)
    println("current brokerList:"+brokerList)

    val topicsSet = propBean.topics.split(",").toSet
    println(topicsSet.toArray.mkString("-"))

    val upstateDir     = s"${propBean.appDataPath}/data"
    val upstateDir_bak = s"${propBean.appDataPath}/data_bak"
    val offsetCheckDir = s"${propBean.appDataPath}/offset/offset.txt"
    val initDir        = s"${propBean.appDataPath}/init"


    val offsetInfo = OffsetManager.getFromOffset(HdfsUtil.readHdfs(offsetCheckDir,new Configuration()))
    //    println(s"zookeeper offset:$offset")
    val startOffset    = OffsetManager.getStartOffset(brokerList,propBean.topics,offsetInfo)
    val kafkaParams    = Map[String, String]("metadata.broker.list" -> brokerList)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset, mmd.message) //messageHandler 为获取到的日志处理函数
    val parserClass    = ReflectUtil.getSingletonObject[ParserTrait](propBean.className+"$")

    HdfsUtil.rmDir(s"${propBean.appDataPath}/checkpoint",new Configuration())
    HdfsUtil.copy(upstateDir,initDir,false,true,new Configuration())

    val conf = new SparkConf().setMaster("local[*]").setAppName(propBean.appName)
    conf.set("spark.streaming.fileStream.minRememberDuration", "25920000s")
    val interval=Seconds(5)
    val ssc = new StreamingContext(conf,interval) //spark streaming context
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long, String)](ssc, kafkaParams, startOffset, messageHandler)


    //读取存档文件  作为初始化数据
    val checkDstream = ssc.fileStream[LongWritable, Text, TextInputFormat](s"$initDir/data",RddFunctions.pathFilter(_),false)
      .map(_._2.toString)
      .filter(line=>line.length>10)
      .map{
        line=>
          val lineArr = line.split("\\t")
          (lineArr(0),lineArr(1))
      }
    ssc.start()
    ssc.awaitTermination()
  }

}
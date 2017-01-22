package cn.wanda.spark_streaming.main

import java.util.Properties

import cn.wanda.ETLFactory.ParserTrait
import cn.wanda.target.{MysqlConnection, MysqlConnectionPool}
import cn.wanda.util._
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf, TaskContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

/**
  * Created by weishuxiao on 16/8/25.
  */
object LiuliangStatMain {
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
    val eleDstream = directKafkaStream
      .map(line=>line._3)
      .map(line=>line.replace("\\t","\\\\t"))
      .filter(line=>FilterObject.jsonFilter(line))
      .transform{rdd=>parserClass.logparser(rdd,jsonKeys)}

    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)

    val rowRDD = eleDstream.transform{
      rdd =>
        val rowRDD = rdd.map(_.split("\t",-1)).map(p =>Row(p:_*))
        val logDF = sqlContext.createDataFrame(rowRDD, schema)
        logDF.registerTempTable(propBean.precessSourceTable)
        val statDf = sqlContext.sql(propBean.precessSql)
        statDf.rdd
    }
    rowRDD.print()

    rowRDD.foreachRDD{
      rdd=>
        val logDF = sqlContext.createDataFrame(rdd, schema)
        logDF.registerTempTable(propBean.batchComputeSourceTable)
        val statDf = sqlContext.sql(propBean.batchComputeSql)
        statDf.show()
        statDf.coalesce(2).foreachPartition{
          partition=>{
            if (!partition.isEmpty){
              JvmIdGetter.IdGetter()
              val connectionPool =MysqlConnectionPool.pool
              val connection = connectionPool.filter(conn=>conn!= None).get.getConnection
              MysqlConnection.insertIntoMySQL(connection,propBean.jdbcSql,partition)
              MysqlConnection.closeConnection(connection)
            }
          }
        }
    }

    //读取存档文件  作为初始化数据
    val checkDstream = ssc.fileStream[LongWritable, Text, TextInputFormat](s"$initDir/data",RddFunctions.pathFilter(_),false)
      .map(_._2.toString)
      .filter(line=>line.length>10)
      .map{
        line=>
          val lineArr = line.split("\\t")
          (lineArr(0),lineArr(1))
      }

    val holdedRdd =
      {
        rowRDD.transform
        {
          rdd=>
            val logDF = sqlContext.createDataFrame(rdd, schema)
            logDF.registerTempTable(propBean.preUpdateKeyStateTable)
            val statDf = sqlContext.sql(propBean.preUpdateKeyStateSql)
            statDf.rdd
        }
          .map(line=>(line(0).toString,line(1).toString))
      }
        .union(checkDstream)
        .window(interval*3,interval*3)
        .updateStateByKey[String](RddFunctions.stateSumbykey)

    ssc.checkpoint(s"${propBean.appDataPath}/checkpoint")
    holdedRdd.checkpoint(interval*9)


    val holdedDataSchema =JsonKeys.getStructType(propBean.updateStateComputeFields,",")
    holdedRdd.map(ele=>ele._1+"\t"+ele._2).window(interval*3,interval*3)
      .foreachRDD{
        rdd=>
          //将历史存档状态保存到HDFS
          val logger = Logger.getLogger(LiuliangStatMain.getClass.getName)
          logger.info("updateStateByKey保存的历史数据写入到HDFS")
          HdfsUtil.rmDir(s"$upstateDir",new Configuration())
          rdd.saveAsTextFile(upstateDir)

          //对历史累计数据进行统计
          val rddTmp = rdd.map(_.split("\\t|\\|")).map(p =>Row(p:_*))
          val logDF = sqlContext.createDataFrame(rddTmp, holdedDataSchema)
          logDF.registerTempTable(propBean.updateStateComputeTable)
          val statDf = sqlContext.sql(propBean.updateStateComputeSql)
          statDf.show()
      }


    // OffsetRange当中主要字段包含 topic,partition,fromOffset,untilOffset
    //参考http://www.tuicool.com/articles/vaUzquJ
    var offsetRanges = Array[OffsetRange]()
    directKafkaStream.foreachRDD {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(println(_))
        val offsetStr = OffsetManager.offsetRanges2String(offsetRanges)
        HdfsUtil.write2Hdfs(offsetCheckDir,offsetStr, new Configuration())
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
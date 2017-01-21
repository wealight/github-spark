//package cn.wanda.spark_streaming.main
//
//import java.util.Properties
//
//import cn.wanda.ETLFactory.ParserTrait
//import cn.wanda.target.{MysqlConnection, MysqlConnectionPool}
//import cn.wanda.util._
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.{Text, LongWritable}
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkContext, SparkConf, TaskContext}
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import scala.collection.JavaConversions._
//
///**
//  * Created by weishuxiao on 16/8/25.
//  */
//object LiuliangStatMainTest {
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val propObject = PropertiesUtil.getPropObject("/Users/weishuxiao/Documents/spark/ffan_spark_buss/src/main/resources/Spark_streaming_double_dan_liuliang_stat.properties")
//    val appName = propObject.getProperty("spark.streaming.appName")
//    val zkConnect = propObject.getProperty("spark.streaming.zkConnect")
//    val kafkaRoot = propObject.getProperty("spark.streaming.kafkaRoot")
//    val topics    = propObject.getProperty("spark.streaming.topics")
//    val className = propObject.getProperty("spark.streaming.parser.className")
//    val checkpointPath = propObject.getProperty("spark.streaming.checkpointPath")
//    val jdbcUrl = propObject.getProperty("spark.target.jdbc")
//    val jdbcSql = propObject.getProperty("spark.target.jdbc.query")
//    val keys      = propObject.getProperty("parser.keys")
//    val appDataPath      = propObject.getProperty("spark.streaming.appDataPath")
//    val date      = propObject.getProperty("load.date")
//    val dimensions     = propObject.getProperty("spark.streaming.dimensions")
//    val measures       = propObject.getProperty("spark.streaming.measures")
//
//    val jsonKeys =JsonKeys(keys)
//    val keysString = jsonKeys.getKeyString
//    println("待解析的字段为: "+keysString)
//
//    val keysNum = keysString.trim.split(",").length
//    val schema = StructType(keysString.trim.split(",").map(fieldName => StructField(fieldName, StringType, true))) //埋点解析字段schema
//
//    val brokerList = KafkaManger.getBrokerList(zkConnect, kafkaRoot)
//    println("current brokerList:"+brokerList)
//
//    val topicsSet = topics.split(",").toSet
//    println(topicsSet.toArray.mkString("-"))
//
//    val upstateDir = s"$appDataPath/data"
//    val upstateDir_bak = s"$appDataPath/data_bak"
//    val offsetCheckDir = s"$appDataPath/offset/offset.txt"
//    val initDir = s"$appDataPath/init"
//
//    val offset =HdfsUtil.readHdfs(offsetCheckDir,new Configuration())
//    val offsetInfo = OffsetManager.getFromOffset(offset)
//    //    println(s"zookeeper offset:$offset")
//    val startOffset = OffsetManager.getStartOffset(brokerList,topics,offsetInfo)
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
//    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset, mmd.message) //messageHandler 为获取到的日志处理函数
//    val parserClass = ReflectUtil.getSingletonObject[ParserTrait](className+"$")
//    //    val rddFunctions = new RddFunctions
//
//    HdfsUtil.rmDir(s"$appDataPath/checkpoint",new Configuration())
//    HdfsUtil.copy(upstateDir,initDir,false,true,new Configuration())
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
//    conf.set("spark.streaming.fileStream.minRememberDuration", "25920000s")
//    val interval=Seconds(5)
//    val ssc = new StreamingContext(conf,interval) //spark streaming context
//    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long, String)](ssc, kafkaParams, startOffset, messageHandler)
//    val jsonStream = directKafkaStream.map(line=>line._3).map(line=>line.replace("\\t","\\\\t")).filter(line=>FilterObject.jsonFilter(line))
//    val eleDstream = jsonStream.transform{rdd=>parserClass.logparser(rdd,jsonKeys)}
//    val directory = s"$initDir/data"
//    val reg="""(.*),(.*)""".r //用于将tuple形式的字符串解析
//    val pathFilter=(path: Path)=>true
//    val checkDstream = ssc.fileStream[LongWritable, Text, TextInputFormat](directory,pathFilter(_),false)
//      .map(_._2.toString)
//      .filter(line=>line.length>10)
//      .map{
//        line=>
//          val reg(logwords,pv)=line.replaceAll("\\(|\\)","")
//          (logwords,pv.toInt)
//      }
//
//    val rowRDD = eleDstream.transform{
//        rdd =>
//          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
//          val rowRDD = rdd.map(_.split("\t",-1)).map(p =>Row(p:_*))
//          val logDF = sqlContext.createDataFrame(rowRDD, schema)
//          logDF.registerTempTable("event_log1")
//          val statDf = sqlContext.sql(s"""select ${dimensions.replaceAll("\\|",",")},$measures from event_log1""")
//          statDf.show()
//          statDf.rdd
//      }
//
//    val dimensions2=dimensions.replaceAll("[^\\|]+\\s", "").replaceAll("\\|",",")
//    println(s"$dimensions      $dimensions2")
//    rowRDD.foreachRDD{
//      rdd=>
//        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
//        val logDF = sqlContext.createDataFrame(rdd, schema)
//          logDF.registerTempTable("event_log")
//          val statDf = sqlContext.sql(s"select $dimensions2,count(*)pv,count(distinct device_id) uv from event_log group by $dimensions2")
//          statDf.show()
//          statDf.coalesce(2).foreachPartition{
//            partition=>{
//              if (!partition.isEmpty){
//                JvmIdGetter.IdGetter()
//                val connectionPool =MysqlConnectionPool.pool
//                val connection = connectionPool.filter(conn=>conn!= None).get.getConnection
//                MysqlConnection.insertIntoMySQL(connection,jdbcSql,partition)
//                MysqlConnection.closeConnection(connection)
//              }
//            }
//          }
//      }
//
//    val holdedRdd = rowRDD.map(line=>(line.mkString("(",",",")"),1)).union(checkDstream).window(interval,interval).reduceByKey(_+_).updateStateByKey[Int](RddFunctions.stateSumbykey)
//    holdedRdd.foreachRDD{
//      rdd=>
//        HdfsUtil.mkDir(s"$upstateDir_bak",new Configuration())
//        rdd.foreachPartition{
//          partition=>{
//            val pathString = s"$upstateDir_bak/spark-streaming-$appName-${TaskContext.getPartitionId()}"
//            HdfsUtil.write2Hdfs(pathString,partition.mkString("","\n","\n"),new Configuration())
//          }
//        }
//        HdfsUtil.rmDir(s"$upstateDir",new Configuration())
//        HdfsUtil.mvDir(s"$upstateDir_bak",s"$upstateDir",true,new Configuration())
//    }
//
//    ssc.checkpoint(s"$appDataPath/checkpoint")
//
//    // OffsetRange当中主要字段包含 topic,partition,fromOffset,untilOffset
//    //参考http://www.tuicool.com/articles/vaUzquJ
//    var offsetRanges = Array[OffsetRange]()
//    directKafkaStream.foreachRDD { rdd =>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      offsetRanges.foreach(println(_))
//      val offsetStr = OffsetManager.offsetRanges2String(offsetRanges)
//      HdfsUtil.write2Hdfs(offsetCheckDir,offsetStr, new Configuration())
//    }
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
package cn.wanda.spark_streaming.etl.camus.main

import java.text.SimpleDateFormat
import java.util.Date

import cn.wanda.spark.etl.ffan_event_point_etl.filter.FilterObject
import cn.wanda.spark.etl.ffan_event_point_etl.map.EventLogMapObject
import com.alibaba.fastjson.JSON
import factory.SimpleLogParse
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafkaUnils.KafkaZookeeperOffsetManager
import org.apache.hadoop.yarn.factories.RecordFactory
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

    //读取输入参数
    val cmd: Cmd = new Cmd("")
    cmd.addParam("zkConnect", "zookeeper地址")
    cmd.addParam("kafkaRoot", "kafka在zookeeper当中的根目录")
    cmd.addParam("topics", "kafka topic")
    cmd.addParam("className", "解析类名")
    cmd.addParam("keys", "字段")
    cmd.addParam("checkpointPath", "check point路径")
    cmd.addParam("date", "日期")
    cmd.parse(args)
    if (args.length == 0) {
      cmd.printHelp
    }

    val zkConnect = cmd.getArgValue("zkConnect")
    val kafkaRoot = cmd.getArgValue("kafkaRoot")
    val topics    = cmd.getArgValue("topics")
    val className = cmd.getArgValue("className")
    val checkpointPath = cmd.getArgValue("checkpointPath")
    val keys      = cmd.getArgValue("keys")
    val date      = cmd.getArgValue("date")

    val inputKeys = new InputKeys()
    inputKeys.keysParser(keys)
    val keysString = inputKeys.getKeysString
    println("待解析的字段为: "+keysString)

    val keysNum = keysString.trim.split(",").length
    val schema = StructType(keysString.trim.split(",").map(fieldName => StructField(fieldName, StringType, true))) //埋点解析字段schema

    val brokerCollector = new BrokerCollector(zkConnect, kafkaRoot)
    val brokerList = brokerCollector.getBrokerList()
    println("current brokerList:"+brokerList)

    val topicsSet = topics.split(",").toSet
    println(topicsSet.toArray.mkString("-"))
    val zookeeperManager = new ZookeeperManager(zkConnect, "/spark-streaming-test", topicsSet.toArray.mkString("-"))
    val offsetInfo = zookeeperManager.getChildZnodeInfo() //从zookeeper获取记录在znode上的kafka位移信息
    val topicAndPartitionAndOffset = KafkaZookeeperOffsetManager.getTopicAndPartition(offsetInfo) //解析kafka各topic partitions的位移信息

    println(topicAndPartitionAndOffset)
    val startOffsetJava = OffsetManager.getStartOffset(brokerList,topics,topicAndPartitionAndOffset) //本次执行开始的位移 返回为java Map
    val startOffset = (startOffsetJava.keys).zip(startOffsetJava.values().map(Long2long)).toMap //转为scala Map类型 注意对map当中的类型也进行了转换
    println(startOffset)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset, mmd.message) //messageHandler 为获取到的日志处理函数

    def createContext() ={
      val conf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming-"+topics)
      val interval=Seconds(5)
      val ssc = new StreamingContext(conf,interval) //spark streaming context
      val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long, String)](ssc, kafkaParams, startOffset, messageHandler)
      //    val recordFactory = if (outputDataClassFactory != null) Class.forName(outputDataClassFactory).newInstance.asInstanceOf[RecordFactory] else null
      //    directKafkaStream.print()
      val jsonStream = directKafkaStream.map(line=>line._3).filter(line=>FilterObject.jsonFilter(line))
      val eleDstream = jsonStream.transform{rdd=>EventLogMapObject.logparser(inputKeys,rdd,className)}.filter(line=>FilterObject.wordFilter(line,keysNum)).persist()

      val minutesDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm") //格式化时间实例

      val stateDstream = eleDstream.map(line=>line.split("\t")).map(
        line=>
        {
          //将日志当中的时间戳字符串转变为分钟时间格式yyyy-MM-dd HH:mm
          val minutesDate = try {
            val stamp = line(3).toLong
            minutesDateFormat.format(new Date(stamp))
          }catch {
            case ex:Exception =>minutesDateFormat.format(new Date())//如果转换异常则将时间标记为当前程序时间
          }
          ((line(0),minutesDate,line(2)),1)
        }
      )

      stateDstream.reduceByKey(_+_).print()
      val stateFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
        val currentCount = currValues.sum //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
        val previousCount = prevValueState.getOrElse(0) // 已累加的值
        Some(currentCount + previousCount) // 返回累加后的结果，是一个Option[Int]类型
      }

      val holdedRdd = stateDstream.updateStateByKey[Int](stateFunc)
      ssc.checkpoint(checkpointPath)
      holdedRdd.checkpoint(Seconds(10))

      holdedRdd.print()

      //    eleDstream.print()
      eleDstream.foreachRDD{
        rdd =>
          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
          import sqlContext.implicits._
          val rowRDD = rdd.map(_.split("\t",-1)).map(p =>Row(p:_*))
          val logDF = sqlContext.createDataFrame(rowRDD, schema)
          logDF.registerTempTable("words")
          val wordCountsDataFrame = sqlContext.sql("select event_id,count(*),count(distinct device_id) from words group by event_id")
          wordCountsDataFrame.show()
      }

      // OffsetRange当中主要字段包含 topic,partition,fromOffset,untilOffset
      //参考http://www.tuicool.com/articles/vaUzquJ
      var offsetRanges = Array[OffsetRange]()
      directKafkaStream.transform {
        rdd => offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
      }.foreachRDD { rdd => OffsetManager.offsetRangeUpdate(zookeeperManager,offsetRanges) }
      ssc
    }

    val ssc=StreamingContext.getOrCreate(checkpointPath,createContext)
    ssc.start()
    ssc.awaitTermination()
  }

}
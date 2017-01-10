//package cn.wanda.spark_streaming.main
//
//import cn.wanda.ETLFactory.ParserTrait
//import cn.wanda.target.{MysqlConnection, MysqlConnectionPool}
//import cn.wanda.util._
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import utils.kafkaUtils.KafkaZookeeperOffsetManager
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}
//import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
//import utils._
//import scala.collection.JavaConversions._
//import org.apache.log4j.{Level, Logger}
//
///**
//  * Created by weishuxiao on 16/8/25.
//  */
//object Test {
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    //读取输入参数
//    val cmd: Cmd = new Cmd("")
//    cmd.addParam("zkConnect", "zookeeper地址")
//    cmd.addParam("kafkaRoot", "kafka在zookeeper当中的根目录")
//    cmd.addParam("topics", "kafka topic")
//    cmd.addParam("className", "解析类名")
//    cmd.addParam("keys", "字段")
//    cmd.addParam("checkpointPath", "check point路径")
//    cmd.addParam("date", "日期")
//    cmd.parse(args)
//    if (args.length == 0) {
//      cmd.printHelp
//    }
//
//    val zkConnect = cmd.getArgValue("zkConnect")
//    val kafkaRoot = cmd.getArgValue("kafkaRoot")
//    val topics    = cmd.getArgValue("topics")
//    val className = cmd.getArgValue("className")
//    val checkpointPath = cmd.getArgValue("checkpointPath")
//    val keys      = cmd.getArgValue("keys")
//    val date      = cmd.getArgValue("date")
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
//    val zookeeperManager = new ZookeeperManager(zkConnect, "/spark-streaming-test/"+topicsSet.toArray.mkString("-"))
//    val offsetInfo = zookeeperManager.getChildZnodeInfo() //从zookeeper获取记录在znode上的kafka位移信息
//    val topicAndPartitionAndOffset = KafkaZookeeperOffsetManager.getTopicAndPartition(offsetInfo) //解析kafka各topic partitions的位移信息
//
//    println(topicAndPartitionAndOffset)
//    val startOffsetJava = OffsetManager.getStartOffset(brokerList,topics,topicAndPartitionAndOffset) //本次执行开始的位移 返回为java Map
//    val startOffset = (startOffsetJava.keys).zip(startOffsetJava.values().map(Long2long)).toMap //转为scala Map类型 注意对map当中的类型也进行了转换
//
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
//    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.offset, mmd.message) //messageHandler 为获取到的日志处理函数
//    val parserClass = ReflectUtil.getSingletonObject[ParserTrait](className+"$")
//    val rddFunctions = new RddFunctions
//
//    val jdbclUrl="jdbc:mysql://localhost:3306/mysqltest?useUnicode=true&characterEncoding=utf-8&useSSL=false"
//    val jdbcSql="REPLACE INTO sparkTest(op_date,event_id,pv,uv) VALUES (?,?,?,?)"
////    lazy val connectionPool = MysqlConnectionPool.createPool(jdbclUrl, "root", "881234")
//    def createContext() ={
//      val conf = new SparkConf().setMaster("local[*]").setAppName("spark-streaming-"+topics)
//      val interval=Seconds(5)
//      val ssc = new StreamingContext(conf,interval) //spark streaming context
//      val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Long, String)](ssc, kafkaParams, startOffset, messageHandler)
////          directKafkaStream.print()
//      val jsonStream = directKafkaStream.map(line=>line._3).map(line=>line.replace("\\t","\\\\t")).filter(line=>FilterObject.jsonFilter(line))
//      val eleDstream = jsonStream.transform{rdd=>parserClass.logparser(rdd,jsonKeys)}.filter(line=>FilterObject.wordFilter(line,keysNum)).persist()
//      val stateDstream = eleDstream.map(line=>line.split("\t")).map(line=> ((line(0),rddFunctions.unixtime2date(line(3),"yyyy-MM-dd HH:mm"),line(2)),1))
//      stateDstream.reduceByKey(_+_).print()
//
//      val holdedRdd = stateDstream.updateStateByKey[Int](rddFunctions.stateSumbykey)
//      ssc.checkpoint(checkpointPath)
////      holdedRdd.checkpoint(Seconds(10))
//
//      holdedRdd.count().print()
//
//      var flag=false
//      eleDstream.window(Seconds(10),Seconds(10)).foreachRDD{
//        rdd =>
//          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
//          val rowRDD = rdd.map(_.split("\t",-1)).map(p =>Row(p:_*))
//          val logDF = sqlContext.createDataFrame(rowRDD, schema)
//          logDF.registerTempTable("event_log")
//          val statDf = sqlContext.sql("select from_unixtime(receive_time/1000,\"yyyy-MM-dd HH:mm\"),event_id,count(*)pv,count(distinct device_id) uv from event_log group by event_id,from_unixtime(receive_time/1000,\"yyyy-MM-dd HH:mm\")")
////          val statRDD = statDf.map(line=>line(1)) //df转为rdd
//          statDf.show()
//          statDf.repartition(2).foreachPartition{
//            partition=>{
//              if (!partition.isEmpty){
//                println(partition.mkString(","))
//                JvmIdGetter.IdGetter()
//                val connectionPool =MysqlConnectionPool.pool
////                MysqlConnection.insertIntoMySQL( MysqlConnection(jdbclUrl, "root", "881234").getConnection,jdbcSql,partition)
//                val connection = connectionPool.filter(a=>a!= None).get.getConnection
//                MysqlConnection.insertIntoMySQL(connection,jdbcSql,partition)
//                MysqlConnection.closeConnection(connection)
//              }
//            }
//          }
//      }
//
//      // OffsetRange当中主要字段包含 topic,partition,fromOffset,untilOffset
//      //参考http://www.tuicool.com/articles/vaUzquJ
//      var offsetRanges = Array[OffsetRange]()
//      directKafkaStream.transform {
//        rdd => offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//          rdd
//      }.foreachRDD { rdd => OffsetManager.offsetRangeUpdate(zookeeperManager,offsetRanges) }
//      ssc
//    }
//
//    val ssc=StreamingContext.getOrCreate(checkpointPath,createContext)
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//}
package cn.wanda.util

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import kafka.common.TopicAndPartition
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.OffsetRange
import utils.kafkaUtils.KafkaZookeeperOffsetManager

import scala.collection.JavaConversions._


/**
  * Created by weishuxiao on 16/9/2.
  */
object OffsetManager {
  val logger = Logger.getLogger("HdfsUtil")
  /**
    * 更新位移信息至zookeeper
    * */
  def offsetRangeUpdate(zookeeperManager: ZookeeperManager, offsetRangesArr: Array[OffsetRange]) = {
    val offsetRanges = offsetRanges2String(offsetRangesArr)
    zookeeperManager.updateChildZnodeInfo(offsetRanges)
  }

  /**
    * 将OffsetRange对象数组相关字段进行提取,并生成json格式的字符串
 *
    * @param offsetRanges OffsetRange对象数组
    *
    * */
  def offsetRanges2String(offsetRanges: Array[OffsetRange]) = {
    val offsetJsonArr = offsetRange2Json(offsetRanges)
    offsetJsonArr.toJSONString
  }

  /**
    * 将OffsetRange当中的相关字段提取,生成json对象,便于后续处理
    *
    * @return 为JSONArray 对象
    * @param offsetRanges OffsetRange对象数组
    * */
  def offsetRange2Json(offsetRanges: Array[OffsetRange]) = {
    val jsonArray: JSONArray = new JSONArray
    for (offsetRange <- offsetRanges) {
      val jsonObject=new JSONObject()
      jsonObject.put("date", (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date()))
      jsonObject.put("timestamp", System.currentTimeMillis)
      jsonObject.put("topic",offsetRange.topic)
      jsonObject.put("partitions", offsetRange.partition)
      jsonObject.put("fromOffset", offsetRange.fromOffset)
      jsonObject.put("untilOffset", offsetRange.untilOffset)
      jsonArray.add(jsonObject)
    }
    jsonArray
  }



  def offsetScala2Java(offset:scala.collection.mutable.Map[TopicAndPartition,Long]) ={
    import collection.JavaConverters._
    (offset.keys).zip(offset.values.map(value=>long2Long(value))).toMap.asJava
  }

  def offsetJava2Scala(offset:java.util.Map[TopicAndPartition,java.lang.Long]) ={
    (offset.keys).zip(offset.values().map(value=>Long2long(value))).toMap
  }

  /**
    * 获取本次执行开始的offset
    * */
  def getStartOffset(brokerList: String,topics: String, offset:Option[scala.collection.mutable.Map[TopicAndPartition,Long]])={
    val topicList=topics.split(",").toList
    val latestOffset = KafkaZookeeperOffsetManager.getOffsetInfo(brokerList, topicList, -1l)
    val earliestOffset = KafkaZookeeperOffsetManager.getOffsetInfo(brokerList, topicList, -2l)
    println("show offset->" + latestOffset) //show offset->{[test,0]=0}

    offset match {
      case Some(offsetMap)=>{
        if (KafkaZookeeperOffsetManager.isOutOfRange(offsetScala2Java(offset.get), earliestOffset)) {
          println("WARNING! Kafka out of range. Turn it to earliest offset")
          collection.immutable.Map(offsetJava2Scala(earliestOffset).toSeq: _*)
        } else {
          collection.immutable.Map(offset.get.toSeq: _*)
        }
      }
      case None=>{
        println("WARNING! Init topic. Turn it to latest offset")
        collection.immutable.Map(offsetJava2Scala(latestOffset).toSeq: _*)
      }
    }
  }

  def getFromOffset(offset:String)={
    val offsetMap = scala.collection.mutable.Map[TopicAndPartition,Long]()
    try {
      val jSONArr = JSON.parseArray(offset)
      (0 until jSONArr.size()).foreach{index=>
        val jsonObject = jSONArr.getJSONObject(index)
        offsetMap(new TopicAndPartition(jsonObject.getString("topic"),jsonObject.getInteger("partitions")))= jsonObject.getLong("fromOffset")
      }
      Some(offsetMap)
    }catch {
      case ex:Exception=>{
        logger.error("offset 存档信息解析失败"+ex.printStackTrace())
        None
      }
    }
  }
}

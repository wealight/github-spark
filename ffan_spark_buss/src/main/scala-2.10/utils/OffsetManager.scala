package utils

import com.alibaba.fastjson.{JSONObject, JSONArray}
import kafka.common.TopicAndPartition
import kafkaUnils.KafkaZookeeperOffsetManager
import org.apache.spark.streaming.kafka.OffsetRange
import scala.collection.JavaConversions._


/**
  * Created by weishuxiao on 16/9/2.
  */
object OffsetManager {

  /**
    * 将OffsetRange当中的相关字段提取,生成json对象,便于后续处理
    * @return 为JSONArray 对象
    * @param offsetRanges OffsetRange对象数组
    * */
  def offsetRange2Json(offsetRanges: Array[OffsetRange]) = {
    val jsonArray: JSONArray = new JSONArray
    for (offsetRange <- offsetRanges) {
      val jsonObject=new JSONObject()
      jsonObject.put("topic",offsetRange.topic)
      jsonObject.put("partitions", offsetRange.partition)
      jsonObject.put("fromOffset", offsetRange.fromOffset)
      jsonObject.put("untilOffset", offsetRange.untilOffset)
      jsonObject.put("timestamp", System.currentTimeMillis)
      //        scala Map直接转json对象
      //        val offsetMap = Map(
      //          "topic" -> offsetRange.topic,
      //          "partitions" -> offsetRange.partition,
      //          "fromOffset" -> offsetRange.fromOffset,
      //          "untilOffset" -> offsetRange.untilOffset,
      //          "timestamp" -> System.currentTimeMillis
      //        )
      //        val offsetRangeJson = scala.util.parsing.json.JSONObject(offsetMap)

      jsonArray.add(jsonObject)
    }
    jsonArray
  }


  /**
    * 将OffsetRange对象数组相关字段进行提取,并生成json格式的字符串
    * @param offsetRanges OffsetRange对象数组
    *
    * */
  def offsetRanges2String(offsetRanges: Array[OffsetRange]) = {
    val offsetJsonArr = offsetRange2Json(offsetRanges)
    offsetJsonArr.toJSONString

  }

  /**
    * 更新位移信息至zookeeper
    * */
  def offsetRangeUpdate(zookeeperManager: ZookeeperManager, offsetRangesArr: Array[OffsetRange]) = {
    val offsetRanges = offsetRanges2String(offsetRangesArr)
      zookeeperManager.updateChildZnodeInfo(offsetRanges)
    }

  /**
    * 获取本次执行开始的offset
    * */
  def getStartOffset(brokerList: String,topics: String, topicAndPartitionAndOffset:java.util.Map[TopicAndPartition, java.lang.Long])={
    val topicList=topics.split(",").toList
    val latestOffset = KafkaZookeeperOffsetManager.getOffsetInfo(brokerList, topicList, -1l)
    val earliestOffset = KafkaZookeeperOffsetManager.getOffsetInfo(brokerList, topicList, -2l)

    println("show offset->" + latestOffset) //show offset->{[test,0]=0}


    if (topicAndPartitionAndOffset == null) {
      println("WARNING! Init topic. Turn it to latest offset")
      latestOffset
    } else {
      if (KafkaZookeeperOffsetManager.isOutOfRange(topicAndPartitionAndOffset, earliestOffset)) {
        println("WARNING! Kafka out of range. Turn it to earliest offset")
        earliestOffset
      } else {
        topicAndPartitionAndOffset
      }
    }
  }

}

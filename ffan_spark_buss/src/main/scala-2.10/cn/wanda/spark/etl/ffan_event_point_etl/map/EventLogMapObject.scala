package cn.wanda.spark.etl.ffan_event_point_etl.map

import cn.wanda.spark.etl.ffan_event_point_etl.filter.FilterObject
import com.alibaba.fastjson.{JSONObject, JSON}
import factory.{SimpleLogParse, GeneralLogParse}
import org.apache.spark.rdd.RDD
import utils.InputKeys
import scala.collection.JavaConversions._

/**
  * Created by weishuxiao on 16/5/22.
  */
object EventLogMapObject {

  /**
    * 调用日志解析类  对日志进行解析
    *
    * @param inputKeys
    * */
  def logparser(inputKeys: InputKeys,rdd:RDD[String],className: String)={

    className match {
      case "noMsgParser"     => noMsgParser(inputKeys,rdd)
      case "genPointParser"  => genPointParser(inputKeys,rdd)
      case "autoPointParserAndroid"  => autoPointParserAndroid(inputKeys,rdd)
      case "autoPointParserIos"  => autoPointParserIos(inputKeys,rdd)
      case "simpleParser"  => simpleParser(inputKeys,rdd)
      case "genPointParser2"  => genPointParser2(inputKeys,rdd)
      case _                 => genPointParser(inputKeys,rdd)
    }
  }

  /**
    * 日志不需要从massage字段获取的日志解析
 *
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def noMsgParser(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.distinct().filter(line=>FilterObject.jsonFilter(line))
    //    logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), inputKeys))

    eventRdd
  }


  /**
    * 简单日志格式解析  只是单纯获取log当中的字段   不涉及event_log[]  嵌套形式
    * 日志需要从massage字段获取的日志解析
    *
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def simpleParser(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.filter(line=>FilterObject.jsonFilter(line) && line.contains("message")).distinct()
      .map(line=>JSON.parseObject(line).getString("message"))
      .filter(line=>FilterObject.jsonFilter(line))
    //    logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=> (new SimpleLogParse()).parser(JSON.parseObject(line), inputKeys))

    eventRdd
  }


  /**
    * 手工埋点数据解析
    * 日志需要从massage字段获取的日志解析
    *
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def genPointParser(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.distinct().filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log") && !line.contains("auto_record_point"))
      .map(line=>JSON.parseObject(line).getString("message")).distinct()
      .filter(line=>FilterObject.jsonFilter(line))
    //    logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), inputKeys))

    eventRdd
  }

  /**
    * 手工埋点数据解析
    * 日志需要从massage字段获取的日志解析
 *
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def genPointParser2(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.distinct().filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log") && !line.contains("auto_record_point"))
      .map(line=>jsonMsgPutKey(line,"receive_time"))
      .filter(line=>FilterObject.jsonFilter(line))
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), inputKeys))
    eventRdd
  }

  /**
    * 向json message字段当中添加外层字段 便于解析函数统一处理字段
    * */
  def jsonMsgPutKey(jsonString: String,key: String) = {

    try{
      val jSONObject1=JSON.parseObject(jsonString)
      //首先获取json当中的嵌套为字符串 然后该字符串再转换为jsonObject 解决老版app当中/的问题
      val msgJsonObjs = jSONObject1.getString("message")
      val msgJsonObj=JSON.parseObject(msgJsonObjs)
      msgJsonObj.put(key,getJsonValue(jSONObject1,key))
      msgJsonObj.toJSONString
    }
    catch {
      case ex:Exception => {
        println("日志格式错误:"+jsonString)
        "-"
      }
    }
  }

  /**
    * 根据key获取json当中的value
    *
    * @param jsonObject json对象
    * @param key        key值
    *
    **/
  def getJsonValue(jsonObject: JSONObject, key: String): String = {
    if (jsonObject.containsKey(key)) {
      return jsonObject.getString(key)
    }
    else {
      return "-"
    }
  }

  /**
    * Android 自动化埋点日志解析
    * 日志需要从massage字段获取的日志解析
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def autoPointParserAndroid(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log") && line.contains("auto_record_point"))
      .map(line=>JSON.parseObject(line).getString("message")).distinct()
      .filter(line=>FilterObject.jsonFilter(line))
    //    logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), inputKeys))

    eventRdd
  }

  /**
    * Ios自动化埋点日志解析
    * 日志需要从massage字段获取的日志解析
    * @param inputKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */
  def autoPointParserIos(inputKeys: InputKeys,rdd:RDD[String])={
    val logRdd = rdd.filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log"))
      .filter(line=>line.contains("FFVCLOAD") || line.contains("FFVCDISAPPEAR"))
      .map(line=>JSON.parseObject(line).getString("message")).distinct()
      .filter(line=>FilterObject.jsonFilter(line))
    //    logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), inputKeys))

    eventRdd
  }

}

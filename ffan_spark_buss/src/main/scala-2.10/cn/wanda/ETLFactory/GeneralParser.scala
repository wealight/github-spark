package cn.wanda.ETLFactory

import cn.wanda.ETLFactory.ETLParser.GeneralLogParse
import cn.wanda.util.{FilterObject, JsonKeys}
import com.alibaba.fastjson.{JSONObject, JSON}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
  * Created by weishuxiao on 16/11/25.
  */

/**
  * Created by weishuxiao on 16/11/25.
  */
trait ParserTrait extends Serializable{
  def logparser(rdd:RDD[String],jsonKeys: JsonKeys):RDD[String]
}


/**
  * 手工埋点数据解析
  * 日志需要从massage字段获取的日志解析
  * */
object GeneralParser extends ParserTrait with Serializable{
  /**
    * @param jsonKeys 带解析的字段
    * @param rdd 原始rdd输入
    * */

  def logparser(rdd:RDD[String], jsonKeys: JsonKeys)={
    val logRdd = rdd.distinct().filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log") && !line.contains("auto_record_point"))
      .map(line=>JSON.parseObject(line).getString("message")).distinct()
      .filter(line=>FilterObject.jsonFilter(line))
//        logRdd.foreach(println)
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), jsonKeys))
    eventRdd
  }
}


/**
  *将头部信息塞入操作日志当中 然后获取字段
  */
object GeneralParser2 extends ParserTrait with Serializable{
  def logparser(rdd:RDD[String], jsonKeys: JsonKeys)={
    val logRdd = rdd.filter(line=>FilterObject.jsonFilter(line) && line.contains("message") && line.contains("event_log") && !line.contains("auto_record_point"))
    .map(line=>jsonMsgPutKey(line,"receive_time"))
    .filter(line=>FilterObject.jsonFilter(line))
    .flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), jsonKeys))
    logRdd
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

}

package cn.wanda.ETLFactory.ETLParser

import java.util
import cn.wanda.util.JsonKeys
import com.alibaba.fastjson.JSONObject
import scala.collection.JavaConversions._
/**
  * Created by weishuxiao on 16/12/16.
  */

/**
  * 简单json格式日志解析
  */
class SimpleLogParse extends LogParserTrait with Serializable{

  def parser(jsonObject: JSONObject, jsonKeys: JsonKeys): List[String]={
    val treeMap =new util.TreeMap[Int,String]
    jsonKeys.getSimplekeyList.foreach{
      simpleKey => treeMap.put(simpleKey.order,getJsonValue(jsonObject,simpleKey.key))
    }
    List(treeMap.values().toIterable.mkString("\t"))
  }

  /**
    * 根据key获取json当中的value
    *
    * @param jsonObject json对象
    * @param key        key值
    *
    **/
  def getJsonValue(jsonObject: JSONObject, key: String): String = {
    if (jsonObject.containsKey(key))  jsonObject.getString(key) else "-"
  }
}

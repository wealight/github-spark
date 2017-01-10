package cn.wanda.ETLFactory.ETLParser

import java.util

import cn.wanda.util.JsonKeys
import com.alibaba.fastjson.JSONObject
import scala.collection.JavaConversions._
/**
  * Created by weishuxiao on 16/12/16.
  */
class GeneralLogParse extends LogParserTrait with Serializable{
  override def parser(jsonObject: JSONObject, jsonKeys: JsonKeys): List[String] = {

    val simpleAndnestMap = new util.TreeMap[Int,String]()
    val treeMap = new util.TreeMap[Int,String]()
    var treeMapList = List[util.TreeMap[Int,String]]()

    /**
      * 简单格式日志解析
      */
    jsonKeys.getSimplekeyList.foreach{
      simpleKey => simpleAndnestMap.put(simpleKey.order,getJsonValue(jsonObject,simpleKey.key))
    }

    /**
      * 嵌套格式字段日志解析
      */
    jsonKeys.getNestkeyList.foreach{
      nestKey=>
        var subJsonObject = new JSONObject()
        nestKey.layers.foreach{
          lay=> subJsonObject=Option(subJsonObject.getJSONObject(lay)).getOrElse(new JSONObject())
        }
        simpleAndnestMap.put(nestKey.order,getJsonValue(subJsonObject,nestKey.key))
    }

    /**
      * 数组格式字段日志解析
      */
    if (!jsonKeys.getArraykeyList.isEmpty){
      val arrKeyList = jsonKeys.getArraykeyList
      try {
        var jsonList = List[JSONObject]()
        val jsonArray = jsonObject.getJSONArray(jsonKeys.getArraykeyList(0).arrayName)
        Range(0,jsonArray.size()).foreach{index=> jsonList = jsonList:+jsonArray.getJSONObject(index)}
        jsonList.map{
          json=>
            val jsonMap = new util.TreeMap[Int,String]
            arrKeyList.map{ arrKey=> jsonMap.put(arrKey.order,getJsonValue(json,arrKey.key))}
            jsonMap.putAll(simpleAndnestMap)
            treeMapList=treeMapList:+jsonMap
        }
      }catch {
        case _:Exception=>{
          val jsonMap = new util.TreeMap[Int,String]
          arrKeyList.foreach{key=>jsonMap.put(key.order,"")}
          jsonMap.putAll(simpleAndnestMap)
          treeMapList=treeMapList:+jsonMap
        }
      }
    }

    val logList = treeMapList.map{treeMap=>treeMap.values().mkString("\t")}
    logList
  }


  /**
    * 根据key获取json当中的value
    *
    * @param jsonObject json对象
    * @param key        key值
    *
    **/
  def getJsonValue(jsonObject: JSONObject, key: String): String = {
    Option(jsonObject.getString(key)).getOrElse("-")
  }
}

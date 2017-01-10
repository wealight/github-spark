package cn.wanda.util

/**
  * Created by weishuxiao on 16/12/15.
  */

class Key()

/**
  *简单格式json 字段名称
 *
  * @param order 字段在输入字段列表当中的顺序
  * @param key  字段名称
  * */
case class SimpleKey(val order:Int,val key:String) extends Key

/**
  *数组格式json 字段名称 event_log[event_id]  仅用于json当中解析一个 数组字段
 *
  * @param order 字段在输入字段列表当中的顺序
  * @param arrayName 数组字段名称
  * @param key  字段名称
  * */
case class ArrayKey(val order: Int,val arrayName:String,val key:String) extends Key

/**
  *数组格式json 字段名称 event_log.event_id.id
 *
  * @param order 字段在输入字段列表当中的顺序
  * @param layers 层级列表
  * @param key  字段名称
  * */
case class NestKey(val order: Int,val layers:Array[String],val key:String) extends Key


/**
  * json 字段输入解析类
  * 解析类型包括简单字段类型 key  数组字段类型key[key_child]  嵌套字段类型key1.key2.key3
  */
class JsonKeys(val keyString:String) extends Serializable{

  val simple_reg = """^(\w+)$"""r
  val array_reg = """^(\w+)\[(\w+)\]$"""r
  val nest_reg = """^(.*)\.(\w+)$""" r

  val keyArrayBuffer = scala.collection.mutable.ArrayBuffer[String]()
  private var simplekeyList = List[SimpleKey]()
  private var arraykeyList  = List[ArrayKey]()
  private var nestkeyList   = List[NestKey]()
  keyParser(keyString)

  def keyParser(keyString:String): Unit ={
    val keyArray =keyString.trim.split(",")
    var i = 0
    keyArray.foreach{ key=>
      i=i+1
      key match
      {
        case simple_reg(key) => {simplekeyList=simplekeyList:+SimpleKey(i,key);keyArrayBuffer+=key}
        case array_reg(arrayName,key)=>{arraykeyList=arraykeyList:+ArrayKey(i,arrayName,key);keyArrayBuffer+=key}
        case nest_reg(nest,key)=>{nestkeyList=nestkeyList:+NestKey(i,nest.split("\\."),key);keyArrayBuffer+=key}
      }
    }
  }

  def getSimplekeyList=simplekeyList
  def getArraykeyList=arraykeyList
  def getNestkeyList=nestkeyList
  def getKeyList = (nestkeyList,arraykeyList,nestkeyList)
  def getKeyString =keyArrayBuffer.mkString(",")
}

object JsonKeys{
  def apply(keyString:String)={
    new JsonKeys(keyString)
  }

}

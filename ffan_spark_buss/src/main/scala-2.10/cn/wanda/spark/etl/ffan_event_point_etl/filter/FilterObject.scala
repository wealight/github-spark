package cn.wanda.spark.etl.ffan_event_point_etl.filter

import com.alibaba.fastjson.JSON


/**
  * Created by weishuxiao on 16/5/20.
  */
object FilterObject {

  /**
    * 过滤无法进行json解析的字符串
    * 对输入的字符串转变为json对象
    */
  val jsonFilter = (line:String)=>{
    try {
      val  jsonObject = JSON.parseObject(line)
      true
    }catch {
      case ex:Exception => {
        println("日志格式错误:"+line)
        false
      }
    }
  }

  /**
    * 字段长度过滤
    * */
  val wordFilter=(line:String,len:Int)=>{
    val lineLen=line.trim.split("\t").length
    if (lineLen==len){
      true
    }else{
      println(lineLen+"****************************"+len)
      false
    }
  }
}

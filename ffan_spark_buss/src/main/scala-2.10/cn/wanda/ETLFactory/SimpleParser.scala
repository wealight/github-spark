package cn.wanda.ETLFactory

import cn.wanda.ETLFactory.ETLParser.{GeneralLogParse, SimpleLogParse}
import cn.wanda.util.{FilterObject, JsonKeys}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

/**
  * Created by weishuxiao on 16/12/16.
  */
/**
  * 简单日志格式解析  只是单纯获取log当中的字段   不涉及event_log[]  嵌套形式
  * 日志需要从massage字段获取的日志解析
  * */
object SimpleParser extends ParserTrait  with Serializable{
  def logparser(rdd:RDD[String], jsonKeys: JsonKeys)={
    val logRdd = rdd.filter(line => FilterObject.jsonFilter(line) && line.contains("message")).distinct()
      .map(line => JSON.parseObject(line).getString("message"))
      .filter(line => FilterObject.jsonFilter(line))
    val eventRdd = logRdd.flatMap(line => (new SimpleLogParse()).parser(JSON.parseObject(line), jsonKeys))

    eventRdd
  }
}


/**
  *日志不需要从massage字段获取的日志解析
  */
object NoMsgParser extends ParserTrait with Serializable{
  def logparser(rdd:RDD[String], jsonKeys: JsonKeys)={
    val logRdd = rdd.distinct().filter(line=>FilterObject.jsonFilter(line))
    val eventRdd=logRdd.flatMap(line=>(new GeneralLogParse()).parser(JSON.parseObject(line), jsonKeys))
    eventRdd
  }
}
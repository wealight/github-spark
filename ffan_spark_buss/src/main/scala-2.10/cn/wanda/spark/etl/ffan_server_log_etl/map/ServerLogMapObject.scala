package cn.wanda.spark.etl.ffan_server_log_etl.map
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.Map
import scala.collection.JavaConversions._


/**
  * Created by weishuxiao on 16/5/22.
  */
object ServerLogMapObject {

  val apache_reg="""(.*?)\s.*\s\[.*?:(.*?)\s.*?\]\s(\S+)\s\"\S+\s(/.*?)\s.*?\"\s(.*?)\s(.*?)\s(.*?)\s\"(.*?)\"\s\"(.*?)\"\s.*?\s\"(.*?)\"""".r
  val nginx_reg ="""(.*?)\s.*\s\[.*?:(.*?)\s.*?\]\s(.*?)\s\".*?\"\s\".*?\s(/.*?)\s.*?\"\s(.*?)\s(.*?)\s.*?\s.*?\s\"(.*?)\"\s\"(.*?)\"\s\".*?\"\s\"(.*?)\"\s\"(.*?)\"""".r

  case class ServerLog(client_ip:String,hourkey:String,request:String,status:String,status_vice:String,bytes:String,
                       refer:String,client_parameter:String,cookie:String,uid:String,wpid:String,u_uid:String,city_id:String,plaza_id:String,
                       promotion_from:String,dt: String,source:String,domain:String)
  /**
    * 调用日志解析类,实现对apache\nginx日志进行统一解析
    *
    * @param rdd
    * 返回apache\nginx日志解析后合并的rdd
    *
    * */
  def logparser(rdd:RDD[String],date: String)= {

    val jsonRdd   = rdd.filter(line=>jsonFilter(line)).map(line=>JSON.parseObject(line)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val apacheRdd = jsonRdd.filter(line=>line.getString("type")=="apache").map(line=>line.getString("message")).distinct().map(line=>apachePaser(line))
    val nginxRdd  = jsonRdd.filter(line=>line.getString("type")=="nginx").map(line=>line.getString("message")).distinct().map(line=>nginxPaser(line))

    val apacheMapRdd  = apacheRdd.filter(_.length>0).map(arr=>ServerLog(arr(0),arr(1),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),date,"apache",arr(2)))
    val nginxMapRdd   = nginxRdd.filter(_.length>0).map(arr=>ServerLog(arr(0),arr(1),arr(3),arr(4),arr(5),arr(6),arr(8),arr(7),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),date,"ngnix",arr(2)))
    val unionRdd=apacheMapRdd.union(nginxMapRdd)
    unionRdd
  }

  /**
    * apache日志进行解析
 *
    * @param line
    * */
  def apachePaser(line: String) ={
    try{
      val apache_reg(client_ip,hourkey,domain,request,status,status_vice,bytes,refer,client_parameter,cookie)=line
      val cookieMap = cookiePaser(cookie)
      val valuesArr = strConcat(client_ip,hourkey,domain,request,status,status_vice,bytes,refer,client_parameter,cookie,
        cookieMap.getOrElse("uid","-"),cookieMap.getOrElse("wpid","-"),cookieMap.getOrElse("U_UID","-"),
        cookieMap.getOrElse("city_id","-"),cookieMap.getOrElse("plaza_id","-"),cookieMap.getOrElse("promotion_from","-")).toArray
      valuesArr
    }catch {
      case ex:Exception =>new Array[String](0)
    }
  }

  /**
    * nginx日志解析
 *
    * @param line
    * */
  def nginxPaser(line: String) ={
    try {
      val nginx_reg(client_ip,hourkey,domain,request,status,status_vice,bytes,refer,client_parameter,cookie)=line
      val cookieMap = cookiePaser(cookie)
      val valuesArr = strConcat(client_ip,hourkey,domain,request,status,status_vice,bytes,refer,client_parameter,cookie,
        cookieMap.getOrElse("uid","-"),cookieMap.getOrElse("wpid","-"),cookieMap.getOrElse("U_UID","-"),
        cookieMap.getOrElse("city_id","-"),cookieMap.getOrElse("plaza_id","-"),cookieMap.getOrElse("promotion_from","-")).toArray
      valuesArr
    }catch {
      case ex:Exception =>new Array[String](0)
    }
  }

  /**
    * 过滤无法进行json解析的字符串
    * 对输入的字符串转变为json对象
 *
    * @param line
    */
  def jsonFilter(line: String)={
    try {
      val  jsonObject = JSON.parseObject(line)
      true
    }catch {
      case ex:Exception => false
    }
  }

  /**
    * 输入单个变量转变为数组
 *
    * @param string
    * */
  def strConcat(string: String*)= string

  /**
    * cookie字段解析cookiePaser
    * 解析结果存入Map
 *
    * @param cookie
    * */
  def cookiePaser(cookie: String)={

    val cookie_reg="""(\w+)=([\w|-]+)""".r
    var cookieMap:Map[String,String] = Map()
    for (cookie_reg(key,value)<-cookie_reg.findAllIn(cookie).toArray)cookieMap += (key->value)
    if (cookieMap.contains("plaza_id")) cookieMap += ("wpid" -> "-")
    cookieMap
  }

}

package cn.wanda.util

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

/**
  * Created by weishuxiao on 16/12/26.
  */
object PropertiesUtil {

  def getPropObject(filePath:String) ={
    val logger = Logger.getLogger("PropertiesUtil")
    val prop = new Properties()
    val in = new BufferedInputStream (new FileInputStream(filePath))
    prop.load(in)
    logger.info("read property file!")
    in.close()
    prop
  }

  def getPropField(properties: Properties,field:String) ={
    properties.getProperty(field)
  }

}


class PropertiesBean(path:String) extends Serializable{

  val propObject             = PropertiesUtil.getPropObject(path)
  val appName                = propObject.getProperty("spark.streaming.appName")
  val zkConnect              = propObject.getProperty("spark.streaming.zkConnect")
  val kafkaRoot              = propObject.getProperty("spark.streaming.kafkaRoot")
  val topics                 = propObject.getProperty("spark.streaming.topics")
  val className              = propObject.getProperty("spark.streaming.parser.className")
  val jdbcSql                = propObject.getProperty("spark.target.jdbc.query")
  val keys                   = propObject.getProperty("parser.keys")
  val appDataPath            = propObject.getProperty("spark.streaming.appDataPath")
  val precessSourceTable     = propObject.getProperty("spark.streaming.precessSourceTable")
  val precessSql             = propObject.getProperty("spark.streaming.precessSql")
  val batchComputeSourceTable     = propObject.getProperty("spark.streaming.batchComputeSourceTable")
  val batchComputeSql             = propObject.getProperty("spark.streaming.batchComputeSql")
  val preUpdateKeyStateTable         = propObject.getProperty("spark.streaming.preUpdateKeyStateTable")
  val preUpdateKeyStateSql           = propObject.getProperty("spark.streaming.preUpdateKeyStateSql")
  val updateStateComputeTable           = propObject.getProperty("spark.streaming.updateStateComputeTable")
  val updateStateComputeFields           = propObject.getProperty("spark.streaming.updateStateComputeFields")
  val updateStateComputeSql           = propObject.getProperty("spark.streaming.updateStateComputeSql")
}
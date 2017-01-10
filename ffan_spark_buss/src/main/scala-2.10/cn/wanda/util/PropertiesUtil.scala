package cn.wanda.util

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

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

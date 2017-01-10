package cn.wanda.spark_streaming.main

import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by weishuxiao on 17/1/9.
  */
object SparkTest {
  def main(args: Array[String]) {
    val conf =new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val prop = new Properties
    prop.put("user","root")
    prop.put("password","881234")
    val url = "jdbc:mysql://localhost:3306/mysqltest?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    //读取sparkTest表的所有内容,无分区
    val df1 = sqlContext.read.jdbc(url, "sparkTest",prop)
    println(df1.rdd.partitions.size)
    //读取sparkTest表的pv,并且pv>=0 and pv<=10所有内容,分区数量为3
    val df2 = sqlContext.read.jdbc(url, "sparkTest", "pv", 1, 10, 3, prop)
    println(df2.rdd.partitions.size)
    //读取sparkTest表event_id='1111_MAINPAGE_XF_AGJ' and uv=2的所有字段
    val df = sqlContext.read.jdbc(url, "sparkTest",Array("event_id='1111_MAINPAGE_XF_AGJ' and uv=2","event_id='1111_MAINPAGE_XF_AGJ' and uv=1"), prop)
    println(df.rdd.partitions.size)
  }
}

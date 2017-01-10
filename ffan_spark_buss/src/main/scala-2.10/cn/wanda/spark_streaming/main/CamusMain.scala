package cn.wanda.spark_streaming.main

/**
  * Created by weishuxiao on 16/8/23.
  */

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object CamusMain {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val interval=Seconds(10)
    val ssc = new StreamingContext(conf, interval)
    val lines1 = ssc.socketTextStream("localhost", 9999)
    val lines2=lines1.map(line=>line+"ss")
    lines1.window(Seconds(20),Seconds(20)).count().print()
    lines2.window(Seconds(30),Seconds(30)).count().print()
    ssc.start()
    ssc.awaitTermination()
  }

//  def createContextFunc()={
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(4))
//
//    ssc
//  }
}

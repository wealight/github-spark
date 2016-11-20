package cn.wanda.spark_streaming.etl.camus.main

/**
  * Created by weishuxiao on 16/8/23.
  */

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object CamusMain {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    conf.set("spark.streaming.fileStream.minRememberDuration", "25920000s")
    val ssc = new StreamingContext(conf, Seconds(4))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    //    val wordCounts = pairs.reduceByKey(_ + _)

    //采用updateStateByKey算子必须设置checkpoint
    ssc.checkpoint("/Users/weishuxiao/Documents/spark/ffan_spark_etl/source/checkpoint")
    val wordCounts = pairs.updateStateByKey[Int](addFunc).persist()
    //设置rdd checkpoint 刷入时间间隔
    //    wordCounts.checkpoint(Seconds(10))
    wordCounts.print()

    val lines1 = ssc.fileStream("hdfs://localhost:9000/user/weishuxiao/sparktest")
    lines1.print()

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

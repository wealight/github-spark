package cn.wanda.spark_streaming.etl.camus.main

import java.text.SimpleDateFormat
import java.util.Date


/**
  * Created by weishuxiao on 16/7/8.
  */
object MyClass {

  def main(args: Array[String]): Unit = {

    //获取当前时间
    val date = new Date()
    //Date对象时间转字符串
    println(date.toString)
    //Date对象时间转时间戳
    println(date.getTime)


    //格式化时间实例
    val ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    //格式化Date对象
    println(ft.format(date))
    //时间字符串转变为Date对象
    println(ft.parse("2016-11-08 14:49:25"))

    //时间戳字符串转变为时间格式字符串
    val  a="1478589193167"
    println((new Date(a.toLong).toString))
  }
}

package cn.wanda.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by weishuxiao on 16/11/25.
  */
object RddFunctions extends Serializable{

  /**
    * 用于updateStateByKey 更新
    *
    * currValues 新的Dsream 当中的RDD 每个key的Seq[Int]
    * prevValueState 状态存储的Dstream 每个key的State
    * */
  val stateSumbykey=(currValues: Seq[Int], prevValueState: Option[Int])=>{
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果，是一个Option[Int]类型

    Some(currentCount + previousCount)
  }

/**
  * "yyyy-MM-dd HH:mm"
  * */
  def getNow(format:String) = {
    val dateFormat = new SimpleDateFormat(format) //格式化时间实例
    dateFormat.format(new Date())
  }


  def unixtime2date(unixstring:String,format:String) = {
    val minutesDateFormat = new SimpleDateFormat(format) //格式化时间实例
    try {
      val stamp = unixstring.toLong
      minutesDateFormat.format(new Date(stamp))
    } catch {
      case ex: Exception => minutesDateFormat.format(new Date()) //如果转换异常则将时间标记为当前程序时间
    }
  }

  def string2tuple(string: String): Unit ={

  }
}

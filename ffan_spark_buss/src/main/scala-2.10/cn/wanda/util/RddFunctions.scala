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
  def stateSumbykey=(currValues: Seq[String], prevValueState: Option[String])=>{
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和

    val mergedState = prevValueState match {
      case Some(state)=>currValues:+state
      case None=>currValues
    }

    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val date = formatter.format(new Date())
    val currentCount = mergedState.map{
      ele=>
        val pair = ele.split("\\|")
        (pair(0),pair(1).toInt)
    }
      .filter(_._1==date)
      .groupBy(_._1).map(ele=>(ele._1,ele._2.map(_._2).reduce(_+_)))

    currentCount.get(date) match {
      case Some(num)=>Some((`date`+"|"+num))
      case _=>None
    }
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

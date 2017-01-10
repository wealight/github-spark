package cn.wanda.util

import java.lang.management.ManagementFactory

/**
  * Created by weishuxiao on 16/12/23.
  */
object JvmIdGetter {
  def IdGetter(): Unit ={
    val pid = ManagementFactory.getRuntimeMXBean.getName
    val index = pid.indexOf('@')
    if (index>0){
      println("the jvm pid is:"+pid.substring(0, index)+"; the thread id  is "+Thread.currentThread().getId)
    }
  }
}

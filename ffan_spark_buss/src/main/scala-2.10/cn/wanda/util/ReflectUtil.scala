package cn.wanda.util

/**
  * Created by weishuxiao on 16/11/25.
  */
object ReflectUtil {

  def getSingletonObject[T](className:String) ={
    //object 反射
    val cons = Class.forName(className).getDeclaredConstructors()
    cons(0).setAccessible(true)
    cons(0).newInstance().asInstanceOf[T]
  }

  def getClassObject[T](className:String) ={
    Class.forName(className).newInstance().asInstanceOf[T]
  }
}

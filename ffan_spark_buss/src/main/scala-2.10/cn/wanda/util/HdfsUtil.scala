package cn.wanda.util

import java.io.{InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.log4j.Logger

/**
  * Created by weishuxiao on 16/12/26.
  */
class HdfsUtil(configuration: Configuration) {

  val fs = FileSystem.get(configuration)
  val logger = Logger.getLogger("HdfsUtil")

  /**
    * 将字符串写入到指定hdfs文件.
    *
    * @param pathString  所写入的文件
    * @param words 待写入字符串
    */
  def write2Hdfs(pathString: String,words: String){
    val path = new Path(pathString)
    val out = fs.create(path, true)
    out.write(words.getBytes("UTF-8"))
    out.close()
    logger.info(s"write2Hdfs日志成功写入路径: $pathString")
  }

  /**
    *
    * 读取hdfs指定路径文件
    *
    * @param pathString  待读入的文件
    */
  def readHdfs(pathString: String)={
    val path = new Path(pathString)

    if (fs.exists(path)) {
      val in = fs.open(path)
      val bufferReader = new BufferedReader(new InputStreamReader(in))
      var stringBuffer = ""
      var line = bufferReader.readLine()
      while (line!= null){
        stringBuffer=stringBuffer +"\n"+ line
        line = bufferReader.readLine()
      }
      in.close()
      stringBuffer
    }else{
      ""
    }


  }

  /**
    * 删除原有路径
    *
    * @param pathString 待删除路径
    * */
  def rmDir(pathString: String): Unit ={
    val path = new Path(pathString)
    if (fs.exists(path)) {
      fs.delete(path, true)
      logger.info(s"rmDir指定目录:$pathString 存在,删除原始目录!")
    }else{
      logger.info(s"rmDir指定目录:$pathString 不存在,无需删除操作!")
    }
  }


  /**
    * 创建目录路径,可以创造层级路径
    *
    * @param pathString 待创建路径
    * */
  def mkDir(pathString: String): Unit ={
    val path = new Path(pathString)
    if (fs.exists(path)) {
      logger.info(s"mkDir指定目录:$pathString 已经存在,无法进行创建!")
    }else{
      //fs.create(path)//指定路径创建问价
      fs.mkdirs(path)
      logger.info(s"mkDir指定目录:$pathString 不存在,创建成功!")
      fs.getChildFileSystems
    }
  }

  /**
    * 移动路径
    * 目前hdfs当中并没有提供move的api 但是可以通过rename实现
    * */
  def mvDir(srcDir: String,destDir: String,overwrite:Boolean): Unit ={
    val reg =""".*(/.*)$""".r
    val srcPath = new Path(srcDir)
    val destPath = new Path(destDir)
    if (overwrite==true && fs.exists(new Path("$destDir$file"))){
      val reg(file)=srcDir
      rmDir(s"$destDir$file")
    }
    fs.rename(srcPath,destPath)
  }


  def copy(srcDir: String,destDir: String,deleteSource: Boolean,overwrite: Boolean): Unit ={
    val reg =""".*(/.*)$""".r
    val reg(file)=srcDir
    val srcPath = new Path(srcDir)
    val destPath = new Path(destDir)
    rmDir(destDir+file)
    FileUtil.copy(fs,srcPath,fs,destPath,deleteSource,overwrite,configuration)
    logger.info(s"把目录$srcDir 复制到 $destDir!")
  }
}


object HdfsUtil{

  def apply(configuration: Configuration) ={
    new HdfsUtil(configuration: Configuration)
  }

  def write2Hdfs(pathString: String,words: String,configuration: Configuration): Unit ={
    val hdfsUtil =HdfsUtil(configuration)
    hdfsUtil.write2Hdfs(pathString,words)
  }

  def readHdfs(pathString: String,configuration: Configuration) = {
    val hdfsUtil = HdfsUtil(configuration)
    hdfsUtil.readHdfs(pathString)
  }

  def rmDir(pathString: String,configuration: Configuration): Unit ={
    val hdfsUtil =HdfsUtil(configuration)
    hdfsUtil.rmDir(pathString)
  }

  def mkDir(pathString: String,configuration: Configuration): Unit ={
    val hdfsUtil =HdfsUtil(configuration)
    hdfsUtil.mkDir(pathString)
  }

  def mvDir(srcDir: String,destDir: String,overwrite:Boolean,configuration: Configuration): Unit ={
    val hdfsUtil =HdfsUtil(configuration)
    hdfsUtil.mvDir(srcDir,destDir,overwrite)
  }

  def copy(srcDir: String,destDir: String,deleteSource: Boolean,overwrite: Boolean,configuration: Configuration): Unit ={
    val hdfsUtil =HdfsUtil(configuration)
    hdfsUtil.copy(srcDir: String,destDir: String,deleteSource: Boolean,overwrite: Boolean)
  }
}


object Tester{
  def main(args: Array[String]) {
    val  hdfsUtil = HdfsUtil(new Configuration())
//    val  conf = new Configuration()
//    conf.set("fs.default.name", "hdfs://localhost:9000")
//    val  hdfsUtil = HdfsUtil(conf)
//    hdfsUtil.write2Hdfs("/Users/weishuxiao/Documents/scala/tester/1.txt", "HdfsUtil test 111 \nHdfsUtil test 111 \n dsdsd ")
//    val s = hdfsUtil.readHdfs("/Users/weishuxiao/Documents/scala/tester/2.txt")
//    println(s)


    val fs = FileSystem.get(new Configuration())
    fs.getName()

    hdfsUtil.mkDir("/user/weishuxiao/test/spark/spark-streaming/LiuliangStat/")
//    hdfsUtil.mvDir("/Users/weishuxiao/Documents/scala/tester2/12/23","/Users/weishuxiao/Documents/scala/tester2/12/24",true)

//    hdfsUtil.copy("/Users/weishuxiao/Documents/scala/tester2/12/24","/Users/weishuxiao/Documents/scala/tester2",false,true)
  }
}
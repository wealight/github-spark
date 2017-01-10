package cn.wanda.util

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException.NoNodeException

/**
  * Created by weishuxiao on 16/8/26.
  *
  * 该类用于管理存储在zookeeper 当中的znode信息
  */
class ZookeeperManager(private var zkConnect:String, private var zkPath:String)  extends Serializable {

  //链接zookeeper  重连时间间隔  尝试重连次数
  @transient val retryPolicy = new ExponentialBackoffRetry(1000, 29)
  //创建zookeeper链接客户端
  private var curatorFramework = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy)
  curatorFramework.start
  println("创建zookeeper client实例 "+zkPath)


  /**
    * 对ZookeeperManager进行序列化
    * 自定义序列化格式 主要是对当中的关键字符串属性进行序列化,便于根据这些关键属性恢复
    * */
  @throws(classOf[java.io.IOException])
  private def writeObject(out : java.io.ObjectOutputStream) : Unit ={
    out.writeUTF(this.zkConnect)
    out.writeUTF(this.zkPath)
  }

  /**
    * 对ZookeeperManager进行反序列化
    * 获取关键字段重新组建对象
    * */
  @throws(classOf[java.io.IOException])
  private def readObject(in : java.io.ObjectInputStream) : Unit ={
    this.zkConnect = in.readUTF()
    this.zkPath = in.readUTF()
    this.curatorFramework=CuratorFrameworkFactory.newClient(this.zkConnect, new ExponentialBackoffRetry(1000, 29))
    this.curatorFramework.start
  }

  /**
    * 获取的offset
    * @param default 当znode信息为null或者获取出现异常时默认znode信息
    */
  def getChildZnodeInfo(default:String = "")={
    try {
      if (curatorFramework.getData.forPath(zkPath)==null) default
      else new String(curatorFramework.getData.forPath(zkPath))
    }catch {
      case ex:NoNodeException => {
        println("znode不存在:"+zkPath+ex.printStackTrace())
        default
      }
      case _:Exception=>{
        println(">>>>>>>>>>>>>>>>获取znode"+zkPath+" 数据出现问题")
        default
      }
    }
  }

  /**
    * 更新zookeeper znode内容
    * */
  def updateChildZnodeInfo(offset:String)={
    if(curatorFramework.checkExists().forPath(zkPath)==null){
      println("zookeeper"+zkPath+"路径不存在,创建目录")
      curatorFramework.create().forPath(zkPath,"init".getBytes)//"init".getBytes为该目录(znode关联字符串)
    }
    curatorFramework.setData().forPath(zkPath,offset.getBytes)
  }

  /**
    * 关闭zookeeper链接实例
    * */
  def closeZkConnect(curatorFramework: CuratorFramework){
    curatorFramework.close()
  }
}

object ZookeeperManager{
  def  getChildZnodeInfo(zkConnect:String,zkPath:String)={
    (new ZookeeperManager(zkConnect,zkPath)).getChildZnodeInfo()
  }
}
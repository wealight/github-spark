package utils

import com.alibaba.fastjson.JSON
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by weishuxiao on 16/8/24.
  * 该类主要是通过zookeeper获得kafka的brokerlist
  * @param zkConnect 提供zookeeper地址 e.g. k11003a.wdds.kfk.com:11003,k11003b.wdds.kfk.com:11003
  * @param kafkaRootPath kafka在zookeeper当中的根目录路径 e.g. /sparktest
  * kafkaRootPath为zookeeper根目录是直接为/
  */


class BrokerCollector(val zkConnect:String,val kafkaRootPath:String){

  private var curatorFramework: CuratorFramework=null
  /**
    * 链接zookeeper 创建Curator对象
    * @param zookeeperConnect zookeeper链接地址
  * */
  def startZkConnect(zookeeperConnect:String)={

    //链接zookeeper  重连时间间隔  尝试重连次数
    val retryPolicy = new ExponentialBackoffRetry(1000, 29)
    //创建zookeeper链接客户端
    curatorFramework = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy)
    curatorFramework.start
  }

  /**
    * 关闭zookeeper链接实例
    * @param curatorFramework kafka客户端实例
    * */
  def closeZkConnect(curatorFramework: CuratorFramework){
    curatorFramework.close()
  }

  /**
    * 返回指定zookeeper 的root路径的znode信息 对应于kafka为broker
    * */
  def getBrokerList() ={
    val idsPath= if (kafkaRootPath.equals("/")) "/brokers/ids" else kafkaRootPath+"/brokers/ids"
    startZkConnect(zkConnect)
    val childZnodePathArray = curatorFramework.getChildren.forPath(idsPath).toArray()
    val childZnodeJsonInfo = childZnodePathArray.map(element=>getChildInfo(curatorFramework,idsPath,element))
    val brokerList=childZnodeJsonInfo.map(element=>element.getString("host")+":"+element.getString("port"))
//    closeZkConnect(curatorFramework)
    brokerList.mkString(",")
  }


  /**
    *
    * 获取child目录信息  返回为json对象
    * e.g.  {"jmx_port":-1,"timestamp":"1472112457520","host":"localhost","version":1,"port":9092}
    *
    * @param curatorFramework zookeeper链接实例
    * @param idsPath zookeeper上/brokers/ids绝对目录  e.g.  /test/brokers/ids
    * @param child zookeeeper znode子目录
    *
    * */
  def getChildInfo(curatorFramework: CuratorFramework,idsPath:String,child: Any)={
    //getData返回值为Byte数组类型  注意转变为字符串
    val childInfoByte = curatorFramework.getData.forPath(idsPath+"/"+child)
    JSON.parseObject(new String(childInfoByte))
  }
  
  /**
    * 获取链接实例
    * */
  def getCuratorFramework()={
    curatorFramework
  }

}

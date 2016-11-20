package cn.wanda.spark.etl.ffan_server_log_etl.main

import scala.collection.mutable.Map

/**
  * Created by weishuxiao on 16/6/29.
  */
object MyTest {
  def main(args: Array[String]) {
    val log="10.209.37.77 - - [12/Jun/2016:11:59:59 +0800] zzq.intra.ffan.com \"127.0.0.1:9000\" \"POST /ajax_send_verify_codes HTTP/1.1\" 200 - 74 0.002 \"0.002\" \"http://www.ffan.com/zzq/h5_activity?activityId=4&source=MN&promotion_from=1-14-1-8707\" \"Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.99 Safari/537.36\" \"222.161.207.50\" \"SHARE_STRING=%7B%22content_tencent%22%3A%22N%u591A%u4F18%u60E0%u5927%u5238%uFF0C%u73B0%u5728%u5F00%u62A2%uFF01%22%2C%22content_weibo%22%3A%22N%u591A%u4F18%u60E0%u5927%u5238%uFF0C%u73B0%u5728%u5F00%u62A2%uFF01%22%2C%22title%22%3A%22N%u591A%u4F18%u60E0%u5927%u5238%uFF0C%u73B0%u5728%u5F00%u62A2%uFF01%22%2C%22url%22%3A%22http%3A//www.ffan.com/zzq/h5_activity%3FactivityId%3D4%26source%3DMN%22%2C%22picsrc%22%3A%22T1fzxTBQdT1RCvBVdK%22%7D; SESSIONID=abb86d5594b272e8aa59ba746ba6de6e; promotion_from=1-14-1-8707; SESSIONID=abb86d5594b272e8aa59ba746ba6de6e\""
    val nginx_reg="""(.*?)\s.*\s\[.*?:(.*?)\s.*?\]\s(.*?)\s\".*?\"\s\".*?\s(/.*?)\s.*?\"\s(.*?)\s(.*?)\s.*?\s.*?\s\"(.*?)\"\s\"(.*?)\"\s\".*?\"\s\"(.*?)\"\s\"(.*?)\"""".r
    val nginx_reg(client_ip,hourkey,domain,request,status,status_vice,bytes,refer,client_parameter,cookie)=log

    val cookie_reg="""(\w+)=(\w+)""".r
    var cookieMap:Map[String,String] = Map()

    val str=""
    for (cookie_reg(key,value)<-cookie_reg.findAllIn(cookie).toArray)cookieMap += (key->value)
    println(cookieMap.getOrElse("plaza_id","xx"))



  }
}

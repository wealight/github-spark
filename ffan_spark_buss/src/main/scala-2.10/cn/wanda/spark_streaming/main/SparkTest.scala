package cn.wanda.spark_streaming.main

import java.util.Properties
import org.apache.spark.{HashPartitioner, TaskContext, SparkContext, SparkConf}
import org.apache.spark.RangePartitioner

/**
  * Created by weishuxiao on 17/1/9.
  */
object SparkTest {
  def main(args: Array[String]) {
    val conf =new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    val prop = new Properties
//    prop.put("user","root")
//    prop.put("password","881234")
//    val url = "jdbc:mysql://localhost:3306/mysqltest?useUnicode=true&characterEncoding=utf-8&useSSL=false"
//    //读取sparkTest表的所有内容,无分区
//    val df1 = sqlContext.read.jdbc(url, "sparkTest",prop)
//    println(df1.rdd.partitions.size)
//    //读取sparkTest表的pv,并且pv>=0 and pv<=10所有内容,分区数量为3
//    val df2 = sqlContext.read.jdbc(url, "sparkTest", "pv", 1, 10, 3, prop)
//    println(df2.rdd.partitions.size)
//    //读取sparkTest表event_id='1111_MAINPAGE_XF_AGJ' and uv=2的所有字段
//    val df = sqlContext.read.jdbc(url, "sparkTest",Array("event_id='1111_MAINPAGE_XF_AGJ' and uv=2","event_id='1111_MAINPAGE_XF_AGJ' and uv=1"), prop)
////    println(df.rdd.partitions.size)
//    df.show()
//    df.write.mode(SaveMode.Overwrite).parquet("namesAndAges.parquet")
//    sqlContext.read.parquet("namesAndAges.parquet").show()
//    df.foreach(println)
//    sqlContext.read
    val seq = Seq(1 to 100:_*)
    sc.parallelize(List(1,2,3,4,5,6),2).foreachPartition {
      par => println(s"partitions id:${TaskContext.getPartitionId()}  data:${par.mkString(",")}")
    }

    val seq2=Seq(100 to 1 by -1:_*)

    val rdd = sc.parallelize(seq.zip(seq2),5)
    rdd.distinct()
    rdd.foreachPartition {
      par => println(s"partitions id:${TaskContext.getPartitionId()}  data:${par.mkString(",")}")
    }

    rdd.partitionBy(new RangePartitioner(5,rdd, false)).foreachPartition {
      par => println(s"partitions id:${TaskContext.getPartitionId()}  data:${par.mkString(",")}")
    }
sc.textFile("1.txt").map(_+100).collect()

    import  scala.math
    val seqOp=(a:Int,b:Int)=>{
      println(s"seqOp  1st:$a    2nd:$b   answer:${math.max(a, b)}")
      math.max(a,b)
    }

    val combOp=(a:Int,b:Int)=>{
      println(s"combOp  1st:$a    2nd:$b   answer:${a+b}")
      a+b
    }

    sc.parallelize(Seq((1,7),(2,2),(6,4),(2,1),(2,2),(2,5))).sortByKey(true,1).foreach(println)
    val rdd1 = sc.parallelize(Seq((1,7),(1,4),(2,1),(2,2),(2,5),(4,6)))
    val rdd2 = sc.parallelize(Seq((1,4),(2,1),(2,2),(3,5)))
     rdd1.cogroup(rdd2).sortByKey(true,1).foreach(println)

  }
}

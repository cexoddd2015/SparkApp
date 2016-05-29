package com.xlei.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/5/26 0026.
  */
object Test1 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("first")

    val sc=new SparkContext(sparkConf)
    val ssc=new StreamingContext(sc,Seconds(5))

    val lines=ssc.socketTextStream("127.0.0.1",9999)

    lines.flatMap(_.split("\\s+")).map(word=>(word,1)).updateStateByKey((seq:Seq[Int],s:Option[Int])=>{
      val sum=seq.sum
      val lastVal=s.getOrElse(0)
      Option(sum+lastVal)
    }).print()

  }
}

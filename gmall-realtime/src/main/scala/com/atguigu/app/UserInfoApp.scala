package com.atguigu.app

import com.atguigu.constants.GmallConstant
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

//将用户表新增及变化数据缓存至Redis
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka用户主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO, ssc)

    //打印测试
    kafkaDStream.foreachRDD(rdd => {
      rdd.foreach(record => println(record.value()))
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

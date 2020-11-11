package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.atguigu.bean.UserInfo

//将用户表新增及变化数据缓存至Redis
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka用户主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO, ssc)

    //4.取出Value
    val userJsonDStream: DStream[String] = kafkaDStream.map(_.value())

    //5.将用户数据写入Redis
    userJsonDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.写库
        iter.foreach(userJson => {
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          val redisKey = s"UserInfo:${userInfo.id}"
          jedisClient.set(redisKey, userJson)
        })
        //c.归还连接
        jedisClient.close()
      })

    })

    //打印测试
    //    kafkaDStream.foreachRDD(rdd => {
    //      rdd.foreach(record => println(record.value()))
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

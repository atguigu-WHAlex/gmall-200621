package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject

//将OrderInfo与OrderDetail数据进行双流JOIN,并根据user_id查询Redis,补全用户信息
object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)

    //4.将数据转换为样例类对象并转换结构为KV
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.将value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time
      //c.给时间重新赋值
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //d.数据脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //e.返回结果
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //a.转换为样例类
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //b.返回数据
      (detail.order_id, detail)
    })

    //双流JOIN(普通JOIN)
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    value.print(100)

    //5.全外连接
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //6.处理JOIN之后的数据
    fullJoinDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放关联上的数据
      val details = new ListBuffer[SaleDetail]

      //遍历iter,做数据处理
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) =>

        val infoRedisKey = s"OrderInfo:$orderId"
        val detailRedisKey = s"OrderDetail:$orderId"

        if (infoOpt.isDefined) {
          //a.判断infoOpt不为空
          //取出infoOpt数据
          val orderInfo: OrderInfo = infoOpt.get

          //a.1 判断detailOpt不为空
          if (detailOpt.isDefined) {
            //取出detailOpt数据
            val orderDetail: OrderDetail = detailOpt.get
            //创建SaleDetail
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //添加至集合
            details :+ saleDetail
          }

          //a.2 将info数据写入redis,给后续的detail数据使用
          // val infoStr: String = JSON.toJSONString(orderInfo)//编译通不过
          import org.json4s.native.Serialization
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)

          //a.3

        } else {
          //b.判断infoOpt为空
        }

      }

      //归还连接
      jedisClient.close()

      //最终返回值
      details.iterator

    })


    //测试
    //    orderInfoKafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>println(record.value()))
    //    })
    //    orderDetailKafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>println(record.value()))
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

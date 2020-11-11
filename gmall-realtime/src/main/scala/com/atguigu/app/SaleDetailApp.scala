package com.atguigu.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstant
import com.atguigu.utils.{JdbcUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._
import org.json4s.native.Serialization

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
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放关联上的数据
      val details = new ListBuffer[SaleDetail]

      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

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
            details += saleDetail
          }

          //a.2 将info数据写入redis,给后续的detail数据使用
          // val infoStr: String = JSON.toJSONString(orderInfo)//编译通不过
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)
          jedisClient.expire(infoRedisKey, 100)

          //a.3
          if (jedisClient.exists(detailRedisKey)) {
            val detailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
            detailJsonSet.asScala.foreach(detailJson => {
              //转换为样例类对象,并创建SaleDetail存入集合
              val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              details += new SaleDetail(orderInfo, detail)
            })
          }

        } else {
          //b.判断infoOpt为空
          //获取detailOpt数据
          val orderDetail: OrderDetail = detailOpt.get

          if (jedisClient.exists(infoRedisKey)) {
            //b.1 查询Redis中有OrderInfo数据
            //取出Redis中OrderInfo数据
            val infoJson: String = jedisClient.get(infoRedisKey)
            //转换数据为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
            //创建SaleDetail存入集合
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            //b.2 查询Redis中没有OrderInfo数据
            val detailStr: String = Serialization.write(orderDetail)
            //写入Redis
            jedisClient.sadd(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }
        }

      }

      //归还连接
      jedisClient.close()

      //最终返回值
      details.iterator

    })

    //7.根据UserID查询Redis中的数据,补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //查询Redis补充信息
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        val userRedisKey = s"UserInfo:${saleDetail.user_id}"

        if (jedisClient.exists(userRedisKey)) {
          //查询数据
          val userJson: String = jedisClient.get(userRedisKey)
          //将用户数据转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          //补全信息
          saleDetail.mergeUserInfo(userInfo)
        } else {
          //Redis中没有查询到数据,访问MySQL
          val connection: Connection = JdbcUtil.getConnection
          //根据UserID查询MySQL用户信息
          val userStr: String = JdbcUtil.getUserInfoFromMysql(
            connection,
            "select * from user_info where id=?",
            Array(saleDetail.user_id))
          //将用户数据转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userStr, classOf[UserInfo])
          //补全信息
          saleDetail.mergeUserInfo(userInfo)
          //关闭连接
          connection.close()
        }

        //返回数据
        saleDetail
      })

      //归还连接
      jedisClient.close()

      //返回数据
      details
    })

    //8.将三张表JOIN的结果写入ES
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //IndexName
        val indexName = s"${GmallConstant.ES_SALE_DETAIL_INDEX_PRE}-${sdf.format(new Date(System.currentTimeMillis()))}"
        //准备数据集
        val detailIdToSaleDetail: List[(String, SaleDetail)] = iter.toList.map(saleDetail => (saleDetail.order_detail_id, saleDetail))
        //执行批量数据写入
        MyEsUtil.insertBulk(indexName, detailIdToSaleDetail)

      })

    })


    //测试
    //    saleDetailDStream.print(100)
    //测试
    //    noUserSaleDetailDStream.print(100)
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

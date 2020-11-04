package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstant
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.消费Kafka启动主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP, ssc)

    //4.将每一行数据转换为样例类对象,并补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      //a.获取Value
      val value: String = record.value()
      //b.取出时间戳字段
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      //c.将时间戳转换为字符串
      val dateHourStr: String = sdf.format(new Date(ts))
      //d.给时间字段重新赋值
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //e.返回数据
      startUpLog
    })

    //5.根据Redis进行跨批次去重
    val filterdByRedis: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)

    //    startLogDStream.cache()
    //    startLogDStream.count().print()
    //
    //    filterdByRedis.cache()
    //    filterdByRedis.count().print()

    //6.同批次去重(根据Mid)
    val filterdByMid: DStream[StartUpLog] = DauHandler.filterByMid(filterdByRedis)

    //    filterdByMid.cache()
    //    filterdByMid.count().print()

    //7.将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    DauHandler.saveMidToRedis(filterdByMid)

    //8.将去重之后的数据明细写入Pheonix
    filterdByMid.foreachRDD(rdd => {

      rdd.saveToPhoenix("GMALL200621_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //打印Value
    //    kafkaDStream.foreachRDD(rdd => {
    //      rdd.foreach(record => {
    //        println(record.value())
    //      })
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

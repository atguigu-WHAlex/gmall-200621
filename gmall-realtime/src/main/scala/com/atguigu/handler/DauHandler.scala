package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  /**
    * 同批次去重(根据Mid)
    *
    * @param filterdByRedis 经过Redis去重之后的数据集
    * @return
    */
  def filterByMid(filterdByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.转换数据结构 log ==> (mid_logDate,log)
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterdByRedis.map(log => ((log.mid, log.logDate), log))

    //2.按照Key分组
    val midDateToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

    //KV结构数据:   是否需要Key,数据是否需要压平
    //map            不需要Key,不需要压平
    //mapValues        需要Key,不需要压平
    //flatMap        不需要Key,需要压平
    //flatMapValues    需要Key,需要压平
    //3.对Value按照时间排序取第一条
    //    val midDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midDateToLogIterDStream.mapValues(iter => {
    //      iter.toList.sortWith(_.ts < _.ts).take(1)
    //    })
    //4.将集合压平
    //    midDateToLogListDStream.flatMap(_._2)

    midDateToLogIterDStream.flatMap { case ((mid, date), iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }

  }


  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 根据Redis进行跨批次去重
    *
    * @param startLogDStream 原始数据
    */
  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：使用filter单条数据过滤
    //    val value1: DStream[StartUpLog] = startLogDStream.filter(log => {
    //      //a.获取连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.判断数据是否存在
    //      val redisKey = s"DAU:${log.logDate}"
    //      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
    //      //c.归还连接
    //      jedisClient.close()
    //      //d.返回值
    //      !boolean
    //    })

    //方案二：分区内获取连接
    //    val value2: DStream[StartUpLog] = startLogDStream.transform(rdd => {
    //      rdd.mapPartitions(iter => {
    //        //a.获取连接
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        //b.过滤
    //        val logs: Iterator[StartUpLog] = iter.filter(log => {
    //          val redisKey = s"DAU:${log.logDate}"
    //          !jedisClient.sismember(redisKey, log.mid)
    //        })
    //        //c.归还连接
    //        jedisClient.close()
    //        //d.返回数据
    //        logs
    //      })
    //    })

    //方案三：一个批次获取一次连接,在Driver端获取数据广播至Executor端
    startLogDStream.transform(rdd => {
      //获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //获取mid数据
      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      //归还连接
      jedisClient.close()
      //封装广播变量
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //操作RDD做去重
      rdd.filter(log => {
        !midsBC.value.contains(log.mid)
      })
    })

    //    value1
    //    value2

  }


  /**
    * 将去重之后的数据中的Mid保存到Redis(为了后续批次去重)
    *
    * @param startLogDStream 经过2次去重之后的数据集
    */
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {

    startLogDStream.foreachRDD(rdd => {

      //使用分区操作,减少连接的获取与释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历写库
        iter.foreach(log => {
          val redisKey = s"DAU:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })

        //c.归还连接
        jedisClient.close()

      })

    })

  }

}

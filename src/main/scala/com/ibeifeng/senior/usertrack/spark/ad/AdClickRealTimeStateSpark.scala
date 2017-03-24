package com.ibeifeng.senior.usertrack.spark.ad

import java.util.Properties

import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.jdbc.JDBCHelper
import com.ibeifeng.senior.usertrack.spark.util.{SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.util.{Failure, Random, Success, Try}

/**
  * 广告实时流量分析，是一个SparkStreaming程序，不会存在taskID的参数
  * Created by ibf on 03/19.
  */
object AdClickRealTimeStateSpark {
  // 数据分隔符
  val delimeter = " "
  // 是否是本地执行
  var isLocal = false

  def main(args: Array[String]): Unit = {

    //HBaseConfiguration

    // 一、 创建上下文
    val (appName, sc, ssc) = {
      // 1. 获取相关环境变量
      isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
      val appName = Constants.SPARK_APP_NAME_AD
      // 2. 获取SparkConf上下文配置对象
      val conf = SparkConfUtil.generateSparkConf(appName, isLocal)
      conf.set("spark.streaming.blockInterval", "1s")
      // 3. 创建SparkContext对象
      val sc = SparkContextUtil.getSparkContext(conf)
      // 4. 创建StreamingContext
      val ssc = new StreamingContext(sc, Seconds(30))
      // 5. 由于程序中会使用到updateStateByKey API，所以需要设置checkpointdir
      val path = "/beifeng/spark-project/streaming/checkpoint/AdClickRealTimeStateSpark"
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(path), true)
      ssc.checkpoint(path)
      // 6. 结果返回
      (appName, sc, ssc)
    }

    // 二、Kafka集成形成DStream
    val dstream: DStream[String] = {
      //1. 初始化Kafka配置信息
      val kafkaParams = Map(
        "zookeeper.connect" -> ConfigurationManager.getProperty(Constants.KAFKA_ZOOKEEPER_URL),
        "group.id" -> s"${appName}",
        "zookeeper.connection.timeout.ms" -> "10000")
      val topicNames = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
      val topics = topicNames
        .split(",")
        .map(v => {
          Try {
            val arr = v.split(":")
            (arr(0).trim, arr(1).trim.toInt)
          }
        })
        .filter(_.isSuccess)
        .map(_.get)
        .toMap

      // 2. 构建DStream ===> topics中元素(元组)的第二个值是给定线程数量，必须是大于等于1
      val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_2)

      // 3. 数据转换(只要value数据，key不需要)并返回DStream
      dstream.map(_._2)
    }

    // 三、数据格式转换
    /**
      * 在真实项目中，这一部分代码可能会比较多，因为kafka中的数据可能还需要进行必要的数据格式转换
      **/
    val formattedDStream = this.formatAdRealTimeDStreamData(dstream)

    // 四、黑名单的更新操作
    dynamicUpdateBlackList(formattedDStream)

    // 五、过滤黑名单用户数据
    val filteredDStream = this.filterByBlackList(formattedDStream)

    // 六、实时累加广告点击量
    /**
      * 实时累加计算 每天 每个省份 每个城市 各个广告的点击量
      * 一条数据就算一次，不涉及去重
      * 返回值类型类型是:DStream[((date,省份,城市,广告id),点击量)]
      **/
    val aggregatedDStream = this.calculateRealTimeState(filteredDStream)

    // 七、获取各个省份Top5的累加广告点击量结果
    calculateProvinceTop5Ad(aggregatedDStream)

    // 八、分析最近一段时间广告流量点击情况
    /**
      * 实时统计最近10分钟的某个广告点击数量
      * 使用window进行分析的一个窗口分析函数(窗口聚合函数)
      * -1. 窗口大小
      * window interval： 10 * 60 = 600s
      * -2. 执行批次
      * slider interval： 1 * 60 = 60s
      * 必须是父DStream的slider interval的整数倍(DStream默认的slider interval就是创建StreamingContext的时候给定的batchInterval)
      * 需要的字段信息：
      * 时间字符串(批次产生时间)、广告id、点击次数
      * 主键：时间字符串 + 广告id
      * 在当前批次来讲，广告id是区分数据的唯一主键
      */
    calculateAdClickCountByWindow(filteredDStream)

    // 九、启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 获取连接jdbc的资源对象
    **/
  lazy val fetchJDBCProperties: (String, Properties) = {
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val props = new Properties()
    props.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    props.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    (url, props)
  }

  /**
    * 格式化广告点击数据
    *
    * @param dstream 一行一条点击数据
    * @return
    */
  def formatAdRealTimeDStreamData(dstream: DStream[String]): DStream[AdClickRecord] = {
    dstream
      .map(line => {
        // a. 按照给定的分隔符进行数据分割(数据必须都不为空)
        val arrs = line.split(this.delimeter).map(_.trim).filter(_.nonEmpty)

        // b. 对符合数据格式要求的数据进行数据转换操作
        if (arrs.length == 5) {
          // TODO: 在实际开发中，这里可能存在一些必要的转换操作，主要根据业务来定
          // 这里简化操作
          Some(
            AdClickRecord(
              arrs(0).toLong,
              arrs(1),
              arrs(2),
              arrs(3).toInt,
              arrs(4).toInt
            )
          )
        } else {
          // 异常数据，需要丢弃，直接返回一个None
          None
        }
      })
      .filter(_.isDefined)
      .map(_.get)

    /**
      * val s: DStream[AdClickRecord] = dstream.flatMap(line => {
      * // a. 按照给定的分隔符进行数据分割(数据必须都不为空)
      * val arrs = line.split(this.delimeter).map(_.trim).filter(_.nonEmpty)
      *
      * // b. 对符合数据格式要求的数据进行数据转换操作
      * val result: Iterator[AdClickRecord] = if (arrs.length == 5) {
      * // TODO: 在实际开发中，这里可能存在一些必要的转换操作，主要根据业务来定
      * // 这里简化操作
      *Iterator.single(
      * AdClickRecord(
      * arrs(0).toLong,
      * arrs(1),
      * arrs(2),
      * arrs(3).toInt,
      * arrs(4).toInt
      * )
      * )
      * } else {
      * // 异常数据，需要丢弃，直接返回一个None
      * Iterator.empty
      * }
      * *
      *
      * 结果返回
      * result.toIterable
      *
      * }
      * *
      * )
      **/

    // TODO: 考虑优化？当RDD的map、filter、flatMap连续存的的时候，最好换成只调用一个API
  }

  /**
    * 基于用户点击广告数据进行黑名单更新操作，将黑名单数据写出到RDBMs中
    *
    * @param dstream
    */
  def dynamicUpdateBlackList(dstream: DStream[AdClickRecord]): Unit = {
    /**
      * 规则：
      * --0. 最近10分钟用户广告点击次数超过100次
      * --1. 黑名单列表每隔3分钟更新一次
      * --2. 如果一个用户被添加到黑名单中，在程序判断中，该用户永远是黑名单用户；除非工作人员人工干预，手动删除该用户黑名单的标记
      * --3. 支持白名单(白名单中的用户不管点击多少次，都不算是黑名单中存在的)
      * ====>
      * 使用DStream的窗口分析函数进行数据更新操作
      * a. 统计用户点击广告的次数
      * 次数统计规则：一条数据就算一次
      * b. 过滤少于100的数据
      * c.支持白名单用户过滤
      * 白名单用户有工作人员添加到RDBMs中，过滤过程中需要读取数据
      **/
    val blackListUserIDDStream: DStream[(Int, Int)] = dstream
      .map(record => (record.userId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Minutes(10), // 窗口大小，数据范围是10分钟
        Minutes(3) // 批次/执行间隔大小，批次产生的间隔时间是3分钟
      )
      .filter(tuple => tuple._2 > 100)
      .transform(rdd => {
        // 在这里进行白名单用户数据过滤，过滤之后的数据就是黑名单用户
        // TODO: 作业-->自己实现读取RDBMs中的白名单用户
        // 这里使用模拟数据
        // 数据量不会特别大，主要包含用户id ==> 可以使用广播变量的形式进行数据过滤
        // 1. 获取白名单
        val sc = rdd.sparkContext
        val whiteListRDD = sc.parallelize(0 until 100)
        val broadcastOfWhiteList = sc.broadcast(whiteListRDD.collect())

        // 2. 数据过滤
        // 方式一：基于广播变量进行数据过滤
        rdd.filter(tuple => !broadcastOfWhiteList.value.contains(tuple._1))
        // 方式二：基于Left Join进行数据过滤 ==> 可能存储数据shuffle的过程
        /*rdd
          .leftOuterJoin(whiteListRDD.map((_, 1)))
          .filter(tuple => tuple._2._2.isEmpty)
          .map(tuple => (tuple._1, tuple._2._1))*/
      })

    // 2. 将黑名单用户写入到关系型数据库中
    /**
      * DStream数据写出的方式
      * 1. 直接使用DStream的写出相关API进行数据输出操作, eg： dstream.saveXXX （仅支持输出到HDFS）
      * 2. 将DStream转换为RDD或者DataFrame进行数据输出操作，主要使用方法foreachPartition;
      * TODO: SparkStreaming中一个批次一定/永远只存在一个RDD(不管是普通类型还是window类型)
      **/
    blackListUserIDDStream.foreachRDD(rdd => {
      // 采用RDD调用foreachParitionAPI的形式进行数据输出操作
      rdd.foreachPartition(iter => {
        // 一. 获取数据库管理对象
        val jdbcHelper = JDBCHelper.getInstance()

        // 二. 数据输出
        Try {
          // 1. 获取连接
          val conn = jdbcHelper.getConnection
          val oldAutoCommit = conn.getAutoCommit
          conn.setAutoCommit(false)

          // 2. 获取statement
          val sql = "INSERT INTO tb_black_users(`user_id`,`count`) VALUES(?,?) ON DUPLICATE KEY UPDATE `count`=VALUES(`count`)"
          val pstmt = conn.prepareStatement(sql)

          // 3. 数据输出/设置
          var recordCount = 0
          iter.foreach {
            case (userID, count) => {
              // 3.1 设置输出值
              pstmt.setInt(1, userID)
              pstmt.setInt(2, count)

              // 3.2 添加批次
              pstmt.addBatch()
              recordCount += 1

              // 3.3 当批次达到的时候进行提交操作
              if (recordCount % 500 == 0) {
                pstmt.executeBatch()
                conn.commit()
              }
            }
          }

          // 4. 关闭操作
          pstmt.executeBatch()
          conn.commit()
          pstmt.close()

          // 5. 返回结果
          (oldAutoCommit, conn)
        } match {
          case Success((oldAutoCommit, conn)) => {
            Try(conn.setAutoCommit(oldAutoCommit))
            Try(jdbcHelper.returnConnection(conn))
          }
          case Failure(exception) => {
            throw exception
          }
        }
      })
    })
  }

  /**
    * 根据黑名单用户进行数据过滤操作
    *
    * @param dstream
    * @return
    */
  def filterByBlackList(dstream: DStream[AdClickRecord]): DStream[AdClickRecord] = {
    // 将DStream数据的过滤转换为RDD数据的过滤
    dstream.transform(rdd => {
      // 一. 读取存储在关系型数据库中的黑名单信息
      val blackListRDD: RDD[(Int, Int)] = {
        // 1. 读取配置信息
        val (url, props) = fetchJDBCProperties
        val table = "tb_black_users"
        // 2. 创建上下文
        val sc = rdd.sparkContext
        val sqlContext = SQLContextUtil.getInstance(sc, integratedHive = !isLocal)
        // 3. 读取数据库数据，并形成RDD返回
        sqlContext
          .read
          .jdbc(url, table, props)
          .map(row => {
            val userID = row.getAs[Int]("user_id")
            val count = row.getAs[Int]("count")
            (userID, count)
          })
      }

      // 二、数据过滤（方式两种：Map端的过滤和Reduce端的过滤）
      /**
        * Map端的过滤===>利用广播变量进行数据过滤
        * Reduce端的数据过滤 ===> 利用LeftOuterJoin后rdd的数据进行过滤
        **/
      /**
        * 步：
        * a. 将RDD转换为key/value键值对，方便按照key进行数据join
        * b. 调用leftOuterJoin/RightOuterJoin
        * c. 调用filter过滤数据
        * d. map数据转换
        **/
      val fiteredRDD = rdd
        .map(record => (record.userId, record))
        .leftOuterJoin(blackListRDD)
        .filter {
          case (userID, (record, option)) => {
            // 当option为Some类型的时候，表示当前userID在blackListRDD中出现了；否则表示没有出现
            // 最终期望结果是用户在黑名单中没有出现 ==> 要求option为None
            option.isEmpty
          }
        }
        .map {
          case (_, (record, _)) => record
        }

      // 三、返回结果
      fiteredRDD
    })
  }

  /**
    * 实时累加计算 每天 每个省份 每个城市 各个广告的点击量
    * 一条数据就算一次，不涉及去重
    * 返回值类型类型是:DStream[((date,省份,城市,广告id),点击量)]
    *
    * @param dstream
    * @return
    */
  def calculateRealTimeState(dstream: DStream[AdClickRecord]): DStream[((String, String, String, Int), Long)] = {
    // 1. 将dstream转换为key.value键值对
    val mappedDStream = dstream.map {
      case AdClickRecord(timestamp, province, city, userId, adId) => {
        // a. 根据timestamp获取实际格式字符串, 格式为:yyyyMMdd
        val date = DateUtils.parseLong2String(timestamp, DateUtils.DATEKEY_FORMAT)
        // b. 返回结果
        ((date, province, city, adId), 1)
      }
    }

    // 2. 累加计算结果值 ===> 需要考虑使用updateStateByKey API
    /**
      * updateStateByKey: 随着数据规模的执行时间延续，结果数据会越来越大，对性能会有一定的影响(某些不会出现的key，updateStateByKey会进行保存)； 内部通过返回None，表示不缓存该key的数据
      * mapWithState: 可以缓解updateStateByKey API的问题
      * TODO: 作业 --> 测试各种不同返回值以及长时间、大数据量运行下是否会存在问题
      **/
    val aggregatedDStream = mappedDStream
      .reduceByKey(_ + _)
      .updateStateByKey(
        (values: Seq[Int], state: Option[(Long, Int)]) => {
          // 1. 获取当前key传递的值
          val currentValue = values.sum

          // 2. 获取状态值
          val preValue = state.getOrElse((0L, 0))._1
          val preState = state.getOrElse((0L, 0))._2


          // 3. 更新状态值
          if (currentValue == 0) {
            // 没有新数据
            if (preState > 10) {
              // 连续10个批次，该key都没有新数据
              // preState要正确，要保证数据过期对数据的累加计算没有营销
              None
            } else {
              Some((preValue + currentValue, preState + 1))
            }
          } else {
            // 有新数据
            Some((preValue + currentValue, 0))
          }
        }
      )
      .map {
        case ((date, province, city, adID), (count, _)) => {
          ((date, province, city, adID), count)
        }
      }

    // 3. 数据输出到关系型数据库
    /**
      * 表结构
      * 名称：tb_ad_real_time_state
      * 字段:
      * date 日期
      * province 省份
      * city 城市
      * ad_id 广告id
      * click_count 点击次数
      * 其中(date,province,city,ad_id)为主键
      * 数据插入方式Insert Or Update
      **/
    /*aggregatedDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        // TODO: 作业自己实现
      })
    })*/
    aggregatedDStream.print(5)

    // 4. 结果返回
    aggregatedDStream
  }

  /**
    * 计算各个省份Top5的热门广告数据
    * 需要考虑的字段：
    * 省份、广告id、时间 --> 点击量
    *
    * @param dstream
    */
  def calculateProvinceTop5Ad(dstream: DStream[((String, String, String, Int), Long)]): Unit = {
    // 1. 数据转换，获取需要的字段信息
    val mappedDStream = dstream.map {
      case ((date, province, _, adID), clickCount) => {
        ((date, province, adID), clickCount)
      }
    }

    // 2. 计算点击量  ((date, province, adID), clickCount)
    val dailyAdClickCountByProvinceDStream: DStream[((String, String, Int), Long)] = mappedDStream.reduceByKey(_ + _)

    // 3. 获取每个省份Top5的数据
    /**
      * 该问题属于分组排序TopN的程序，DStream的数据获取转换为RDD操作即可
      * RDD实现方式：两阶段聚合==> 如果不考虑数据倾斜的问题，不需要两阶段聚合
      * a. 添加前缀，随机数据
      * b. 按照key进行数据聚合
      * c. 对每组数据进行数据处理(局部)
      * d. 继续对全局数据进行聚合
      * e. 对每组数据进行数据处理(全局)
      **/
    val top5ProvinceAdClickCountDStream = dailyAdClickCountByProvinceDStream.transform(rdd => {
      rdd
        .mapPartitions(iter => {
          val random = Random
          iter.map {
            case ((date, province, adId), clickCount) => {
              ((random.nextInt(100), date, province), (adId, clickCount))
            }
          }
        })
        .groupByKey()
        .flatMap {
          case ((_, date, province), iter) => {
            // iter中获取Top5的数据 ==> 局部数据操作
            val top5AdIdAndCountIter = iter
              .toList
              .sortBy(_._2)
              .takeRight(5)
            // 数据转换输出
            top5AdIdAndCountIter.map(tuple => ((date, province), tuple))
          }
        }
        .groupByKey()
        .flatMap {
          case ((date, province), iter) => {
            // iter中获取Top5的数据 ==> 局部数据操作
            val top5AdIdAndCountIter = iter
              .toList
              .sortBy(_._2)
              .takeRight(5)
            // 数据转换输出
            top5AdIdAndCountIter.map(tuple => ((date, province), tuple))
          }
        }
    })

    // 4. 结果输出
    // TODO: 作业 --> 自己实现将数据输出到JDBC的代码
    /**
      * 输出的表结构：
      * 名称: tb_top5_province_ad_click_count
      * 字段：
      * date 日期
      * province 城市
      * ad_id 广告id
      * click_count 点击次数
      * 插入方式：Insert Or Update
      **/
    top5ProvinceAdClickCountDStream.print(5)
  }

  /**
    * 实时统计最近10分钟的某个广告点击数量
    * 使用window进行分析的一个窗口分析函数(窗口聚合函数)
    * -1. 窗口大小
    * window interval： 10 * 60 = 600s
    * -2. 执行批次
    * slider interval： 1 * 60 = 60s
    * 必须是父DStream的slider interval的整数倍(DStream默认的slider interval就是创建StreamingContext的时候给定的batchInterval)
    * 需要的字段信息：
    * 时间字符串(批次产生时间)、广告id、点击次数
    * 主键：时间字符串 + 广告id
    * 在当前批次来讲，广告id是区分数据的唯一主键
    *
    * @param dstream
    */
  def calculateAdClickCountByWindow(dstream: DStream[AdClickRecord]): Unit = {
    // 1. 获取计算需要的字段信息
    val mappedDStream = dstream.map(record => (record.adId, 1))
    // 2. 数据聚合 ==> 使用窗口分析函数进行数据聚合
    /**
      * TODO: 作业 --> 测试一下reduceByKeyAndWindow api中六种实现方式优缺点以及各个参数的作用
      **/
    val aggDStream = mappedDStream.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a + b,
      Minutes(10),
      Minutes(1)
    )
    // 3. 添加执行批次时间字符串 ==> 数据转换操作
    val finaledDStream = aggDStream.transform((rdd, time) => {
      // 将time添加到rdd中 ==> rdd数据格式转换
      val dateStr = DateUtils.parseLong2String(time.milliseconds, "yyyyMMddHHmmss")
      rdd.map {
        case (adId, clickCount) => {
          (dateStr, adId, clickCount)
        }
      }
    })

    // 4. 数据结果保存
    /**
      * 表名称: tb_ad_click_count_of_window
      * 字段：
      * date: 时间格式字符串
      * ad_id: 广告点击id
      * click_count：点击基础
      * 数据插入方式：Insert Or Error
      * TODO: 作业 -- 就是一个普通的数据插入操作
      **/
    finaledDStream.print(5)

  }
}

case class AdClickRecord(
                          timestamp: Long,
                          province: String,
                          city: String,
                          userId: Int,
                          adId: Int
                        )
package com.ibeifeng.senior.usertrack.spark.product

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ibeifeng.senior.usertrack.conf.ConfigurationManager
import com.ibeifeng.senior.usertrack.constant.Constants
import com.ibeifeng.senior.usertrack.dao.factory.DAOFactory
import com.ibeifeng.senior.usertrack.jdbc.JDBCHelper
import com.ibeifeng.senior.usertrack.mock.MockDataUtils
import com.ibeifeng.senior.usertrack.spark.session.UserVisitSessionRecord
import com.ibeifeng.senior.usertrack.spark.util.{SQLContextUtil, SparkConfUtil, SparkContextUtil}
import com.ibeifeng.senior.usertrack.util.ParamUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

/**
  * SparkSQL实现Top10各区域热门商品统计
  * Created by ibf on 03/18.
  */
object AreaTop10ProductSpark {
  def main(args: Array[String]): Unit = {
    // 一、获取参数==> 和SparkCore的相关应用类似
    val (taskID, taskParam) = {
      // 1. 从args中获取传入的taskID的值
      val taskID = ParamUtils.getTaskIdFromArgs(args)
      /**
        * 程序和关系型数据库打交道(交互), 可选方案很多：
        * 1. 直接JDBC进行数据操作
        * 2. 使用第三方的工具：MyBatis.....
        **/
      // 2. 从数据库获取taskID对应的task对象
      val task = if (taskID == null) {
        throw new IllegalArgumentException(s"参数异常，无法解析task id：${args.mkString("[", ",", "]")}")
      } else {
        // 2.1 创建数据库连接对象
        val taskDao = DAOFactory.getTaskDAO
        // 2.2 数据库交互获取数据库中对应taskID的task对象, 如果数据不存在，返回的是一个空的task对象
        taskDao.findByTaskId(taskID)
      }
      // 3. 从task中获取任务参数
      val taskParam: JSONObject = ParamUtils.getTaskParam(task)
      // 4. 数据过滤
      if (taskParam == null || taskID != task.getTaskId) {
        throw new IllegalArgumentException(s"无法获取数据过滤参数，或者taskID在数据库中不存在:${taskID}")
      }
      // 5. 返回结果
      (taskID, taskParam)
    }

    // 二、创建上下文
    // a. 创建上下文
    val (sc, sqlContext) = {
      // 0. 创建必要的属性
      val appName = Constants.SPARK_APP_NAME_PRODUCT + taskID
      val isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
      // 1. 创建SparkConf上下文配置信息
      val conf = SparkConfUtil.generateSparkConf(appName, isLocal)
      // 2. 创建SparkContext上下文对象
      val sc = SparkContextUtil.getSparkContext(conf)
      // 3. 创建SQLContext对象
      /**
        * 实际开发中，数据一般在hive中，需要集成hive环境
        * 在开发测试运行中，连接hive比较麻烦，这个时候采用模拟数据进行测试
        * ==> 本地情况不集成hive，集群运行集成hive
        * NOTE: 这里由于在后面使用到row_number分析函数，所以必须创建的是HiveContext对象；
        * 注意点：
        * -1. 可选，将hive-site.xml复制到resources文件夹中(如果配置了metastore，需要启动服务；如果没有配置，需要在pom文件中添加jdbc驱动《hive元数据管理数据库的连接驱动》){前提是不访问Hive中的数据}
        * -2. 将项目放到一个没有中文的文件夹中执行（hive使用默认内置的数据库derby，会在当前项目文件夹下创建一个子文件夹metastore_db）
        **/
      val sqlContext = SQLContextUtil.getInstance(sc, true, (sc: SparkContext, sqlContext: SQLContext) => {
        // 当时本地值的时候，需要创建模拟数据
        if (isLocal) {
          // 创建模拟数据
          MockDataUtils.mockData(sc, sqlContext)
          // 加载ProductInfo的数据
          MockDataUtils.loadProductInfoMockData(sc, sqlContext)
        }
      })
      // 4. 返回对象
      (sc, sqlContext)
    }
    // b. 将SQLContext的隐式转换导入
    import sqlContext.implicits._
    // c. 自定义函数
    def isEmpty(str: String): Boolean = str == null || "".equals(str.trim) || "null".equalsIgnoreCase(str.trim)
    // UDF自定义
    sqlContext.udf.register("isNotEmpty", (str: String) => !isEmpty(str))
    sqlContext.udf.register("concat_int_string", (id: Int, name: String) => s"${id}:${name}")
    sqlContext.udf.register("get_string_from_json", (json: String, field: String) => {
      JSON.parseObject(json).getString(field)
    })
    // UDAF自定义
    sqlContext.udf.register("group_contact_distinct", GroupConcatDistinctUDAF)

    // 三、业务代码编写
    // 1. 执行参数过滤，获取过滤后的用户行为数据==>类似SparkCore项目中的数据过滤
    // SparkSQL集成Hive
    val actionDataFrame = this.getActionDataFrameByFilter(sqlContext, taskParam)

    // 2. 读取RDBMs中的城市信息，形成城市信息数据
    // SparkSQL读取外部数据源
    val cityInfoDataFrame = this.getCityInfoDataFrame(sqlContext)

    // 3. 将用户行为数据和城市信息数据进行join操作，并且注册成为临时表
    // 不同源的数据直接进行join操作
    generateTempProductBasicTable(sqlContext, actionDataFrame, cityInfoDataFrame)

    // 4. 统计各个区域各个商品的点击次数，并注册成为临时表
    // 数据聚合(UDF和UDAF的使用方式)
    generateTempAreaProductCountTable(sqlContext)

    // 5. 获取各个区域Top10的点击数据，并注册成为临时表
    // 分组排序TopN ==> row_number的使用
    generateTempAreaTop10ProductCountTable(sqlContext)

    // 6. 将Top10的结果数据和商品表进行关联，得到具体的商品信息，并注册成为临时表
    // case when语句和if函数的使用方式
    generateTempAreaTop10CountFullProductTable(sqlContext)

    // 7. 持久化保存在临时表中的结果数据到关系型数据库中
    // DataFrame数据写出JDBC如何实现
    persistAreaTop10CountFullProductData(sqlContext, taskID)

    Thread.sleep(100000000L)
  }

  /**
    * 按照给定任务条件进行数据过滤，代码和SparkCore应用的参数过滤类似
    *
    * @param sqlContext
    * @param taskParam
    * @return
    */
  def getActionDataFrameByFilter(sqlContext: SQLContext, taskParam: JSONObject): DataFrame = {
    // TODO: 将过滤参数简化为只有date字段的过滤
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    // 构建sql
    val sql =
    s"""
       |SELECT
       |  uva.city_id, uva.click_product_id
       |FROM
       |  user_visit_action uva
       |WHERE
       |  isNotEmpty(uva.click_product_id)
       |  ${startDate.map(date => s" AND uva.date >='${date}'").getOrElse("")}
       |  ${endDate.map(date => s" AND uva.date <= '${date}'").getOrElse("")}
      """.stripMargin
    println(s"==================\n${sql}\n==================")

    // 获取DataFrame
    sqlContext.sql(sql)
  }

  /**
    * 获取保存在RDBMs中的城市信息数据，并形成DataFrame返回
    * 实现方式：
    * 一、通过自定义InputFormat, 通过SparkContext的相应的API进行数据读取
    * 二、通过SparkSQL的read编程模型，SparkSQL中的read数据读取器提供读取RDBMs中的数据
    * TODO: 作业--->SparkSQL通过read的jdbc来读取RDBMs中数据的时候，三种接口的区别??
    *
    * @param sqlContext
    * @return
    */
  def getCityInfoDataFrame(sqlContext: SQLContext): DataFrame = {
    // 1. 根据相关配置信息获取jdbc的连接信息
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val table = "city_info"
    val props = new Properties()
    props.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    props.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))

    // 2. 构建DataFrame并返回
    val df = sqlContext
      .read
      .jdbc(url, table, props)

    // 3. 将DataFrame返回
    df
  }

  /**
    * 合并两个DataFrame的数据，并将合并后的数据保存为临时表
    *
    * @param sqlContext
    * @param action   用户行为信息数据，必须包含:city_id和click_product_id两列
    * @param cityInfo 城市信息表，必须包含:city_id、city_name、area三列
    */
  def generateTempProductBasicTable(sqlContext: SQLContext, action: DataFrame, cityInfo: DataFrame): Unit = {
    /*
    * 这里可以考虑一下，如果不使用DataFrame的DSL语言是否有其它方式?
    * 1、DSL编程
    * 2、将DataFrame转换为RDD，RDD进行数据join，join后的结果转换为DataFrame，并注册成为临时表
    * 3、将DataFrame注册成为临时表，然后同HQL语句进行数据Join操作
    * */
    import sqlContext.implicits._
    // 1. 数据的Join
    //    action.join(cityInfo) // 当两个DataFrame中存在相关的字段的时候（名称）, 而且又是按照相同字段进行join的时候，当前的调用方式可以
    val joinDataFrame = action
      .toDF("ci", "click_product_id")
      .join(cityInfo, $"ci" === $"city_id")

    // 2. 对数据进行提取操作
    val selectedDataFrame = joinDataFrame.select("city_id", "click_product_id", "city_name", "area")

    // 3. 注册成为临时表
    selectedDataFrame.registerTempTable("tmp_product_basic")
  }

  /**
    * 从临时表tmp_product_basic中读取数据，并聚合结果得到各个区域 各个商品的点击次数
    *
    * @param sqlContext
    */
  def generateTempAreaProductCountTable(sqlContext: SQLContext): Unit = {
    /**
      * 各个区域、各个商品的点击次数?
      * 各个区域是否需要包含具体的地址信息(city_id和city_name)
      * eg：
      * 华东区 101 上海
      * 华东区 102 杭州
      * 华东区 103 苏州
      * .........
      *
      * 期望结果：
      * 华东区 101:上海:number,102:杭州:number,103:苏州:number.......
      *
      * SparkSQL自定义函数支持两类：
      * UDF: 一个输入一个输出，普通函数
      * UDAF: 一组输入，一个输出，用于groupBy情况的函数
      *
      * 这里的数据聚合分为两步操作来做：
      * -1. 将id和name合并 ==> UDF函数
      * concat_int_string(city_id,city_name)
      * -2. 将合并之后的数据按照area分组后，进行数据的聚合操作；如果一个城市出现多次，只计算一次，但是记录下该城市的触发次数
      * number的计算：出现多少次(相同值)，number的值就是多少次
      * group_contact_distinct(concat_string)
      **/
    // 1. 创建sql语句
    val sql =
    """
      |SELECT
      |  area, click_product_id,
      |  COUNT(1) AS click_count,
      |  group_contact_distinct(concat_int_string(city_id, city_name)) AS city_infos
      |FROM
      |  tmp_product_basic
      |GROUP BY area, click_product_id
    """.stripMargin

    // 2. 执行sql语句，得到DataFrame
    val df = sqlContext.sql(sql)

    // 3. 将DataFrame注册成为临时表
    df.registerTempTable("tmp_area_product_count")
  }

  /**
    * 从临时表tmp_area_product_count获取各个区域访问次数最多的前10个商品
    * ---> 分组TopN的一个程序
    * ---> 在SparkSQL中使用row_number来实现
    * ---> 由于row_number是Hive中的一个函数，所以要求sqlContext对象必须是HiveContext类型
    *
    * @param sqlContext
    */
  def generateTempAreaTop10ProductCountTable(sqlContext: SQLContext): Unit = {
    // 1. 构建sql语句
    val sql =
    """
      |SELECT
      |  tmp.area, tmp.click_product_id,
      |  tmp.click_count, tmp.city_infos
      |FROM(
      |  SELECT
      |    area, click_product_id, click_count, city_infos,
      |    ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rnk
      |  FROM
      |    tmp_area_product_count
      |) AS tmp
      |WHERE tmp.rnk <= 10
    """.stripMargin

    // 2. 获取DataFrame对象
    val df = sqlContext.sql(sql)

    // 3. DataFrame注册成为临时表
    df.registerTempTable("tmp_area_top10_product_count")
  }

  /**
    * 将临时表tmp_area_top10_product_count和商品信息表product_info进行数据join操作，并将最终结果数据保存为临时表
    *
    * @param sqlContext
    */
  def generateTempAreaTop10CountFullProductTable(sqlContext: SQLContext): Unit = {
    /**
      * 解析商品扩展信息，判断商品属于第三方商品还是自营商品
      * 0：自营商品
      * 1：第三方商品
      *
      * 数据存储在字段extend_info中，数据格式为json格式 ==> 获取数据，并判断数据
      * 使用if函数进行数据判断操作，语法为：
      * if(condition, true-value, false-value): 当条件condition为true的时候，返回值为true-value；当condition执行结果为false的时候，返回值为false-value
      **/
    /**
      * area: 华东、华南、华中、华北、西北、西南、东北
      *
      * 地域级别划分为：
      * 华东、华南、华北 ==> A
      * 东北 ==> B
      * 华中、西南 ==> C
      * 西北 => D
      * 其它 => E
      *
      * 使用case when语句
      **/
    // 1. 构建sql语句
    val sql =
    """
      | SELECT
      |   tatpc.area,
      |   CASE
      |     WHEN tatpc.area = "华东" OR tatpc.area = "华南" OR tatpc.area = "华北" THEN "A"
      |     WHEN tatpc.area = "东北" THEN "B"
      |     WHEN tatpc.area = "华中" OR tatpc.area = "西南" THEN "C"
      |     WHEN tatpc.area = "西北" THEN "D"
      |     ELSE "E"
      |   END AS area_level,
      |   tatpc.click_product_id AS product_id,
      |   tatpc.city_infos,
      |   tatpc.click_count,
      |   pi.product_name,
      |   if(get_string_from_json(pi.extend_info, "product_type") == "0", "自营商品", "第三方商品") AS product_type
      | FROM
      |   tmp_area_top10_product_count tatpc
      |   JOIN product_info pi ON tatpc.click_product_id = pi.product_id
    """.stripMargin

    // 2. 执行sql语句
    val df = sqlContext.sql(sql)

    // 3. 注册成为临时表
    df.registerTempTable("tmp_area_top10_full_product")
  }

  /**
    * 将临时表tmp_area_top10_full_product中的数据持久化到关系型数据库中
    *
    * @param sqlContext
    * @param taskID
    */
  def persistAreaTop10CountFullProductData(sqlContext: SQLContext, taskID: Long): Unit = {
    // DataFrame数据写出到关系型数据库主要两种方式
    // 方式一：直接调用DataFrame的write编程模式将数据写出(jdbc接口， eg: df.write.jdbc)
    // 方式二：将DataFrame转换为RDD，RDD数据输出到RDBMs(foreachPartition或者直接自定义OutputFormat{调用RDD的API是saveAsNewAPIHadoopDataset})
    /**
      * 这里采用方式二进行数据输出，原因是程序输出数据需要实现Insert or Update的操作
      * Insert or Update: 当不存在的时候进行插入操作，当存在的时候进行更新操作(存放与否根据主键进行判断)
      * TODO: 作业 --> 各种不同数据库中(Oracle\MySQL\SQLServer...)， 实现Insert or Update的方式(策略)
      **/
    // 1. 读取临时数据数据形成DataFrame
    val df = sqlContext.sql(
      """
        |SELECT
        |  area, area_level, product_id, city_infos,
        |  click_count, product_name, product_type
        |FROM tmp_area_top10_full_product
      """.stripMargin)

    // 2. 转换为RDD然后调用foreachPartition API进行数据输出 ===> 直接调用DataFrame的foreachPartition
    df.foreachPartition(iter => {
      // a. 获取JDBC管理器
      val jdbcHelper = JDBCHelper.getInstance()

      // b. 设置数据输出代码
      Try {
        // 1. 获取connection
        val conn = jdbcHelper.getConnection
        val oldAutoCommit = conn.getAutoCommit
        conn.setAutoCommit(false)
        // 2. 创建Statement对象
        val sql = "INSERT INTO tb_area_top10_product(`task_id`,`area`,`product_id`,`area_level`,`count`, `city_infos`, `product_name`, `product_type`) VALUES(?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `area_level`=VALUES(`area_level`), `count`=VALUES(`count`), `city_infos`=VALUES(`city_infos`), `product_name`=VALUES(`product_name`), `product_type`=VALUES(`product_type`)"
        val pstmt = conn.prepareStatement(sql)
        var recordCount = 0
        // 3. 对数据进行迭代输出操作
        iter.foreach(row => {
          val area = row.getAs[String]("area")
          val areaLevel = row.getAs[String]("area_level")
          val productID = row.getAs[String]("product_id").toLong
          val clickCount = row.getAs[Long]("click_count")
          val cityInfos = row.getAs[String]("city_infos")
          val productName = row.getAs[String]("product_name")
          val productType = row.getAs[String]("product_type")

          // 设置参数
          pstmt.setLong(1, taskID)
          pstmt.setString(2, area)
          pstmt.setLong(3, productID)
          pstmt.setString(4, areaLevel)
          pstmt.setLong(5, clickCount)
          pstmt.setString(6, cityInfos)
          pstmt.setString(7, productName)
          pstmt.setString(8, productType)

          // 添加批
          pstmt.addBatch()
          recordCount += 1

          // 提交
          if (recordCount % 500 == 0) {
            pstmt.executeBatch()
            conn.commit()
          }
        })
        // 4. 进行连接关闭操作
        pstmt.executeBatch()
        conn.commit()

        // 5. 返回结果
        (oldAutoCommit, conn)
      } match {
        case Success((oldAutoCommit, conn)) => {
          // 执行成功
          Try(conn.setAutoCommit(oldAutoCommit))
          Try(jdbcHelper.returnConnection(conn))
        }
        case Failure(execption) => {
          // 执行失败
          jdbcHelper.returnConnection(null)
          throw execption
        }
      }
    })
  }
}

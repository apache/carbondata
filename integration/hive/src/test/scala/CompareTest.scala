/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File
import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ListBuffer
import scala.util.Try

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.datanucleus.store.rdbms.connectionpool.DatastoreDriverNotFoundException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

case class Query(sqlText: String, queryType: String, desc: String)

case class HiveOrcTablePerformance(query: String, time: String, desc: String)

case class HiveCarbonTablePerformance(query: String, time: String, desc: String)


object CompareTest {

  val hiveCarbonTableName = "comparetest_hive_carbon"
  val hiveOrcTableName = "hive_orctable"

  val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val warehouse = s"$rootPath/integration/hive/target/warehouse"
  val metastoredb = s"$rootPath/integration/hive/target/comparetest_metastore_db"

  val carbonTableName = "comparetest_hive_carbon"

  var resultSet: ResultSet = _

  def main(args: Array[String]) {

    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")

    import org.apache.spark.sql.CarbonSession._

    val carbon = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
      .getOrCreateCarbonSession(
        "hdfs://localhost:54310/opt/carbonStore",metastoredb)

    loadCarbonTable(generateDataFrame(carbon))

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort

    Try(Class.forName("org.apache.hive.jdbc.HiveDriver")).getOrElse(
      throw new DatastoreDriverNotFoundException("driver not found "))

    val con = DriverManager
      .getConnection(s"jdbc:hive2://localhost:$port/default", "anonymous", "anonymous")
    val stmt = con.createStatement

    println(s"============HIVE CLI IS STARTED ON PORT $port ==============")


    logger.info("Creating Hive Orc Table")

    stmt.execute(s"DROP TABLE IF EXISTS $hiveOrcTableName")

    stmt
      .execute(
        s"CREATE TABLE IF NOT EXISTS $hiveOrcTableName(city string,country string,planet string ," +
        s"id string,m1 smallint,m2 int,m3 bigint,m4 double,m5 double)stored as orc")

    stmt
      .execute(
        s"CREATE TABLE IF NOT EXISTS $hiveCarbonTableName(city string,country string,planet " +
        s"string,id string,m1 smallint,m2 int,m3 bigint,m4 double,m5 double)")
    stmt
      .execute(
        "ALTER TABLE comparetest_hive_carbon SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "ALTER TABLE comparetest_hive_carbon SET LOCATION " +
        s"'hdfs://localhost:54310/opt/carbonStore/default/$carbonTableName' ".stripMargin)

    stmt
      .execute(s"INSERT INTO $hiveOrcTableName SELECT * FROM $hiveCarbonTableName")

    val hive_Carbon_Result = new ListBuffer[HiveCarbonTablePerformance]()
    val hive_carbon_resultSet = new ListBuffer[(ResultSet)]()
    carbon.stop()

    val queries = getQueries

    val hive_Orc_Result = new ListBuffer[HiveOrcTablePerformance]()
    val hive_orc_resultSet = new ListBuffer[(ResultSet)]()

    queries.zipWithIndex.foreach { case (query, index) =>
      val sqlTextForHive_Orc = query.sqlText.replace("$table", s"$hiveOrcTableName")
      print(s"running query ${ index + 1 }: $sqlTextForHive_Orc ")
      val rt = time {
        resultSet = stmt.executeQuery(sqlTextForHive_Orc)
      }
      hive_orc_resultSet += resultSet
      println("time taken by orc on hive")
      println(s"**************=> $rt sec ********************")
      hive_Orc_Result += HiveOrcTablePerformance(sqlTextForHive_Orc, rt.toString, query.desc)

    }
    System.gc()
    Thread.sleep(1000)

    queries.zipWithIndex.foreach { case (query, index) =>
      val sqlTextForHive_Carbon = query.sqlText.replace("$table", s"$hiveCarbonTableName")

      println(s"running query ${ index + 1 }: $sqlTextForHive_Carbon ")
      val rt = time {
        resultSet = stmt.executeQuery(sqlTextForHive_Carbon)
      }
      hive_carbon_resultSet += resultSet

      hive_Carbon_Result +=
      HiveCarbonTablePerformance(sqlTextForHive_Carbon, rt.toString, query.desc)
      println("time taken by carbon on hive")

      println(s"**************=> $rt sec ********************")
    }

    println("Complete Stats Are Here ")
    println(
      "+---++-------+------------------------------------------------------------------------------------------------------------------------------+")
    println("|Id|" + "| Hive_orc Execution Time |" + "| Hive_Carbon Execution Time|" +
            "hive_carbon_desc                                                             |")

    println("+---+" +
            "+------------++-------------------------------------------------------------------------------------------------------------------------+")

    for (i <- hive_Orc_Result.indices) {
      val hive_OrcExecutionTime = hive_Orc_Result(i).time
      val hive_CarbonExecutionTime = hive_Carbon_Result(i).time
      val desc = hive_Carbon_Result(i).desc
      val queryIndex = i + 1

      println(s"| $queryIndex |" + s"| $hive_OrcExecutionTime                     ||" +
              s"      $hive_CarbonExecutionTime               |" + s"$desc")
      println("+---+" +
              "+------------++------------------------------------------------------------------------------------------------------------------------------+")

    }

    System.exit(0)

  }

  private def loadCarbonTable(input: DataFrame): Unit = {
    // Table schema:
    // +-------------+-----------+-------------+-------------+------------+
    // | Column name | Data type | Cardinality | Column type | Dictionary |
    // +-------------+-----------+-------------+-------------+------------+
    // | city        | string    | 8           | dimension   | yes        |
    // +-------------+-----------+-------------+-------------+------------+
    // | country     | string    | 1103        | dimension   | yes        |
    // +-------------+-----------+-------------+-------------+------------+
    // | planet      | string    | 100,007     | dimension   | yes        |
    // +-------------+-----------+-------------+-------------+------------+
    // | id          | string    | 10,000,000  | dimension   | no         |
    // +-------------+-----------+-------------+-------------+------------+
    // | m1          | short     | NA          | measure     | no         |
    // +-------------+-----------+-------------+-------------+------------+
    // | m2          | int       | NA          | measure     | no         |
    // +-------------+-----------+-------------+-------------+------------+
    // | m3          | big int   | NA          | measure     | no         |
    // +-------------+-----------+-------------+-------------+------------+
    // | m4          | double    | NA          | measure     | no         |
    // +-------------+-----------+-------------+-------------+------------+
    // | m5          | double    | NA          | measure     | no         |
    // +-------------+-----------+-------------+-------------+------------+
    input.write
      .format("carbondata")
      .option("tableName", carbonTableName)
      .option("tempCSV", "false")
      .option("single_pass", "true")
      .option("dictionary_exclude", "id") // id is high cardinality column
      .option("table_blocksize", "32")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def generateDataFrame(spark: SparkSession): DataFrame = {
    val rdd = spark.sparkContext
      .parallelize(1 to 5000000, 4)
      .map { value =>
        ("city" + value % 8, "country" + value % 1103, "planet" + value % 10007, "IDENTIFIER" +
                                                                                 value.toString,
          (value % 16).toShort, value / 2, (value << 1).toLong, value.toDouble / 13,
          value.toDouble / 11)
      }.map { value =>
      Row(value._1, value._2, value._3, value._4, value._5, value._6, value._7, value._8, value._9)
    }

    val schema = StructType(
      Seq(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("planet", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("m1", ShortType, nullable = false),
        StructField("m2", IntegerType, nullable = false),
        StructField("m3", LongType, nullable = false),
        StructField("m4", DoubleType, nullable = false),
        StructField("m5", DoubleType, nullable = false)
      )
    )

    val dataFrame = spark.createDataFrame(rdd, schema)
    dataFrame.printSchema()
    dataFrame
  }

  private def getQueries: Array[Query] = {
    Array(
      // ===========================================================================
      // ==                     FULL SCAN AGGREGATION                             ==
      // ===========================================================================
      Query(
        "select sum(m1) from $table",
        "full scan",
        "full scan query, 1 aggregate"
      ),
      Query(
        "select sum(m1), sum(m2) from $table",
        "full scan",
        "full scan query, 2 aggregate"
      ),
      Query(
        "select sum(m1), sum(m2), sum(m3) from $table",
        "full scan",
        "full scan query, 3 aggregate"
      ),
      Query(
        "select sum(m1), sum(m2), sum(m3), sum(m4) from $table",
        "full scan",
        "full scan query, 4 aggregate"
      ),
      Query(
        "select sum(m1), sum(m2), sum(m3), sum(m4), avg(m5) from $table",
        "full scan",
        "full scan query, 5 aggregate"
      ),
      Query(
        "select count(distinct id) from $table",
        "full scan",
        "full scan and count distinct of high card column"
      ),
      Query(
        "select count(distinct country) from $table",
        "full scan",
        "full scan and count distinct of medium card column"
      ),
      Query(
        "select count(distinct city) from $table",
        "full scan",
        "full scan and count distinct of low card column"
      ),
      // ===========================================================================
      // ==                      FULL SCAN GROUP BY AGGREGATE                     ==
      // ===========================================================================
      Query(
        "select country, sum(m1) from $table group by country",
        "aggregate",
        "group by on big data, on medium card column, medium result set,"
      ),
      Query(
        "select city, sum(m1) from $table group by city",
        "aggregate",
        "group by on big data, on low card column, small result set,"
      ),
      Query(
        "select id, sum(m1) as metric from $table group by id order by metric desc limit 100",
        "topN",
        "top N on high card column"
      ),
      Query(
        "select country,sum(m1) as metric from $table group by country order by metric desc limit" +
        " 10",
        "topN",
        "top N on medium card column"
      ),
      Query(
        "select city,sum(m1) as metric from $table group by city order by metric desc limit 10",
        "topN",
        "top N on low card column"
      ),
      // ===========================================================================
      // ==                  FILTER SCAN GROUP BY AGGREGATION                     ==
      // ===========================================================================
      Query(
        "select country, sum(m1) from $table where city='city8' group by country ",
        "filter scan and aggregate",
        "group by on large data, small result set"
      ),
      Query(
        "select id, sum(m1) from $table where planet='planet10' group by id",
        "filter scan and aggregate",
        "group by on medium data, large result set"
      ),
      Query(
        "select city, sum(m1) from $table where country='country12' group by city ",
        "filter scan and aggregate",
        "group by on medium data, small result set"
      ),
      // ===========================================================================
      // ==                             FILTER SCAN                               ==
      // ===========================================================================
      Query(
        "select * from $table where city = 'city3' limit 10000",
        "filter scan",
        "filter on low card dimension, limit, medium result set, fetch all columns"
      ),
      Query(
        "select * from $table where country = 'country9' ",
        "filter scan",
        "filter on low card dimension, medium result set, fetch all columns"
      ),
      Query(
        "select * from $table where planet = 'planet101' ",
        "filter scan",
        "filter on medium card dimension, small result set, fetch all columns"
      ),
      Query(
        "select * from $table where id = '408938' ",
        "filter scan",
        "filter on high card dimension"
      ),
      Query(
        "select * from $table where country='country10000'  ",
        "filter scan",
        "filter on low card dimension, not exist"
      ),
      Query(
        "select * from $table where country='country2' and city ='city8' ",
        "filter scan",
        "filter on 2 dimensions, small result set, fetch all columns"
      ),
      Query(
        "select * from $table where city='city1' and country='country2' and planet ='planet3' ",
        "filter scan",
        "filter on 3 dimensions, small result set, fetch all columns"
      ),
      Query(
        "select * from $table where m1 < 3",
        "filter scan",
        "filter on measure, small result set, fetch all columns"
      ),
      Query(
        "select * from $table where id like '1%' ",
        "fuzzy filter scan",
        "like filter, big result set"
      ),
      Query(
        "select * from $table where id like '%111'",
        "fuzzy filter scan",
        "like filter, medium result set"
      ),
      Query(
        "select * from $table where id like 'xyz%' ",
        "fuzzy filter scan",
        "like filter, full scan but not exist"
      )
    )
  }

  // Run all queries for the specified table
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }

}


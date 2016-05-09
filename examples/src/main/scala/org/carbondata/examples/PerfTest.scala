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

package org.carbondata.examples

import java.io.File

import scala.util.Random

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructType}

import org.carbondata.core.util.CarbonProperties
import org.carbondata.examples.PerfTest._

// scalastyle:off println

/**
 * represent one test case
 */
class Query(val queryType: String, val queryNo: Int, val sqlString: String) {

  /**
   * run the test case in a batch and calculate average time
   *
   * @param sqlContext context to run the test case
   * @param runs run how many time
   * @param datasource datasource to run
   */
  def run(sqlContext: SQLContext, runs: Int, datasource: String): QueryResult = {
    // run repeated and calculate average time elapsed
    require(runs >= 1)
    println(s"running $queryType query No.$queryNo against $datasource...")

    val sqlToRun = makeSQLString(datasource)

    val firstTime = withTime {
      sqlContext.sql(sqlToRun).collect
    }

    var totalTime: Long = 0
    var result: Array[Row] = null
    (1 to (runs - 1)).foreach { x =>
      totalTime += withTime {
        result = sqlContext.sql(sqlToRun).collect
      }
    }

    val avgTime = totalTime / (runs - 1)
    println(s"$datasource completed in {${firstTime/1000000}ms, ${avgTime/1000000}ms}")
    QueryResult(datasource, result, avgTime, firstTime)
  }

  private def makeSQLString(datasource: String): String = {
    sqlString.replaceFirst("tableName", PerfTest.makeTableName(datasource))
  }

}

/**
 * records performance of a testcase
 */
case class QueryResult(datasource: String, result: Array[Row], avgTime: Long, firstTime: Long)

class QueryRunner(sqlContext: SQLContext, dataFrame: DataFrame, datasources: Seq[String]) {

  /**
   * run a testcase on each datasource
   */
  def run(testCase: Query, runs: Int): Seq[QueryResult] = {
    var results = Seq[QueryResult]()
    datasources.foreach { datasource =>
      results :+= testCase.run(sqlContext, runs, datasource)
    }
    checkResult(results)
    results
  }

  private def checkResult(results: Seq[QueryResult]): Unit = {
    results.foldLeft(results.head) { (last, cur) =>
      if (last.result.sameElements(cur.result)) cur
      else sys.error(s"result is not the same between " +
          s"${last.datasource}(${last.result.mkString(", ")}) and " +
          s"${cur.datasource}(${cur.result.mkString(", ")})")
    }
  }

  private def loadToNative(datasource: String): Unit = {
    val savePath = PerfTest.savePath(datasource)
    println(s"loading data into $datasource, path: $savePath")
    dataFrame.write
        .mode(SaveMode.Overwrite)
        .format(datasource)
        .save(savePath)
    sqlContext.read
        .format(datasource)
        .load(savePath)
        .registerTempTable(PerfTest.makeTableName(datasource))
  }

  /**
   * load data to each datasource
   */
  def loadData: Seq[QueryResult] = {
    // load data into all datasources
    var results = Seq[QueryResult]()
    datasources.foreach { datasource =>
      val time = withTime {
        datasource match {
          case "parquet" =>
            dataFrame.sqlContext.setConf(s"spark.sql.$datasource.compression.codec", "snappy")
            loadToNative(datasource)
          case "orc" =>
            dataFrame.sqlContext.sparkContext.hadoopConfiguration.set("orc.compress", "SNAPPY")
            loadToNative(datasource)
          case "carbon" =>
            sqlContext.sql(s"drop cube if exists ${PerfTest.makeTableName(datasource)}")
            println(s"loading data into $datasource, path: " +
                s"${dataFrame.sqlContext.asInstanceOf[CarbonContext].storePath}")
            dataFrame.write
                .format("org.apache.spark.sql.CarbonSource")
                .option("tableName", PerfTest.makeTableName(datasource))
                .mode(SaveMode.Overwrite)
                .save()
          case _ => sys.error("unsupported data source")
        }
      }
      println(s"load data into $datasource completed, time taken ${time/1000000}ms")
      results :+= QueryResult(datasource, null, time, time)
    }
    results
  }

  def shutDown(): Unit = {
    // drop all tables and temp files
    datasources.foreach { datasource =>
      datasource match {
        case "parquet" | "orc" =>
          val f = new File(PerfTest.savePath(datasource))
          if (f.exists()) f.delete()
        case "carbon" =>
          sqlContext.sql(s"drop cube if exists ${PerfTest.makeTableName("carbon")}")
        case _ => sys.error("unsupported data source")
      }
    }
  }
}

/**
 * template for table data generation
 *
 * @param dimension number of dimension columns and their cardinality
 * @param measure number of measure columns
 */
case class TableTemplate(dimension: Seq[(Int, Int)], measure: Int)

/**
 * utility to generate random data according to template
 */
class TableGenerator(sqlContext: SQLContext) {

  /**
   * generate a dataframe from random data
   */
  def genDataFrame(template: TableTemplate, rows: Int): DataFrame = {
    val measures = template.measure
    val dimensions = template.dimension.foldLeft(0) {(x, y) => x + y._1}
    val cardinality = template.dimension.foldLeft(Seq[Int]()) {(x, y) =>
      x ++ (1 to y._1).map(z => y._2)
    }
    print(s"generating data: $rows rows of $dimensions dimensions and $measures measures. ")
    println("cardinality for each dimension: " + cardinality.mkString(", "))

    val dimensionFields = (1 to dimensions).map { id =>
      DataTypes.createStructField(s"c$id", DataTypes.StringType, false)
    }
    val measureFields = (dimensions + 1 to dimensions + measures).map { id =>
      DataTypes.createStructField(s"c$id", DataTypes.IntegerType, false)
    }
    val schema = StructType(dimensionFields ++ measureFields)
    val data = sqlContext.sparkContext.parallelize(1 to rows).map { x =>
      val random = new Random()
      val dimSeq = (1 to dimensions).map { y =>
        s"P${y}_${random.nextInt(cardinality(y - 1))}"
      }
      val msrSeq = (1 to measures).map { y =>
        random.nextInt(10)
      }
      Row.fromSeq(dimSeq ++ msrSeq)
    }
    sqlContext.createDataFrame(data, schema)
  }
}

object PerfTest {

  private val olap: Seq[String] = Seq(
    """select c3, c4, count(distinct c8) from tableName where c1 = 'P1_23' and c2 = 'P2_43'
       group by c3, c4""",

    """select c2, count(distinct c1), sum(c3) from tableName where c4 = 'P4_3' and c5 = 'P5_2' """,

    """Select phone_type, count(distinct user_id), sum(traffic) from tableName where
      title="manager" and country="UK" and income>50000 and brand="SK" group by phone_type" """,

    """Select sex, color, count(distinct user_id) from tableName where title="manager" and
       country="UK" and income>50000 and phone_type="P9" group by sex, color""",

    """Select subquery + case when tgo show the age grouping of customers"""
  )

  private val join: Seq[String] = Seq(
    """Select brand, phone_type, color, count(user_id) from tableName, vip join on
      t1.user_id=vip.user_id group by brand, phone_type, color""",

    """Select brand, phone_type, color, count(user_id) as count from tableName, vip join on
      t1.user_id=vip.user_id group by brand, phone_type, color order by count desc limit 100 """
  )

  private val filter: Seq[String] = Seq(
    """Select * from tableName where city="AUSTIN" and connect_status="failure" and
      time between xxx and xxxx""",

    """Select c1,c3,c5,c7,c50-c120 from tableName where city="AUSTIN" and
      connect_status="failure" and time between xxx and xxxx""",

    """Select * from tableName where userid="134333223" and city="AUSTIN" and
      time between xxx and xxxx"""
  )

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf()
        .setAppName("CarbonExample")
        .setMaster("local[2]"))
    sc.setLogLevel("ERROR")
    val cc = createCarbonContext(sc)

    // prepare performance queries
    var workload = Seq[Query]()
    workload :+= new Query("olap", 0, olap.head)
    //    olap.zipWithIndex.foreach(x => workload :+= new TestCase("olap", x._2, x._1))
    //    join.zipWithIndex.foreach(x => workload :+= new TestCase("join", x._2, x._1))
    //    filter.zipWithIndex.foreach(x => workload :+= new TestCase("filter", x._2, x._1))

    // prepare data
    val rows = 1000 * 1000
    // val dimension = Seq((1, 1000*1000), (9, 100*1000), (20, 1000), (120, 100))
    val dimension = Seq((1, 1 * 1000), (1, 100), (1, 50), (2, 10)) // cardinality for each column
    val measure = 5 // number of measure
    val template = TableTemplate(dimension, measure)
    val df = new TableGenerator(cc).genDataFrame(template, rows)

    // materialize the dataframe and save it to hive table, and load it from all data sources
    df.write.mode(SaveMode.Overwrite).saveAsTable("PerfTestGeneratedTable")
    println("generate data completed")

    // run all queries against all data sources
    val datasource = Seq("parquet", "orc", "carbon")
    val runner = new QueryRunner(cc, cc.table("PerfTestGeneratedTable"), datasource)

    val results = runner.loadData
    println(formatLoadResult(results))

    workload.foreach { testcase =>
      // run 4 times each round, will print performance of first run and avg time of last 3 runs
      val results = runner.run(testcase, 4)
      println(formatQueryResult(testcase, results))
    }

    runner.shutDown()
  }

  private def createCarbonContext(sc: SparkContext): CarbonContext = {
    val storeLocation = currentPath + "/target/store"
    val kettleHome = new File(currentPath + "/../processing/carbonplugins").getCanonicalPath
    val hiveMetaPath = currentPath + "/target/hivemetadata"

    val cc = new CarbonContext(sc, storeLocation)
    cc.setConf("carbon.kettle.home", kettleHome)
    cc.setConf("hive.metastore.warehouse.dir", hiveMetaPath)
    cc.setConf(HiveConf.ConfVars.HIVECHECKFILEFORMAT.varname, "false")
    CarbonProperties.getInstance().addProperty("carbon.table.split.partition.enable", "false")
    cc
  }

  private def formatLoadResult(results: Seq[QueryResult]): String = {
    s"load, 0, ${formatResults(results)}"
  }

  private def formatQueryResult(testCase: Query, results: Seq[QueryResult]): String = {
    s"${testCase.queryType}, ${testCase.queryNo}, ${formatResults(results)}"
  }

  private def formatResults(results: Seq[QueryResult]): String = {
    val builder = new StringBuilder
    results.foreach { result =>
      builder.append(s"${result.datasource}{${result.firstTime.toDouble / 1000000}ms, " +
          s"${result.avgTime.toDouble / 1000000}ms}, ")
    }
    builder.toString
  }

  def makeTableName(datasource: String): String = {
    s"${datasource}_perftest_table"
  }

  def currentPath: String = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

  def savePath(datasource: String): String = s"${currentPath}/target/perftest/${datasource}"

  def withTime(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    System.nanoTime() - start
  }

}
// scalastyle:on println

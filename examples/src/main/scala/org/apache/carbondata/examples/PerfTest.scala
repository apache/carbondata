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

package org.apache.carbondata.examples

import java.io.File

import scala.util.Random

import org.apache.spark.sql.{CarbonContext, DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructType}

import org.apache.carbondata.examples.PerfTest._
import org.apache.carbondata.examples.util.InitForExamples

// scalastyle:off println

/**
 * represent one query
 */
class Query(val queryType: String, val queryNo: Int, val sqlString: String) {

  /**
   * run the query in a batch and calculate average time
   *
   * @param sqlContext context to run the query
   * @param runs run how many time
   * @param datasource datasource to run
   */
  def run(sqlContext: SQLContext, runs: Int, datasource: String): QueryResult = {
    // run repeated and calculate average time elapsed
    require(runs >= 1)
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
    QueryResult(datasource, result, avgTime, firstTime)
  }

  private def makeSQLString(datasource: String): String = {
    sqlString.replaceFirst("tableName", PerfTest.makeTableName(datasource))
  }

}

/**
 * query performance result
 */
case class QueryResult(datasource: String, result: Array[Row], avgTime: Long, firstTime: Long)

class QueryRunner(sqlContext: SQLContext, dataFrame: DataFrame, datasources: Seq[String]) {

  /**
   * run a query on each datasource
   */
  def run(query: Query, runs: Int): Seq[QueryResult] = {
    var results = Seq[QueryResult]()
    datasources.foreach { datasource =>
      val result = query.run(sqlContext, runs, datasource)
      results :+= result
    }
    checkResult(results)
    results
  }

  private def checkResult(results: Seq[QueryResult]): Unit = {
    results.foldLeft(results.head) { (last, cur) =>
      if (last.result.sortBy(_.toString()).sameElements(cur.result.sortBy(_.toString()))) cur
      else sys.error(s"result is not the same between " +
          s"${last.datasource} and " +
          s"${cur.datasource}")
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
            sqlContext.sql(s"DROP TABLE IF EXISTS ${PerfTest.makeTableName(datasource)}")
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
          sqlContext.sql(s"DROP TABLE IF EXISTS ${PerfTest.makeTableName("carbon")}")
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
    val df = sqlContext.createDataFrame(data, schema)
    df.write.mode(SaveMode.Overwrite).parquet(PerfTest.savePath("temp"))
    sqlContext.parquetFile(PerfTest.savePath("temp"))
  }
}

object PerfTest {

  private val olap: Seq[String] = Seq(
    """SELECT c3, c4, sum(c8) FROM tableName
      |WHERE c1 = 'P1_23' and c2 = 'P2_43'
      |GROUP BY c3, c4""".stripMargin,

    """SELECT c2, c3, sum(c9) FROM tableName
      |WHERE c1 = 'P1_432' and c4 = 'P4_3' and c5 = 'P5_2'
      |GROUP by c2, c3 """.stripMargin,

    """SELECT c2, count(distinct c1), sum(c8) FROM tableName
      |WHERE c3="P3_4" and c5="P5_4"
      |GROUP BY c2 """.stripMargin,

    """SELECT c2, c5, count(distinct c1), sum(c7) FROM tableName
      |WHERE c4="P4_4" and c5="P5_7" and c8>4
      |GROUP BY c2, c5 """.stripMargin
  )

  private val point: Seq[String] = Seq(
    """SELECT c4 FROM tableName
      |WHERE c1="P1_43" """.stripMargin,

    """SELECT c3 FROM tableName
      |WHERE c1="P1_542" and c2="P2_23" """.stripMargin,

    """SELECT c3, c5 FROM tableName
      |WHERE c1="P1_52" and c7=4""".stripMargin,

    """SELECT c4, c9 FROM tableName
      |WHERE c1="P1_43" and c8<3""".stripMargin
  )

  private val filter: Seq[String] = Seq(
    """SELECT * FROM tableName
      |WHERE c2="P2_43" """.stripMargin,

    """SELECT * FROM tableName
      |WHERE c3="P3_3"  """.stripMargin,

    """SELECT * FROM tableName
      |WHERE c2="P2_32" and c3="P3_23" """.stripMargin,

    """SELECT * FROM tableName
      |WHERE c3="P3_28" and c4="P4_3" """.stripMargin
  )

  private val scan: Seq[String] = Seq(
    """SELECT sum(c7), sum(c8), avg(c9), max(c10) FROM tableName """.stripMargin,

    """SELECT sum(c7) FROM tableName
      |WHERE c2="P2_32" """.stripMargin,

    """SELECT sum(c7), sum(c8), sum(9), sum(c10) FROM tableName
      |WHERE c4="P4_4" """.stripMargin,

    """SELECT sum(c7), sum(c8), sum(9), sum(c10) FROM tableName
      |WHERE c2="P2_75" and c6<5 """.stripMargin
  )

  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("PerfTest")

    // prepare performance queries
    var workload = Seq[Query]()
    olap.zipWithIndex.foreach(x => workload :+= new Query("OLAP Query", x._2, x._1))
    point.zipWithIndex.foreach(x => workload :+= new Query("Point Query", x._2, x._1))
    filter.zipWithIndex.foreach(x => workload :+= new Query("Filter Query", x._2, x._1))
    scan.zipWithIndex.foreach(x => workload :+= new Query("Scan Query", x._2, x._1))

    // prepare data
    val rows = 3 * 1000 * 1000
    val dimension = Seq((1, 1 * 1000), (1, 100), (1, 50), (2, 10)) // cardinality for each column
    val measure = 5 // number of measure
    val template = TableTemplate(dimension, measure)
    val df = new TableGenerator(cc).genDataFrame(template, rows)
    println("generate data completed")

    // run all queries against all data sources
    val datasource = Seq("parquet", "orc", "carbon")
    val runner = new QueryRunner(cc, df, datasource)

    val results = runner.loadData
    println(s"load performance: ${results.map(_.avgTime / 1000000L).mkString(", ")}")

    var parquetTime: Double = 0
    var orcTime: Double = 0
    var carbonTime: Double = 0

    println(s"query id: ${datasource.mkString(", ")}, result in millisecond")
    workload.foreach { query =>
      // run 4 times each round, will print performance of first run and avg time of last 3 runs
      print(s"${query.queryType} ${query.queryNo}: ")
      val results = runner.run(query, 4)
      print(s"${results.map(_.avgTime / 1000000L).mkString(", ")} ")
      println(s"[sql: ${query.sqlString.replace('\n', ' ')}]")
      parquetTime += results(0).avgTime
      orcTime += results(1).avgTime
      carbonTime += results(2).avgTime
    }

    println(s"Total time: ${parquetTime / 1000000}, ${orcTime / 1000000}, " +
        s"${carbonTime / 1000000} = 1 : ${parquetTime / orcTime} : ${parquetTime / carbonTime}")
    runner.shutDown()
  }

  def makeTableName(datasource: String): String = {
    s"${datasource}_perftest_table"
  }

  def savePath(datasource: String): String =
      s"${InitForExamples.currentPath}/target/perftest/${datasource}"

  def withTime(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    System.nanoTime() - start
  }

}
// scalastyle:on println

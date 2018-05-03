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
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.spark.sql.{CarbonSession, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * An example that demonstrate how to run queries in search mode,
 * and compare the performance between search mode and SparkSQL
 */
// scalastyle:off
object SearchModeExample {

  def main(args: Array[String]) {
    import org.apache.spark.sql.CarbonSession._
    val master = Option(System.getProperty("spark.master"))
      .orElse(sys.env.get("MASTER"))
      .orElse(Option("local[8]"))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")

    val filePath = if (args.length > 0) {
      args(0)
    } else {
      val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
      s"$rootPath/examples/spark2/src/main/resources/data.csv"
    }
    val storePath = if (args.length > 1) {
      args(1)
    } else {
      val rootPath = new File(this.getClass.getResource("/").getPath
        + "../../../..").getCanonicalPath
      s"$rootPath/examples/spark2/target/store"
    }

    val spark = SparkSession
      .builder()
      .master(master.get)
      .appName("SearchModeExample")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreateCarbonSession(storePath)

    spark.sparkContext.setLogLevel("ERROR")
    exampleBody(spark, filePath)
    println("Finished!")
    spark.close()
  }

  def exampleBody(spark: SparkSession, path: String): Unit = {

    spark.sql("DROP TABLE IF EXISTS carbonsession_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbonsession_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbonsession_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    val pool = Executors.newCachedThreadPool()

    // start search mode
    spark.asInstanceOf[CarbonSession].startSearchMode()
    runAsynchrousSQL(spark, pool, 1)

    println("search mode asynchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runAsynchrousSQL(spark, pool, 100)
    }

    println("search mode synchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runSynchrousSQL(spark, 100)
    }

    // stop search mode
    spark.asInstanceOf[CarbonSession].stopSearchMode()

    println("sparksql asynchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runAsynchrousSQL(spark, pool, 100)
    }

    println("sparksql synchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runSynchrousSQL(spark, 100)
    }

    // start search mode again
    spark.asInstanceOf[CarbonSession].startSearchMode()

    println("search mode asynchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runAsynchrousSQL(spark, pool, 100)
    }

    println("search mode synchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runSynchrousSQL(spark, 100)
    }

    // stop search mode
    spark.asInstanceOf[CarbonSession].stopSearchMode()

    println("sparksql asynchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runAsynchrousSQL(spark, pool, 100)
    }

    println("sparksql synchronous query")
    org.apache.spark.sql.catalyst.util.benchmark {
      runSynchrousSQL(spark, 100)
    }

    spark.sql("DROP TABLE IF EXISTS carbonsession_table")
    pool.shutdownNow()
  }

  private def runAsynchrousSQL(spark: SparkSession, pool: ExecutorService, round: Int): Unit = {
    val futures = (1 to round).map { i =>
      pool.submit(new Runnable {
        override def run(): Unit = {
          spark.sql(
            s"""
             SELECT charField, stringField, intField, dateField
             FROM carbonsession_table
             WHERE stringfield = 'spark' AND decimalField > $i % 37
              """.stripMargin
          ).collect()
        }
      })
    }

    futures.foreach(_.get())
  }

  private def runSynchrousSQL(spark: SparkSession, round: Int): Unit = {
    (1 to round).map { i =>
      spark.sql(
        s"""
             SELECT charField, stringField, intField, dateField
             FROM carbonsession_table
             WHERE stringfield = 'spark' AND decimalField > $i % 37
              """.stripMargin
      ).collect()
    }
  }
}
// scalastyle:on
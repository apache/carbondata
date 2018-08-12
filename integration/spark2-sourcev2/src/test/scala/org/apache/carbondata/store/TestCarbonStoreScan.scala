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

package org.apache.carbondata.store

import java.io.File

import org.apache.carbondata.sdk.store.conf.StoreConf
import org.apache.carbondata.store.impl.worker.Worker
import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, SparkSession}

class TestCarbonStoreScan extends org.scalatest.FunSuite {

  test("Test Scan with pruning and filter") {

    val projectFolder = new File(classOf[TestCarbonStoreScan].getResource("/")
      .getPath + "../../../../").getCanonicalPath

    val storeConfPath = projectFolder + "/store/conf/store.conf"
    System.setProperty("CARBON_STORE_CONF", storeConfPath)
    val thread = new Thread {
      override def run {
        org.apache.carbondata.store.impl.master.Master.main(
          Array(projectFolder + "/store/conf/log4j.properties", storeConfPath))
      }
    }
    thread.start()
    Thread.sleep(5000)
    val w = new Worker(new StoreConf(System.getProperty("CARBON_STORE_CONF")))
    w.start()

    val conf = new SparkConf().
      set("hive.metastore.warehouse.dir", "/tmp/spark-warehouse")
        .set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .setAppName("carbon-store-datasource")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession.builder()
      .config(conf)
      .master("local")
      .getOrCreateCarbonSession("/tmp/carbon.store/")


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

    val path = projectFolder + "/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbonsession_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#','timestampformat'='yyyy/MM/dd HH:mm:ss', 'dateformat'='yyyy/MM/dd')
       """.stripMargin)

    spark.sql("select * from carbonsession_table").show()

    val df2 = spark.read
      .format("carbon")
      .option("tableName", "carbonsession_table")
      .load()

    // Step 1 (Schema verification)
    df2.printSchema()
    // Step 2 (Read data)
    df2.show()

    df2.selectExpr("intfield", "stringfield", "timestampfield").filter("stringfield = 'spark'").show()

    assert(df2.selectExpr("intfield", "stringfield", "timestampfield", "shortfield")
      .filter("stringfield = 'spark'")
      .groupBy("intfield", "shortfield", "stringfield")
      .agg(functions.collect_list("intfield"))
      .count() == 2)
  }
}

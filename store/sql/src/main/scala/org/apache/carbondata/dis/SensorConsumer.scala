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

package org.apache.carbondata.dis

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object SensorConsumer {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      // scalastyle:off println
      System.err.println(
          "Usage: SensorConsumer <stream name> <endpoint> <region> <ak> <sk> <project id>")
      // scalastyle:on println
      return
    }

    val streamName = args(0)
    val spark = createCarbonSession("DisConsumer")
    spark.conf.set("carbon.source.endpoint", args(1))
    spark.conf.set("carbon.source.region", args(2))
    spark.conf.set("carbon.source.ak", args(3))
    spark.conf.set("carbon.source.sk", args(4))
    spark.conf.set("carbon.source.projectid", args(5))

    spark.sql("drop table if exists sensor")
    spark.sql("drop table if exists sensor_table")

    spark.sql(
      s"""
         |create table sensor(
         |  event_id bigint,
         |  building string,
         |  device_id int,
         |  record_time timestamp,
         |  temperature double
         |)
         |STORED AS carbondata
         |TBLPROPERTIES (
         |  'streaming'='source',
         |  'format'='dis',
         |  'streamname'='$streamName'
         |)
      """.stripMargin)

    spark.sql(
      s"""
         |CREATE TABLE sensor_table(
         | event_id bigint,
         | building string,
         | device_id int,
         | record_time timestamp,
         | temperature double
         | )
         |STORED AS carbondata
         |TBLPROPERTIES(
         |  'streaming'='sink',
         |  'sort_columns'='device_id, record_time'
         |)
      """.stripMargin)

    spark.sql(
      """
        |CREATE STREAM stream123 ON TABLE sensor_table
        |STMPROPERTIES(
        |  'trigger'='ProcessingTime',
        |  'interval'='5 seconds',
        |  'carbon.stream.parser'='org.apache.carbondata.streaming.parser.CSVStreamParserImp')
        |AS
        |  SELECT *
        |  FROM sensor
      """.stripMargin).show(false)


    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    (0 to 100).foreach { x =>
      val time = format.format(new Date())
      Thread.sleep(5000)
      spark.sql("select * from sensor_table " +
                "where record_time > current_timestamp() - INTERVAL 1 MINUTE")
        .show(100, false)
    }
  }

  def createCarbonSession(appName: String, workThreadNum: Int = 1): SparkSession = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/store/sql/target/store"
    val warehouse = s"$rootPath/store/sql/target/warehouse"
    val metastoredb = s"$rootPath/store/sql/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")

    val masterUrl = if (workThreadNum <= 1) {
      "local"
    } else {
      "local[" + workThreadNum.toString() + "]"
    }

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

}

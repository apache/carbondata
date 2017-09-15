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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.streaming.ProcessingTime

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.utils.{StreamingExampleUtil}

/**
 * Covers spark structured streaming scenario where user streams data
 * from a file source (input source) and write into carbondata table(output sink).
 * This example uses csv file as a input source and writes
 * into target carbon table. The target carbon table must exist.
 */

object CarbonStreamingIngestFileSourceExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val csvDataDir = s"$rootPath/examples/spark2/resources/csvDataDir"
    val streamTableName = s"_carbon_file_stream_table_"
    val streamTablePath = s"$storeLocation/default/$streamTableName"
    val ckptLocation = s"$rootPath/examples/spark2/resources/ckptDir"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // cleanup residual files, if any
    StreamingExampleUtil.cleanUpDir(csvDataDir, ckptLocation)

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonFileStreamingExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("ERROR")

    // Writes Dataframe to CarbonData file:
    import spark.implicits._
    import org.apache.spark.sql.types._
    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")
    // Create target carbon table
    spark.sql(
      s"""
         | CREATE TABLE ${streamTableName}(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT
         | )
         | STORED BY 'carbondata'""".stripMargin)

    // Generate CSV data and write to CSV file
    StreamingExampleUtil.generateCSVDataFile(spark, 1, csvDataDir, SaveMode.Overwrite)

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvDataDir'
         | INTO TABLE ${streamTableName}
         | OPTIONS('FILEHEADER'='id,name,city,salary'
         | )""".stripMargin)
    // scalastyle:on

    // check initial table data
    spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // define custom schema
    val inputSchema = new StructType().
      add("id", "integer").
      add("name", "string").
      add("city", "string").
      add("salary", "float")

    // setup csv file as a input streaming source
    val csvReadDF = spark.readStream.
      format("csv").
      option("sep", ",").
      schema(inputSchema).
      option("path", csvDataDir).
      option("header", "true").
      load()

    // Write data from csv format streaming source to carbondata target format
    // set trigger to every 1 second
    val qry = csvReadDF.writeStream
      .format("carbondata")
      .trigger(ProcessingTime("1 seconds"))
      .option("checkpointLocation", ckptLocation)
      .option("path", streamTablePath)
      .start()

    // In a separate thread append data every 2 seconds to existing csv
    val gendataThread: Thread = new Thread() {
      override def run(): Unit = {
        for (i <- 1 to 5) {
          Thread.sleep(2)
          StreamingExampleUtil.
            generateCSVDataFile(spark, i * 10 + 1, csvDataDir, SaveMode.Append)
        }
      }
    }
    gendataThread.start()
    gendataThread.join()

    // stop streaming execution after 5 sec delay
    Thread.sleep(5000)
    qry.stop()

    // verify streaming data is added into the table
    // spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Cleanup residual files and table data
    StreamingExampleUtil.cleanUpDir(csvDataDir, ckptLocation)
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")
  }
}

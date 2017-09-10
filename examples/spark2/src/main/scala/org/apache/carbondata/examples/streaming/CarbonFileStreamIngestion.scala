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

import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.utils.StreamingCleanupUtil

object CarbonDataFileStreamingExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val csvDataDir = s"$rootPath/examples/spark2/resources/csvDataDir"
    // val csvDataFile = s"$csvDataDir/sampleData.csv"
    // val csvDataFile = s"$csvDataDir/sample.csv"
    val streamTableName = s"_carbon_file_stream_table_"
    val stremTablePath = s"$storeLocation/default/$streamTableName"
    val ckptLocation = s"$rootPath/examples/spark2/resources/ckptDir"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // cleanup any residual files
    StreamingCleanupUtil.main(Array(csvDataDir, ckptLocation))

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

    // Generate random data
    val dataDF = spark.sparkContext.parallelize(1 to 10)
      .map(id => (id, "name_ABC", "city_XYZ", 10000.00*id)).
      toDF("id", "name", "city", "salary")

    // drop table if exists previously
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")

    // Create Carbon Table
    // Saves dataframe to carbondata file
    dataDF.write
      .format("carbondata")
      .option("tableName", streamTableName)
      .option("compress", "true")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()

    spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Create csv data frame file
    val csvDataDF = spark.sparkContext.parallelize(11 to 30)
      .map(id => (id,
        s"name_${RandomStringUtils.randomAlphabetic(4).toUpperCase}",
        s"city_${RandomStringUtils.randomAlphabetic(2).toUpperCase}",
        10000.00*id)).toDF("id", "name", "city", "salary")

    // write data into csv file ( It will be used as a stream source)
    csvDataDF.write.
      format("com.databricks.spark.csv").
      option("header", "true").
      save(csvDataDir)

    // define custom schema
    val inputSchema = new StructType().
      add("id", "integer").
      add("name", "string").
      add("city", "string").
      add("salary", "float")

    // Read csv data file as a streaming source
    val csvReadDF = spark.readStream.
      format("csv").
      option("sep", ",").
      schema(inputSchema).
      option("path", csvDataDir).
      option("header", "true").
      load()

    // Write data from csv format streaming source to carbondata target format
    val qry = csvReadDF.writeStream.
      format("carbondata").
      option("checkpointLocation", ckptLocation).
      option("path", stremTablePath).
      start()

    // stop streaming query after 5 sec delay
    Thread.sleep(5000)
    qry.stop()

    // verify streaming data is added into the table
    // spark.sql(s""" SELECT * FROM ${streamTableName} """).show()

    // Cleanup residual files and table data
    StreamingCleanupUtil.main(Array(csvDataDir, ckptLocation))
    spark.sql(s"DROP TABLE IF EXISTS ${streamTableName}")
  }
}

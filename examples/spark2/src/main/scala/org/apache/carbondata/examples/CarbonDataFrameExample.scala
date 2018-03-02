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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonDataFrameExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonDataFrameExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation)

    spark.sparkContext.setLogLevel("ERROR")

    // Writes Dataframe to CarbonData file:
    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 100)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbondata file
    df.write
      .format("carbondata")
      .option("tableName", "carbon_df_table")
      .option("compress", "true")
      .option("tempCSV", "false")
      .mode(SaveMode.Overwrite)
      .save()

    spark.sql(""" SELECT * FROM carbon_df_table """).show()

    // Specify schema
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
    val customSchema = StructType(Array(
      StructField("c1", StringType),
      StructField("c2", StringType),
      StructField("number", IntegerType)))

    // Reads carbondata to dataframe
    val carbondf = spark.read
      .format("carbondata")
      .schema(customSchema)
      // .option("dbname", "db_name") the system will use "default" as dbname if not set this option
      .option("tableName", "carbon_df_table")
      .load()

    // Dataframe operations
    carbondf.printSchema()
    carbondf.select($"c1", $"number" + 10).show()
    carbondf.filter($"number" > 31).show()

    spark.sql("DROP TABLE IF EXISTS carbon_df_table")

    spark.stop()
  }
}

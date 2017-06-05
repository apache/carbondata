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

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonDataFactory {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/integration/presto/data/store"
    val warehouse = s"$rootPath/integration/presto/data/warehouse"
    val metaStoreDb = s"$rootPath/integration/presto/data"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonDataFactory")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metaStoreDb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('FILEHEADER'='shortField,
         | intField,bigintField,doubleField,stringField,
         | timestampField,decimalField,dateField,charField,
         | floatField,complexData',
         | 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

  }
}


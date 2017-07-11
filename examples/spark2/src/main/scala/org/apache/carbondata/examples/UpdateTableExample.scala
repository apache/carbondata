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


object UpdateTableExample {

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"

    import org.apache.spark.sql.CarbonSession._

    val carbonSession = SparkSession
      .builder()
      .master("local")
      .appName("UpdateTableExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    carbonSession.sparkContext.setLogLevel("WARN")


    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")


    carbonSession.sql("DROP TABLE IF EXISTS CARBON_UPDATE_EXAMPLE")

    carbonSession.sql(
      s"""
         | CREATE TABLE CARBON_UPDATE_EXAMPLE(
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

    carbonSession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE CARBON_UPDATE_EXAMPLE
         | OPTIONS('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData',
         | 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    carbonSession.sql("""UPDATE CARBON_UPDATE_EXAMPLE d set (d.intField) = (d.intField + 1) where d.stringField = 'spark'""").show()

    carbonSession.sql("SELECT * FROM CARBON_UPDATE_EXAMPLE").show()

    carbonSession.sql("DROP TABLE IF EXISTS CARBON_UPDATE_EXAMPLE")

  }
}
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

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty("carbon.kettle.home", s"$rootPath/processing/carbonplugins")
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    // Drop table

    spark.sql("DROP TABLE IF EXISTS uniq_shared_dictionary")

    // Create table, with shared columns

    spark.sql(
      """ CREATE TABLE uniq_shared_dictionary (CUST_ID int,CUST_NAME String,
      ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,
      BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,
      10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY
    'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_ID,
    Double_COLUMN2,DECIMAL_COLUMN2','columnproperties.CUST_ID.shared_column'='shared.CUST_ID',
    'columnproperties.decimal_column2.shared_column'='shared.decimal_column2')"""
        .stripMargin).show

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string,
         |    timestampField timestamp,
         |    decimalField decimal(18,2),
         |    dateField date,
         |    charField char(5),
         |    floatField float,
         |    complexData array<string>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | options('FILEHEADER'='shortField,intField,bigintField,doubleField,stringField,timestampField,decimalField,dateField,charField,floatField,complexData','COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql("""
             SELECT *
             FROM carbon_table
             where stringfield = 'spark' and decimalField > 40
              """).show

    spark.sql("""
             SELECT *
             FROM carbon_table where length(stringField) = 5
              """).show

    spark.sql("""
             SELECT *
             FROM carbon_table where date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
              """).show

    spark.sql("""
             select count(stringField) from carbon_table
              """.stripMargin).show

    spark.sql("""
           SELECT sum(intField), stringField
           FROM carbon_table
           GROUP BY stringField
              """).show

    spark.sql(
      """
        |select t1.*, t2.*
        |from carbon_table t1, carbon_table t2
        |where t1.stringField = t2.stringField
      """.stripMargin).show

    spark.sql(
      """
        |with t1 as (
        |select * from carbon_table
        |union all
        |select * from carbon_table
        |)
        |select t1.*, t2.*
        |from t1, carbon_table t2
        |where t1.stringField = t2.stringField
      """.stripMargin).show

    spark.sql("""
             SELECT *
             FROM carbon_table
             where stringfield = 'spark' and floatField > 2.8
              """).show

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
  }

}

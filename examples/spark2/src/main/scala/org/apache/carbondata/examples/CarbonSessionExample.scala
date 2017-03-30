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
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    spark.sql("DROP TABLE IF EXISTS carbon_table")

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

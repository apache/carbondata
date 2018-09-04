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

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark2/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark2/src/main/resources/log4j.properties")

//    CarbonProperties.getInstance()
//      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("INFO")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    spark.sql("DROP TABLE IF EXISTS carbonsession_table")
    spark.sql("DROP TABLE IF EXISTS stored_as_carbondata_table")


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
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('local_dictionary_enable'='true')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbonsession_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    spark.sql(
      s"""
         | SELECT *
         | FROM carbonsession_table
      """.stripMargin).show()

//    import spark.implicits._
//
//    import scala.util.Random
//    val r = new Random()
//    val df = spark.sparkContext.parallelize(1 to 10 * 10 * 1000)
//      .map(x => ("No." + r.nextInt(100000), "name" + x % 8, "city" + x % 50, x % 60))
//      .toDF("ID", "name", "city", "age")
//
//    // Create table with pre-aggregate
//    spark.sql("DROP TABLE IF EXISTS personTable")
//    spark.sql("DROP TABLE IF EXISTS personTableWithoutAgg")
//    df.write.format("carbondata")
//      .option("tableName", "personTable")
//      .option("compress", "true")
//      .mode(SaveMode.Overwrite).save()

//    spark.sql("select count(ID),count(name),count(city),count(age) from personTable").show()

//    spark.sql(
//      s"""
//         | SELECT *
//         | FROM carbonsession_table WHERE length(stringField) = 5
//       """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         | SELECT *
//         | FROM carbonsession_table WHERE date_format(dateField, "yyyy-MM-dd") = "2015-07-23"
//       """.stripMargin).show()
//
//    spark.sql("SELECT count(stringField) FROM carbonsession_table").show()
//
//    spark.sql(
//      s"""
//         | SELECT sum(intField), stringField
//         | FROM carbonsession_table
//         | GROUP BY stringField
//       """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         | SELECT t1.*, t2.*
//         | FROM carbonsession_table t1, carbonsession_table t2
//         | WHERE t1.stringField = t2.stringField
//      """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         | WITH t1 AS (
//         | SELECT * FROM carbonsession_table
//         | UNION ALL
//         | SELECT * FROM carbonsession_table
//         | )
//         | SELECT t1.*, t2.*
//         | FROM t1, carbonsession_table t2
//         | WHERE t1.stringField = t2.stringField
//      """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         | SELECT *
//         | FROM carbonsession_table
//         | WHERE stringField = 'spark' and floatField > 2.8
//       """.stripMargin).show()
//
//    spark.sql(
//      s"""
//         | CREATE TABLE stored_as_carbondata_table(
//         |    name STRING,
//         |    age INT
//         |    )
//         | STORED AS carbondata
//       """.stripMargin)
//    spark.sql("INSERT INTO stored_as_carbondata_table VALUES ('Bob',28) ")
//    spark.sql("SELECT * FROM stored_as_carbondata_table").show()
//
//    // Drop table
//    spark.sql("DROP TABLE IF EXISTS carbonsession_table")
//    spark.sql("DROP TABLE IF EXISTS stored_as_carbondata_table")
  }
}

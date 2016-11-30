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

package org.apache.spark.carbondata

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CarbonDataSourceSuite extends FunSuite with BeforeAndAfterAll {
  var spark: SparkSession = null
  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("CarbonExample")
      .enableHiveSupport()
      .config(CarbonCommonConstants.STORE_LOCATION,
        s"examples/spark2/target/store")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Drop table
    spark.sql("DROP TABLE IF EXISTS carbon_table")
    spark.sql("DROP TABLE IF EXISTS csv_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_testtable(
         |    shortField short,
         |    intField int,
         |    bigintField long,
         |    doubleField double,
         |    stringField string
         | )
         | USING org.apache.spark.sql.CarbonSource
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    spark.sql("drop table carbon_testtable")
    spark.sparkContext.stop()
    spark = null
  }

  test("project") {
    spark.sql("select * from carbon_testtable").collect()
  }


  test("agg") {
    spark.sql("select stringField, sum(intField) from carbon_testtable group by stringField").collect()
  }

}

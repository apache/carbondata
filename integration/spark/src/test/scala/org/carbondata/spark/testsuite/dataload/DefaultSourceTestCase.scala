/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.spark.testsuite.dataload

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.util.Random

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.common.util.CarbonHiveContext.sql
import org.apache.spark.sql.common.util.QueryTest

import org.scalatest.BeforeAndAfterAll

/**
 * Test Case for new defaultsource: com.databricks.spark.csv.newapi
 *
 * @date: Apr 10, 2016 10:34:58 PM
 * @See org.carbondata.spark.util.GlobalDictionaryUtil
 */
class DefaultSourceTestCase extends QueryTest with BeforeAndAfterAll {

  var filePath: String = _

  override def beforeAll {
    buildTestData
    buildTable
  }

  def buildTestData() = {
    val workDirectory = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    filePath = workDirectory + "/target/defaultsource.csv"
    val file = new File(filePath)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write("c1,c2,c3,c4")
    writer.newLine()
    var i = 0
    val random = new Random
    for (i <- 0 until 2000000) {
      writer.write("   aaaaaaa" + i + "  ,   " +
        "bbbbbbb" + i % 1000 + "," +
        i % 1000000 + "," + i % 10000 + "\n")
    }
    writer.close
  }

  def buildTable() = {
    try {
      sql("drop table if exists defaultsource")
      sql("""create table if not exists defaultsource
             (c1 string, c2 string, c3 int, c4 int)
             STORED BY 'org.apache.carbondata.format'""")
    } catch {
      case ex: Throwable => logError(ex.getMessage + "\r\n" + ex.getStackTraceString)
    }
  }

  test("test new defaultsource: com.databricks.spark.csv.newapi") {
    val df1 = CarbonHiveContext.read
      .format("com.databricks.spark.csv.newapi")
      .option("header", "true")
      .option("delimiter", ",")
      .option("parserLib", "univocity")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load(filePath)

    assert(!df1.first().getString(0).startsWith(" "))
    assert(df1.count() == 2000000)
    assert(df1.rdd.partitions.length == 3)
  }

  test("test defaultsource: com.databricks.spark.csv") {
    val df2 = CarbonHiveContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("parserLib", "univocity")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load(filePath)

    assert(!df2.first().getString(0).startsWith(" "))
    assert(df2.count() == 2000000)
    assert(df2.rdd.partitions.length == 3)
  }
}

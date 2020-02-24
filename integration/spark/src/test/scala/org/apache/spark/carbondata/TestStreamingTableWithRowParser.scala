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

import java.io.PrintWriter
import java.math.BigDecimal
import java.net.{BindException, ServerSocket}
import java.sql.{Date, Timestamp}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatus}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

case class FileElement(school: Array[String], age: Integer)
case class StreamData(id: Integer, name: String, city: String, salary: java.lang.Float,
    tax: BigDecimal, percent: java.lang.Double, birthday: String,
    register: String, updated: String,
    file: FileElement)

class TestStreamingTableWithRowParser extends QueryTest with BeforeAndAfterAll {

  private val spark = sqlContext.sparkSession
  private val dataFilePath = s"$resourcesPath/streamSample.csv"

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("DROP DATABASE IF EXISTS streaming1 CASCADE")
    sql("CREATE DATABASE streaming1")
    sql("USE streaming1")

    dropTable()

    createTable(tableName = "stream_table_filter", streaming = true, withBatchLoad = true)
    createTable(tableName = "stream_table_with_mi", streaming = true, withBatchLoad = true)

    createTableWithComplexType(
      tableName = "stream_table_filter_complex", streaming = true, withBatchLoad = true)
  }

  override def afterAll {
    dropTable()
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming1 CASCADE")
  }

  def dropTable(): Unit = {
    sql("drop table if exists streaming1.stream_table_filter")
    sql("drop table if exists streaming1.stream_table_with_mi")

    sql("drop table if exists streaming1.stream_table_filter_complex")
  }

  test("query on stream table with dictionary, sort_columns") {
    executeStreamingIngest(
      tableName = "stream_table_filter",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force",
      autoHandoff = false
    )

    // non-filter
    val result = sql("select * from streaming1.stream_table_filter order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(1).isNullAt(0))
    assert(result(1).getString(1) == "name_6")
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")

    // filter
    checkAnswer(
      sql("select * from stream_table_filter where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name in ('name_9','name_10', 'name_11', 'name_12') and id <> 10 and id not in (11, 12)"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name = 'name_3'"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name like '%me_3%' and id < 30"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(sql("select count(*) from stream_table_filter where name like '%ame%'"),
      Seq(Row(49)))

    checkAnswer(sql("select count(*) from stream_table_filter where name like '%batch%'"),
      Seq(Row(5)))

    checkAnswer(
      sql("select * from stream_table_filter where name >= 'name_3' and id < 4"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and name <> 'name_10' and name not in ('name_11', 'name_12')"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city like '%ty_1%' and ( id < 10 or id >= 100000001)"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(sql("select count(*) from stream_table_filter where city like '%city%'"),
      Seq(Row(54)))

    checkAnswer(
      sql("select * from stream_table_filter where city > 'city_09' and city < 'city_10'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city between 'city_09' and 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0")),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and city <> 'city_10' and city not in ('city_11', 'city_12')"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary = 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary > 80000 and salary <= 100000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary between 80001 and 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10, 11, 12) and salary <> 100000.0 and salary not in (110000.0, 120000.0)"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax = 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax >= 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax < 0.05 and tax > 0.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax between 0.02 and 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and tax <> 0.01"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent = 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent >= 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent < 80.05 and percent > 80.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent between 80.02 and 80.05 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and percent <> 80.01"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday = '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday > '1990-01-03' and birthday <= '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and birthday <> '1990-01-01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and register <> '2010-01-01 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where updated between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0")),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id in (9, 10) and updated <> '2010-01-01 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id is null order by name"),
      Seq(Row(null, "", "", null, null, null, null, null, null),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where name = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and name <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where city = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and city <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where salary is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where tax is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and tax is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where percent is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and percent is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where birthday is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and birthday is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where register is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and register is not null"),
      Seq())

    checkAnswer(
      sql("select * from stream_table_filter where updated is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null)))

    checkAnswer(
      sql("select * from stream_table_filter where id is null and updated is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"))))

    // agg
    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(id) as integer), sum(id) " +
          "from stream_table_filter where id >= 2 and id <= 100000004"),
      Seq(Row(51, 100000004, "batch_1", 7843162, 400001276)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(id) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_filter " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 50000001, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 100000002, 0.2, 0.2),
        Row("city_3", 2, 100000006, 50000003, 30000.0, 0.3)))

    // batch loading
    for(_ <- 0 to 2) {
      executeBatchLoad("stream_table_filter")
    }
    checkAnswer(
      sql("select count(*) from streaming1.stream_table_filter"),
      Seq(Row(25 * 2 + 5 + 5 * 3)))

    sql("alter table streaming1.stream_table_filter compact 'minor'")
    Thread.sleep(5000)
    val result1 = sql("show segments for table streaming1.stream_table_filter").collect()
    result1.foreach { row =>
      if (row.getString(0).equals("1")) {
        assertResult(SegmentStatus.STREAMING.getMessage)(row.getString(1))
        assertResult(FileFormat.ROW_V1.toString)(row.getString(5).toLowerCase)
      } else if (row.getString(0).equals("0.1")) {
        assertResult(SegmentStatus.SUCCESS.getMessage)(row.getString(1))
        assertResult(FileFormat.COLUMNAR_V3.toString)(row.getString(5).toLowerCase)
      } else {
        assertResult(SegmentStatus.COMPACTED.getMessage)(row.getString(1))
        assertResult(FileFormat.COLUMNAR_V3.toString)(row.getString(5).toLowerCase)
      }
    }

  }

  test("query on stream table with dictionary, sort_columns and complex column") {
    executeStreamingIngest(
      tableName = "stream_table_filter_complex",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force",
      autoHandoff = false
    )

    // non-filter
    val result = sql("select * from streaming1.stream_table_filter_complex order by id, name").collect()
    assert(result != null)
    assert(result.length == 55)
    // check one row of streaming data
    assert(result(0).isNullAt(0))
    assert(result(0).getString(1) == "")
    assert(result(0).getStruct(9).isNullAt(1))
    // check one row of batch loading
    assert(result(50).getInt(0) == 100000001)
    assert(result(50).getString(1) == "batch_1")
    assert(result(50).getStruct(9).getInt(1) == 20)
    sql("select * from streaming1.stream_table_filter_complex where id = 1").show
    // filter
    checkAnswer(
      sql("select * from stream_table_filter_complex where id = 1"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id > 49 and id < 100000002"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id between 50 and 100000001"),
      Seq(Row(50, "name_50", "city_50", 500000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_50", "school_5050")), 50)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name = 'name_3'"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name like '%me_3%' and id < 30"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where name like '%ame%'"),
      Seq(Row(49)))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where name like '%batch%'"),
      Seq(Row(5)))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name >= 'name_3' and id < 4"),
      Seq(Row(3, "name_3", "city_3", 30000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_3", "school_33")), 3))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city like '%ty_1%' and ( id < 10 or id >= 100000001)"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(sql("select count(*) from stream_table_filter_complex where city like '%city%'"),
      Seq(Row(54)))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city > 'city_09' and city < 'city_10'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city between 'city_09' and 'city_1'"),
      Seq(Row(1, "name_1", "city_1", 10000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 1)),
        Row(100000001, "batch_1", "city_1", 0.1, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_1", "school_11")), 20))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary = 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary > 80000 and salary <= 100000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(10, "name_10", "city_10", 100000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_10", "school_1010")), 10))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary between 80001 and 90000"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax = 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax >= 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax < 0.05 and tax > 0.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax between 0.02 and 0.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent = 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent >= 80.04 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent < 80.05 and percent > 80.02 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent between 80.02 and 80.05 and id < 100"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday = '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday > '1990-01-03' and birthday <= '1990-01-04'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday between '1990-01-04' and '1990-01-05'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")),50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated = '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated > '2010-01-03 10:01:01' and register <= '2010-01-04 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated between '2010-01-04 10:01:01' and '2010-01-05 10:01:01'"),
      Seq(Row(9, "name_9", "city_9", 90000.0, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_9", "school_99")), 9)),
        Row(100000004, "batch_4", "city_4", 0.4, BigDecimal.valueOf(0.04), 80.04, Date.valueOf("1990-01-04"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Timestamp.valueOf("2010-01-04 10:01:01.0"), Row(wrap(Array("school_4", "school_44")), 50)),
        Row(100000005, "batch_5", "city_5", 0.5, BigDecimal.valueOf(0.05), 80.05, Date.valueOf("1990-01-05"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Timestamp.valueOf("2010-01-05 10:01:01.0"), Row(wrap(Array("school_5", "school_55")), 60))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null order by name"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null)),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where name = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and name <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where city = ''"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and city <> ''"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where salary is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where tax is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and tax is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where percent is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and salary is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where birthday is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and birthday is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where register is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null)),
        Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and register is not null"),
      Seq())

    checkAnswer(
      sql("select * from stream_table_filter_complex where updated is null"),
      Seq(Row(null, "", "", null, null, null, null, null, null, Row(wrap(Array(null)), null))))

    checkAnswer(
      sql("select * from stream_table_filter_complex where id is null and updated is not null"),
      Seq(Row(null, "name_6", "city_6", 60000.0, BigDecimal.valueOf(0.01), 80.01, Date.valueOf("1990-01-01"), null, Timestamp.valueOf("2010-01-01 10:01:01.0"), Row(wrap(Array("school_6", "school_66")), 6))))

    // agg
    checkAnswer(
      sql("select count(*), max(id), min(name), cast(avg(file.age) as integer), sum(file.age) " +
          "from stream_table_filter_complex where id >= 2 and id <= 100000004"),
      Seq(Row(51, 100000004, "batch_1", 27, 1406)))

    checkAnswer(
      sql("select city, count(id), sum(id), cast(avg(file.age) as integer), " +
          "max(salary), min(salary) " +
          "from stream_table_filter_complex " +
          "where name in ('batch_1', 'batch_2', 'batch_3', 'name_1', 'name_2', 'name_3') " +
          "and city <> '' " +
          "group by city " +
          "order by city"),
      Seq(Row("city_1", 2, 100000002, 10, 10000.0, 0.1),
        Row("city_2", 1, 100000002, 30, 0.2, 0.2),
        Row("city_3", 2, 100000006, 21, 30000.0, 0.3)))
  }

  test("alter on stream table with dictionary, sort_columns and complex column") {
    executeStreamingIngest(
      tableName = "stream_table_filter_complex",
      batchNums = 2,
      rowNumsEachBatch = 25,
      intervalOfSource = 5,
      intervalOfIngest = 5,
      continueSeconds = 20,
      generateBadRecords = true,
      badRecordAction = "force",
      autoHandoff = false
    )

    sql("SHOW SEGMENTS FOR TABLE streaming1.stream_table_filter_complex").show
    sql("ALTER TABLE streaming1.stream_table_filter_complex COMPACT 'close_streaming'")
    sql("SHOW SEGMENTS FOR TABLE streaming1.stream_table_filter_complex").show

  }

  def createWriteSocketThread(
      serverSocket: ServerSocket,
      writeNums: Int,
      rowNums: Int,
      intervalSecond: Int,
      badRecords: Boolean = false): Thread = {
    new Thread() {
      override def run(): Unit = {
        // wait for client to connection request and accept
        val clientSocket = serverSocket.accept()
        val socketWriter = new PrintWriter(clientSocket.getOutputStream())
        var index = 0
        for (_ <- 1 to writeNums) {
          // write 5 records per iteration
          val stringBuilder = new StringBuilder()
          for (_ <- 1 to rowNums) {
            index = index + 1
            if (badRecords) {
              if (index == 2) {
                // null value
                stringBuilder.append(",,,,,,,,,")
              } else if (index == 6) {
                // illegal number
                stringBuilder.append(index.toString + "abc,name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-0110:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else if (index == 9) {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.04,80.04" +
                                     ",1990-01-04,2010-01-04 10:01:01,2010-01-04 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              } else {
                stringBuilder.append(index.toString + ",name_" + index
                                     + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                     ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                     ",school_" + index + ":school_" + index + index + "$" + index)
              }
            } else {
              stringBuilder.append(index.toString + ",name_" + index
                                   + ",city_" + index + "," + (10000.00 * index).toString + ",0.01,80.01" +
                                   ",1990-01-01,2010-01-01 10:01:01,2010-01-01 10:01:01" +
                                   ",school_" + index + ":school_" + index + index + "$" + index)
            }
            stringBuilder.append("\n")
          }
          socketWriter.append(stringBuilder.toString())
          socketWriter.flush()
          Thread.sleep(1000 * intervalSecond)
        }
        socketWriter.close()
      }
    }
  }

  def createSocketStreamingThread(
      spark: SparkSession,
      port: Int,
      tablePath: String,
      tableIdentifier: TableIdentifier,
      badRecordAction: String = "force",
      intervalSecond: Int = 2,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Thread = {
    new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          import spark.implicits._
          val readSocketDF = spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", port)
            .load()
            .as[String]
            .map(_.split(","))
            .map { fields => {
              if (fields.length == 0) {
                StreamData(null, "", "", null, null, null, null, null, null, null)
              } else {
                val tmp = fields(9).split("\\$")
                val file = FileElement(tmp(0).split(":"), tmp(1).toInt)
                if (fields(1).equals("name_6")) {
                  StreamData(null, fields(1), fields(2), fields(3).toFloat,
                      BigDecimal.valueOf(fields(4).toDouble), fields(5).toDouble,
                      fields(6), fields(7), fields(8), file)
                } else {
                  StreamData(fields(0).toInt, fields(1), fields(2), fields(3).toFloat,
                      BigDecimal.valueOf(fields(4).toDouble), fields(5).toDouble,
                      fields(6), fields(7), fields(8), file)
                }
              }
            }
            }

          // Write data from socket stream to carbondata file
          qry = readSocketDF.writeStream
            .format("carbondata")
            .trigger(ProcessingTime(s"$intervalSecond seconds"))
            .option("checkpointLocation", CarbonTablePath.getStreamingCheckpointDir(tablePath))
            .option("bad_records_action", badRecordAction)
            .option("dbName", tableIdentifier.database.get)
            .option("tableName", tableIdentifier.table)
            .option(CarbonCommonConstants.HANDOFF_SIZE, handoffSize)
            .option("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
            .option(CarbonCommonConstants.ENABLE_AUTO_HANDOFF, autoHandoff)
            .start()
          qry.awaitTermination()
        } catch {
          case ex: Throwable =>
            throw new Exception(ex.getMessage)
        } finally {
          if (null != qry) {
            qry.stop()
          }
        }
      }
    }
  }

  /**
   * start ingestion thread: write `rowNumsEachBatch` rows repeatly for `batchNums` times.
   */
  def executeStreamingIngest(
      tableName: String,
      batchNums: Int,
      rowNumsEachBatch: Int,
      intervalOfSource: Int,
      intervalOfIngest: Int,
      continueSeconds: Int,
      generateBadRecords: Boolean,
      badRecordAction: String,
      handoffSize: Long = CarbonCommonConstants.HANDOFF_SIZE_DEFAULT,
      autoHandoff: Boolean = CarbonCommonConstants.ENABLE_AUTO_HANDOFF_DEFAULT.toBoolean
  ): Unit = {
    val identifier = new TableIdentifier(tableName, Option("streaming1"))
    val carbonTable = CarbonEnv.getInstance(spark).carbonMetaStore.lookupRelation(identifier)(spark)
      .asInstanceOf[CarbonRelation].carbonTable
    var server: ServerSocket = null
    try {
      server = getServerSocket()
      val thread1 = createWriteSocketThread(
        serverSocket = server,
        writeNums = batchNums,
        rowNums = rowNumsEachBatch,
        intervalSecond = intervalOfSource,
        badRecords = generateBadRecords)
      val thread2 = createSocketStreamingThread(
        spark = spark,
        port = server.getLocalPort,
        tablePath = carbonTable.getTablePath,
        tableIdentifier = identifier,
        badRecordAction = badRecordAction,
        intervalSecond = intervalOfIngest,
        handoffSize = handoffSize,
        autoHandoff = autoHandoff)
      thread1.start()
      thread2.start()
      Thread.sleep(continueSeconds * 1000)
      thread2.interrupt()
      thread1.interrupt()
    } finally {
      if (null != server) {
        server.close()
      }
    }
  }

  def createTable(tableName: String, streaming: Boolean, withBatchLoad: Boolean): Unit = {
    sql(
      s"""
         | CREATE TABLE streaming1.$tableName(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def createTableWithComplexType(
      tableName: String,
      streaming: Boolean,
      withBatchLoad: Boolean): Unit = {
    sql(
      s"""
         | CREATE TABLE streaming1.$tableName(
         | id INT,
         | name STRING,
         | city STRING,
         | salary FLOAT,
         | tax DECIMAL(8,2),
         | percent double,
         | birthday DATE,
         | register TIMESTAMP,
         | updated TIMESTAMP,
         | file struct<school:array<string>, age:int>
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(${if (streaming) "'streaming'='true', " else "" }
         | 'sort_columns'='name')
         | """.stripMargin)

    if (withBatchLoad) {
      // batch loading 5 rows
      executeBatchLoad(tableName)
    }
  }

  def executeBatchLoad(tableName: String): Unit = {
    sql(
      s"LOAD DATA LOCAL INPATH '$dataFilePath' INTO TABLE streaming1.$tableName OPTIONS" +
      "('HEADER'='true','COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
  }

  def wrap(array: Array[String]) = {
    new mutable.WrappedArray.ofRef(array)
  }

  /**
   * get a ServerSocket
   * if the address was already used, it will retry to use new port number.
   *
   * @return ServerSocket
   */
  def getServerSocket(): ServerSocket = {
    var port = 7071
    var serverSocket: ServerSocket = null
    var retry = false
    do {
      try {
        retry = false
        serverSocket = new ServerSocket(port)
      } catch {
        case ex: BindException =>
          retry = true
          port = port + 2
          if (port >= 65535) {
            throw ex
          }
      }
    } while (retry)
    serverSocket
  }

}

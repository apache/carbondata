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

package org.apache.carbondata.spark.testsuite.dataload

import java.io.{File, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.classTag

import org.apache.spark.DataSkewRangePartitioner
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.{CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.load.PrimtiveOrdering

class TestRangeColumnDataLoad extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  var filePath: String = s"$resourcesPath/globalsort"
  var filePath2: String = s"$resourcesPath/range_compact_test"
  var filePath3: String = s"$resourcesPath/range_compact_test1"
  var filePath4: String = s"$resourcesPath/range_compact_test2"

  override def beforeAll(): Unit = {
    defaultConfig()
    dropTable
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    dropTable
  }

  def dropTable(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql("DROP TABLE IF EXISTS carbon_range_column2")
    sql("DROP TABLE IF EXISTS carbon_range_column3")
    sql("DROP TABLE IF EXISTS carbon_range_column4")
    sql("DROP TABLE IF EXISTS carbon_range_column5")
    sql("DROP TABLE IF EXISTS carbon_range_column6")
  }

  test("range_column with option GLOBAL_SORT_PARTITIONS") {
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='1')")

    assert(getIndexFileCount("carbon_range_column1") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_range_column1"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_range_column1"),
      sql("SELECT * FROM carbon_range_column1 ORDER BY name"))
  }

  test("range_column with option scale_factor") {
    sql(
      """
        | CREATE TABLE carbon_range_column2(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column2 " +
        "OPTIONS('scale_factor'='10')")

    assert(getIndexFileCount("carbon_range_column2") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_range_column2"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_range_column2"),
      sql("SELECT * FROM carbon_range_column2 ORDER BY name"))
  }

  test("only support single column for create table") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql(
        """
          | CREATE TABLE carbon_range_column3(id INT, name STRING, city STRING, age INT)
          | STORED AS carbondata
          | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='name,id')
        """.stripMargin)
    }
    assertResult("range_column not support multiple columns")(ex.getMessage)
  }

  test("load data command not support range_column") {
    sql(
      """
        | CREATE TABLE carbon_range_column3(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='name')
      """.stripMargin)

    val ex = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column3 " +
          "OPTIONS('scale_factor'='10', 'range_column'='name')")
    }
    assertResult("Error: Invalid option(s): range_column")(ex.getMessage)
  }

  test("range_column with data skew") {
    sql(
      """
        | CREATE TABLE carbon_range_column4(c1 int, c2 string)
        | STORED AS carbondata
        | TBLPROPERTIES('sort_columns'='c1,c2', 'sort_scope'='local_sort', 'range_column'='c2')
      """.stripMargin)
    val dataSkewPath = s"$resourcesPath/range_column"
    sql(
      s"""LOAD DATA LOCAL INPATH '$dataSkewPath'
         | INTO TABLE carbon_range_column4
         | OPTIONS('FILEHEADER'='c1,c2', 'global_sort_partitions'='10')
        """.stripMargin)

    assert(getIndexFileCount("carbon_range_column4") === 9)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_range_column4"), Seq(Row(20)))
  }

  test("Describe formatted for Range Column") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age SHORT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS'='age, city',
        | 'range_column'='age')
      """.stripMargin)
    val desc = sql("Desc formatted carbon_range_column1").collect()
    assert(desc.exists(_.toString().contains("RANGE COLUMN")))
    assert(desc.exists(_.toString().contains("age")))
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - SHORT Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age SHORT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='age, city', 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
  }

  test("Test compaction for range_column - INT Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='age, city', 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - Partition Column") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING)
        | PARTITIONED BY (age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS'='age, city',
        | 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - 2 levels") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS'='age, city',
        | 'range_column'='age')
      """.stripMargin)

    for (i <- 0 until 12) {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
          "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")
    }

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MINOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - CUSTOM Compaction") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS'='age, city',
        | 'range_column'='age')
      """.stripMargin)

    for (i <- 0 until 12) {
      sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
          "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")
    }

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'CUSTOM' WHERE SEGMENT.ID IN(3,4,5)")

    checkAnswer(sql("select * from carbon_range_column1"), res)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - INT Datatype with null values") {
    deleteFile(filePath3)
    createFile(filePath3, 2000, 3)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city',
        | 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    deleteFile(filePath3)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - BOOLEAN Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    val exception = intercept[MalformedCarbonCommandException](
      sql(
        """
          | CREATE TABLE carbon_range_column1(id Boolean, name STRING, city STRING, age INT)
          | STORED AS carbondata
          | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='id, city',
          | 'range_column'='id')
        """.stripMargin)
    )

    assertResult("RANGE_COLUMN doesn't support boolean data type: id")(exception.getMessage)
  }

  test("Test compaction for range_column - DECIMAL Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    val exception = intercept[MalformedCarbonCommandException](
      sql(
        """
          | CREATE TABLE carbon_range_column1(id decimal, name STRING, city STRING, age INT)
          | STORED AS carbondata
          | TBLPROPERTIES('range_column'='id')
        """.stripMargin)
    )

    assertResult("RANGE_COLUMN doesn't support decimal data type: id")(exception.getMessage)
  }

  test("Test compaction for range_column - INT Datatype with no overlapping") {
    deleteFile(filePath2)
    createFile(filePath2, 1000, 4)
    deleteFile(filePath3)
    createFile(filePath3, 1000, 5)
    deleteFile(filePath4)
    createFile(filePath4, 1000, 6)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='id, city',
        | 'range_column'='id')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='1')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='2')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath4' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    deleteFile(filePath2)
    deleteFile(filePath3)
    deleteFile(filePath4)

    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - INT Datatype with overlapping") {
    deleteFile(filePath2)
    createFile(filePath2, 10, 9)
    deleteFile(filePath3)
    createFile(filePath3, 10, 10)
    deleteFile(filePath4)
    createFile(filePath4, 10, 11)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='id, city',
        | 'range_column'='id')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath3' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath4' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    deleteFile(filePath2)
    deleteFile(filePath3)
    deleteFile(filePath4)

    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Compact range_column with data skew") {
    sql("DROP TABLE IF EXISTS carbon_range_column4")
    sql(
      """
        | CREATE TABLE carbon_range_column4(c1 int, c2 string)
        | STORED AS carbondata
        | TBLPROPERTIES('sort_columns'='c1,c2', 'sort_scope'='local_sort', 'range_column'='c2')
      """.stripMargin)

    val dataSkewPath = s"$resourcesPath/range_column"

    sql(
      s"""LOAD DATA LOCAL INPATH '$dataSkewPath'
         | INTO TABLE carbon_range_column4
         | OPTIONS('FILEHEADER'='c1,c2', 'global_sort_partitions'='10')
        """.stripMargin)

    sql(
      s"""LOAD DATA LOCAL INPATH '$dataSkewPath'
         | INTO TABLE carbon_range_column4
         | OPTIONS('FILEHEADER'='c1,c2', 'global_sort_partitions'='10')
        """.stripMargin)

    val res = sql("SELECT * FROM carbon_range_column4").collect()

    sql("ALTER TABLE carbon_range_column4 COMPACT 'MAJOR'")

    checkAnswer(sql("SELECT * FROM carbon_range_column4"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column4")
  }

  test("Test compaction for range_column - INT Datatype without SORT Column") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
  }

  test("Test compaction for range_column - INT Datatype with single value in range column") {
    deleteFile(filePath2)
    createFile(filePath2, 10, 8)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('range_column'='id')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
    deleteFile(filePath2)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - LONG Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age LONG)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='age, city', 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
  }

  test("Test compaction for range_column - LONG Datatype without SORT Column") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age LONG)
        | STORED AS carbondata
        | TBLPROPERTIES('range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)
  }

  test("Test compaction for range_column - STRING Datatype") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age LONG)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city',
        | 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('SORT_SCOPE'='GLOBAL_SORT','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column1 " +
        "OPTIONS('SORT_SCOPE'='NO_SORT','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")
  }

  test("Test compaction for range_column - STRING Datatype null values") {
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    deleteFile(filePath2)
    createFile(filePath2, 20, 14)
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age LONG)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='city',
        | 'range_column'='city')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('BAD_RECORDS_ACTION'='FORCE','HEADER'='false')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('BAD_RECORDS_ACTION'='FORCE','HEADER'='false')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")
    deleteFile(filePath2)
  }

  test("Test compaction for range_column - STRING Datatype min/max not stored") {
    deleteFile(filePath2)
    createFile(filePath2, 1000, 7)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT, "10")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age LONG)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city',
        | 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        "OPTIONS('HEADER'='false','GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select name from carbon_range_column1 order by name").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select name from carbon_range_column1 order by name"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT,
        CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT_DEFAULT)
    deleteFile(filePath2)
  }

  test("Test compaction for range_column - DATE Datatype") {
    deleteFile(filePath2)
    createFile(filePath2, 12, 0)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age DATE)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='age, city' ,
        | 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")

    deleteFile(filePath2)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("Test compaction for range_column - TIMESTAMP Datatype skewed data") {
    deleteFile(filePath2)
    createFile(filePath2, 12, 1)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:SS")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, age TIMESTAMP)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='city' ,
        | 'range_column'='age')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")

    deleteFile(filePath2)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("Test compaction for range_column - Float Datatype") {
    deleteFile(filePath2)
    createFile(filePath2, 12, 2)
    sql("DROP TABLE IF EXISTS carbon_range_column1")
    sql(
      """
        | CREATE TABLE carbon_range_column1(id INT, name STRING, city STRING, floatval FLOAT)
        | STORED AS carbondata
        | TBLPROPERTIES('range_column'='floatval')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    sql(s"LOAD DATA LOCAL INPATH '$filePath2' INTO TABLE carbon_range_column1 " +
        s"OPTIONS('HEADER'='false', 'GLOBAL_SORT_PARTITIONS'='3')")

    var res = sql("select * from carbon_range_column1").collect()

    sql("ALTER TABLE carbon_range_column1 COMPACT 'MAJOR'")

    checkAnswer(sql("select * from carbon_range_column1"), res)

    sql("DROP TABLE IF EXISTS carbon_range_column1")

    deleteFile(filePath2)

  }

  test("DataSkewRangePartitioner.combineDataSkew") {
    val partitioner =
      new DataSkewRangePartitioner(1, null,
        false)(new PrimtiveOrdering(DataTypes.STRING),
        classTag[Object])

    testCombineDataSkew(
      partitioner,
      Array("a", "b"),
      0)

    testCombineDataSkew(
      partitioner,
      Array("a", "a"),
      1,
      Array(0),
      Array(2))

    testCombineDataSkew(
      partitioner,
      Array("a", "b", "c"),
      0)

    testCombineDataSkew(
      partitioner,
      Array("a", "b", "b", "c", "c", "c"),
      2,
      Array(1, 2),
      Array(2, 3))

    testCombineDataSkew(
      partitioner,
      Array("a", "b", "b", "b", "c", "c"),
      2,
      Array(1, 2),
      Array(3, 2))

    testCombineDataSkew(
      partitioner,
      Array("a", "a", "b", "b", "c", "c"),
      3,
      Array(0, 1, 2),
      Array(2, 2, 2))

    testCombineDataSkew(
      partitioner,
      Array("a", "a", "a", "b", "c", "c"),
      2,
      Array(0, 2),
      Array(3, 2))

    testCombineDataSkew(
      partitioner,
      Array("a", "a", "a", "b", "b", "c"),
      2,
      Array(0, 1),
      Array(3, 2))

    testCombineDataSkew(
      partitioner,
      Array("a", "a", "b", "b", "b", "c"),
      2,
      Array(0, 1),
      Array(2, 3))
  }

  private def testCombineDataSkew(partitioner: DataSkewRangePartitioner[Object, Nothing],
      bounds: Array[String], skewCount: Int, skewIndexes: Array[Int] = null,
      skewWeights: Array[Int] = null
  ): Unit = {
    val boundsBuffer = new ArrayBuffer[Object]()
    bounds.map(_.getBytes()).foreach(boundsBuffer += _)
    val (_, actualSkewCount, actualSkewIndexes, actualSkewWeights) =
      partitioner.combineDataSkew(boundsBuffer)
    assertResult(skewCount)(actualSkewCount)
    if (skewCount > 0) {
      assertResult(skewIndexes)(actualSkewIndexes)
      assertResult(skewWeights)(actualSkewWeights)
    }
  }

  test("range_column with system property carbon.range.column.scale.factor") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_RANGE_COLUMN_SCALE_FACTOR,
      "10"
    )

    sql(
      """
        | CREATE TABLE carbon_range_column5(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='name')
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$filePath' INTO TABLE carbon_range_column5 ")

    assert(getIndexFileCount("carbon_range_column5") === 1)
    checkAnswer(sql("SELECT COUNT(*) FROM carbon_range_column5"), Seq(Row(12)))
    checkAnswer(sql("SELECT * FROM carbon_range_column5"),
      sql("SELECT * FROM carbon_range_column5 ORDER BY name"))
  }

  test("set and unset table property: range_column") {
    sql(
      """
        | CREATE TABLE carbon_range_column6(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city')
      """.stripMargin)

    sql("ALTER TABLE carbon_range_column6 SET TBLPROPERTIES('range_column'='city')")
    sql("ALTER TABLE carbon_range_column6 SET TBLPROPERTIES('range_column'='name')")
    sql("ALTER TABLE carbon_range_column6 UNSET TBLPROPERTIES('range_column')")
    sql("ALTER TABLE carbon_range_column6 SET TBLPROPERTIES('range_column'='name')")
  }

  private def getIndexFileCount(tableName: String, segmentNo: String = "0"): Int = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", tableName)
    val segmentDir = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, segmentNo)
    if (FileFactory.isFileExist(segmentDir)) {
      new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir).size()
    } else {
      val segment = Segment.getSegment(segmentNo, carbonTable.getTablePath)
      new SegmentFileStore(carbonTable.getTablePath, segment.getSegmentFileName).getIndexCarbonFiles
        .size()
    }
  }

  def createFile(fileName: String, line: Int = 10000, lastCol: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(new File(fileName))
      val start = 0
      if (0 == lastCol) {
        // Date data generation
        for (i <- start until (start + line)) {
          write.println(i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i) + "-10-10")
        }
      } else if (1 == lastCol) {
        // Timestamp data generation
        for (i <- start until (start + line)) {
          if (i == start) {
            write
              .println(i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i) + "-10-10 " +
                       "00:00:00")
          } else {
            write.println(i + "," + "n" + i + "," + "c" + (i % 10000) + ",")
          }
        }
      } else if (2 == lastCol) {
        // Float data generation
        for (i <- start until (start + line)) {
          write
            .println(i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i) + (i % 3.14))
        }
      } else if (3 == lastCol) {
        // Null data generation
        for (i <- start until (start + line)) {
          if (i % 3 != 0) {
            write
              .println(i + "," + "," + "c" + (i % 10000) + "," + (1990 + i))
          } else {
            write
              .println(i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i))
          }
        }
      } else if (4 <= lastCol && 6 >= lastCol) {
        // No overlap data generation 1
        for (i <- start until (start + line)) {
          write
            .println(
              (line * lastCol + i) + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i))
        }
      } else if (7 == lastCol) {
        // Min/max not stored data generation
        for (i <- start until (start + line)) {
          write
            .println(
              (100 * lastCol + i) + "," + "nnnnnnnnnnnn" + i + "," + "c" + (i % 10000) + "," +
              (1990 + i))
        }
      } else if (8 == lastCol) {
        // Range values less than default parallelism (Single value)
        for (i <- start until (start + line)) {
          write
            .println(
              100 + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i))
        }
      } else if (9 <= lastCol && 13 >= lastCol) {
        for (i <- lastCol until (lastCol + line)) {
          write
            .println(
              i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i))
        }
      } else if (14 == lastCol) {
        // Null data generation for string col
        for (i <- lastCol until (lastCol + line)) {
          if (i % 3 != 0) {
            write
              .println(
                i + "," + "n" + i + "," + "c" + (i % 10000) + "," + (1990 + i))
          } else {
            write
              .println(i + ",")
          }
        }
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }
}

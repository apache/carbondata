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

package org.apache.carbondata.index.bloom

import java.util.{Random, UUID}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{CarbonEnv, SaveMode, SparkSession}
import org.apache.spark.sql.test.SparkTestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonV3DataFormatConstants}
import org.apache.carbondata.core.index.status.IndexStatus
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.index.bloom.BloomCoarseGrainIndexTestUtil.{checkBasicQuery, createFile, deleteFile}

class BloomCoarseGrainIndexFunctionSuite
  extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val bigFile = s"$resourcesPath/bloom_index_function_test_big.csv"
  val normalTable = "carbon_normal"
  val bloomSampleTable = "carbon_bloom"
  val indexName = "bloom_dm"

  override protected def beforeAll(): Unit = {
    deleteFile(bigFile)
    createFile(bigFile, line = 2000)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  test("test bloom index: index column is integer, dictionary, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    sql(s"select * from $bloomSampleTable where id = 1").collect()
    sql(s"select * from $bloomSampleTable where city = 'city_1'").collect()
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP INDEX $indexName ON TABLE $bloomSampleTable")
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test bloom index: index column is integer, dictionary, not sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='name')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    sql(s"select * from $bloomSampleTable where id = 1").collect()
    sql(s"select * from $bloomSampleTable where city = 'city_1'").collect()
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test bloom index: index column is integer, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city, id)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    IndexStatusUtil.checkIndexStatus(bloomSampleTable, indexName, IndexStatus.ENABLED.name(),
      sqlContext.sparkSession, IndexType.BLOOMFILTER)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    sql(s"select * from $bloomSampleTable where id = 1").collect()
    sql(s"select * from $bloomSampleTable where city = 'city_1'").collect()
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test bloom index: index column is float, not dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (salary)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test bloom index: index column is float, dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (salary)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  // since float cannot be sort_columns, we skip the test case

  test("test bloom index: index column is date") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (doj)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  test("test bloom index: index column is date, dictionary, sort column") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno', 'sort_columns'='doj')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (doj)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  // timestamp is naturally not dictionary
  test("test bloom index: index column is timestamp") {
    val timeStampData = s"$resourcesPath/timeStampFormatData1.csv"
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $normalTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String,
         |  serialname String, salary Int)
         | STORED AS carbondata
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $normalTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $bloomSampleTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String,
         |  serialname String, salary Int)
         | STORED AS carbondata
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (starttime)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $bloomSampleTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:30.0'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:31.0'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:30.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:30.0'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:31.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:31.0'"))
  }

  test("test bloom index: index column is timestamp, dictionary, sort_column") {
    val timeStampData = s"$resourcesPath/timeStampFormatData1.csv"
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $normalTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String,
         |  serialname String, salary Int)
         | STORED AS carbondata
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $normalTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS $bloomSampleTable (
         | ID Int, date date, starttime Timestamp, country String, name String, phonetype String,
         |  serialname String, salary Int)
         | STORED AS carbondata
         | TBLPROPERTIES('dictionary_column'='starttime', 'sort_columns'='starttime')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (starttime)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$timeStampData' into table $bloomSampleTable
         | OPTIONS('dateformat' = 'yyyy/MM/dd','timestampformat'='yyyy-MM-dd HH:mm:ss')
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE starttime=null").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:30.0'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:31.0'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:30.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:30.0'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE starttime='2016-07-25 01:03:31.0'"),
      sql(s"SELECT * FROM $normalTable WHERE starttime='2016-07-25 01:03:31.0'"))
  }

  // it seems the CI env will be timeout on this test, just ignore it here
  ignore("test bloom index: loading and querying with empty values on index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(s"CREATE TABLE $bloomSampleTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(
      s"""
         | CREATE INDEX $indexName
         | on $bloomSampleTable (c1, c2)
         | as 'bloomfilter'
       """.stripMargin)

    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomSampleTable SELECT '', null, 'xxx'")

    // query on null fields
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable"),
      sql(s"SELECT * FROM $normalTable"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE c1 = null"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE c1 = ''"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = ''"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE isNull(c1)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c1)"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE isNull(c2)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c2)"))
  }

  test("test bloom index: querying with longstring index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) " +
        "STORED AS carbondata TBLPROPERTIES('long_string_columns'='c3')")
    sql(s"CREATE TABLE $bloomSampleTable(c1 string, c2 int, c3 string) " +
        "STORED AS carbondata TBLPROPERTIES('long_string_columns'='c3')")
    // create index on longstring columns
    sql(
      s"""
         | CREATE INDEX $indexName
         | on $bloomSampleTable (c3)
         | as 'bloomfilter'
       """.stripMargin)

    sql(s"INSERT INTO $normalTable SELECT 'c1v1', 1, 'xxx'")
    sql(s"INSERT INTO $bloomSampleTable SELECT 'c1v1', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT 'c1v1', 1, 'yyy'")
    sql(s"INSERT INTO $bloomSampleTable SELECT 'c1v1', 1, 'yyy'")

    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE c3 = 'xxx'"),
      sql(s"SELECT * FROM $normalTable WHERE c3 = 'xxx'"))
  }

  test("test rebuild bloom index: index column is integer, dictionary, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city,id,age,name)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test rebuild bloom index: index column is integer, dictionary, not sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='name')
         |  """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city,id,age,name)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test rebuild bloom index: index column is integer, sort_column") {
    sql(
      s"""
         | CREATE TABLE $normalTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata
         | TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $normalTable
         | OPTIONS('header'='false')
         """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$bigFile' INTO TABLE $bloomSampleTable
         | OPTIONS('header'='false')
         """.stripMargin)

    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (city,id,age,name)
         | AS 'bloomfilter'
         | properties('BLOOM_SIZE'='640000')
      """.stripMargin)

    sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect()
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, indexName)
    checkBasicQuery(indexName, bloomSampleTable, normalTable)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }

  test("test rebuild bloom index: index column is float, not dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (salary)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test drop index when more than one bloom index exists") {
    sql(s"CREATE TABLE $bloomSampleTable " +
      "(id int,name string,salary int)STORED as carbondata TBLPROPERTIES('SORT_COLUMNS'='id')")
    sql(s"CREATE index index1 ON TABLE $bloomSampleTable(id) as 'bloomfilter' " +
      "PROPERTIES ( 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001', 'BLOOM_COMPRESS'='true')")
    sql(s"CREATE index index2 ON TABLE $bloomSampleTable (name) as 'bloomfilter' " +
      "PROPERTIES ('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001', 'BLOOM_COMPRESS'='true')")
    sql(s"insert into $bloomSampleTable values(1,'nihal',20)")
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, "index1", "index2")
    sql(s"drop index index1 on $bloomSampleTable")
    checkExistence(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable"), true, "index2")
    sql(s"drop index index2 on $bloomSampleTable")
    val carbonTable = CarbonEnv.getCarbonTable(
      Option("default"), bloomSampleTable)(sqlContext.sparkSession)
    val isIndexExists = carbonTable.getTableInfo
      .getFactTable
      .getTableProperties
      .get("indexexists")
    assertResult("false")(isIndexExists)
    assert(sql(s"SHOW INDEXES ON TABLE $bloomSampleTable").collect().isEmpty)
  }

  test("test rebuild bloom index: index column is float, dictionary") {
    val floatCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$floatCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (salary)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040.56'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040.56'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE salary='1040'"),
      sql(s"SELECT * FROM $normalTable WHERE salary='1040'"))
  }

  test("test rebuild bloom index: index column is date") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (doj)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  test("test rebuild bloom index: index column is date, dictionary, sort_colum") {
    val dateCsvPath = s"$resourcesPath/datasamplefordate.csv"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
    sql(
      s"""
         | CREATE TABLE $normalTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno')
       """.stripMargin)
    sql(
      s"""
         | CREATE TABLE $bloomSampleTable(empno string, doj date, salary float)
         | STORED AS carbondata
         | TBLPROPERTIES('SORT_COLUMNS'='empno,doj')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $normalTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA INPATH '$dateCsvPath' INTO TABLE $bloomSampleTable OPTIONS(
         | 'DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE')
       """.stripMargin)
    sql(
      s"""
         | CREATE INDEX $indexName
         | ON $bloomSampleTable (doj)
         | AS 'bloomfilter'
       """.stripMargin)
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'").collect()
    sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'").collect()
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-14'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-14'"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE doj='2016-03-15'"),
      sql(s"SELECT * FROM $normalTable WHERE doj='2016-03-15'"))
  }

  ignore("test rebuild bloom index: loading and querying with empty values on index column") {
    sql(s"CREATE TABLE $normalTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(s"CREATE TABLE $bloomSampleTable(c1 string, c2 int, c3 string) STORED AS carbondata")

    // load data with empty value
    sql(s"INSERT INTO $normalTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $bloomSampleTable SELECT '', 1, 'xxx'")
    sql(s"INSERT INTO $normalTable SELECT '', null, 'xxx'")
    sql(s"INSERT INTO $bloomSampleTable SELECT '', null, 'xxx'")

    sql(
      s"""
         | CREATE INDEX $indexName
         | on $bloomSampleTable (c1, c2)
         | as 'bloomfilter'
       """.stripMargin)

    // query on null fields
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable"),
      sql(s"SELECT * FROM $normalTable"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE c1 = null"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = null"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE c1 = ''"),
      sql(s"SELECT * FROM $normalTable WHERE c1 = ''"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE isNull(c1)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c1)"))
    checkAnswer(sql(s"SELECT * FROM $bloomSampleTable WHERE isNull(c2)"),
      sql(s"SELECT * FROM $normalTable WHERE isNull(c2)"))
  }

  test("test bloom index: deleting & clearning segment will clear index files") {
    sql(s"CREATE TABLE $bloomSampleTable(c1 string, c2 int, c3 string) STORED AS carbondata")
    sql(
      s"""
         | CREATE INDEX $indexName
         | on $bloomSampleTable (c1, c2)
         | as 'bloomfilter'
       """.stripMargin)
    sql(s"INSERT INTO $bloomSampleTable SELECT 'c1v1', 1, 'c3v1'")
    sql(s"INSERT INTO $bloomSampleTable SELECT 'c1v2', 2, 'c3v2'")

    // two segments both has index files
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), bloomSampleTable)(
      SparkTestQueryExecutor.spark)
    import scala.collection.JavaConverters._
    (0 to 1).foreach { segId =>
      val indexPath = CarbonTablePath.getIndexesStorePath(carbonTable.getTablePath,
        segId.toString, indexName)
      assert(FileUtils.listFiles(FileUtils.getFile(indexPath), Array("bloomindexmerge"), true)
        .asScala.nonEmpty)
    }
    // delete and clean the first segment, the corresponding index files should be cleaned too
    sql(s"DELETE FROM TABLE $bloomSampleTable WHERE SEGMENT.ID IN (0)")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE $bloomSampleTable options('force'='true')")
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)
    var indexPath = CarbonTablePath.getIndexesStorePath(carbonTable.getTablePath, "0", indexName)
    assert(!FileUtils.getFile(indexPath).exists(),
      "index file of this segment has been deleted, should not exist")
    indexPath = CarbonTablePath.getIndexesStorePath(carbonTable.getTablePath, "1", indexName)
    assert(FileUtils.listFiles(FileUtils.getFile(indexPath), Array("bloomindexmerge"), true)
      .asScala.nonEmpty)
  }

  // two blocklets in one block are hit by bloom index while block cache level hit this block
  test("CARBONDATA-2788: enable block cache level and bloom index") {
    // minimum per page is 2000 rows
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "2000")
    // minimum per blocklet is 16MB
    CarbonProperties.getInstance()
      .addProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB, "16")
    // these lines will result in 3 blocklets in one block and bloom will hit at least 2 of them
    val lines = 100000
    sql("drop table if exists test_rcd").collect()
    val r = new Random()
    import sqlContext.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to lines)
      .map(x => ("No." + r.nextInt(10000), "country" + x % 10000, "city" + x % 10000, x % 10000,
        UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString,
        UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString,
        UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString,
        UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString))
      .toDF("ID", "country", "city", "population",
        "random1", "random2", "random3",
        "random4", "random5", "random6",
        "random7", "random8", "random9",
        "random10", "random11", "random12")
    df.write
      .format("carbondata")
      .option("tableName", "test_rcd")
      .option("SORT_COLUMNS", "id")
      .option("SORT_SCOPE", "LOCAL_SORT")
      .mode(SaveMode.Overwrite)
      .save()

    val withoutBloom = sql("select count(*) from test_rcd where city = 'city40'").collect().toSeq
    sql("CREATE INDEX dm_rcd " +
        "ON TABLE test_rcd (city)" +
        "AS 'bloomfilter' " +
        "properties ('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')")
    checkAnswer(sql("select count(*) from test_rcd where city = 'city40'"), withoutBloom)

    sql("drop table if exists test_rcd").collect()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
      CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
    CarbonProperties.getInstance().addProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB,
      CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE)
  }

  override def afterAll(): Unit = {
    deleteFile(bigFile)
    sql(s"DROP TABLE IF EXISTS $normalTable")
    sql(s"DROP TABLE IF EXISTS $bloomSampleTable")
  }
}

object IndexStatusUtil {
  def checkIndexStatus(tableName: String,
      indexName: String,
      indexStatus: String,
      sparkSession: SparkSession,
      indexProvider: IndexType): Unit = {
    val carbonTable = CarbonEnv.getCarbonTable(Some(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      tableName)(sparkSession)
    val secondaryIndexMap = carbonTable.getIndexesMap.get(indexProvider.getIndexProviderName)
    if (null != secondaryIndexMap) {
      val indexes = secondaryIndexMap.asScala
        .filter(p => p._2.get(CarbonCommonConstants.INDEX_STATUS).equalsIgnoreCase(indexStatus))
      assert(indexes.exists(p => p._1.equals(indexName) &&
                                 p._2.get(CarbonCommonConstants.INDEX_STATUS) == indexStatus))
    }
  }
}

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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.math.BigDecimal

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.util.BadRecordUtil

class TestLoadDataGeneral extends QueryTest with BeforeAndAfterEach {

  val badRecordAction = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION);
  val testdata = s"$resourcesPath/MoreThan32KChar.csv"
  val longChar: String = RandomStringUtils.randomAlphabetic(33000)

  override def beforeEach {
    sql("DROP TABLE IF EXISTS loadtest")
    sql(
      """
        | CREATE TABLE loadtest(id int, name string, city string, age int)
        | STORED AS carbondata
      """.stripMargin)
    sql("drop table if exists longerThan32kChar")
  }

  private def checkSegmentExists(
      segmentId: String,
      databaseName: String,
      tableName: String): Boolean = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(databaseName, tableName)
    val partitionPath =
      CarbonTablePath.getPartitionDir(carbonTable.getAbsoluteTableIdentifier.getTablePath)
    val segment = Segment.getSegment(segmentId, carbonTable.getAbsoluteTableIdentifier.getTablePath)
    segment != null
  }

  test("test explain with case sensitive") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table IF NOT EXISTS carbon_table(`BEGIN_TIME` BIGINT," +
      " `SAI_CGI_ECGI` STRING) stored as carbondata")
    sql("create table IF NOT EXISTS parquet_table(CELL_NAME string, CGISAI string)" +
      " stored as parquet")
    val df = sql("explain extended with grpMainDatathroughput as (select" +
      " from_unixtime(begin_time, 'yyyyMMdd') as data_time, SAI_CGI_ECGI from carbon_table)," +
      " grpMainData as (select * from grpMainDatathroughput a JOIN(select CELL_NAME, CGISAI from" +
      " parquet_table) b ON b.CGISAI=a.SAI_CGI_ECGI) " +
      "select * from grpMainData a left join grpMainData b on a.cell_name=b.cell_name").collect()
    assert(df(0).getString(0).contains("carbon_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test data loading CSV file") {
    val testData = s"$resourcesPath/sample.csv"
    checkAnswer(
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest"),
      Seq(Row("0"))
    )
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(6))
    )
  }

  test("test data loading CSV file without extension name") {
    val testData = s"$resourcesPath/sample"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading GZIP compressed CSV file") {
    val testData = s"$resourcesPath/sample.csv.gz"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading BZIP2 compressed CSV file") {
    val testData = s"$resourcesPath/sample.csv.bz2"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading CSV file with delimiter char \\017") {
    val testData = s"$resourcesPath/sample_withDelimiter017.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest options ('delimiter'='\\017')")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(4))
    )
  }

  test("test data loading with invalid values for mesasures") {
    val testData = s"$resourcesPath/invalidMeasures.csv"
    sql("drop table if exists invalidMeasures")
    sql("CREATE TABLE invalidMeasures (" +
        "country String, salary double, age decimal(10,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table invalidMeasures " +
        s"options('Fileheader'='country,salary,age')")
    checkAnswer(
      sql("SELECT * FROM invalidMeasures"),
      Seq(Row("India", null, new BigDecimal("22.44")),
        Row("Russia", null, null),
        Row("USA", 234.43, null))
    )
  }

  test("test data loading into table whose name has '_'") {
    sql("DROP TABLE IF EXISTS load_test")
    sql(""" CREATE TABLE load_test(id int, name string, city string, age int)
        STORED AS carbondata """)
    val testData = s"$resourcesPath/sample.csv"
    try {
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table load_test")
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table load_test")
    } catch {
      case ex: Exception =>
        assert(false)
    }
    assert(checkSegmentExists("0", "default", "load_test"))
    assert(checkSegmentExists("1", "default", "load_test"))
    sql("DROP TABLE load_test")
  }

  test("test load data with decimal type and sort intermediate files as 1") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists carbonBigDecimalLoad")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "1")
      .addProperty(CarbonCommonConstants.SORT_SIZE, "1")
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE, "1")
    // scalastyle:off lineLength
    sql("create table if not exists carbonBigDecimalLoad (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(27, 10)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table carbonBigDecimalLoad")
    // scalastyle:on lineLength
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)
      .addProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE,
        CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT)
    sql("drop table if exists carbon_table")
  }

  private def createTableAndLoadData (badRecordAction: String): Unit = {
    BadRecordUtil.cleanBadRecordPath("default", "longerthan32kchar")
    sql("CREATE TABLE longerthan32kchar(dim1 String, dim2 String, mes1 int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$testdata' into table longerThan32kChar " +
        "OPTIONS('FILEHEADER'='dim1,dim2,mes1', " +
        s"'BAD_RECORDS_ACTION'='${badRecordAction}','BAD_RECORDS_LOGGER_ENABLE'='TRUE')")
  }

  test("test load / insert / update with data more than 32000 characters and" +
       " bad record action as Redirect") {
    createTableAndLoadData("REDIRECT")
    var redirectCsvPath = BadRecordUtil
      .getRedirectCsvPath("default", "longerthan32kchar", "0", "0")
    assert(BadRecordUtil.checkRedirectedCsvContentAvailableInSource(testdata, redirectCsvPath))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "REDIRECT");
    sql(s"insert into longerthan32kchar values('33000', '$longChar', 4)")
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", "hi", 1), Row("itsok", "hello", 2)))
    redirectCsvPath = BadRecordUtil.getRedirectCsvPath("default", "longerthan32kchar", "1", "0")
    var redirectedFileLineList = FileUtils.readLines(redirectCsvPath)
    var iterator = redirectedFileLineList.iterator()
    while (iterator.hasNext) {
      assert(iterator.next().equals("33000," + longChar + ",4"))
    }

    // Update strings of length greater than 32000
    sql(s"update longerthan32kchar set(longerthan32kchar.dim2)=('$longChar') " +
      "where longerthan32kchar.mes1=1").collect()
    checkAnswer(sql("select * from longerthan32kchar"), Seq(Row("itsok", "hello", 2)))
    redirectCsvPath = BadRecordUtil.getRedirectCsvPath("default", "longerthan32kchar", "2", "0")
    redirectedFileLineList = FileUtils.readLines(redirectCsvPath)
    iterator = redirectedFileLineList.iterator()
    while (iterator.hasNext) {
      assert(iterator.next().equals("ok," + longChar + ",1"))
    }
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")

    // Insert longer string without converter step will throw exception
    intercept[Exception] {
      sql(s"insert into longerthan32kchar values('32000', '$longChar', 3)")
    }
    BadRecordUtil.cleanBadRecordPath("default", "longerthan32kchar")
  }

  test("test load / insert / update with data more than 32000 characters " +
       "and bad record action as Force") {
    createTableAndLoadData("FORCE")
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", "hi", 1), Row("itsok", "hello", 2), Row("32123", null, 3)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE");
    sql(s"insert into longerthan32kchar values('33000', '$longChar', 4)")
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", "hi", 1),
        Row("itsok", "hello", 2),
        Row("32123", null, 3),
        Row("33000", null, 4)))

    // Update strings of length greater than 32000
    sql(s"update longerthan32kchar set(longerthan32kchar.dim2)=('$longChar') " +
      "where longerthan32kchar.mes1=1").collect()
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", null, 1),
        Row("itsok", "hello", 2),
        Row("32123", null, 3),
        Row("33000", null, 4)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")

    // Insert longer string without converter step will throw exception
    intercept[Exception] {
      sql(s"insert into longerthan32kchar values('32000', '$longChar', 3)")
    }
  }

  test("test load / insert / update with data more than 32000 characters " +
       "and bad record action as Fail") {
    sql("CREATE TABLE longerthan32kchar(dim1 String, dim2 String, mes1 int) STORED AS carbondata")
    var exception = intercept[Exception] {
      sql(s"LOAD DATA LOCAL INPATH '$testdata' into table longerThan32kChar " +
          "OPTIONS('FILEHEADER'='dim1,dim2,mes1', " +
          "'BAD_RECORDS_ACTION'='FAIL','BAD_RECORDS_LOGGER_ENABLE'='TRUE')")
    }

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FAIL");
    exception = intercept[Exception] {
      sql(s"insert into longerthan32kchar values('33000', '$longChar', 4)")
    }
    assert(exception.getMessage.contains(
      s"Record of column dim2 exceeded ${CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT} " +
      "characters. Please consider long string data type."))
    // Update strings of length greater than 32000
    sql(s"insert into longerthan32kchar values('ok', 'hi', 1)")
    exception = intercept[Exception] {
      sql(s"update longerthan32kchar set(longerthan32kchar.dim2)=('$longChar') " +
        "where longerthan32kchar.mes1=1").collect()
    }
    assert(exception.getMessage.contains(
      s"Record of column dim2 exceeded ${CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT} " +
      s"characters. Please consider long string data type."))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")

    // Insert longer string without converter step will throw exception
    intercept[Exception] {
      sql(s"insert into longerthan32kchar values('32000', '$longChar', 3)")
    }
  }

  test("test load / insert / update with data more than 32000 characters " +
       "and bad record action as Ignore") {
    createTableAndLoadData("IGNORE")
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", "hi", 1), Row("itsok", "hello", 2)))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "IGNORE");
    sql(s"insert into longerthan32kchar values('33000', '$longChar', 4)")
    checkAnswer(sql("select * from longerthan32kchar"),
      Seq(Row("ok", "hi", 1), Row("itsok", "hello", 2)))

    // Update strings of length greater than 32000
    sql(s"update longerthan32kchar set(longerthan32kchar.dim2)=('$longChar') " +
      "where longerthan32kchar.mes1=1").collect()
    checkAnswer(sql("select * from longerthan32kchar"), Seq(Row("itsok", "hello", 2)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "false")

    // Insert longer string without converter step will throw exception
    intercept[Exception] {
      sql(s"insert into longerthan32kchar values('32000', '$longChar', 3)")
    }
  }

  test("test load / insert with data more than 32000 bytes - dictionary_exclude") {
    val testdata = s"$resourcesPath/unicodechar.csv"
    sql("drop table if exists load32000bytes")
    sql("create table load32000bytes(name string) STORED AS carbondata")
    sql("insert into table load32000bytes select 'aaa'")
    checkAnswer(sql("select count(*) from load32000bytes"), Seq(Row(1)))

    // Below load will be inserted as null because Strings greater than 32000 is bad record.
    sql(s"load data local inpath '$testdata' into table load32000bytes " +
        "OPTIONS ('FILEHEADER'='name')")
    checkAnswer(sql("select count(*) from load32000bytes"), Seq(Row(2)))
    checkAnswer(sql("select * from load32000bytes"), Seq(Row("aaa"), Row(null)))

    val source = scala.io.Source.fromFile(testdata, CarbonCommonConstants.DEFAULT_CHARSET)
    val data = source.mkString

    // Insert will throw exception as it is without converter step.
    intercept[Exception] {
      sql(s"insert into load32000bytes values('$data')")
    }

    sql("drop table if exists load32000bytes")
  }

  test("test data load with stale folders") {
    sql("drop table if exists stale")
    sql("create table stale(a string) STORED AS carbondata")
    sql("insert into stale values('k')")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "stale")
    val tableStatusFile = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath)
    FileFactory.getCarbonFile(tableStatusFile).delete()
    sql("insert into stale values('k')")
    // if table lose tablestatus file, the system should keep all data.
    checkAnswer(sql("select * from stale"), Seq(Row("k")))
  }

  test("test data loading with directly writing fact data to hdfs") {
    val originStatus = CarbonProperties.getInstance().getProperty(
      CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
      CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH_DEFAULT)
    CarbonProperties.getInstance().addProperty(
      CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, "true")

    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(6))
    )

    CarbonProperties.getInstance().addProperty(
      CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
      originStatus)
  }

  test("test data loading with page size less than 32000") {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.BLOCKLET_SIZE, "16000")

    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
    checkAnswer(
      sql("SELECT COUNT(*) FROM loadtest"),
      Seq(Row(6))
    )

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
      CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
  }

  test("test table creation with special char and other commands") {
    sql("drop table if exists special_char")
    sql("create table special_char(`i#d` string, `nam(e` string,`ci)&#@!ty` string," +
        "`a\be` int, `ag!e` float, `na^me1` Decimal(8,4), ```a``bc``!!d``` int)" +
        " stored as carbondata" +
        " tblproperties('INVERTED_INDEX'='`a`bc`!!d`', 'SORT_COLUMNS'='`a`bc`!!d`')")
    sql("insert into special_char values('1','joey','hud', 2, 2.2, 2.3456, 5)")
    checkAnswer(sql("select * from special_char"), Seq(Row("1", "joey", "hud", 2, 2.2, 2.3456, 5)))
    val df = sql("describe formatted special_char").collect()
    assert(df.exists(_.get(0).toString.contains("i#d")))
    assert(df.exists(_.get(0).toString.contains("nam(e")))
    assert(df.exists(_.get(0).toString.contains("ci)&#@!ty")))
    assert(df.exists(_.get(0).toString.contains("a\be")))
    assert(df.exists(_.get(0).toString.contains("ag!e")))
    assert(df.exists(_.get(0).toString.contains("na^me1")))
    assert(df.exists(_.get(0).toString.contains("`a`bc`!!d`")))
  }

  test("test load with multiple inserts") {
    sql("drop table if exists catalog_returns_5")
    sql("drop table if exists catalog_returns_6")
    sql("create table catalog_returns_5(cr_returned_date_sk int,cr_returned_time_sk int," +
        "cr_item_sk int)ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n'")
    sql("insert into catalog_returns_5 values(1,2,3)")
    sql("create table catalog_returns_6(cr_returned_time_sk int,cr_item_sk int) partitioned by" +
        " (cr_returned_date_sk int) stored as carbondata")
    val df = sql(
      "from catalog_returns_5 insert overwrite table catalog_returns_6 partition " +
      "(cr_returned_date_sk) select cr_returned_time_sk, cr_item_sk, cr_returned_date_sk where " +
      "cr_returned_date_sk is not null distribute by cr_returned_date_sk insert overwrite table " +
      "catalog_returns_6 partition (cr_returned_date_sk) select cr_returned_time_sk, cr_item_sk, " +
      "cr_returned_date_sk where cr_returned_date_sk is null distribute by cr_returned_date_sk")
    assert(df.collect().size == 2)
    checkAnswer(df, Seq(Row("0"), Row("1")))
    checkAnswer(sql("select * from catalog_returns_6"), Seq(Row(2, 3, 1)))
    sql("drop table if exists catalog_returns_5")
    sql("drop table if exists catalog_returns_6")
  }

  override def afterEach {
    sql("DROP TABLE if exists loadtest")
    sql("drop table if exists invalidMeasures")
    sql("drop table if exists longerThan32kChar")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)
      .addProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE,
        CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, badRecordAction)
  }
}

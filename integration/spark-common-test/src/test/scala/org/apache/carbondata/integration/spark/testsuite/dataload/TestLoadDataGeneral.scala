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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestLoadDataGeneral extends QueryTest with BeforeAndAfterEach {

  override def beforeEach {
    sql("DROP TABLE IF EXISTS loadtest")
    sql(
      """
        | CREATE TABLE loadtest(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  private def checkSegmentExists(
      segmentId: String,
      datbaseName: String,
      tableName: String): Boolean = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(datbaseName, tableName)
    val partitionPath = CarbonStorePath
      .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier).getPartitionDir
    val fileType: FileFactory.FileType = FileFactory.getFileType(partitionPath)
    val carbonFile = FileFactory.getCarbonFile(partitionPath, fileType)
    val segments: ArrayBuffer[String] = ArrayBuffer()
    carbonFile.listFiles.foreach { file =>
      segments += CarbonTablePath.DataPathUtil.getSegmentId(file.getAbsolutePath + "/dummy")
    }
    segments.contains(segmentId)
  }

  test("test data loading CSV file") {
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table loadtest")
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
    sql("CREATE TABLE invalidMeasures (country String, salary double, age decimal(10,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table invalidMeasures options('Fileheader'='country,salary,age')")
    checkAnswer(
      sql("SELECT * FROM invalidMeasures"),
      Seq(Row("India",null,new BigDecimal("22.44")), Row("Russia",null,null), Row("USA",234.43,null))
    )
  }

  test("test data loading into table whose name has '_'") {
    sql("DROP TABLE IF EXISTS load_test")
    sql(""" CREATE TABLE load_test(id int, name string, city string, age int)
        STORED BY 'org.apache.carbondata.format' """)
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

  test("test data loading into table with Single Pass") {
    sql("DROP TABLE IF EXISTS load_test_singlepass")
    sql(""" CREATE TABLE load_test_singlepass(id int, name string, city string, age int)
        STORED BY 'org.apache.carbondata.format' """)
    val testData = s"$resourcesPath/sample.csv"
    try {
      sql(s"LOAD DATA LOCAL INPATH '$testData' into table load_test_singlepass options ('SINGLE_PASS'='TRUE')")
    } catch {
      case ex: Exception =>
        assert(false)
    }
    checkAnswer(
      sql("SELECT id,name FROM load_test_singlepass where name='eason'"),
      Seq(Row(2,"eason"))
    )
    sql("DROP TABLE load_test_singlepass")
  }

  test("test load data with decimal type and sort intermediate files as 1") {
    sql("drop table if exists carbon_table")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT, "1")
      .addProperty(CarbonCommonConstants.SORT_SIZE, "1")
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE, "1")
    sql("create table if not exists carbonBigDecimal (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary decimal(27, 10)) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/decimalBoundaryDataCarbon.csv' into table carbonBigDecimal")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)
      .addProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE,
        CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT)
    sql("drop table if exists carbon_table")
  }

  test("test insert / update with data more than 32000 characters") {
    val testdata =s"$resourcesPath/32000char.csv"
    sql("drop table if exists load32000chardata")
    sql("drop table if exists load32000chardata_dup")
    sql("CREATE TABLE load32000chardata(dim1 String, dim2 String, mes1 int) STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE load32000chardata_dup(dim1 String, dim2 String, mes1 int) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$testdata' into table load32000chardata OPTIONS('FILEHEADER'='dim1,dim2,mes1')")
    intercept[Exception] {
      sql("insert into load32000chardata_dup select dim1,concat(load32000chardata.dim2,'aaaa'),mes1 from load32000chardata").show()
    }
    intercept[Exception] {
      sql("update load32000chardata_dup set(load32000chardata_dup.dim2)=(select concat(load32000chardata.dim2,'aaaa') from load32000chardata)").show()
    }
  }

  test("test load / insert / update with data more than 32000 bytes - dictionary_exclude") {
    val testdata = s"$resourcesPath/unicodechar.csv"
    sql("drop table if exists load32000bytes")
    sql("create table load32000bytes(name string) stored by 'carbondata'")
    sql("insert into table load32000bytes select 'aaa'")

    assert(intercept[Exception] {
      sql(s"load data local inpath '$testdata' into table load32000bytes OPTIONS ('FILEHEADER'='name')")
    }.getMessage.contains("DataLoad failure: Dataload failed, String size cannot exceed 32000 bytes"))

    val source = scala.io.Source.fromFile(testdata)
    val data = source.mkString

    intercept[Exception] {
      sql(s"insert into load32000bytes values('$data')")
    }

    intercept[Exception] {
      sql(s"update load32000bytes set(name)= ('$data')").show()
    }

    sql("drop table if exists load32000bytes")
  }

  test("test load / insert / update with data more than 32000 bytes - dictionary_include") {
    val testdata = s"$resourcesPath/unicodechar.csv"
    sql("drop table if exists load32000bytes")
    sql("create table load32000bytes(name string) stored by 'carbondata' TBLPROPERTIES('DICTIONARY_INCLUDE'='name')")
    sql("insert into table load32000bytes select 'aaa'")

    assert(intercept[Exception] {
      sql(s"load data local inpath '$testdata' into table load32000bytes OPTIONS ('FILEHEADER'='name')")
    }.getMessage.contains("generate global dictionary failed, Dataload failed, String size cannot exceed 32000 bytes"))

    val source = scala.io.Source.fromFile(testdata)
    val data = source.mkString

    intercept[Exception] {
      sql(s"insert into load32000bytes values('$data')")
    }

    intercept[Exception] {
      sql(s"update load32000bytes set(name)= ('$data')").show()
    }

    sql("drop table if exists load32000bytes")
  }

  test("test if stale folders are deleting on data load") {
    sql("drop table if exists stale")
    sql("create table stale(a string) stored by 'carbondata'")
    sql("insert into stale values('k')")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "stale")
    val tableStatusFile = new CarbonTablePath(null,
      carbonTable.getTablePath).getTableStatusFilePath
    FileFactory.getCarbonFile(tableStatusFile).delete()
    sql("insert into stale values('k')")
    checkAnswer(sql("select * from stale"), Row("k"))
  }

  override def afterEach {
    sql("DROP TABLE if exists loadtest")
    sql("drop table if exists invalidMeasures")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
        CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)
      .addProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)
      .addProperty(CarbonCommonConstants.DATA_LOAD_BATCH_SIZE,
        CarbonCommonConstants.DATA_LOAD_BATCH_SIZE_DEFAULT)
  }
}

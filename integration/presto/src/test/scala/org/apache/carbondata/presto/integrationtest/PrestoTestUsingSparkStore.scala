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

package org.apache.carbondata.presto.integrationtest

import java.io.{File}
import java.util

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Ignore}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.{PrestoServer, PrestoTestUtil}

@Ignore
class PrestoTestUsingSparkStore
  extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoTestNonTransactionalTableFiles].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val writerPath = storePath + "/presto_spark_db/files"
  private val sparkStorePath = s"$rootPath/integration/spark/target/spark_store"
  private val prestoServer = new PrestoServer

  override def beforeAll: Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")
    prestoServer.startServer("presto_spark_db", map)
    prestoServer.execute("drop schema if exists presto_spark_db")
    prestoServer.execute("create schema presto_spark_db")
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile
    (s"$sparkStorePath"))
  }

  def copyStoreContents(tableName: String): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    import java.io.IOException
    val source = s"$sparkStorePath/$tableName/"
    val srcDir = new File(source)

    val destination = s"$storePath/presto_spark_db" +
                      s"/$tableName/"
    val destDir = new File(destination)
    try {
      // Move spark store to presto store path
      FileUtils.copyDirectory(srcDir, destDir)
    }
    catch {
      case e: IOException =>
        throw e
    }
  }

  test("Test update operations without local dictionary") {
    prestoServer.execute("drop table if exists presto_spark_db.update_table")
    prestoServer.execute("drop table if exists presto_spark_db.actual_update_table")
    prestoServer
      .execute(
        "create table presto_spark_db.update_table(smallintColumn smallint, intColumn int, " +
        "bigintColumn bigint, doubleColumn double, decimalColumn decimal(10,3), " +
        "timestampColumn timestamp, dateColumn date, " +
        "stringColumn varchar, booleanColumn boolean ) with(format='CARBON') ")
    prestoServer
      .execute(
        "create table presto_spark_db.actual_update_table(smallintColumn smallint, intColumn int," +
        " " +
        "bigintColumn bigint, doubleColumn double, decimalColumn decimal(10,3), " +
        "timestampColumn timestamp, dateColumn date, " +
        "stringColumn varchar, booleanColumn boolean ) with(format='CARBON') ")
    copyStoreContents("update_table")
    copyStoreContents("actual_update_table")
    assert(prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.update_table").equals(prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.actual_update_table")))
  }

  test("Test delete operations") {
    prestoServer.execute("drop table if exists presto_spark_db.iud_table")
    prestoServer
      .execute(
        "create table presto_spark_db.iud_table(smallintColumn smallint, intColumn int, " +
        "bigintColumn bigint, doubleColumn double, decimalColumn decimal(10,3), " +
        "timestampColumn timestamp, dateColumn date, " +
        "stringColumn varchar, booleanColumn boolean ) with(format='CARBON') ")
    copyStoreContents("iud_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.iud_table")
    assert(result.size == 2)
  }

  test("Test major compaction") {
    prestoServer.execute("drop table if exists presto_spark_db.testmajor")
    prestoServer
      .execute(
        "create table presto_spark_db.testmajor(country varchar, arrayInt array(int) ) with" +
        "(format='CARBON') ")
    copyStoreContents("testmajor")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.testmajor")
    assert(result.size == 4)
    for (i <- 0 to 3) {
      val value = result(i)("country")
      assert(value.equals("India") || value.equals("Egypt") || value.equals("Iceland") ||
             value.equals("China"))
    }
  }

  test("Test minor compaction") {
    prestoServer.execute("drop table if exists presto_spark_db.minor_compaction")
    prestoServer
      .execute(
        "create table presto_spark_db.minor_compaction(empno int, empname varchar, arrayInt array" +
        "(int)) with(format='CARBON') ")
    copyStoreContents("minor_compaction")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.minor_compaction")
    assert(result.size == 2)
    for (i <- 0 to 1) {
      val value = result(i)("empno")
      assert(value.equals(11) || value.equals(12))
    }
  }

  test("Test custom compaction") {
    prestoServer.execute("drop table if exists presto_spark_db.custom_compaction_table")
    prestoServer
      .execute(
        "create table presto_spark_db.custom_compaction_table(ID Int, date Date, country varchar," +
        " name varchar, phonetype varchar, serialname varchar, salary Int, floatField real) with" +
        "(format='CARBON') ")
    copyStoreContents("custom_compaction_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT ID FROM presto_spark_db.custom_compaction_table where ID = 5")
    assert(result.size == 4)
  }

  test("test with add segment") {
    prestoServer.execute("drop table if exists presto_spark_db.segment_table")
    prestoServer
      .execute(
        "create table presto_spark_db.segment_table(a varchar, b int, arrayInt array<int>) with" +
        "(format='CARBON') ")
    copyStoreContents("segment_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.segment_table")
    assert(result.size == 3)
    for (i <- 0 to 2) {
      val value = result(i)("b")
      assert(value.equals(1) || value.equals(2) || value.equals(3))
    }
  }

  test("test with delete segment") {
    prestoServer.execute("drop table if exists presto_spark_db.delete_segment_table")
    prestoServer
      .execute(
        "create table presto_spark_db.delete_segment_table(a varchar, b int, arrayInt array<int>)" +
        " with(format='CARBON') ")
    copyStoreContents("delete_segment_table")
    val result = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.delete_segment_table")
    assert(result.size == 2)
    for (i <- 0 to 1) {
      val value = result(i)("b")
      assert(value.equals(1) || value.equals(3))
    }
  }

  test("test inverted index with update operation") {
    prestoServer.execute("drop table if exists presto_spark_db.inv_table")
    prestoServer
      .execute(
        "create table presto_spark_db.inv_table(name varchar, c_code int, arrayInt array<int>) " +
        "with(format='CARBON') ")
    copyStoreContents("inv_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.inv_table")
    assert(result.size == 4)
    for (i <- 0 to 3) {
      val id = result(i)("c_code")
      val name = result(i)("name")
      assert((id.equals(1) && name.equals("Alex")) || (id.equals(2) && name.equals("John")) ||
             (id.equals(3) && name.equals("Neil")) ||
             (id.equals(4) && name.equals("Neil")))
    }

  }

  test("Test partition columns") {
    prestoServer.execute("drop table if exists presto_spark_db.partition_table")
    prestoServer
      .execute(
        "create table presto_spark_db.partition_table(name varchar, id int, department varchar) " +
        "with (partitioned_by = ARRAY['department'], format='CARBON') ")
    copyStoreContents("partition_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.partition_table")
    assert(result.size == 4)
    for (i <- 0 to 3) {
      val id = result(i)("id")
      val name = result(i)("name")
      val department = result(i)("department")
      assert(id.equals(1) && name.equals("John") && department.equals("dev") ||
             (id.equals(2) && name.equals("Neil")) && department.equals("test") ||
             (id.equals(4) && name.equals("Alex")) && department.equals("Carbon-dev"))
    }
  }

  test("Test bloom index") {
    prestoServer.execute("drop table if exists presto_spark_db.carbon_normal")
    prestoServer.execute("drop table if exists presto_spark_db.carbon_bloom")
    prestoServer
      .execute(
        "create table presto_spark_db.carbon_normal(id INT, name varchar, city varchar, age INT, " +
        "s1 " +
        "varchar, s2 varchar, s3 varchar, s4 varchar, s5 varchar, s6 varchar, s7 varchar, s8 " +
        "varchar) with" +
        "(format='CARBON') ")
    prestoServer
      .execute(
        "create table presto_spark_db.carbon_bloom(id INT, name varchar, city varchar, age INT, " +
        "s1 " +
        "varchar, s2 varchar, s3 varchar, s4 varchar, s5 varchar, s6 varchar, s7 varchar, s8 " +
        "varchar) with" +
        "(format='CARBON') ")
    copyStoreContents("carbon_normal")
    copyStoreContents("carbon_bloom")

    assert(prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.carbon_normal where id = 1").equals(
      prestoServer
        .executeQuery("SELECT * FROM presto_spark_db.carbon_bloom where id = 1")))

    assert(prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.carbon_normal  where id in (999)").equals(
      prestoServer
        .executeQuery("SELECT * FROM presto_spark_db.carbon_bloom  where id in (999)")))

    assert(prestoServer
      .executeQuery(
        "SELECT * FROM presto_spark_db.carbon_normal where id in (999) and city in ('city_999')")
      .equals(
        prestoServer
          .executeQuery(
            "SELECT * FROM presto_spark_db.carbon_bloom where id in (999) and city in " +
            "('city_999')")))

    assert(prestoServer
      .executeQuery(
        "SELECT min(id), max(id), min(name), max(name), min(city), max(city) FROM presto_spark_db" +
        ".carbon_normal where id = 1")
      .equals(
        prestoServer
          .executeQuery(
            "SELECT min(id), max(id), min(name), max(name), min(city), max(city) FROM " +
            "presto_spark_db" +
            ".carbon_bloom where id = 1")))

  }

  test("Test range columns") {
    prestoServer.execute("drop table if exists presto_spark_db.range_table")
    prestoServer
      .execute(
        "create table presto_spark_db.range_table(name varchar, id int) with" +
        "(format='CARBON') ")
    copyStoreContents("range_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.range_table")
    assert(result.size == 4)
    for (i <- 0 to 3) {
      val id = result(i)("id")
      val name = result(i)("name")
      assert(id.equals(1000) && name.equals("John") || (id.equals(1001) && name.equals("Alex")) ||
             (id.equals(5000) && name.equals("Neil")) || (id.equals(4999) && name.equals("Jack")))
    }
  }

  test("Test short vector datatype") {
    prestoServer.execute("drop table if exists presto_spark_db.array_short")
    prestoServer
      .execute(
        "create table presto_spark_db.array_short(salary array(smallint) ) with" +
        "(format='CARBON') ")
    copyStoreContents("array_short")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_short")
    assert(result.size == 1)
    PrestoTestUtil.validateShortData(result)
  }

  test("Test int vector datatype") {
    prestoServer.execute("drop table if exists presto_spark_db.array_int")
    prestoServer
      .execute(
        "create table presto_spark_db.array_int(salary array(int) ) with" +
        "(format='CARBON') ")
    copyStoreContents("array_int")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_int")
    assert(result.size == 2)
    PrestoTestUtil.validateIntData(result)
  }

  test("Test double vector datatype") {
    prestoServer.execute("drop table if exists presto_spark_db.array_double")
    prestoServer
      .execute(
        "create table presto_spark_db.array_double(salary array(double) ) with" +
        "(format='CARBON') ")
    copyStoreContents("array_double")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_double")
    assert(result.size == 6)
    PrestoTestUtil.validateDoubleData(result)
  }

  test("Test long vector datatype") {
    prestoServer.execute("drop table if exists presto_spark_db.array_long")
    prestoServer
      .execute(
        "create table presto_spark_db.array_long(salary array(bigint) ) with" +
        "(format='CARBON') ")
    copyStoreContents("array_long")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_long")
    assert(result.size == 3)
    PrestoTestUtil.validateLongData(result)
  }

  test("Test timestamp vector datatype") {
    prestoServer.execute("drop table if exists presto_spark_db.array_timestamp")
    prestoServer
      .execute(
        "create table presto_spark_db.array_timestamp(time array<timestamp>) with" +
        "(format='CARBON') ")
    copyStoreContents("array_timestamp")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_timestamp")
    assert(result.size == 2)
    PrestoTestUtil.validateTimestampData(result)
  }

  test("Test streaming ") {
    prestoServer.execute("drop table if exists presto_spark_db.streaming_table")
    prestoServer
      .execute(
        "create table presto_spark_db.streaming_table(c1 varchar, c2 int, c3 varchar, c5 varchar)" +
        " " +
        "with" +
        "(format='CARBON') ")
    copyStoreContents("streaming_table")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.streaming_table")
    assert(result.size == 5)
    for (i <- 0 to 4) {
      val c2 = result(i)("c2")
      val c5 = result(i)("c5")
      assert(c2.equals(1) && c5.equals("aaa") || (c2.equals(2) && c5.equals("bbb")) ||
             (c2.equals(3) && c5.equals("ccc")) ||
             (c2.equals(4) && c5.equals("ddd") || (c2.equals(3) && c5.equals("ccc")) ||
              (c2.equals(5) && c5.equals("eee"))))
    }

  }

  test("Test decimal unscaled converter for array") {
    prestoServer.execute("drop table if exists presto_spark_db.array_decimal")
    prestoServer
      .execute(
        "create table presto_spark_db.array_decimal(salary array(decimal(20,3)) ) with" +
        "(format='CARBON') ")
    copyStoreContents("array_decimal")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.array_decimal")
    assert(result.size == 1)
    PrestoTestUtil.validateDecimalData(result)
  }

  test("Test decimal unscaled converter for struct") {
    prestoServer.execute("drop table if exists presto_spark_db.struct_decimal")
    prestoServer
      .execute(
        "create table presto_spark_db.struct_decimal(salary ROW(dec decimal(20,3))) " +
        "with (format='CARBON') ")
    copyStoreContents("struct_decimal")
    val result: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT * FROM presto_spark_db.struct_decimal")
    assert(result.size == 1)
    val data = result(0)("salary").asInstanceOf[java.util.Map[String, Any]]
    assert(data.get("dec") == "922.580")

  }

}

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

package org.apache.carbondata.spark.testsuite.localdictionary

import java.io.{File, PrintWriter}
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTestUtil}

class LocalDictionarySupportLoadTableTest extends QueryTest with BeforeAndAfterAll {

  val file1 = resourcesPath + "/local_dictionary_source1.csv"

  val file2 = resourcesPath + "/local_dictionary_complex_data.csv"

  val storePath = warehouse + "/local2/Fact/Part0/Segment_0"

  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.BLOCKLET_SIZE, "10000")
    createFile(file1)
    createComplexData(file2)
    sql("drop table if exists local2")
  }

  test("test LocalDictionary Load For FallBackScenario") {
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_threshold'='2001','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test successful local dictionary generation") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata " +
        "tblproperties('local_dictionary_enable'='true','local_dictionary_threshold'='9001'," +
        "'local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test successful local dictionary generation for default configs") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata " +
        "tblproperties('local_dictionary_enable'='true')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test local dictionary generation for local dictionary include") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata " +
        "TBLPROPERTIES ('local_dictionary_enable'='true', 'local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test local dictionary generation for local dictioanry exclude") {
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test local dictionary generation when it is disabled") {
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test local dictionary generation for invalid threshold configurations") {
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name string) STORED AS carbondata " +
      "tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name'," +
      "'local_dictionary_threshold'='300000')")
    sql("load data inpath '" + file1 + "' into table local2 OPTIONS('header'='false')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
  }

  test("test local dictionary generation for include and exclude") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string, age string) STORED AS carbondata " +
        "tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name', " +
        "'local_dictionary_exclude'='age')")
    sql("insert into table local2 values('vishal', '30')")
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 1)))
  }

  test("test local dictionary generation for complex type") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name struct<i:string,s:string>) STORED AS carbondata " +
        "tblproperties('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("load data inpath '" + file2 +
        "' into table local2 OPTIONS('header'='false','COMPLEX_DELIMITER_LEVEL_1'='$', " +
        "'COMPLEX_DELIMITER_LEVEL_2'=':')")
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 1)))
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 2)))
  }

  test("test local dictionary generation for map type") {
    if (SPARK_VERSION.startsWith("3.")) {
      sql("set spark.sql.mapKeyDedupPolicy=LAST_WIN")
    }
    sql("drop table if exists local2")
    sql(
      "CREATE TABLE local2(name map<string,string>) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("insert into local2 values(" +
        "map('Manish','Gupta','Manish','Nalla','Shardul','Singh','Vishal','Kumar'))")
    checkAnswer(sql("select * from local2"), Seq(
      Row(Map("Manish" -> "Nalla", "Shardul" -> "Singh", "Vishal" -> "Kumar"))))
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 0)))
    assert(!CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 1)))
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 2)))
    assert(CarbonTestUtil.checkForLocalDictionary(CarbonTestUtil.getDimRawChunk(storePath, 3)))
  }

  test("test local dictionary data validation") {
    sql("drop table if exists local_query_enable")
    sql("drop table if exists local_query_disable")
    sql(
      "CREATE TABLE local_query_enable(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='false','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local_query_enable OPTIONS('header'='false')")
    sql(
      "CREATE TABLE local_query_disable(name string) STORED AS carbondata tblproperties" +
      "('local_dictionary_enable'='true','local_dictionary_include'='name')")
    sql("load data inpath '" + file1 + "' into table local_query_disable OPTIONS('header'='false')")
    checkAnswer(sql("select name from local_query_enable"),
      sql("select name from local_query_disable"))
  }

  test("test to validate local dictionary values") {
    sql("drop table if exists local2")
    sql("CREATE TABLE local2(name string) STORED AS carbondata " +
        "tblproperties('local_dictionary_enable'='true')")
    sql("load data inpath '" + resourcesPath + "/localdictionary.csv" + "' into table local2")
    val dimRawChunk = CarbonTestUtil.getDimRawChunk(storePath, 0)
    val dictionaryData = Array("vishal", "kumar", "akash", "praveen", "brijoo")
    try
      assert(CarbonTestUtil.validateDictionary(dimRawChunk.get(0), dictionaryData))
    catch {
      case e: Exception =>
        assert(false)
    }
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists local2")
    deleteFile(file1)
    deleteFile(file2)
    CarbonProperties.getInstance
      .addProperty(CarbonCommonConstants.BLOCKLET_SIZE,
        CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL)
  }

  /**
   * create the csv data file
   *
   * @param fileName
   */
  private def createFile(fileName: String, line: Int = 9000, start: Int = 0): Unit = {
    val writer = new PrintWriter(new File(fileName))
    val data: util.ArrayList[String] = new util.ArrayList[String]()
    for (i <- start until line) {
      data.add("n" + i)
    }
    Collections.sort(data)
    data.asScala.foreach { eachdata =>
      // scalastyle:off println
      writer.println(eachdata)
      // scalastyle:on println
    }
    writer.close()
  }

  /**
   * create the csv for complex data
   *
   * @param fileName
   */
  private def createComplexData(fileName: String, line: Int = 9000, start: Int = 0): Unit = {
    val writer = new PrintWriter(new File(fileName))
    val data: util.ArrayList[String] = new util.ArrayList[String]()
    for (i <- start until line) {
      data.add("n" + i + "$" + (i + 1))
    }
    Collections.sort(data)
    data.asScala.foreach { eachdata =>
      // scalastyle:off println
      writer.println(eachdata)
      // scalastyle:on println
    }
    writer.close()
  }

  /**
   * delete csv file after test execution
   *
   * @param fileName
   */
  private def deleteFile(fileName: String): Unit = {
    val file: File = new File(fileName)
    if (file.exists) file.delete
  }
}

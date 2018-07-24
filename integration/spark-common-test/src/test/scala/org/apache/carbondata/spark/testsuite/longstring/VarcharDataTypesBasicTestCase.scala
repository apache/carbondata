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

package org.apache.carbondata.spark.testsuite.longstring

import java.io.{File, PrintWriter}

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class VarcharDataTypesBasicTestCase extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  private val longStringTable = "long_string_table"
  private val inputDir = s"$resourcesPath${File.separator}varchartype${File.separator}"
  private val fileName = s"longStringData.csv"
  private val inputFile = s"$inputDir$fileName"
  private val fileName_2g_column_page = s"longStringData_exceed_2gb_column_page.csv"
  private val inputFile_2g_column_page = s"$inputDir$fileName_2g_column_page"
  private val lineNum = 1000
  private var content: Content = _
  private var longStringDF: DataFrame = _
  private var originMemorySize = CarbonProperties.getInstance().getProperty(
    CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB,
    CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT)

  case class Content(head: Int, desc_line_head: String, note_line_head: String,
      mid: Int, desc_line_mid: String, note_line_mid: String,
      tail: Int, desc_line_tail: String, note_line_tail: String)

  override def beforeAll(): Unit = {
    // for one 32000 lines * 32000 characters column page, it use about 1GB memory, but here we have only 1000 lines
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB,
      CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT)
    deleteFile(inputFile)
    if (!new File(inputDir).exists()) {
      new File(inputDir).mkdir()
    }
    content = createFile(inputFile, line = lineNum)
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, originMemorySize)
    deleteFile(inputFile)
    deleteFile(inputFile_2g_column_page)
    if (new File(inputDir).exists()) {
      new File(inputDir).delete()
    }
  }

  override def beforeEach(): Unit = {
    sql(s"drop table if exists $longStringTable")
  }

  override def afterEach(): Unit = {
    sql(s"drop table if exists $longStringTable")
  }

  test("long string columns cannot be dictionary include") {
    val exceptionCaught = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED BY 'carbondata'
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note', 'dictionary_include'='address')
           |""".
          stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("DICTIONARY_INCLUDE is unsupported for long string datatype column: address"))
  }

  test("long string columns cannot be dictionay exclude") {
    val exceptionCaught = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED BY 'carbondata'
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note', 'dictionary_exclude'='address')
           |""".
          stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("DICTIONARY_EXCLUDE is unsupported for long string datatype column: address"))
  }

  test("long string columns cannot be sort_columns") {
    val exceptionCaught = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED BY 'carbondata'
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='name, note', 'SORT_COLUMNS'='name, address')
           |""".
          stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("sort_columns is unsupported for long string datatype column: name"))
  }

  test("long string columns can only be string columns") {
    val exceptionCaught = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED BY 'carbondata'
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='id, note')
           |""".
          stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("long_string_columns: id"))
    assert(exceptionCaught.getMessage.contains("its data type is not string"))
  }

  private def prepareTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING, note STRING
         | ) STORED BY 'carbondata'
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='description, note', 'SORT_COLUMNS'='name')
         |""".stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputFile' INTO TABLE $longStringTable
         | OPTIONS('header'='false')
       """.stripMargin)
  }

  private def checkQuery(): Unit = {
    // query without long_string_column
    checkAnswer(sql(s"SELECT id, name, address FROM $longStringTable where id = ${content.tail}"),
      Row(content.tail, s"name_${content.tail}", s"address_${content.tail}"))
    // query return long_string_column in the middle position
    checkAnswer(sql(s"SELECT id, name, description, address FROM $longStringTable where id = ${content.head}"),
      Row(content.head, s"name_${content.head}", content.desc_line_head, s"address_${content.head}"))
    // query return long_string_column at last position
    checkAnswer(sql(s"SELECT id, name, address, description FROM $longStringTable where id = ${content.mid}"),
      Row(content.mid, s"name_${content.mid}", s"address_${content.mid}", content.desc_line_mid))
    // query return 2 long_string_columns
    checkAnswer(sql(s"SELECT id, name, note, address, description FROM $longStringTable where id = ${content.mid}"),
      Row(content.mid, s"name_${content.mid}", content.note_line_mid, s"address_${content.mid}", content.desc_line_mid))
    // query by simple string column
    checkAnswer(sql(s"SELECT id, note, address, description FROM $longStringTable where name = 'name_${content.tail}'"),
      Row(content.tail, content.note_line_tail, s"address_${content.tail}", content.desc_line_tail))
    // query by long string column
    checkAnswer(sql(s"SELECT id, name, address, description FROM $longStringTable where note = '${content.note_line_tail}'"),
      Row(content.tail, s"name_${content.tail}", s"address_${content.tail}", content.desc_line_tail))
  }

  test("Load and query with long string datatype: safe sort & safe columnpage") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")

    prepareTable()
    checkQuery()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
  }

  test("Load and query with long string datatype: safe sort & unsafe column page") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "false")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")

    prepareTable()
    checkQuery()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
  }

  test("Load and query with long string datatype: unsafe sort & safe column page") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")

    prepareTable()
    checkQuery()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
  }

  test("Load and query with long string datatype: unsafe sort & unsafe column page") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")

    prepareTable()
    checkQuery()

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
      CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
      CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
  }

  // ignore this test in CI, because it will need at least 4GB memory to run successfully
  ignore("Exceed 2GB per column page for varchar datatype") {
    deleteFile(inputFile_2g_column_page)
    if (!new File(inputDir).exists()) {
      new File(inputDir).mkdir()
    }
    // 7000000 characters with 3200 rows will exceed 2GB constraint for one column page.
    content = createFile2(inputFile_2g_column_page, line = 3200, varcharLen = 700000)

    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING
         | ) STORED BY 'carbondata'
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='description', 'SORT_COLUMNS'='name')
         |""".stripMargin)
    val exceptionCaught = intercept[Exception] {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$inputFile_2g_column_page' INTO TABLE $longStringTable
           | OPTIONS('header'='false')
       """.stripMargin)
    }
    // since after exception wrapper, we cannot get the root cause directly
  }

  private def prepareDF(): Unit = {
    val schema = StructType(
      StructField("id", IntegerType, nullable = true) ::
      StructField("name", StringType, nullable = true) ::
      StructField("description", StringType, nullable = true) ::
      StructField("address", StringType, nullable = true) ::
      StructField("note", StringType, nullable = true) :: Nil
    )
    longStringDF = sqlContext.sparkSession.read
      .schema(schema)
      .csv(inputFile)
  }

  test("write from dataframe with long string datatype") {
    prepareDF()
    // write spark dataframe to carbondata with `long_string_columns` property
    longStringDF.write
      .format("carbondata")
      .option("tableName", longStringTable)
      .option("single_pass", "false")
      .option("sort_columns", "name")
      .option("long_string_columns", "description, note")
      .mode(SaveMode.Overwrite)
      .save()

    checkQuery()
  }

  test("desc table shows long_string_columns property") {
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING, note STRING
         | ) STORED BY 'carbondata'
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note', 'dictionary_include'='name', 'sort_columns'='id')
         |""".
        stripMargin)
    checkExistence(sql(s"desc formatted $longStringTable"), true, "long_string_columns".toUpperCase, "address", "note")
  }

  // will create 2 long string columns
  private def createFile(filePath: String, line: Int = 10000, start: Int = 0,
      varcharLen: Int = Short.MaxValue + 1000): Content = {
    val head = 0
    val mid = line / 2
    var tail = line - 1
    var desc_line_head: String = ""
    var desc_line_mid: String = ""
    var desc_line_tail: String = ""
    var note_line_head: String = ""
    var note_line_mid: String = ""
    var note_line_tail: String = ""
    if (new File(filePath).exists()) {
      deleteFile(filePath)
    }
    val write = new PrintWriter(new File(filePath))
    for (i <- start until (start + line)) {
      val description = RandomStringUtils.randomAlphabetic(varcharLen)
      val note = RandomStringUtils.randomAlphabetic(varcharLen)
      val line = s"$i,name_$i,$description,address_$i,$note"
      if (head == i) {
        desc_line_head = description
        note_line_head = note
      } else if (mid == i) {
        desc_line_mid = description
        note_line_mid = note
      } else if (tail == i) {
        desc_line_tail = description
        note_line_tail = note
      }
      write.println(line)
    }
    write.close()
    Content(head, desc_line_head, note_line_head,
      mid, desc_line_mid, note_line_mid, tail,
      desc_line_tail, note_line_tail)
  }

  // will only create 1 long string column
  private def createFile2(filePath: String, line: Int = 10000, start: Int = 0,
      varcharLen: Int = Short.MaxValue + 1000): Content = {
    val head = 0
    val mid = line / 2
    var tail = line - 1
    var desc_line_head: String = ""
    var desc_line_mid: String = ""
    var desc_line_tail: String = ""
    if (new File(filePath).exists()) {
      deleteFile(filePath)
    }
    val write = new PrintWriter(new File(filePath))
    for (i <- start until (start + line)) {
      val description = RandomStringUtils.randomAlphabetic(varcharLen)
      val note = RandomStringUtils.randomAlphabetic(varcharLen)
      val line = s"$i,name_$i,$description,address_$i"
      if (head == i) {
        desc_line_head = description
      } else if (mid == i) {
        desc_line_mid = description
      } else if (tail == i) {
        desc_line_tail = description
      }
      write.println(line)
    }
    write.close()
    Content(head, desc_line_head, "",
      mid, desc_line_mid, "", tail,
      desc_line_tail, "")
  }

  private def deleteFile(filePath: String): Unit = {
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
  }
}

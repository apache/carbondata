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

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.util.CarbonProperties
import scala.collection.mutable

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
  private var longChar : String = _

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
    longChar = RandomStringUtils.randomAlphabetic(33000)
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

  test("long string columns cannot be sort_columns") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='name, note', 'SORT_COLUMNS'='name, address')
           |""".
          stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("sort_columns is unsupported for long string datatype column: name"))
  }

  test("long string columns can only be string columns") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='id, note')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("long_string_columns: id"))
    assert(exceptionCaught.getMessage.contains("its data type is not string"))
  }

  test("long string columns cannot contain duplicate columns") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note, Note')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("long_string_columns"))
    assert(exceptionCaught.getMessage.contains("Duplicate columns are not allowed"))
  }

  test("long_string_columns: column does not exist in table ") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note, NoteS')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("long_string_columns"))
    assert(exceptionCaught.getMessage.contains("does not exist in table"))
  }

  test("long_string_columns: columns cannot exist in partitions columns") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING
           | ) partitioned by (note string) STORED AS carbondata
           | TBLPROPERTIES('LONG_STRING_COLUMNS'='note')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("both in partition and long_string_columns"))
  }

  test("long_string_columns: columns cannot exist in no_inverted_index columns") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('no_inverted_index'='note', 'LONG_STRING_COLUMNS'='address, note')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("both in no_inverted_index and long_string_columns"))
  }

  test("test alter table properties for long string columns") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT, "true")
    sql("drop table if exists testlongstring")
    sql(
      s"""
         | CREATE TABLE if not exists testlongstring(id INT, name STRING, description STRING
         | ) STORED AS carbondata
         |""".stripMargin)
    sql("insert into testlongstring select 1, 'ab', 'cool'")
    // describe formatted should not have long_string_columns
    checkExistence(sql("describe formatted testlongstring"), false, "long_string_columns")
    //Alter table add table property of long string columns
    sql("ALTER TABLE testlongstring SET TBLPROPERTIES('long_String_columns'='name,description')")
    sql(s""" insert into testlongstring select 1, 'ab1', '$longChar'""")
    checkAnswer(sql("select * from testlongstring"), Seq(Row(1, "ab", "cool"), Row(1, "ab1", longChar)))
    // describe formatted should have long_string_columns
    checkExistence(sql("describe formatted testlongstring"), true, "LONG_STRING_COLUMNS")
    //insert without without local dictionary
    sql("ALTER TABLE testlongstring SET TBLPROPERTIES('local_dictionary_enable'='false')")
    sql(s""" insert into testlongstring select 1, 'abc', '$longChar'""")
    checkAnswer(sql("select * from testlongstring"),
      Seq(Row(1, "ab", "cool"), Row(1, "ab1", longChar), Row(1, "abc", longChar)))
    // Unset the long_String_columns
    sql("ALTER TABLE testlongstring UNSET TBLPROPERTIES('long_string_columns')")
    // describe formatted should not have long_string_columns
    checkExistence(sql("describe formatted testlongstring"), false, "long_string_columns")
    // query should pass
    checkAnswer(sql("select * from testlongstring"),
      Seq(Row(1, "ab", "cool"), Row(1, "ab1", longChar), Row(1, "abc", longChar)))
    // insert long string should fail as unset is done
    val e = intercept[Exception] {
      sql(s""" insert into testlongstring select 1, 'abc', '$longChar'""")
    }
    assert(e.getMessage.contains("DataLoad failure: Column description is too long"))
    sql("ALTER TABLE testlongstring SET TBLPROPERTIES('long_String_columns'='description')")
    sql(s""" insert into testlongstring select 1, 'ab1', '$longChar'""")
    sql("drop table if exists testlongstring")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT,
        CarbonCommonConstants.CARBON_ENABLE_BAD_RECORD_HANDLING_FOR_INSERT_DEFAULT)
  }

  test("test alter table properties for long_string_columns with complex columns") {
    sql("DROP TABLE IF EXISTS varchar_complex_table")
    sql("""
          | CREATE TABLE varchar_complex_table
          | (m1 int,arr1 array<string>,varchar1 string,s1 string,varchar2 string,arr2 array<string>)
          | STORED AS carbondata TBLPROPERTIES('sort_columns'='s1,m1')
          | """.stripMargin)
    sql(
      s"""insert into varchar_complex_table values(1, array('ar1.0','ar1.1'),
         |'xx', 'normal string1', 'xx', array('ar2.0','ar2.1'))""".stripMargin)

    sql("ALTER TABLE varchar_complex_table SET TBLPROPERTIES('long_String_columns'='varchar1,varchar2')")

    sql(
      s"""insert into varchar_complex_table values(1, array('ar1.0','ar1.1'), '$longChar',
         |'normal string1', '$longChar', array('ar2.0','ar2.1')),(2, array('ar1.2','ar1.3'),
         |'$longChar', 'normal string2', '$longChar', array('ar2.2','ar2.3'))""".stripMargin)

    checkAnswer(
      sql("SELECT * FROM varchar_complex_table where m1=1"),
      Seq(Row(1,mutable.WrappedArray.make(Array("ar1.0","ar1.1")),"xx","normal string1",
        "xx",mutable.WrappedArray.make(Array("ar2.0","ar2.1"))),
      Row(1,mutable.WrappedArray.make(Array("ar1.0","ar1.1")),longChar,"normal string1",
        longChar,mutable.WrappedArray.make(Array("ar2.0","ar2.1")))))
    checkAnswer(
      sql(
        """
          |SELECT varchar1,arr2,s1,m1,varchar2,arr1
          |FROM varchar_complex_table
          |WHERE arr1[1]='ar1.3'
          |""".stripMargin),
      Seq(Row(longChar,mutable.WrappedArray.make(Array("ar2.2","ar2.3")),"normal string2",2,
        longChar,mutable.WrappedArray.make(Array("ar1.2","ar1.3")))))
    sql("DROP TABLE IF EXISTS varchar_complex_table")
  }

  test("inverted index columns cannot be present in long_string_cols as they do not support sort_cols") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        s"""
           | CREATE TABLE if not exists $longStringTable(
           | id INT, name STRING, description STRING, address STRING, note STRING
           | ) STORED AS carbondata
           | TBLPROPERTIES('inverted_index'='note', 'long_string_columns'='note,description')
           |""".stripMargin)
    }
    assert(exceptionCaught.getMessage.contains("INVERTED_INDEX column: note should be present in SORT_COLUMNS"))
  }

  private def prepareTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING, note STRING
         | ) STORED AS carbondata
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

  test("create table with varchar column and complex column") {
    sql("DROP TABLE IF EXISTS varchar_complex_table")
    sql("""
        | CREATE TABLE varchar_complex_table
        | (m1 int,arr1 array<string>,varchar1 string,s1 string,varchar2 string,arr2 array<string>)
        | STORED AS carbondata
        | TBLPROPERTIES('long_string_columns'='varchar1,varchar2')
        | """.stripMargin)
    sql("insert into varchar_complex_table values(1, array('ar1.0','ar1.1'), 'longstr10', 'normal string1', 'longstr11', array('ar2.0','ar2.1')),(2, array('ar1.2','ar1.3'), 'longstr20', 'normal string2', 'longstr21', array('ar2.2','ar2.3'))")
    checkAnswer(
      sql("SELECT * FROM varchar_complex_table where varchar1='longstr10'"),
      Seq(Row(1,mutable.WrappedArray.make(Array("ar1.0","ar1.1")),"longstr10","normal string1",
        "longstr11",mutable.WrappedArray.make(Array("ar2.0","ar2.1")))))
    checkAnswer(
      sql(
        """
          |SELECT varchar1,arr2,s1,m1,varchar2,arr1
          |FROM varchar_complex_table
          |WHERE arr1[1]='ar1.3'
          |""".stripMargin),
      Seq(Row("longstr20",mutable.WrappedArray.make(Array("ar2.2","ar2.3")),"normal string2",2,
        "longstr21",mutable.WrappedArray.make(Array("ar1.2","ar1.3")))))

    sql("DROP TABLE IF EXISTS varchar_complex_table")
  }
  
  test("update table with long string column") {
    prepareTable()
    // update non-varchar column
    sql(s"update $longStringTable set(id)=(0) where name is not null").show()
    // update varchar column
    sql(s"update $longStringTable set(description)=('empty') where name is not null").show()
    // update non-varchar&varchar column
    sql(s"update $longStringTable set(description, id)=('sth.', 1) where name is not null").show()
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
         | ) STORED AS carbondata
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
      .option("sort_columns", "name")
      .option("long_string_columns", "description, note")
      .mode(SaveMode.Overwrite)
      .save()

    checkQuery()
  }

  test("write from dataframe with long_string datatype whose order of fields is not the same as that in table") {
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING, note STRING
         | ) STORED AS carbondata
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='description, note', 'sort_columns'='id')
         |""".
        stripMargin)

    prepareDF()
    // the order of fields in dataframe is different from that in create table
    longStringDF.select("note", "address", "description", "name", "id")
      .write
      .format("carbondata")
      .option("tableName", longStringTable)
      .mode(SaveMode.Append)
      .save()

    checkQuery()
  }

  test("desc table shows long_string_columns property") {
    sql(
      s"""
         | CREATE TABLE if not exists $longStringTable(
         | id INT, name STRING, description STRING, address STRING, note STRING
         | ) STORED AS carbondata
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='address, note', 'sort_columns'='id')
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

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
package org.apache.carbondata.spark.testsuite.sortcolumns

import org.apache.spark.sql.{AnalysisException, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestSortColumns extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")

    SparkUtil4Test.createTaskMockUp(sqlContext)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    dropTestTables
    sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")

    sql("CREATE TABLE tableOne(id int, name string, city string, age int) STORED AS carbondata")
    sql("CREATE TABLE tableTwo(id int, age int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table tableOne")
  }

  test("create table sort columns dictionary include - int") {
    sql(
      "CREATE TABLE sortint (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata TBLPROPERTIES " +
      "('sort_columns'='empno')")
  }

  test("create table sort columns dictionary exclude - int") {
    sql(
      "CREATE TABLE sortint1 (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata TBLPROPERTIES " +
      "('sort_columns'='empno')")
  }

  test("create table sort columns dictionary include - bigint") {
    sql(
      "CREATE TABLE sortbigint (empno bigint, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata TBLPROPERTIES " +
      "('sort_columns'='empno')")
  }

  test("create table sort columns dictionary exclude - bigint") {
    sql(
      "CREATE TABLE sortbigint1 (empno bigint, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata TBLPROPERTIES " +
      "( 'sort_columns'='empno')")
  }

  test("create table with no dictionary sort_columns") {
    sql("CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empno')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select empno from sorttable1"), sql("select empno from sorttable1 order by empno"))
  }

  test("create table with no dictionary sort_columns with dictionary exclude") {
    sql(
      "CREATE TABLE sorttable1a (empno String, empname String, designation String, doj Timestamp," +
      " workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata tblproperties" +
      "('sort_columns'='empno', 'SORT_SCOPE'='local_sort')")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable1a OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    checkAnswer(sql("select empname from sorttable1a"),
      sql("select empname from origintable1 order by empname"))
  }

  test(
    "create table with no dictionary sort_columns where NumberOfNoDictSortColumns is less than " +
    "NoDictionaryCount")
  {
    sql("drop table if exists sorttable1b")
    sql(
      "CREATE TABLE sorttable1b (empno String, empname String, designation String, doj Timestamp," +
      " workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata tblproperties" +
      "('sort_columns'='empno,empname'," +
      "'SORT_SCOPE'='local_sort')")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable1b OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    checkAnswer(sql("select empname from sorttable1b"),
      sql("select empname from origintable1 order by empname"))
  }

  test("create table with dictionary sort_columns") {
    sql("CREATE TABLE sorttable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empname')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select empname from sorttable2"),sql("select empname from origintable1"))
  }

  test("create table with direct-dictioanry sort_columns") {
    sql("CREATE TABLE sorttable3 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='doj')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select doj from sorttable3"), sql("select doj from sorttable3 order by doj"))
  }

  test("create table with multi-sort_columns and data loading with offheap safe") {
    try {
      setLoadingProperties("true", "false")
      sql("CREATE TABLE sorttable4_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_offheap_safe"), sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with offheap and unsafe sort") {
    try {
      setLoadingProperties("true", "true")
      sql(
        "CREATE TABLE sorttable4_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_offheap_unsafe"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with heap") {
    try {
      setLoadingProperties("false", "false")
      sql(
        "CREATE TABLE sorttable4_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_heap_safe"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with heap and unsafe sort") {
    try {
      setLoadingProperties("false", "true")
      sql(
        "CREATE TABLE sorttable4_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_heap_unsafe"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("compaction on sort_columns table") {
    sql("CREATE TABLE origintable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql("alter table origintable2 compact 'minor'")

    sql("CREATE TABLE sorttable5 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empno')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql("alter table sorttable5 compact 'minor'")

    checkAnswer(sql("select empno from sorttable5"), sql("select empno from origintable2 order by empno"))
  }

  test("filter on sort_columns include no-dictionary, direct-dictionary and dictioanry") {
    sql("CREATE TABLE sorttable6 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, doj, empname')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    // no dictionary
    checkAnswer(sql("select * from sorttable6 where workgroupcategory = 1"), sql("select * from origintable1 where workgroupcategory = 1 order by doj"))
    // direct dictionary
    checkAnswer(sql("select * from sorttable6 where doj = '2007-01-17 00:00:00'"), sql("select * from origintable1 where doj = '2007-01-17 00:00:00'"))
    // dictionary
    checkAnswer(sql("select * from sorttable6 where empname = 'madhan'"), sql("select * from origintable1 where empname = 'madhan'"))
  }

  test("unsorted table creation, query data loading with heap and safe sort config") {
    try {
      sql("drop table if exists origintable1")
      sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      setLoadingProperties("false", "false")
      sql("CREATE TABLE unsortedtable_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_heap_safe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_heap_safe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and data loading with heap and unsafe sort config") {
    try {
      sql("drop table if exists origintable1")
      sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      setLoadingProperties("false", "true")
      sql("CREATE TABLE unsortedtable_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_heap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_heap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and data loading with offheap and safe sort config") {
    try {
      sql("drop table if exists origintable1")
      sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      setLoadingProperties("true", "false")
      sql("CREATE TABLE unsortedtable_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_offheap_safe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_offheap_safe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and data loading with offheap and unsafe sort config") {
    try {
      sql("drop table if exists origintable1")
      sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      setLoadingProperties("true", "true")
      sql("CREATE TABLE unsortedtable_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_offheap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_offheap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with invalid values for numeric data type columns specified as sort_columns") {
    // load hive data
    sql("CREATE TABLE test_sort_col_hive (id INT, name STRING, age INT) row format delimited fields terminated by ','")
    sql(s"LOAD DATA local inpath '$resourcesPath/numeric_column_invalid_values.csv' INTO TABLE test_sort_col_hive")
    // load carbon data
    sql("CREATE TABLE test_sort_col (id INT, name STRING, age INT) STORED AS carbondata TBLPROPERTIES('SORT_COLUMNS'='id,age')")
    sql(s"LOAD DATA local inpath '$resourcesPath/numeric_column_invalid_values.csv' INTO TABLE test_sort_col OPTIONS('FILEHEADER'='id,name,age')")
    // compare hive and carbon data
    checkAnswer(sql("select * from test_sort_col_hive"), sql("select * from test_sort_col"))
    checkAnswer(sql("select * from test_sort_col_hive where age < 25"), sql("select * from test_sort_col where age < 25"))
    checkAnswer(sql("select * from test_sort_col_hive where age <= 25"), sql("select * from test_sort_col where age <= 25"))
    checkAnswer(sql("select * from test_sort_col_hive where age > 25"), sql("select * from test_sort_col where age > 25"))
    checkAnswer(sql("select * from test_sort_col_hive where age >= 25"), sql("select * from test_sort_col where age >= 25"))
    checkAnswer(sql("select * from test_sort_col_hive where age is null"), sql("select * from test_sort_col where age is null"))
    checkAnswer(sql("select * from test_sort_col_hive where age is not null"), sql("select * from test_sort_col where age is not null"))
  }

  test("describe formatted for sort_columns") {
    sql("CREATE TABLE sorttableDesc (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empno,empname')")
    checkExistence(sql("describe formatted sorttableDesc"),true,"Sort Columns empno, empname")
  }

  test("duplicate columns in sort_columns") {
    sql("drop table if exists sorttable1")
    val exceptionCaught = intercept[MalformedCarbonCommandException]{
      sql("CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empno,empname,empno')")
    }
    assert(exceptionCaught.getMessage.equals("SORT_COLUMNS Either having duplicate columns : empno or it contains illegal argumnet."))
  }

  test("Test tableTwo data") {
    sql("insert into table tableTwo select id, count(age) from tableOne group by id")
    checkAnswer(
      sql("select id,age from tableTwo order by id"),
      Seq(Row(1, 1), Row(2, 1), Row(3, 2), Row(4, 2)))
  }

  test("Measure columns in sort_columns") {
    sql("drop table if exists sorttable1")
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      sql(
        "CREATE TABLE sorttable1 (empno Double, empname String, designation String, doj Timestamp, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
        "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        "utilization int,salary int) STORED AS carbondata tblproperties" +
        "('sort_columns'='empno')")
    }
    assert(exceptionCaught.getMessage
      .equals(
        "sort_columns is unsupported for double datatype column: empno"))
  }

  test("test if equal to 0 filter on sort column gives correct result") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR,
      "true")
    sql("create table test1(a bigint) STORED AS carbondata TBLPROPERTIES('sort_columns'='a')")
    sql("insert into test1 select 'k'")
    sql("insert into test1 select '1'")
    assert(sql("select * from test1 where a = 1 or a = 0").count() == 1)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR,
      CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR_DEFAULT)
  }

  override def afterAll = {
    dropTestTables
    CarbonProperties.getInstance()
      .addProperty(
        CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .removeProperty(CarbonCommonConstants.LOAD_SORT_SCOPE)
      .addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR,
        CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR_DEFAULT)
  }

  def dropTestTables = {
    sql("drop table if exists test1")
    sql("drop table if exists sortint")
    sql("drop table if exists sortint1")
    sql("drop table if exists sortlong")
    sql("drop table if exists sortlong1")
    sql("drop table if exists sortbigint")
    sql("drop table if exists sortbigint1")
    sql("drop table if exists origintable1")
    sql("drop table if exists origintable2")
    sql("drop table if exists sorttable1")
    sql("drop table if exists sorttableDesc")
    sql("drop table if exists sorttable1a")
    sql("drop table if exists sorttable1b")
    sql("drop table if exists sorttable2")
    sql("drop table if exists sorttable3")
    sql("drop table if exists sorttable4_offheap_safe")
    sql("drop table if exists sorttable4_offheap_unsafe")
    sql("drop table if exists sorttable4_offheap_inmemory")
    sql("drop table if exists sorttable4_heap_safe")
    sql("drop table if exists sorttable4_heap_unsafe")
    sql("drop table if exists sorttable4_heap_inmemory")
    sql("drop table if exists sorttable5")
    sql("drop table if exists sorttable6")
    sql("drop table if exists unsortedtable_offheap_safe")
    sql("drop table if exists unsortedtable_offheap_unsafe")
    sql("drop table if exists unsortedtable_offheap_inmemory")
    sql("drop table if exists unsortedtable_heap_safe")
    sql("drop table if exists unsortedtable_heap_unsafe")
    sql("drop table if exists unsortedtable_heap_inmemory")
    sql("drop table if exists test_sort_col")
    sql("drop table if exists test_sort_col_hive")
    sql("drop table if exists sorttable1b")
    sql("DROP TABLE IF EXISTS tableOne")
    sql("DROP TABLE IF EXISTS tableTwo")
  }

  def setLoadingProperties(offheap: String, unsafe: String): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, offheap)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, unsafe)
  }

  def defaultLoadingProperties = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }
}

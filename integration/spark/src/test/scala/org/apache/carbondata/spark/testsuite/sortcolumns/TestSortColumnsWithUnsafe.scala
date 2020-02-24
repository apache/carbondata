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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class TestSortColumnsWithUnsafe extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION, "true")
    dropTable
    sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
  }

  test("create table with no dictionary sort_columns") {
    sql("CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='empno')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select empno from sorttable1"), sql("select empno from origintable1 order by empno"))
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
      setLoadingProperties("true", "false", "false")
      sql("CREATE TABLE sorttable4_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_offheap_safe"), sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with offheap and unsafe sort") {
    try {
      setLoadingProperties("true", "true", "false")
      sql(
        "CREATE TABLE sorttable4_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_offheap_unsafe"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with offheap and inmemory sort") {
    try {
      setLoadingProperties("true", "false", "true")
      sql(
        "CREATE TABLE sorttable4_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_offheap_inmemory"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with heap") {
    try {
      setLoadingProperties("false", "false", "false")
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
      setLoadingProperties("false", "true", "false")
      sql(
        "CREATE TABLE sorttable4_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_heap_unsafe"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("create table with multi-sort_columns and data loading with heap and inmemory sort") {
    try {
      setLoadingProperties("false", "false", "true")
      sql(
        "CREATE TABLE sorttable4_heap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_heap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_heap_inmemory"),
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
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
    // no dictionary
    checkAnswer(sql("select * from sorttable6 where workgroupcategory = 1"), sql("select * from origintable1 where workgroupcategory = 1 order by doj"))
    // direct dictionary
    checkAnswer(sql("select * from sorttable6 where doj = '2007-01-17 00:00:00'"), sql("select * from origintable1 where doj = '2007-01-17 00:00:00'"))
    // dictionary
    checkAnswer(sql("select * from sorttable6 where empname = 'madhan'"), sql("select * from origintable1 where empname = 'madhan'"))
  }

  test("unsorted table creation, query data loading with heap and safe sort config") {
    try {
      setLoadingProperties("false", "false", "false")
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
      setLoadingProperties("false", "true", "false")
      sql("CREATE TABLE unsortedtable_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_heap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_heap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and loading with heap and inmemory sort config") {
    try {
      setLoadingProperties("false", "false", "true")
      sql("CREATE TABLE unsortedtable_heap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_heap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_heap_inmemory where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_heap_inmemory order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and data loading with offheap and safe sort config") {
    try {
      setLoadingProperties("true", "false", "false")
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
      setLoadingProperties("true", "true", "false")
      sql("CREATE TABLE unsortedtable_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_offheap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_offheap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("unsorted table creation, query and data loading with offheap and inmemory sort config") {
    try {
      setLoadingProperties("true", "false", "true")
      sql("CREATE TABLE unsortedtable_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED AS carbondata tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE unsortedtable_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'TIMESTAMPFORMAT'='dd-MM-yyyy')""")
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno < 15 order by empno"), sql("select * from origintable1 where empno < 15 order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno <= 15 order by empno"), sql("select * from origintable1 where empno <= 15 order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno > 15 order by empno"), sql("select * from origintable1 where empno > 15 order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno >= 15 order by empno"), sql("select * from origintable1 where empno >= 15 order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno <> 15 order by empno"), sql("select * from origintable1 where empno <> 15 order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno in (15, 16, 17) order by empno"), sql("select * from origintable1 where empno in (15, 16, 17) order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno is null"), sql("select * from origintable1 where empno is null order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory where empno is not null"), sql("select * from origintable1 where empno is not null order by empno"))
      checkAnswer(sql("select * from unsortedtable_offheap_inmemory order by empno"), sql("select * from origintable1 order by empno"))
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
  }

  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION,
        CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)

    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
        CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }

  def dropTable = {
    sql("drop table if exists origintable1")
    sql("drop table if exists origintable2")
    sql("drop table if exists sorttable1")
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
  }

  def setLoadingProperties(offheap: String, unsafe: String, useBatch: String): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, offheap)
    if (useBatch.equalsIgnoreCase("true")) {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "true")
    }
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, useBatch)
  }

  def defaultLoadingProperties = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_SORT_SCOPE, CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  }
}

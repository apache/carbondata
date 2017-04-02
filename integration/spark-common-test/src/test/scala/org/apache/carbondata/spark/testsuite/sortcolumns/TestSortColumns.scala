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

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestSortColumns extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable

    sql("CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
  }

  test("create table with no dictionary sort_columns") {
    sql("CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select empno from sorttable1"), sql("select empno from sorttable1 order by empno"))
  }

  test("create table with dictionary sort_columns") {
    sql("CREATE TABLE sorttable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empname')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select empname from sorttable2"),sql("select empname from origintable1"))
  }

  test("create table with direct-dictioanry sort_columns") {
    sql("CREATE TABLE sorttable3 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    checkAnswer(sql("select doj from sorttable3"), sql("select doj from sorttable3 order by doj"))
  }

  test("create table with multi-sort_columns and data loading with offheap safe") {
    try {
      setLoadingProperties("true", "false", "false")
      sql("CREATE TABLE sorttable4_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
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
        "CREATE TABLE sorttable4_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
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
        "CREATE TABLE sorttable4_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
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
        "CREATE TABLE sorttable4_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
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
        "CREATE TABLE sorttable4_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
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
        "CREATE TABLE sorttable4_heap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable4_heap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select workgroupcategory, empname from sorttable4_heap_inmemory"),
        sql("select workgroupcategory, empname from origintable1 order by workgroupcategory"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("compaction on sort_columns table") {
    sql("CREATE TABLE origintable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql("alter table origintable2 compact 'minor'")

    sql("CREATE TABLE sorttable5 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    sql("alter table sorttable5 compact 'minor'")

    checkAnswer(sql("select empno from sorttable5"), sql("select empno from origintable2 order by empno"))
  }

  test("filter on sort_columns include no-dictionary, direct-dictionary and dictioanry") {
    sql("CREATE TABLE sorttable6 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, doj, empname')")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
    // no dictionary
    checkAnswer(sql("select * from sorttable6 where workgroupcategory = 1"), sql("select * from origintable1 where workgroupcategory = 1 order by doj"))
    // direct dictionary
    checkAnswer(sql("select * from sorttable6 where doj = '2007-01-17 00:00:00'"), sql("select * from origintable1 where doj = '2007-01-17 00:00:00'"))
    // dictionary
    checkAnswer(sql("select * from sorttable6 where empname = 'madhan'"), sql("select * from origintable1 where empname = 'madhan'"))
  }

  test("no sort_columns with and data loading with heap and safe sort config") {
    try {
      setLoadingProperties("false", "false", "false")
      sql("CREATE TABLE sorttable7_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_heap_safe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_heap_safe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("no sort_columns with and data loading with heap and unsafe sort config") {
    try {
      setLoadingProperties("false", "true", "false")
      sql("CREATE TABLE sorttable7_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_heap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_heap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("no sort_columns with and data loading with heap and inmemory sort config") {
    try {
      setLoadingProperties("false", "false", "true")
      sql("CREATE TABLE sorttable7_heap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_heap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_heap_inmemory where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_heap_inmemory order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("no sort_columns with and data loading with offheap and safe sort config") {
    try {
      setLoadingProperties("true", "false", "false")
      sql("CREATE TABLE sorttable7_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_offheap_safe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_offheap_safe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("no sort_columns with and data loading with offheap and unsafe sort config") {
    try {
      setLoadingProperties("true", "true", "false")
      sql("CREATE TABLE sorttable7_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_offheap_unsafe where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_offheap_unsafe order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }

  test("no sort_columns with and data loading with offheap and inmemory sort config") {
    try {
      setLoadingProperties("true", "false", "true")
      sql("CREATE TABLE sorttable7_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')")
      sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE sorttable7_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")
      checkAnswer(sql("select * from sorttable7_offheap_inmemory where empno = 11"), sql("select * from origintable1 where empno = 11"))
      checkAnswer(sql("select * from sorttable7_offheap_inmemory order by empno"), sql("select * from origintable1 order by empno"))
    } finally {
      defaultLoadingProperties
    }
  }


  override def afterAll = {
    dropTable
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
    sql("drop table if exists sorttable7_offheap_safe")
    sql("drop table if exists sorttable7_offheap_unsafe")
    sql("drop table if exists sorttable7_offheap_inmemory")
    sql("drop table if exists sorttable7_heap_safe")
    sql("drop table if exists sorttable7_heap_unsafe")
    sql("drop table if exists sorttable7_heap_inmemory")
  }

  def setLoadingProperties(offheap: String, unsafe: String, useBatch: String): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, offheap)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, unsafe)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_USE_BATCH_SORT, useBatch)
  }

  def defaultLoadingProperties = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT, CarbonCommonConstants.ENABLE_OFFHEAP_SORT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOAD_USE_BATCH_SORT, CarbonCommonConstants.LOAD_USE_BATCH_SORT_DEFAULT)
  }
}

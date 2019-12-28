
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for sortColumnTestCase to verify all scenerios
 */

class SortColumnTestCase extends QueryTest with BeforeAndAfterAll {
         

  //create table with no dictionary sort_columns
  test("Sortcolumn-001_TC001", Include) {
    sql(s"""drop table if exists sorttable1""").collect
     sql(s"""CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select empno from sorttable1""").collect

     sql(s"""drop table if exists sorttable1""").collect
  }


  //create table with dictionary sort_columns
  test("Sortcolumn-001_TC002", Include) {
     sql(s"""CREATE TABLE sorttable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select empname from sorttable2""").collect

     sql(s"""drop table if exists sorttable2""").collect
  }


  //create table with direct-dictioanry sort_columns
  test("Sortcolumn-001_TC003", Include) {
     sql(s"""CREATE TABLE sorttable3 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable3""").collect

     sql(s"""drop table if exists sorttable3""").collect
  }


  //create table with multi-sort_columns and data loading with offheap safe
  test("Sortcolumn-001_TC004", Include) {
     sql(s"""CREATE TABLE sorttable4_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_offheap_safe""").collect

     sql(s"""drop table if exists sorttable4_offheap_safe""").collect
  }


  //create table with multi-sort_columns and data loading with offheap and unsafe sort
  test("Sortcolumn-001_TC005", Include) {
     sql(s"""CREATE TABLE sorttable4_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_offheap_unsafe""").collect

     sql(s"""drop table if exists sorttable4_offheap_unsafe""").collect
  }


  //create table with multi-sort_columns and data loading with offheap and inmemory sort
  test("Sortcolumn-001_TC006", Include) {
     sql(s"""CREATE TABLE sorttable4_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_offheap_inmemory""").collect

     sql(s"""drop table if exists sorttable4_offheap_inmemory""").collect
  }


  //create table with multi-sort_columns and data loading with heap
  test("Sortcolumn-001_TC007", Include) {
     sql(s"""CREATE TABLE sorttable4_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_heap_safe""").collect

     sql(s"""drop table if exists sorttable4_heap_safe""").collect
  }


  //create table with multi-sort_columns and data loading with heap and unsafe sort
  test("Sortcolumn-001_TC008", Include) {
     sql(s"""CREATE TABLE sorttable4_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_heap_unsafe""").collect

     sql(s"""drop table if exists sorttable4_heap_unsafe""").collect
  }


  //create table with multi-sort_columns and data loading with heap and inmemory sort
  test("Sortcolumn-001_TC009", Include) {
     sql(s"""CREATE TABLE sorttable4_heap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable4_heap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select workgroupcategory, empname from sorttable4_heap_inmemory""").collect

     sql(s"""drop table if exists sorttable4_heap_inmemory""").collect
  }


  //create table with multi-sort_columns and data loading with heap and inmemory sort
  test("Sortcolumn-001_TC010", Include) {
    sql(s"""drop table if exists origintable2""").collect
    sql(s"""drop table if exists sorttable5""").collect
     sql(s"""CREATE TABLE origintable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table origintable2 compact 'minor'""").collect
   sql(s"""CREATE TABLE sorttable5 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable5 compact 'minor'""").collect
    sql(s"""select empno from sorttable5""").collect

     sql(s"""drop table if exists sorttable5""").collect
  }


  //filter on sort_columns include no-dictionary
  test("Sortcolumn-001_TC011", Include) {
    sql(s"""drop table if exists sorttable6""").collect
     sql(s"""CREATE TABLE sorttable6 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, doj, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable6 where workgroupcategory = 1""").collect

     sql(s"""drop table if exists sorttable6""").collect
  }


  //filter on sort_columns include direct-dictionary
  test("Sortcolumn-001_TC012", Include) {
     sql(s"""CREATE TABLE sorttable6 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, doj, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable6 where doj = '2007-01-17 00:00:00'""").collect

     sql(s"""drop table if exists sorttable6""").collect
  }


  //filter on sort_columns include dictioanry
  test("Sortcolumn-001_TC013", Include) {
    sql(s"""drop table if exists sorttable6""").collect
     sql(s"""CREATE TABLE sorttable6 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, doj, empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable6 where empname = 'madhan'""").collect

     sql(s"""drop table if exists sorttable6""").collect
  }


  //unsorted table creation, query data loading with heap and safe sort config
  test("Sortcolumn-001_TC014", Include) {
     sql(s"""CREATE TABLE unsortedtable_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_c+C17olumns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_heap_safe where empno = 11""").collect

     sql(s"""drop table if exists unsortedtable_heap_safe""").collect
  }


  //unsorted table creation, query data loading with heap and safe sort config with order by
  test("Sortcolumn-001_TC015", Include) {
    sql(s"""drop table if exists unsortedtable_heap_safe""").collect
     sql(s"""CREATE TABLE unsortedtable_heap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_heap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_heap_safe order by empno""").collect

     sql(s"""drop table if exists unsortedtable_heap_safe""").collect
  }


  //unsorted table creation, query and data loading with heap and unsafe sort config
  test("Sortcolumn-001_TC016", Include) {
     sql(s"""CREATE TABLE unsortedtable_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_heap_unsafe where empno = 11""").collect

     sql(s"""drop table if exists unsortedtable_heap_unsafe""").collect
  }


  //unsorted table creation, query and data loading with heap and unsafe sort config with order by
  test("Sortcolumn-001_TC017", Include) {
    sql(s"""drop table if exists unsortedtable_heap_unsafe""").collect
     sql(s"""CREATE TABLE unsortedtable_heap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_heap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_heap_unsafe order by empno""").collect

     sql(s"""drop table if exists unsortedtable_heap_unsafe""").collect
  }


  //unsorted table creation, query and data loading with offheap and safe sort config
  test("Sortcolumn-001_TC018", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_safe where empno = 11""").collect

     sql(s"""drop table if exists unsortedtable_offheap_safe""").collect
  }


  //unsorted table creation, query and data loading with offheap and safe sort config with order by
  test("Sortcolumn-001_TC019", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_safe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_safe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_safe order by empno""").collect

     sql(s"""drop table if exists unsortedtable_offheap_safe""").collect
  }


  //unsorted table creation, query and data loading with offheap and unsafe sort config
  test("Sortcolumn-001_TC020", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_unsafe where empno = 11""").collect

     sql(s"""drop table if exists unsortedtable_offheap_unsafe""").collect
  }


  //unsorted table creation, query and data loading with offheap and unsafe sort config with order by
  test("Sortcolumn-001_TC021", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_unsafe order by empno""").collect

     sql(s"""drop table if exists unsortedtable_offheap_unsafe""").collect
  }


  //unsorted table creation, query and data loading with offheap and inmemory sort config
  test("Sortcolumn-001_TC022", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_inmemory where empno = 11""").collect

     sql(s"""drop table if exists unsortedtable_offheap_inmemory""").collect
  }


  //unsorted table creation, query and data loading with offheap and inmemory sort config with order by
  test("Sortcolumn-001_TC023", Include) {
     sql(s"""CREATE TABLE unsortedtable_offheap_inmemory (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE unsortedtable_offheap_inmemory OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from unsortedtable_offheap_inmemory order by empno""").collect

     sql(s"""drop table if exists unsortedtable_offheap_inmemory""").collect
  }


  //create table with dictioanry_exclude sort_columns
  test("Sortcolumn-001_TC024", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empname')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }

  //create table with alter table and sort_columns with dimension
  test("Sortcolumn-001_TC027", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable add columns(newField String)  """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataString.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }


  //create table with alter table and sort_columns with measure
  test("Sortcolumn-001_TC028", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable add columns(newField Int)  """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataInt.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }


  //create table with no_inverted_index and sort_columns
  test("Sortcolumn-001_TC029", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj','no_inverted_index'='doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }

  //test sort_column for different order of column name
  test("Sortcolumn-001_TC032", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties( 'sort_columns'='empname,empno,workgroupcategory,doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }


  //default behavior if sort_column not provided
  test("Sortcolumn-001_TC033", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select doj from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }


  //test sort_column for alter table
  test("Sortcolumn-001_TC035", Include) {
    sql(s"""drop table if exists sorttable""").collect
     sql(s"""CREATE TABLE sorttable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable drop columns(doj)""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataDrop.csv' INTO TABLE sorttable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable""").collect

     sql(s"""drop table if exists sorttable""").collect
  }


  //test sort_column for float data_type with alter query
  test("Sortcolumn-001_TC037", Include) {
    sql(s"""drop table if exists sorttable""").collect
    sql(s"""drop table if exists sorttable1""").collect
     sql(s"""CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable1 add columns(newField Float)  """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataFloat.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable1""").collect

     sql(s"""drop table if exists sorttable1""").collect
  }


  //test sort_column for decimal data_type with alter query
  test("Sortcolumn-001_TC038", Include) {
    sql(s"""drop table if exists sorttable""").collect
    sql(s"""drop table if exists sorttable1""").collect
     sql(s"""CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/data.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
   sql(s"""alter table sorttable1 add columns(newField decimal)  """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataDecimal.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable1""").collect

     sql(s"""drop table if exists sorttable1""").collect
  }


  //test sort_column for decimal data_type
  test("Sortcolumn-001_TC039", Include) {
    sql(s"""drop table if exists sorttable""").collect
    sql(s"""drop table if exists sorttable1""").collect
     sql(s"""CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int,newField decimal) STORED BY 'org.apache.carbondata.format' """).collect
   sql(s"""LOAD DATA local inpath '$resourcesPath/Data/sortcolumns/dataDecimal.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect
    sql(s"""select * from sorttable1""").collect

     sql(s"""drop table if exists sorttable1""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.load.sort.scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
  val p2 = prop.getProperty("enable.unsafe.sort", CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT)
  val p3 = prop.getProperty("enable.offheap.sort", CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.load.sort.scope", "local_sort")
    prop.addProperty("enable.unsafe.sort", "true")
    prop.addProperty("enable.offheap.sort", "true")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.load.sort.scope", p1)
    prop.addProperty("enable.unsafe.sort", p2)
    prop.addProperty("enable.offheap.sort", p3)
  }
}
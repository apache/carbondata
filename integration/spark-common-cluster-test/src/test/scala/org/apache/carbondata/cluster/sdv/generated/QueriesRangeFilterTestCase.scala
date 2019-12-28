
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

/**
 * Test Class for QueriesRangeFilterTestCase to verify all scenerios
 */

class QueriesRangeFilterTestCase extends QueryTest with BeforeAndAfterAll {
         

//Range_Filter_01_1
test("Range_Filter_01_1", Include) {
  sql("drop table if exists NO_DICTIONARY_CARBON_8")
  sql("drop table if exists NO_DICTIONARY_CARBON_8_hive")
  sql("drop table if exists NO_DICTIONARY_CARBON_6")
  sql("drop table if exists NO_DICTIONARY_CARBON_6_hive")
  sql("drop table if exists NO_DICTIONARY_CARBON_7")
  sql("drop table if exists NO_DICTIONARY_CARBON_7_hive")
  sql("drop table if exists DICTIONARY_CARBON_6")
  sql("drop table if exists DICTIONARY_CARBON_6_hive")
  sql("drop table if exists directDictionaryTable")
  sql("drop table if exists directDictionaryTable_hive")
  sql("drop table if exists NO_DICTIONARY_CARBON")
  sql("drop table if exists NO_DICTIONARY_CARBON_hive")

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON(empno string, doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp, designation String,attendance Int,utilization Int,salary Int) STORED BY 'org.apache.carbondata.format' """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_hive(empno string, empname String, designation String,doj Timestamp, workgroupcategory Int,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp,attendance Int,utilization Int,salary Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata.csv' INTO TABLE NO_DICTIONARY_CARBON OPTIONS('DELIMITER'= ',', 'QUOTECHAR'='"', 'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata_hive1.csv' INTO TABLE NO_DICTIONARY_CARBON_hive """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_6 (empno string, doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp, designation String,attendance Int,utilization Int,salary Int) STORED BY 'org.apache.carbondata.format' """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_6_hive (empno string, empname String, designation String,doj Timestamp, workgroupcategory Int,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp,attendance Int,utilization Int,salary Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata.csv' INTO TABLE NO_DICTIONARY_CARBON_6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'='"', 'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata_hive2.csv' INTO TABLE NO_DICTIONARY_CARBON_6_hive """).collect

  sql(s"""CREATE TABLE DICTIONARY_CARBON_6 (empno string, doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate timestamp,projectenddate Timestamp, designation String,attendance Int,utilization Int,salary Int) STORED BY 'org.apache.carbondata.format' """).collect

  sql(s"""CREATE TABLE DICTIONARY_CARBON_6_hive (empno string, empname String, designation String,doj Timestamp, workgroupcategory Int,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp,attendance Int,utilization Int,salary Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata.csv' INTO TABLE DICTIONARY_CARBON_6 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'='"', 'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata_hive3.csv' INTO TABLE DICTIONARY_CARBON_6_hive """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_7 (empno string, doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp, designation String,attendance Int,utilization Int,salary Int) STORED BY 'org.apache.carbondata.format' """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_7_hive (empno string, empname String, designation String,doj Timestamp, workgroupcategory Int,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp,attendance Int,utilization Int,salary Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata.csv' INTO TABLE NO_DICTIONARY_CARBON_7  OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"' , 'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata_hive4.csv' INTO TABLE NO_DICTIONARY_CARBON_7_hive  """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_8 (empno string, doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp, designation String,attendance Int,utilization Int,salary Int) STORED BY 'org.apache.carbondata.format' """).collect

  sql(s"""CREATE TABLE NO_DICTIONARY_CARBON_8_hive (empno string, empname String, designation String,doj Timestamp, workgroupcategory Int,workgroupcategoryname String, deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, projectenddate Timestamp,attendance Int,utilization Int,salary Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata.csv' INTO TABLE NO_DICTIONARY_CARBON_8  OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"' , 'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangefilterdata_hive5.csv' INTO TABLE NO_DICTIONARY_CARBON_8_hive  """).collect

  sql(s"""CREATE TABLE if not exists directDictionaryTable (empno int,doj Timestamp, salary int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""CREATE TABLE if not exists directDictionaryTable_hive (empno int,doj Timestamp, salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangedatasample.csv' INTO TABLE directDictionaryTable OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '"','FILEHEADER'='empno,doj,salary')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/RangeFilter/rangedatasample_hive1.csv' INTO TABLE directDictionaryTable_hive """).collect

}
       

//Range_Filter_01
test("Range_Filter_01", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and empno < '15'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and empno < '15'""", "QueriesRangeFilterTestCase_Range_Filter_01")
  
}
       

//Range_Filter_02
test("Range_Filter_02", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '00' and empno < '09'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '00' and empno < '09'""", "QueriesRangeFilterTestCase_Range_Filter_02")
  
}
       

//Range_Filter_03
test("Range_Filter_03", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '22' and empno < '30'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '22' and empno < '30'""", "QueriesRangeFilterTestCase_Range_Filter_03")
  
}
       

//Range_Filter_04
test("Range_Filter_04", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '00' and empno < '30'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '00' and empno < '30'""", "QueriesRangeFilterTestCase_Range_Filter_04")
  
}
       

//Range_Filter_05
test("Range_Filter_05", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno >= '20' and empno <= '30'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno >= '20' and empno <= '30'""", "QueriesRangeFilterTestCase_Range_Filter_05")
  
}
       

//Range_Filter_06
test("Range_Filter_06", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '20' and empno <= '30'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '20' and empno <= '30'""", "QueriesRangeFilterTestCase_Range_Filter_06")
  
}
       

//Range_Filter_07
test("Range_Filter_07", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '11' and empno > '00'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '11' and empno > '00'""", "QueriesRangeFilterTestCase_Range_Filter_07")
  
}
       

//Range_Filter_08
test("Range_Filter_08", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno < '11' and empno > '00'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno < '11' and empno > '00'""", "QueriesRangeFilterTestCase_Range_Filter_08")
  
}
       

//Range_Filter_09
test("Range_Filter_09", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno < '11' and empno > '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno < '11' and empno > '20'""", "QueriesRangeFilterTestCase_Range_Filter_09")
  
}
       

//Range_Filter_10
test("Range_Filter_10", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '11' and empno > '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '11' and empno > '20'""", "QueriesRangeFilterTestCase_Range_Filter_10")
  
}
       

//Range_Filter_11
test("Range_Filter_11", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno < '11' and empno >= '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno < '11' and empno >= '20'""", "QueriesRangeFilterTestCase_Range_Filter_11")
  
}
       

//Range_Filter_12
test("Range_Filter_12", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '11' and empno >= '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '11' and empno >= '20'""", "QueriesRangeFilterTestCase_Range_Filter_12")
  
}
       

//Range_Filter_13
test("Range_Filter_13", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '11' and empno >= '11'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '11' and empno >= '11'""", "QueriesRangeFilterTestCase_Range_Filter_13")
  
}
       

//Range_Filter_14
test("Range_Filter_14", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '20' and empno >= '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '20' and empno >= '20'""", "QueriesRangeFilterTestCase_Range_Filter_14")
  
}
       

//Range_Filter_15
test("Range_Filter_15", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno <= '15' and empno >= '15'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno <= '15' and empno >= '15'""", "QueriesRangeFilterTestCase_Range_Filter_15")
  
}
       

//Range_Filter_16
test("Range_Filter_16", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_16")
  
}
       

//Range_Filter_17
test("Range_Filter_17", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' or empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' or empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_17")
  
}
       

//Range_Filter_18
test("Range_Filter_18", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' or empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' or empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_18")
  
}
       

//Range_Filter_19
test("Range_Filter_19", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and workgroupcategory = '1' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and workgroupcategory = '1' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_19")
  
}
       

//Range_Filter_20
test("Range_Filter_20", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and empno < '20' and workgroupcategory = '1'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and empno < '20' and workgroupcategory = '1'""", "QueriesRangeFilterTestCase_Range_Filter_20")
  
}
       

//Range_Filter_21
test("Range_Filter_21", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and empno < '13' and workgroupcategory = '1' and empno > '14' and empno < '17'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and empno < '13' and workgroupcategory = '1' and empno > '14' and empno < '17'""", "QueriesRangeFilterTestCase_Range_Filter_21")
  
}
       

//Range_Filter_22
test("Range_Filter_22", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno > '11' and empno < '13' and workgroupcategory = '1' or empno > '14' or empno < '17'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where empno > '11' and empno < '13' and workgroupcategory = '1' or empno > '14' or empno < '17'""", "QueriesRangeFilterTestCase_Range_Filter_22")
  
}
       

//Range_Filter_23
test("Range_Filter_23", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '15'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno < '15'""", "QueriesRangeFilterTestCase_Range_Filter_23")
  
}
       

//Range_Filter_24
test("Range_Filter_24", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '00' and empno < '09'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '00' and empno < '09'""", "QueriesRangeFilterTestCase_Range_Filter_24")
  
}
       

//Range_Filter_25
test("Range_Filter_25", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '22' and empno < '30'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '22' and empno < '30'""", "QueriesRangeFilterTestCase_Range_Filter_25")
  
}
       

//Range_Filter_26
test("Range_Filter_26", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '00' and empno < '30'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '00' and empno < '30'""", "QueriesRangeFilterTestCase_Range_Filter_26")
  
}
       

//Range_Filter_27
test("Range_Filter_27", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno < '11' and empno > '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno < '11' and empno > '20'""", "QueriesRangeFilterTestCase_Range_Filter_27")
  
}
       

//Range_Filter_28
test("Range_Filter_28", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_28")
  
}
       

//Range_Filter_29
test("Range_Filter_29", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' or empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_29")
  
}
       

//Range_Filter_30
test("Range_Filter_30", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '11' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' or empno > '11' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_30")
  
}
       

//Range_Filter_31
test("Range_Filter_31", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and workgroupcategory = '1' and empno < '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and workgroupcategory = '1' and empno < '20'""", "QueriesRangeFilterTestCase_Range_Filter_31")
  
}
       

//Range_Filter_32
test("Range_Filter_32", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '20' and workgroupcategory = '1'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno < '20' and workgroupcategory = '1'""", "QueriesRangeFilterTestCase_Range_Filter_32")
  
}
       

//Range_Filter_33
test("Range_Filter_33", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '13' and workgroupcategory = '1' and empno > '14' and empno < '17'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno < '13' and workgroupcategory = '1' and empno > '14' and empno < '17'""", "QueriesRangeFilterTestCase_Range_Filter_33")
  
}
       

//Range_Filter_34
test("Range_Filter_34", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '13' and workgroupcategory = '1' or empno > '14' or empno < '17'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno < '13' and workgroupcategory = '1' or empno > '14' or empno < '17'""", "QueriesRangeFilterTestCase_Range_Filter_34")
  
}
       

//Range_Filter_35
test("Range_Filter_35", Include) {
  
  checkAnswer(s"""select s.empno, s.empname, t.empno, t.empname from DICTIONARY_CARBON_6 s, NO_DICTIONARY_CARBON_6 t where s.empno > '11' and t.empno < '16' and s.empname = t.empname""",
    s"""select s.empno, s.empname, t.empno, t.empname from DICTIONARY_CARBON_6_hive s, NO_DICTIONARY_CARBON_6_hive t where s.empno > '11' and t.empno < '16' and s.empname = t.empname""", "QueriesRangeFilterTestCase_Range_Filter_35")
  
}
       

//Range_Filter_36
test("Range_Filter_36", Include) {
  
  checkAnswer(s"""select s.empno, s.empname, t.empno, t.empname from DICTIONARY_CARBON_6 s, NO_DICTIONARY_CARBON_6 t where s.empno > '09' and t.empno < '30' and s.empname = t.empname""",
    s"""select s.empno, s.empname, t.empno, t.empname from DICTIONARY_CARBON_6_hive s, NO_DICTIONARY_CARBON_6_hive t where s.empno > '09' and t.empno < '30' and s.empname = t.empname""", "QueriesRangeFilterTestCase_Range_Filter_36")
  
}
       

//Range_Filter_37
test("Range_Filter_37", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 and workgroupcategory < 5""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where workgroupcategory > 1 and workgroupcategory < 5""", "QueriesRangeFilterTestCase_Range_Filter_37")
  
}
       

//Range_Filter_38
test("Range_Filter_38", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where workgroupcategory > 1 or workgroupcategory < 5""", "QueriesRangeFilterTestCase_Range_Filter_38")
  
}
       

//Range_Filter_39
test("Range_Filter_39", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3""", "QueriesRangeFilterTestCase_Range_Filter_39")
  
}
       

//Range_Filter_40
test("Range_Filter_40", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3 or workgroupcategory < 10""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where workgroupcategory > 1 or workgroupcategory < 5 and workgroupcategory > 3 or workgroupcategory < 10""", "QueriesRangeFilterTestCase_Range_Filter_40")
  
}
       

//Range_Filter_41
test("Range_Filter_41", Include) {
  
  sql(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:00:17'""").collect
  
}
       

//Range_Filter_42
test("Range_Filter_42", Include) {
  
  sql(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:00:16' and doj < '2016-03-14 15:00:18'""").collect
  
}
       

//Range_Filter_43
test("Range_Filter_43", Include) {
  
  sql(s"""select count(*) from directDictionaryTable where doj > '2016-03-14 15:00:18.0' and doj < '2016-03-14 15:00:09.0'""").collect
  
}
       

//Range_Filter_44
ignore("Range_Filter_44", Include) {
  
  checkAnswer(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:00:09'""",
    s"""select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09'""", "QueriesRangeFilterTestCase_Range_Filter_44")
  
}
       

//Range_Filter_45
  ignore("Range_Filter_45", Include) {
  
  checkAnswer(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15'""",
    s"""select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15'""", "QueriesRangeFilterTestCase_Range_Filter_45")
  
}
       

//Range_Filter_46
  ignore("Range_Filter_46", Include) {
  
  checkAnswer(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15' and doj < '2016-03-14 15:00:13'""",
    s"""select doj from directDictionaryTable_hive where doj > '2016-03-14 15:00:09' or doj > '2016-03-14 15:00:15' and doj < '2016-03-14 15:00:13'""", "QueriesRangeFilterTestCase_Range_Filter_46")
  
}
       

//Range_Filter_47
test("Range_Filter_47", Include) {
  
  checkAnswer(s"""select doj from directDictionaryTable where doj > '2016-03-14 15:05:09' or doj > '2016-03-14 15:05:15' and doj < '2016-03-14 15:50:13'""",
    s"""select doj from directDictionaryTable_hive where doj > '2016-03-14 15:05:09' or doj > '2016-03-14 15:05:15' and doj < '2016-03-14 15:50:13'""", "QueriesRangeFilterTestCase_Range_Filter_47")
  
}
       

//Range_Filter_48
test("Range_Filter_48", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 10""", "QueriesRangeFilterTestCase_Range_Filter_48")
  
}
       

//Range_Filter_49
test("Range_Filter_49", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 and deptno < 10""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 10 and deptno < 10""", "QueriesRangeFilterTestCase_Range_Filter_49")
  
}
       

//Range_Filter_50
test("Range_Filter_50", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 10""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 10 or deptno < 10""", "QueriesRangeFilterTestCase_Range_Filter_50")
  
}
       

//Range_Filter_51
test("Range_Filter_51", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 15 and deptno >12""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 10 or deptno < 15 and deptno >12""", "QueriesRangeFilterTestCase_Range_Filter_51")
  
}
       

//Range_Filter_52
test("Range_Filter_52", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 10 or deptno < 15 and deptno > 12""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 10 or deptno < 15 and deptno > 12""", "QueriesRangeFilterTestCase_Range_Filter_52")
  
}
       

//Range_Filter_53
test("Range_Filter_53", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 or deptno < 10 and deptno > 12""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 14 or deptno < 10 and deptno > 12""", "QueriesRangeFilterTestCase_Range_Filter_53")
  
}
       

//Range_Filter_54
test("Range_Filter_54", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 and deptno < 14""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 14 and deptno < 14""", "QueriesRangeFilterTestCase_Range_Filter_54")
  
}
       

//Range_Filter_55
test("Range_Filter_55", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where deptno > 14 or deptno < 14""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6_hive where deptno > 14 or deptno < 14""", "QueriesRangeFilterTestCase_Range_Filter_55")
  
}
       

//Range_Filter_56
test("Range_Filter_56", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and empno < '15'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and empno < '15'""", "QueriesRangeFilterTestCase_Range_Filter_56")
  
}
       

//Range_Filter_57
test("Range_Filter_57", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '15'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' or empno > '15'""", "QueriesRangeFilterTestCase_Range_Filter_57")
  
}
       

//Range_Filter_58
test("Range_Filter_58", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or empno > '20' and empno < '18'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' or empno > '20' and empno < '18'""", "QueriesRangeFilterTestCase_Range_Filter_58")
  
}
       

//Range_Filter_59
test("Range_Filter_59", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno < '11' or empno > '20'""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno < '11' or empno > '20'""", "QueriesRangeFilterTestCase_Range_Filter_59")
  
}
       

//Range_Filter_60
test("Range_Filter_60", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' and workgroupcategory > 2""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' and workgroupcategory > 2""", "QueriesRangeFilterTestCase_Range_Filter_60")
  
}
       

//Range_Filter_61
test("Range_Filter_61", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6 where empno > '11' or workgroupcategory > 2""",
    s"""select empno,empname,workgroupcategory from DICTIONARY_CARBON_6_hive where empno > '11' or workgroupcategory > 2""", "QueriesRangeFilterTestCase_Range_Filter_61")
  
}
       

//Range_Filter_62
test("Range_Filter_62", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and workgroupcategory < 3""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8_hive where empno > '13' and workgroupcategory < 3""", "QueriesRangeFilterTestCase_Range_Filter_62")
  
}
       

//Range_Filter_63
test("Range_Filter_63", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and workgroupcategory < 3 and deptno > 12 and empno < '17'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8_hive where empno > '13' and workgroupcategory < 3 and deptno > 12 and empno < '17'""", "QueriesRangeFilterTestCase_Range_Filter_63")
  
}
       

//Range_Filter_64
test("Range_Filter_64", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' or workgroupcategory < 3 and deptno > 12 and projectcode > 928478 and empno > '17'""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8_hive where empno > '13' or workgroupcategory < 3 and deptno > 12 and projectcode > 928478 and empno > '17'""", "QueriesRangeFilterTestCase_Range_Filter_64")
  
}
       

//Range_Filter_65
test("Range_Filter_65", Include) {
  
  checkAnswer(s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8 where empno > '13' and empno < '17' and workgroupcategory < 1 and deptno > 14""",
    s"""select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_8_hive where empno > '13' and empno < '17' and workgroupcategory < 1 and deptno > 14""", "QueriesRangeFilterTestCase_Range_Filter_65")
}
       
  override def afterAll {
    sql("drop table if exists NO_DICTIONARY_CARBON_8")
    sql("drop table if exists NO_DICTIONARY_CARBON_8_hive")
    sql("drop table if exists NO_DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_6_hive")
    sql("drop table if exists NO_DICTIONARY_CARBON_7")
    sql("drop table if exists NO_DICTIONARY_CARBON_7_hive")
    sql("drop table if exists DICTIONARY_CARBON_6")
    sql("drop table if exists DICTIONARY_CARBON_6_hive")
    sql("drop table if exists directDictionaryTable")
    sql("drop table if exists directDictionaryTable_hive")
    sql("drop table if exists NO_DICTIONARY_CARBON")
    sql("drop table if exists NO_DICTIONARY_CARBON_hive")
  }
}
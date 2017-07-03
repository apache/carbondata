
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
 * Test Class for sorttable1 to verify all scenerios
 */

class SORTTABLE1TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_03_drop
test("Sort_Column_03_drop", Include) {
  sql(s"""drop table if exists sorttable1""").collect

  sql(s"""drop table if exists sorttable1_hive""").collect

}
       

//Sort_Column_03
test("Sort_Column_03", Include) {
  sql(s"""CREATE TABLE sorttable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect

  sql(s"""CREATE TABLE sorttable1_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_04
test("Sort_Column_04", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable1_hive """).collect

}
       

//Sort_Column_05
test("Sort_Column_05", Include) {
  sql(s"""select empno from sorttable1""").collect
}
       

//Sort_Column_06
test("Sort_Column_06", Include) {
  sql(s"""select empno from sorttable1 order by empno""").collect
}
       

//Limit_1
test("Limit_1", Include) {
  sql(s"""select empno from sorttable1 limit 1""").collect
}
       

//Limit_2
test("Limit_2", Include) {
  sql(s"""select empno from sorttable1 limit 1000""").collect
}
       

//Limit_3
test("Limit_3", Include) {
  sql(s"""select empno from sorttable1 limit 9999""").collect
}
       

//Limit_4
test("Limit_4", Include) {
  sql(s"""select empno from sorttable1 limit 10000""").collect
}
       

//Limit_5
test("Limit_5", Include) {
  sql(s"""select empno from sorttable1 limit 110000""").collect
}
       

//Limit_6
test("Limit_6", Include) {
  sql(s"""select empname from sorttable1 limit 1""").collect
}
       

//Limit_7
test("Limit_7", Include) {
  sql(s"""select empname from sorttable1 limit 1000""").collect
}
       

//Limit_8
test("Limit_8", Include) {
  sql(s"""select empname from sorttable1 limit 9999""").collect
}
       

//Limit_9
test("Limit_9", Include) {
  sql(s"""select empname from sorttable1 limit 10000""").collect
}
       

//Limit_10
test("Limit_10", Include) {
  sql(s"""select empname from sorttable1 limit 110000""").collect
}
       

//Limit_11
test("Limit_11", Include) {
  sql(s"""select doj from sorttable1 limit 1""").collect
}
       

//Limit_12
test("Limit_12", Include) {
  sql(s"""select doj from sorttable1 limit 1000""").collect
}
       

//Limit_13
test("Limit_13", Include) {
  sql(s"""select doj from sorttable1 limit 9999""").collect
}
       

//Limit_14
test("Limit_14", Include) {
  sql(s"""select doj from sorttable1 limit 10000""").collect
}
       

//Limit_15
test("Limit_15", Include) {
  sql(s"""select doj from sorttable1 limit 110000""").collect
}
       
override def afterAll {
sql("drop table if exists sorttable1")
sql("drop table if exists sorttable1_hive")
}
}
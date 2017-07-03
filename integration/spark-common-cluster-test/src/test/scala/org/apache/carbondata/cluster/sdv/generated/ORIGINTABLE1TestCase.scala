
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
 * Test Class for origintable1 to verify all scenerios
 */

class ORIGINTABLE1TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_93_drop
test("Sort_Column_93_drop", Include) {
  sql(s"""drop table if exists  origintable1""").collect

  sql(s"""drop table if exists  origintable1_hive""").collect

}
       

//Sort_Column_01
test("Sort_Column_01", Include) {
  sql(s"""CREATE TABLE origintable1 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""CREATE TABLE origintable1_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_02
test("Sort_Column_02", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable1 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable1_hive """).collect

}
       

//Sort_Column_14
test("Sort_Column_14", Include) {
  sql(s"""select empname from origintable1""").collect
}
       

//Sort_Column_18
test("Sort_Column_18", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_22
test("Sort_Column_22", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_26
test("Sort_Column_26", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_30
test("Sort_Column_30", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_34
test("Sort_Column_34", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_38
test("Sort_Column_38", Include) {
  sql(s"""select workgroupcategory, empname from origintable1 order by workgroupcategory""").collect
}
       

//Sort_Column_57
test("Sort_Column_57", Include) {
  sql(s"""select * from origintable1 where doj = '1994-07-08 18:50:35.0'""").collect
}
       

//Sort_Column_59
test("Sort_Column_59", Include) {
  sql(s"""select * from origintable1 where empname = 'Yolanda'""").collect
}
       
override def afterAll {
sql("drop table if exists origintable1")
sql("drop table if exists origintable1_hive")
}
}
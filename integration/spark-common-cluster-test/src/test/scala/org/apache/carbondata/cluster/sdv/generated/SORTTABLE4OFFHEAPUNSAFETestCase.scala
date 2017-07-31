
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
 * Test Class for sorttable4offheapunsafe to verify all scenerios
 */

class SORTTABLE4OFFHEAPUNSAFETestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_19_drop
test("Sort_Column_19_drop", Include) {
  sql(s"""drop table if exists sorttable4_offheap_unsafe""").collect

  sql(s"""drop table if exists sorttable4_offheap_unsafe_hive""").collect

}
       

//Sort_Column_19
test("Sort_Column_19", Include) {
  sql(s"""CREATE TABLE sorttable4_offheap_unsafe (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='workgroupcategory, empname')""").collect

  sql(s"""CREATE TABLE sorttable4_offheap_unsafe_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_20
test("Sort_Column_20", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable4_offheap_unsafe OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable4_offheap_unsafe_hive """).collect

}
       

//Sort_Column_21
test("Sort_Column_21", Include) {
  sql(s"""select workgroupcategory, empname from sorttable4_offheap_unsafe""").collect
}
       
override def afterAll {
sql("drop table if exists sorttable4_offheap_unsafe")
sql("drop table if exists sorttable4_offheap_unsafe_hive")
}
}

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
 * Test Class for sorttable3 to verify all scenerios
 */

class SORTTABLE3TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_07_drop
test("Sort_Column_07_drop", Include) {
  sql(s"""drop table if exists sorttable3""").collect

  sql(s"""drop table if exists sorttable3_hive""").collect

}
       

//Sort_Column_07
test("Sort_Column_07", Include) {
  sql(s"""CREATE TABLE sorttable3 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='doj')""").collect

  sql(s"""CREATE TABLE sorttable3_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_08
test("Sort_Column_08", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable3 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable3_hive """).collect

}
       

//Sort_Column_09
test("Sort_Column_09", Include) {
  sql(s"""select doj from sorttable3""").collect
}
       

//Sort_Column_10
test("Sort_Column_10", Include) {
  sql(s"""select doj from sorttable3 order by doj""").collect
}
       
override def afterAll {
sql("drop table if exists sorttable3")
sql("drop table if exists sorttable3_hive")
}
}
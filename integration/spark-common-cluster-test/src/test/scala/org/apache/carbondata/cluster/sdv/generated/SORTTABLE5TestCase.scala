
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
 * Test Class for sorttable5 to verify all scenerios
 */

class SORTTABLE5TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_45_drop
test("Sort_Column_45_drop", Include) {
  sql(s"""drop table if exists sorttable5""").collect

  sql(s"""drop table if exists sorttable5_hive""").collect

}
       

//Sort_Column_45
test("Sort_Column_45", Include) {
  sql(s"""CREATE TABLE sorttable5 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format' tblproperties('sort_columns'='empno')""").collect

  sql(s"""CREATE TABLE sorttable5_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_46
test("Sort_Column_46", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5_hive """).collect

}
       

//Sort_Column_47
test("Sort_Column_47", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5_hive """).collect

}
       

//Sort_Column_48
test("Sort_Column_48", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5_hive """).collect

}
       

//Sort_Column_49
test("Sort_Column_49", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE sorttable5_hive """).collect

}
       

//Sort_Column_51
test("Sort_Column_51", Include) {
  sql(s"""select empno from sorttable5""").collect
}
       
override def afterAll {
sql("drop table if exists sorttable5")
sql("drop table if exists sorttable5_hive")
}
}
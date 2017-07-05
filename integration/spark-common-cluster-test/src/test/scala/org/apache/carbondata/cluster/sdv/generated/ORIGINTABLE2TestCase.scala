
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
 * Test Class for origintable2 to verify all scenerios
 */

class ORIGINTABLE2TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sort_Column_39_drop
test("Sort_Column_39_drop", Include) {
  sql(s"""drop table if exists origintable2""").collect

  sql(s"""drop table if exists origintable2_hive""").collect

}
       

//Sort_Column_39
test("Sort_Column_39", Include) {
  sql(s"""CREATE TABLE origintable2 (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""CREATE TABLE origintable2_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sort_Column_40
test("Sort_Column_40", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2_hive """).collect

}
       

//Sort_Column_41
test("Sort_Column_41", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2_hive """).collect

}
       

//Sort_Column_42
test("Sort_Column_42", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2_hive """).collect

}
       

//Sort_Column_43
test("Sort_Column_43", Include) {
  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2 OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""").collect

  sql(s"""LOAD DATA  inpath '$resourcesPath/Data/newdata.csv' INTO TABLE origintable2_hive """).collect

}
       

//Sort_Column_52
test("Sort_Column_52", Include) {
  checkAnswer(s"""select empno from origintable2 order by empno limit 10""",
    s"""select empno from origintable2_hive order by empno limit 10""")
}
       
override def afterAll {
sql("drop table if exists origintable2")
sql("drop table if exists origintable2_hive")
}
}
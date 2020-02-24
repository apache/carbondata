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

package org.apache.carbondata.spark.testsuite.joinquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for join query with orderby and limit
 */

class OrderByLimitTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql(
      "CREATE TABLE carbon1 (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon1 OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin);

    sql(
      "CREATE TABLE carbon2 (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbon2 OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin);

    sql(
      "CREATE TABLE carbon1_hive (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) row format delimited fields terminated by ','")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE carbon1_hive ")

    sql(
      "CREATE TABLE carbon2_hive (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) row format delimited fields terminated by ','")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE carbon2_hive ");


  }

  test("test join with orderby limit") {
    checkAnswer(
      sql(
        "select a.empno,a.empname,a.workgroupcategoryname from carbon1 a full outer join carbon2 " +
        "b on substr(a.workgroupcategoryname," +
        "1,3)" +
        "=substr(b.workgroupcategoryname,1,3) order by a.empname limit 5"),
      sql(
        "select a.empno,a.empname,a.workgroupcategoryname from carbon1_hive a full outer join " +
        "carbon2_hive b on " +
        "substr(a" +
        ".workgroupcategoryname,1,3)=substr(b.workgroupcategoryname,1,3) order by a.empname limit" +
        " 5")
    )
  }

  override def afterAll {
    sql("drop table carbon1")
    sql("drop table carbon2")
    sql("drop table carbon1_hive")
    sql("drop table carbon2_hive")
  }
}

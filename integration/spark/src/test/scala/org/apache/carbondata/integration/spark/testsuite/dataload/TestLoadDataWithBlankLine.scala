/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.Row

/**
  * Test Class for data loading when there are blank lines in data
  *
  */
class TestLoadDataWithBlankLine extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists carbontable")
    sql("CREATE TABLE carbontable (empno int, empname String, designation String, " +
      "doj String, workgroupcategory int, workgroupcategoryname String, deptno int, " +
      "deptname String, projectcode int, projectjoindate String, projectenddate String, " +
      "attendance int,utilization int,salary int) " +
        "STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/datawithblanklines.csv' INTO TABLE" +
        " carbontable OPTIONS('DELIMITER'= ',')")

    sql("drop table if exists carbontable2")
    sql("CREATE TABLE carbontable2 (empno int, empname String, designation String, " +
      "doj String, workgroupcategory int, workgroupcategoryname String, deptno int, " +
      "deptname String, projectcode int, projectjoindate String, projectenddate String, " +
      "attendance int,utilization int,salary int) " +
      "STORED BY 'org.apache.carbondata.format'")
  }
  test("test carbon table data loading when there are  blank lines in data") {
    checkAnswer(sql("select count(*) from carbontable"),
      Seq(Row(18)))
  }

  test("test carbon table data loading when the first line is blank") {
    sql("LOAD DATA LOCAL INPATH './src/test/resources/dataWithNullFirstLine.csv' INTO TABLE " +
      "carbontable2 OPTIONS('DELIMITER'= ',','FILEHEADER'='empno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,salary')")

    checkAnswer(sql("select count(*) from carbontable2"),
      Seq(Row(11)))
  }

  override def afterAll {
    sql("drop table if exists carbontable")
    sql("drop table if exists carbontable2")
  }
}

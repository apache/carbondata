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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading with hive syntax and old syntax
 *
 */
class MultiFilesDataLoagdingTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("create table multifile(empno int, empname String, designation string, doj String," +
      "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String," +
      "projectcode int, projectjoindate String,projectenddate String, attendance double," +
      "utilization double,salary double) STORED AS carbondata")
  }

  test("test data loading for multi files and nested folder") {
    val testData = s"$resourcesPath/loadMultiFiles"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table multifile")
    checkAnswer(
      sql("select count(empno) from multifile"),
      Seq(Row(10))
    )
  }

  override def afterAll {
    sql("drop table multifile")
  }
}

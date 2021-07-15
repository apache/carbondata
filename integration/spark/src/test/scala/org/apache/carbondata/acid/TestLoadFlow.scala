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

package org.apache.carbondata.acid

import java.util

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadFlow extends QueryTest with BeforeAndAfterAll {

  val dbName = "ajantha"

  override protected def beforeAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    sql(s"CREATE DATABASE $dbName")
    sql(s"USE $dbName")
  }

  override protected def afterAll(): Unit = {
    sql(s"use default")
    sql(s"DROP DATABASE $dbName CASCADE")
  }

  test("Test load flow transaction") {
    val tableName = "testajtable"
    sql(s"DROP TABLE IF EXISTS $tableName")
    sql(s"CREATE TABLE $tableName(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int,utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE $tableName")
    sql(s"select * from $tableName").show(200, false)
    /*sql(s"update $tableName set(empname) = ('hill') where empname = 'bill' ")
    sql(s"select * from $tableName").show(200, false)
    sql(s"update $tableName set(empname) = ('jibi') where empname = 'sibi' ")*/
    sql(s"DROP TABLE $tableName").show(false)
  }

}

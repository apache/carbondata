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

package org.apache.spark.sql

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.schema.CarbonGetTableDetailCommand
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CarbonGetTableDetailCommandTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    sql("drop table if exists table_info1")
    sql("create table table_info1 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) stored by 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')")
    sql(s"""load data local inpath '$resourcesPath/data.csv' into table table_info1 options('delimiter'=',', 'quotechar'='\"', 'fileheader'='')""")

    sql("drop table if exists table_info2")
    sql("create table table_info2 (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) stored by 'org.apache.carbondata.format' tblproperties('local_dictionary_enable'='false')")
    sql(s"""load data local inpath '$resourcesPath/data.csv' into table table_info2 options('delimiter'=',', 'quotechar'='\"', 'fileheader'='')""")
  }

  test("collect the information of tables") {
    val logicalPlan = CarbonGetTableDetailCommand("default", Some(Seq("table_info1", "table_info2")))
    val result =new QueryExecution(sqlContext.sparkSession, logicalPlan)
      .executedPlan
      .execute
      .collect

    assertResult(2)(result.length)
    assertResult("table_info1")(result(0).getString(0))
    // 2221 is the size of carbon table. Note that since 1.5.0, we add additional compressor name in metadata
    assertResult(2221)(result(0).getLong(1))
    assertResult("table_info2")(result(1).getString(0))
    assertResult(2221)(result(1).getLong(1))
  }

  override def afterAll: Unit = {
    sql("drop table if exists table_info1")
    sql("drop table if exists table_info2")
  }
}

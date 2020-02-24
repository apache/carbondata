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
package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class ColumnPropertyValidationTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("""drop table if exists employee""")
  }

  test("Validate ColumnProperties_ valid key") {
     try {
       sql("create table employee(empname String,empid String,city String,country String,gender String,salary Double) STORED AS carbondata tblproperties('columnproperties.gender.key'='value')")
       assert(true)
       sql("drop table employee")
     } catch {
       case e: Throwable =>assert(false)
     }
  }
  test("Validate Dictionary include _ invalid key") {
    intercept[Throwable] {
      sql(
        s"""
           | create table employee(
           |    empname String,
           |    empid String,
           |    city String,
           |    country String,
           |    gender String,
           |    salary Double)
           | STORED AS carbondata
           | tblproperties('columnproperties.invalid.key'='value')
         """.stripMargin)
    }
  }

  override def afterAll() {
    sql("drop table if exists employee")
  }
  
}

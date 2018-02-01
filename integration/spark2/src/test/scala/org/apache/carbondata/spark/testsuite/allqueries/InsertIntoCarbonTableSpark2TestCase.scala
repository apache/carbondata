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
package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class InsertIntoCarbonTableSpark2TestCase extends Spark2QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists OneRowTable")
  }

  test("insert select one row") {
    sql("create table OneRowTable(col1 string, col2 string, col3 int, col4 double) stored by 'carbondata'")
    sql("insert into OneRowTable select '0.1', 'a.b', 1, 1.2")
    checkAnswer(sql("select * from OneRowTable"), Seq(Row("0.1", "a.b", 1, 1.2)))
  }

  test("insert double datatype with required precision ") {
    sql("Drop table if exists t1 ")
    sql("create table t1(id int, col1 Decimal(3,2), col2 string) stored by 'carbondata'")
    sql("insert into t1 values(1, '2', 'ab')")
    sql("insert into t1 values(2, '2.1', 'ab')")
    sql("insert into t1 values(3, '2.11', 'ab')")
    sql("insert into t1 values(4, '2.119', 'ab')")
    checkAnswer(sql("select * from t1 where id=4 "), Seq(Row(4, 2.12, "ab")))
  }

  override def afterAll {
    sql("drop table if exists OneRowTable")
  }
}

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
package org.apache.carbondata.spark.testsuite.iud


import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class DeleteRepeatedTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("use default")
    sql("drop database if exists dr_db cascade")
    sql("create database dr_db")
    sql("use dr_db")
    sql("create table t1 (col1 string, col2 int) STORED BY 'org.apache.carbondata.format'")
    sql("insert into t1 (select 'a', 1 union all select 'e', 14 ) ").collect()
    sql("insert into t1 (select 'b', 2 union all select 'b', 15 ) ").collect()
    sql("insert into t1 (select 'e', 3 union all select 'a', 16 )").collect()
    sql("insert into t1 (select 'c', 3 union all select 'b', 17 )").collect()
    sql("insert into t1 (select 'e', 3 union all select 'f', 18 )").collect()
  }

  test("test deduplicate ") {
    checkAnswer(sql("select count(*) from t1"), Seq(Row(10)))
    sql(
      """
        | delete repeated col1
        | from t1
        | where new.segment.id between 3 and 4
        | and old.segment.id between 0 and 2
      """.stripMargin)
    checkAnswer(sql("select count(*) from t1"), Seq(Row(8)))
    checkAnswer(sql("select count(*) from t1 where col2 = 17"), Seq(Row(0)))

    sql(
      """
        | delete repeated col1
        | from t1
        | where new.segment.id between 2 and 2
      """.stripMargin)
    checkAnswer(sql("select count(*) from t1"), Seq(Row(6)))
    checkAnswer(sql("select count(*) from t1 where col2 = 16"), Seq(Row(0)))
  }

  test("test deduplicate failure case") {
    val exception1 = intercept[MalformedCarbonCommandException](
      sql(
        """
          | delete repeated col1
          | from t1
          | where new.segment.id between 3 and 4
          | and new.segment.id between 0 and 2
        """.stripMargin))
    assert(exception1.getMessage.contains("not found the range of old.segment.id"))

    val exception2 = intercept[MalformedCarbonCommandException](
      sql(
        """
          | delete repeated col1
          | from t1
          | where old.segment.id between 3 and 4
          | and old.segment.id between 0 and 2
        """.stripMargin))
    assert(exception2.getMessage.contains("not found the range of new.segment.id"))

    val exception3 = intercept[MalformedCarbonCommandException](
      sql(
        """
          | delete repeated col1
          | from t1
          | where old.segment.id between 3 and 4
        """.stripMargin))
    assert(exception3.getMessage.contains("not found the range of new.segment.id"))
  }

  override def afterAll {
     sql("use default")
     sql("drop database if exists dr_db cascade")
  }
}
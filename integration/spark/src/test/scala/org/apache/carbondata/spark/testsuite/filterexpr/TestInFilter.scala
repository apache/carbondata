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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.ThreadLocalSessionInfo

class TestInFilter extends QueryTest with BeforeAndAfterAll{

  override def beforeAll: Unit = {
    sql("drop table if exists test_table")
    sql("create table test_table(intField INT, floatField FLOAT, doubleField DOUBLE, " +
        "decimalField DECIMAL(18,2))  STORED AS carbondata")

    // turn on  row level filter in carbon
    // because only row level is on, 'in' will be pushdowned into CarbonScanRDD
    //  or in filter will be handled by spark.
    sql("set carbon.push.rowfilters.for.vector=true")
    sql("insert into test_table values(8,8,8,8),(5,5.0,5.0,5.0),(4,1.00,2.00,3.00)," +
        "(6,6.0000,6.0000,6.0000),(4743,4743.00,4743.0000,4743.0),(null,null,null,null)")
  }

  test("sql with in different measurement type") {
    // the precision of filter value is less one digit than column value
    // float type test
    checkAnswer(
      sql("select * from test_table where floatField in(1.0)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where floatField in(4743.0)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where floatField in(5)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where floatField in(6.000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // double type test
    checkAnswer(
      sql("select * from test_table where doubleField in(2.0)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where doubleField in(4743.000)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(5)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(6.000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // decimalField type test
    checkAnswer(
      sql("select * from test_table where decimalField in(3.0)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where decimalField in(4743)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where decimalField in(5)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where decimalField in(6.000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // the precision of filter value is more one digit than column value
    // int type test
    checkAnswer(
      sql("select * from test_table where intField in(4.0)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where intField in(4743.0)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where intField in(5.0)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where intField in(6.0)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // float type test
    checkAnswer(
      sql("select * from test_table where floatField in(1.000)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where floatField in(4743.000)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where floatField in(5.00)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where floatField in(6.00000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // double type test
    checkAnswer(
      sql("select * from test_table where doubleField in(2.000)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where doubleField in(4743.00000)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(5.00)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(6.00000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // decimalField type test
    checkAnswer(
      sql("select * from test_table where decimalField in(3.000)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
    checkAnswer(
      sql("select * from test_table where decimalField in(4743.00)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where decimalField in(5.00)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where decimalField in(6.00000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))

    // case: filter value is null
    checkAnswer(
      sql("select * from test_table where decimalField is null"),
      Seq(Row(null, null, null, null)))

    // filter value and column 's precision are the same
    checkAnswer(
      sql("select * from test_table where doubleField in(5.0) " +
          "and floatField in(5.0) and decimalField in(5.0) and intField in(5)"),
      Seq(Row(5, 5.0, 5.0, 5.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(6.0000) " +
          "and floatField in(6.0000) and decimalField in(6.0000) and intField in(6.0000)"),
      Seq(Row(6, 6.0000, 6.0000, 6.0000)))
    checkAnswer(
      sql("select * from test_table where doubleField in(8) " +
          "and floatField in(8) and decimalField in(8) and intField in(8)"),
      Seq(Row(8, 8, 8, 8)))
    checkAnswer(
      sql("select * from test_table where doubleField in(4743.0000) " +
          "and floatField in(4743.00) and decimalField in(4743.0) and intField in(4743)"),
      Seq(Row(4743, 4743.00, 4743.0000, 4743.0)))
    checkAnswer(
      sql("select * from test_table where doubleField in(2.00) " +
          "and floatField in(1.00) and decimalField in(3.00) and intField in(4)"),
      Seq(Row(4, 1.00, 2.00, 3.00)))
  }

  override def afterAll(): Unit = {
    sql("drop table if exists test_table")
    sql("set carbon.push.rowfilters.for.vector=false")
    defaultConfig()
  }

}

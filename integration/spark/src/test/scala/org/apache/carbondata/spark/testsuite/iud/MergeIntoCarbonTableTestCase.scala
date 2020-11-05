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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class MergeIntoCarbonTableTestCase extends QueryTest with BeforeAndAfterEach {
  var df: DataFrame = _

  override def beforeEach {
    dropTable()
    buildTestData()
  }

  private def buildTestData(): Unit = {
    createTable()
  }

  private def createTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS A(
         |   id Int,
         |   price Int,
         |   state String
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS B(
         |   id Int,
         |   price Int,
         |   state String
         | )
         | STORED AS carbondata
       """.stripMargin)

    sql(s"""INSERT INTO A VALUES (1,100,"MA")""")
    sql(s"""INSERT INTO A VALUES (2,200,"NY")""")
    sql(s"""INSERT INTO A VALUES (3,300,"NH")""")
    sql(s"""INSERT INTO A VALUES (4,400,"FL")""")

    sql(s"""INSERT INTO B VALUES (1,1,"MA (updated)")""")
    sql(s"""INSERT INTO B VALUES (2,3,"NY (updated)")""")
    sql(s"""INSERT INTO B VALUES (3,3,"CA (updated)")""")
    sql(s"""INSERT INTO B VALUES (5,5,"TX (updated)")""")
    sql(s"""INSERT INTO B VALUES (7,7,"LO (updated)")""")
  }

  private def dropTable() = {
    sql("DROP TABLE IF EXISTS A")
    sql("DROP TABLE IF EXISTS B")
  }

  test("test merge into delete") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED THEN DELETE""".stripMargin)

    checkAnswer(sql("select * from A"), Seq(Row(4, 400, "FL")))
  }

  test("test merge into delete and update") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN DELETE
        |WHEN MATCHED AND A.ID=1 THEN UPDATE SET *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 1, "MA (updated)"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL")))
  }

  test("test merge into update with expression") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN DELETE
        |WHEN MATCHED AND A.ID=1 THEN UPDATE SET  A.id=9, A.price=100, A.state='hahaha'
        |""".stripMargin)

    sql("select * from A").show(100, false)

    checkAnswer(sql("select * from A"),
      Seq(Row(9, 100, "hahaha"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL")))
  }

  test("test merge into delete and insert") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN DELETE
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(5, 5, "TX (updated)"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into delete and update and insert") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN DELETE
        |WHEN MATCHED AND A.ID=1 THEN UPDATE  SET *
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 1, "MA (updated)"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(5, 5, "TX (updated)"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into update and insert") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=1 THEN UPDATE  SET *
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 1, "MA (updated)"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(5, 5, "TX (updated)"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into delete with condition") {
    sql(
      """MERGE INTO A
        |USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND B.ID=2 THEN DELETE""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"), Row(3, 300, "NH"), Row(4, 400, "FL")))
  }

  test("test merge into update all cols") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN MATCHED THEN UPDATE SET *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 1, "MA (updated)"),
        Row(2, 3, "NY (updated)"),
        Row(3, 3, "CA (updated)"),
        Row(4, 400, "FL")))
  }

  test("test merge into update all cols with condition") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN UPDATE SET *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"), Row(2, 3, "NY (updated)"), Row(3, 300, "NH"), Row(4, 400, "FL")))
  }

  test("test merge into update all cols with multiple condition") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN MATCHED AND A.ID=2 THEN UPDATE SET *
        |WHEN MATCHED AND A.ID=3 THEN UPDATE SET *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 3, "NY (updated)"),
        Row(3, 3, "CA (updated)"),
        Row(4, 400, "FL")))
  }

  test("test merge into insert all cols") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin)

    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(5, 5, "TX (updated)"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into insert all cols with condition") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN NOT MATCHED AND B.ID=7 THEN INSERT *""".stripMargin)
    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into insert all cols with multiple condition") {
    sql(
      """MERGE INTO A USING B
        |ON A.ID=B.ID
        |WHEN NOT MATCHED AND B.ID=5 THEN INSERT *
        |WHEN NOT MATCHED AND B.ID=7 THEN INSERT *""".stripMargin)
    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(5, 5, "TX (updated)"),
        Row(7, 7, "LO (updated)")))
  }

  test("test merge into insert with literal") {
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED " +
      "AND B.ID=7 THEN INSERT (A" +
      ".ID,A.PRICE, A.state) VALUES (B.ID,B.PRICE, 'test-string')"
    sql(sqlText)
    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(7, 7, "test-string")))
  }

  test("test merge into insert with expression") {
    val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED " +
      "AND B.ID=7 THEN INSERT (A" +
      ".ID,A.PRICE, A.state) VALUES (B.ID,B.PRICE + 10, B.state)"
    sql(sqlText)
    checkAnswer(sql("select * from A"),
      Seq(Row(1, 100, "MA"),
        Row(2, 200, "NY"),
        Row(3, 300, "NH"),
        Row(4, 400, "FL"),
        Row(7, 17, "LO (updated)")))
  }

  test("test merge into not exist table exception") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      val sqlText = "MERGE INTO A USING C ON A.ID=C.ID WHEN NOT MATCHED " +
        "AND C.ID=7 THEN INSERT (A" +
        ".ID,A.PRICE, A.state) VALUES (C.ID,C.PRICE + 10, C.state)"
      sql(sqlText)
    }
    assert(exceptionCaught.getMessage.contains("table default.C not found"))
  }

  test("test merge into parse exception for size of action columns and expression") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      val sqlText = "MERGE INTO A USING B ON A.ID=B.ID WHEN NOT MATCHED " +
        "AND B.ID=7 THEN INSERT (A" +
        ".ID,A.PRICE, A.state) VALUES (B.ID,B.PRICE + 10)"
      sql(sqlText)
    }
    assert(exceptionCaught.getMessage.contains("Parse failed: " +
      "size of columns is not equal to size of expression in not matched action"))
  }

  test("test merge into parse exception for tree node parsing") {
    val exceptionCaught = intercept[MalformedCarbonCommandException] {
      val sqlText = "MERGE INTO A USING  ON A.ID=B.ID WHEN NOT MATCHED " +
        "AND B.ID=7 THEN INSERT (A" +
        ".ID,A.PRICE, A.state) VALUES (B.ID,B.PRICE + 10, B.state)"
      sql(sqlText)
    }
    assert(exceptionCaught.getMessage.contains("Parse failed"))
  }
}

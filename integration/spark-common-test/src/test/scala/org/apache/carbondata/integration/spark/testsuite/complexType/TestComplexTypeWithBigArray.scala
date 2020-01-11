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

package org.apache.carbondata.integration.spark.testsuite.complexType

import java.io.{File, FileOutputStream, PrintStream}

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestComplexTypeWithBigArray extends QueryTest with BeforeAndAfterAll {

  val filePath = "./list.csv"
  val file = new File(filePath)

  override def beforeAll: Unit = {
    // write a CSV containing 32000 row, each row has an array with 10 elements
    val out = new PrintStream(new FileOutputStream(file))
    (1 to 33000).foreach(i=>out.println(s"$i,$i\0011"))
    out.close()
  }

  test("test with big string array") {
    sql("DROP TABLE IF EXISTS big_array")
    sql(
      """
        | CREATE TABLE big_array(
        |  value BIGINT,
        |  list ARRAY<STRING>
        |  )
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)
    checkAnswer(
      sql("select count(*) from big_array"),
      Row(33000)
    )
    checkAnswer(
      sql("select * from big_array limit 1"),
      Row(1, mutable.WrappedArray.make[String](Array("1", "1")))
    )
    checkAnswer(
      sql("select list[1] from big_array limit 1"),
      Row("1")
    )
    checkAnswer(
      sql("select count(*) from big_array where list[0] = '1'"),
      Row(1)
    )
    checkAnswer(
      sql("select count(*) from big_array where array_contains(list, '1') "),
      Row(33000)
    )
    if (sqlContext.sparkContext.version.startsWith("2.")) {
      // explode UDF is supported start from spark 2.0
      checkAnswer(
        sql("select count(x) from (select explode(list) as x from big_array)"),
        Row(66000)
      )
    }
    checkAnswer(
      sql("select * from big_array where value = 15000"),
      Row(15000, mutable.WrappedArray.make[String](Array("15000", "1")))
    )
    checkAnswer(
      sql("select * from big_array where value = 32500"),
      Row(32500, mutable.WrappedArray.make[String](Array("32500", "1")))
    )
    checkAnswer(
      sql("select count(list) from big_array"),
      Row(33000)
    )
    sql("DROP TABLE big_array")
  }

  test("test with big int array") {
    sql("DROP TABLE IF EXISTS big_array")
    sql(
      """
        | CREATE TABLE big_array(
        |  value BIGINT,
        |  list ARRAY<INT>
        |  )
        | STORED AS carbondata
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)
    checkAnswer(
      sql("select count(*) from big_array"),
      Row(33000)
    )
    checkAnswer(
      sql("select * from big_array limit 1"),
      Row(1, mutable.WrappedArray.make[String](Array(1, 1)))
    )
    checkAnswer(
      sql("select list[1] from big_array limit 1"),
      Row(1)
    )
    checkAnswer(
      sql("select count(*) from big_array where list[0] = 1"),
      Row(1)
    )
    checkAnswer(
      sql("select count(*) from big_array where array_contains(list, 1) "),
      Row(33000)
    )
    if (sqlContext.sparkContext.version.startsWith("2.")) {
      // explode UDF is supported start from spark 2.0
      checkAnswer(
        sql("select count(x) from (select explode(list) as x from big_array)"),
        Row(66000)
      )
    }
    checkAnswer(
      sql("select * from big_array where value = 15000"),
      Row(15000, mutable.WrappedArray.make[Int](Array(15000, 1)))
    )
    checkAnswer(
      sql("select * from big_array where value = 32500"),
      Row(32500, mutable.WrappedArray.make[Int](Array(32500, 1)))
    )
    checkAnswer(
      sql("select count(list) from big_array"),
      Row(33000)
    )
    sql("DROP TABLE big_array")
  }

  override def afterAll: Unit = {
    file.delete()
  }

}

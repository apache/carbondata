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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestComplexTypeCompaction extends QueryTest with BeforeAndAfterAll {

  val filePath = "./list.csv"
  val file = new File(filePath)

  override def beforeAll: Unit = {
    // write a CSV containing 32000 row, each row has an array<string>
    val out = new PrintStream(new FileOutputStream(file))
    (1 to 33000).foreach(i=>out.println(s"$i$$1"))
    out.close()
  }

  test("complex data type and no compaction: success") {
    sql("DROP TABLE IF EXISTS big_array")
    sql(
      """
        | CREATE TABLE big_array(
        |  list ARRAY<STRING>
        |  )
        | STORED BY 'carbondata'
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)

    checkAnswer(
      sql("select count(*) from big_array"),
      Row(66000)
    )

    sql("DROP TABLE big_array")
  }
  test("complex data type and compaction: fail") {

    sql("DROP TABLE IF EXISTS big_array")
    sql(
      """
        | CREATE TABLE big_array(
        |  list ARRAY<STRING>
        |  )
        | STORED BY 'carbondata'
      """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '${file.getAbsolutePath}'
         | INTO TABLE big_array
         | OPTIONS ('header'='false')
      """.stripMargin)

    checkAnswer(
      sql("select count(*) from big_array"),
      Row(66000)
    )

    val exception_compaction: Exception = intercept[Exception] {
      sql("alter table big_array compact 'major'")
    }
    assert(exception_compaction.isInstanceOf[Exception] )
    sql("DROP TABLE big_array")
  }

  override def afterAll: Unit = {
    file.delete()
  }

}

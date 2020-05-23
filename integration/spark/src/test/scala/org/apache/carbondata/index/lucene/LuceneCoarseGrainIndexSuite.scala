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

package org.apache.carbondata.index.lucene

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

/**
 * Ignored test class as CG index is not supported yet
 */
@Ignore
class LuceneCoarseGrainIndexSuite extends QueryTest with BeforeAndAfterAll {

  val file2 = resourcesPath + "/index_input.csv"

  override protected def beforeAll(): Unit = {
    //n should be about 5000000 of reset if size is default 1024
    val n = 15000
    LuceneFineGrainIndexSuite.createFile(file2, n * 4, n)
    sql("DROP TABLE IF EXISTS normal_test")
    sql(
      """
        | CREATE TABLE normal_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE normal_test OPTIONS('header'='false')")
  }

  test("test lucene coarse grain index") {
    sql("DROP TABLE IF EXISTS index_test")
    sql(
      """
        | CREATE TABLE index_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)

    sql(
      s"""
         | CREATE INDEX dm
         | ON index_test (name, city)
         | AS 'lucene'
      """.stripMargin)

    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE index_test OPTIONS('header'='false')")

    checkAnswer(sql("select * from index_test where name='n502670'"),
      sql("select * from normal_test where name='n502670'"))
  }

  override protected def afterAll(): Unit = {
    LuceneFineGrainIndexSuite.deleteFile(file2)
    sql("DROP TABLE IF EXISTS normal_test")
    sql("DROP TABLE IF EXISTS index_test")
  }

}

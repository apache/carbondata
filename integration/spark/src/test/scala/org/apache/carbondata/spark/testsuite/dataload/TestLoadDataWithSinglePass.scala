/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for data loading use one pass
  *
  */
class TestLoadDataWithSinglePass extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS table_two_pass")
    sql("DROP TABLE IF EXISTS table_one_pass")
    sql("DROP TABLE IF EXISTS table_one_pass_2")

    sql(
      """
        |CREATE TABLE table_two_pass (ID int, date Timestamp, country String,
        |name String, phonetype String, serialname String, salary int)
        |STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      """
        |LOAD DATA local inpath './src/test/resources/dataDiff.csv' INTO TABLE table_two_pass
        |OPTIONS('DELIMITER'= ',', 'USE_KETTLE'='false', 'SINGLE_PASS'='false')
      """.stripMargin)

    sql(
      """
        |CREATE TABLE table_one_pass (ID int, date Timestamp, country String,
        |name String, phonetype String, serialname String, salary int)
        |STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    sql(
      """
        |LOAD DATA local inpath './src/test/resources/dataDiff.csv' INTO TABLE table_one_pass
        |OPTIONS('DELIMITER'= ',', 'USE_KETTLE'='false', 'SINGLE_PASS'='true')
      """.stripMargin)
  }

  test("test data loading use one pass") {
    checkAnswer(
      sql("select * from table_one_pass"),
      sql("select * from table_two_pass")
    )
  }

  test("test data loading use one pass when offer column dictionary file") {
    sql(
      """
        |CREATE TABLE table_one_pass_2 (ID int, date Timestamp, country String,
        |name String, phonetype String, serialname String, salary int)
        |STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      """
        |LOAD DATA local inpath './src/test/resources/dataDiff.csv' INTO TABLE table_one_pass_2
        |OPTIONS('DELIMITER'= ',', 'USE_KETTLE'='false', 'SINGLE_PASS'='true', 'COLUMNDICT'=
        |'country:./src/test/resources/columndictionary/country.csv, name:./src/test/resources/columndictionary/name.csv')
      """.stripMargin)

    checkAnswer(
      sql("select * from table_one_pass_2"),
      sql("select * from table_two_pass")
    )
  }

  test("test data loading use one pass when do incremental load") {
    sql(
      """
        |LOAD DATA local inpath './src/test/resources/dataIncrement.csv' INTO TABLE table_two_pass
        |OPTIONS('DELIMITER'= ',', 'USE_KETTLE'='false', 'SINGLE_PASS'='false')
      """.stripMargin)
    sql(
      """
        |LOAD DATA local inpath './src/test/resources/dataIncrement.csv' INTO TABLE table_one_pass
        |OPTIONS('DELIMITER'= ',', 'USE_KETTLE'='false', 'SINGLE_PASS'='true')
      """.stripMargin)

    checkAnswer(
      sql("select * from table_one_pass"),
      sql("select * from table_two_pass")
    )
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS table_two_pass")
    sql("DROP TABLE IF EXISTS table_one_pass")
    sql("DROP TABLE IF EXISTS table_one_pass_2")
  }
}

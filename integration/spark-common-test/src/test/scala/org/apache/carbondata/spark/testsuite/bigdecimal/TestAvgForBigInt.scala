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

package org.apache.carbondata.spark.testsuite.bigdecimal

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestAvgForBigInt extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbonTable")
    val csvFilePath = s"$resourcesPath/bigIntData.csv"

    sql(
      """
      CREATE TABLE IF NOT EXISTS carbonTable (ID Int, date Timestamp, country String,
      name String, phonetype String, serialname String, salary bigint)
      STORED AS carbondata
      """
    )

    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table carbonTable"
    )
  }

  test("test avg function on big int column") {
    checkAnswer(
      sql("select avg(salary) from carbonTable"),
      sql("select sum(salary)/count(salary) from carbonTable")
    )
  }

  override def afterAll {
    sql("drop table if exists carbonTable")
  }
}

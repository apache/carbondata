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

package org.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for no inverted index load and query
 *
 */

class TestNoInvertedIndexLoadAndQuery extends QueryTest with BeforeAndAfterAll{

  test("no inverted index load and query") {

    sql("DROP TABLE IF EXISTS index")

    sql("""
           CREATE TABLE IF NOT EXISTS index
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('NO_INVERTED_INDEX'='country,name')
           """)

    sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/invertedIndex.csv' into table index
           """)
    checkAnswer(
      sql("""
           SELECT country, count(salary) AS amount
           FROM index
           WHERE country IN ('china','france')
           GROUP BY country
          """),
      Seq(Row("france", 16), Row("china", 74)))

    sql("DROP TABLE index")
  }

}

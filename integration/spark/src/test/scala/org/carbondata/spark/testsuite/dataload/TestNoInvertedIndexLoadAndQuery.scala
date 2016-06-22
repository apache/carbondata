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

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for no inverted index load and query
 *
 */

class TestNoInvertedIndexLoadAndQuery extends QueryTest with BeforeAndAfterAll{

  def currentPath: String = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath
  val testData = new File(currentPath + "/../../examples/src/main/resources/dimSample.csv")
    .getCanonicalPath
  override def beforeAll {
    sql("DROP TABLE IF EXISTS index")
  }
  test("no inverted index load and query") {

    sql("""
           CREATE TABLE IF NOT EXISTS index
           (id Int, name String, city String)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('NO_INVERTED_INDEX'='name,city')
           """)
    sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table index
           """)
    checkAnswer(
      sql("""
           SELECT * FROM index WHERE city = "Bangalore"
          """),
      Seq(Row(19, "Emily", "Bangalore")))

    sql("DROP TABLE index")
  }

}

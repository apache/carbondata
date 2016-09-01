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

package org.apache.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading when there is single quote in fact data
 *
 */
class TestLoadDataWithSingleQuotechar extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("DROP TABLE IF EXISTS carbontable")
    sql(
      "CREATE TABLE carbontable (id Int, name String) STORED BY 'carbondata'")
  }

  test("test data loading with single quote char") {
    try {
      sql(
        "LOAD DATA LOCAL INPATH './src/test/resources/dataWithSingleQuote.csv' INTO TABLE " +
          "carbontable OPTIONS('DELIMITER'= ',')")
      checkAnswer(
        sql("SELECT * from carbontable"),
        Seq(Row("Tom",1),
          Row("Tony\n3,Lily",2),
          Row("Games\"",4),
          Row("prival\"\n6,\"hello\"",5)
        )
      )
    } catch {
      case e: Throwable =>
        assert(false)
    }
  }

  override def afterAll {
    sql("DROP TABLE carbontable")
  }
}

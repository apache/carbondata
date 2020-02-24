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

package org.apache.carbondata.spark.testsuite.dataload

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestLoadDataWithMalformedCarbonCommandException extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("CREATE table TestLoadTableOptions (ID int, date String, country String, name String," +
        "phonetype String, serialname String, salary int) STORED AS carbondata")

  }

  override def afterAll {
    sql("drop table TestLoadTableOptions")
  }

  test("test load data with invalid option") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
        "TestLoadTableOptions OPTIONS('QUOTECHAR'='\"', 'DELIMITERRR' =  ',')")
    }
    assert(e.getMessage.equals("Error: Invalid option(s): delimiterrr"))
  }

  test("test load data with duplicate options") {
    val e = intercept[MalformedCarbonCommandException] {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE " +
        "TestLoadTableOptions OPTIONS('DELIMITER' =  ',', 'quotechar'='\"', 'DELIMITER' =  '$')")
    }
    assert(e.getMessage.equals("Error: Duplicate option(s): delimiter"))
  }

  test("test load data with case sensitive options") {
    try {
      sql(
        s"LOAD DATA local inpath '$resourcesPath/dataretention1.csv' INTO table " +
          "TestLoadTableOptions options('DeLIMITEr'=',', 'qUOtECHAR'='\"')"
      )
    } catch {
      case _: Throwable => assert(false)
    }
  }

}

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

package org.apache.spark.carbondata

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TestStreamingTableOperation extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("DROP DATABASE IF EXISTS streaming CASCADE")
    sql("CREATE DATABASE streaming")
    sql("USE streaming")
    sql(
      """
        | CREATE TABLE source(
        |    c1 string,
        |    c2 int,
        |    c3 string,
        |    c5 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'true')
      """.stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO TABLE source""")
  }

  test("validate streaming property") {
    sql(
      """
        | CREATE TABLE correct(
        |    c1 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'true')
      """.stripMargin)
    sql("DROP TABLE correct")
    sql(
      """
        | CREATE TABLE correct(
        |    c1 string
        | ) STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES ('streaming' = 'false')
      """.stripMargin)
    sql("DROP TABLE correct")
    intercept[MalformedCarbonCommandException] {
      sql(
        """
          | create table wrong(
          |    c1 string
          | ) STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES ('streaming' = 'invalid')
        """.stripMargin)
    }
  }

  test("test blocking update and delete operation on streaming table") {
    intercept[MalformedCarbonCommandException] {
      sql("""UPDATE source d SET (d.c2) = (d.c2 + 1) WHERE d.c1 = 'a'""").show()
    }
    intercept[MalformedCarbonCommandException] {
      sql("""DELETE FROM source WHERE d.c1 = 'a'""").show()
    }
  }

  override def afterAll {
    sql("USE default")
    sql("DROP DATABASE IF EXISTS streaming CASCADE")
  }
}

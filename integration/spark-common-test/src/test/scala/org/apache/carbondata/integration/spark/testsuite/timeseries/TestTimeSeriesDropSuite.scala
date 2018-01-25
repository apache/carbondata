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
package org.apache.carbondata.integration.spark.testsuite.timeseries

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

class TestTimeSeriesDropSuite extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach{

  override def beforeAll: Unit = {
    sql(s"DROP TABLE IF EXISTS mainTable")
    sql(
      """
        | CREATE TABLE mainTable(
        |   dataTime timestamp,
        |   name string,
        |   city string,
        |   age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
  }

  test("test timeseries drop datamap 3: should support drop datamap") {
    sql(
      """create datamap agg1 on table mainTable
        |using 'preaggregate'
        |DMPROPERTIES (
        |   'timeseries.eventTime'='dataTime',
        |   'timeseries.hierarchy'='month=1,year=1')
        |as select dataTime, sum(age) from mainTable
        |group by dataTime
      """.stripMargin)

    // Before DROP DATAMAP
    checkExistence(sql("show datamap on table mainTable"), true, "agg1_month", "agg1_year")

    // DROP DATAMAP DataMapName
    sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    checkExistence(sql("show datamap on table mainTable"), false, "agg1_month")
    val e: Exception = intercept[MalformedCarbonCommandException] {
      sql(s"DROP DATAMAP agg1_month ON TABLE mainTable")
    }
    assert(e.getMessage.contains("Datamap with name agg1_month does not exist under table mainTable"))
  }

  override def afterAll: Unit = {
    sql(s"DROP TABLE IF EXISTS mainTable")
  }
}

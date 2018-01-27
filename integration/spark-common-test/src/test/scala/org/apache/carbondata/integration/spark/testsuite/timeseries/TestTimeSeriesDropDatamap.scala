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
import org.scalatest.BeforeAndAfterAll

class TestTimeSeriesDropDatamap extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql(
      """
        |CREATE TABLE mainTable(dataTime timestamp, name string, city string, age int)
        |STORED BY 'org.apache.carbondata.format'
        |""".stripMargin)
  }

  // TODO: support drop datamap for timeseries
  test("test timeseries drop table 1: drop datamap should work for timeseries") {

    sql("show tables").show()
    checkExistence(sql("show tables"), false, "maintable_agg1")
    sql(
      """
        |create datamap agg1 on table mainTable
        |using 'preaggregate'
        |DMPROPERTIES (
        |   'timeseries.eventTime'='dataTime',
        |   'timeseries.hierarchy'='second=1,hour=1,day=1,month=1,year=1'
        |   )
        |as select dataTime, sum(age) from mainTable group by dataTime
        |""".stripMargin)
    checkExistence(sql("show tables"), true, "maintable_agg1")

    sql("show datamap on table mainTable").show()
    sql(s"drop datamap if exists agg1 on table mainTable")
//    checkExistence(sql("show datamap on table mainTable"), false, "agg1")
//    checkExistence(sql("show tables"), false, "maintable_agg1")
    sql(s"drop datamap if exists agg1 on table mainTable")
    sql(s"drop datamap if exists agg1_year on table mainTable")
    sql("show tables").show()
  }

  // TODO: support drop datamap for timeseries
  test("test timeseries drop table 2: drop datamap should work") {
    try {
      sql(
        """create datamap agg2 on table mainTable
          |using 'preaggregate'
          |DMPROPERTIES (
          |   'timeseries.eventTime'='dataTime',
          |   'timeseries.hierarchy'='year=1')
          |as select dataTime, sum(age) from mainTable
          |group by dataTime
          |""".stripMargin)
      assert(true)
    } catch {
      case _: Exception =>
        assert(false)
    } finally {
      sql("show tables").show()
      checkExistence(sql("show tables"), false, "maintable_agg2_hour")
      checkExistence(sql("show tables"), false, "maintable_agg2_day")
      checkExistence(sql("show tables"), false, "maintable_agg2_month")
      checkExistence(sql("show tables"), true, "maintable_agg2_year")

      sql("drop datamap if exists agg2_year on table mainTable")
      sql("show tables").show()
      checkExistence(sql("show tables"), false, "maintable_agg2_year")
    }
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}

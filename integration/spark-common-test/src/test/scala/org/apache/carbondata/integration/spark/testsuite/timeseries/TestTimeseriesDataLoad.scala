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

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

@Ignore
class TestTimeseriesDataLoad extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='mytime', 'timeseries.hierarchy'='second=1,minute=1,hour=1,day=1,month=1,year=1') as select mytime, sum(age) from mainTable group by mytime")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
  }

  test("test Year level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_year"),
      Seq(Row(Timestamp.valueOf("2016-01-01 00:00:00.0"),200)))
  }

  test("test month level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_month"),
      Seq(Row(Timestamp.valueOf("2016-02-01 00:00:00.0"),200)))
  }

  test("test day level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_day"),
      Seq(Row(Timestamp.valueOf("2016-02-23 00:00:00.0"),200)))
  }

  test("test hour level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_hour"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:00:00.0"),200)))
  }

  test("test minute level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_minute"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:00.0"),60),
        Row(Timestamp.valueOf("2016-02-23 01:02:00.0"),140)))
  }

  test("test second level timeseries data validation") {
    checkAnswer( sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"),10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"),20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"),30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"),40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"),50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"),50)))
  }

  test("test if timeseries load is successful on table creation") {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='mytime', 'timeseries.hierarchy'='second=1,minute=1,hour=1,day=1,month=1,year=1') as select mytime, sum(age) from mainTable group by mytime")
    checkAnswer( sql("select * from maintable_agg0_second"),
      Seq(Row(Timestamp.valueOf("2016-02-23 01:01:30.0"),10),
        Row(Timestamp.valueOf("2016-02-23 01:01:40.0"),20),
        Row(Timestamp.valueOf("2016-02-23 01:01:50.0"),30),
        Row(Timestamp.valueOf("2016-02-23 01:02:30.0"),40),
        Row(Timestamp.valueOf("2016-02-23 01:02:40.0"),50),
        Row(Timestamp.valueOf("2016-02-23 01:02:50.0"),50)))
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}

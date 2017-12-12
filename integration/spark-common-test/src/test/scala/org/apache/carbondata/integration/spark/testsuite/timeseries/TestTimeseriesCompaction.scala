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
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.Matchers._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

@Ignore
class TestTimeseriesCompaction extends QueryTest with BeforeAndAfterAll {

  var isCompactionEnabled = false
  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    isCompactionEnabled = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE,"false").toBoolean
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(mytime timestamp, name string, age int) STORED BY 'org.apache.carbondata.format'")
    sql("create datamap agg0 on table mainTable using 'preaggregate' DMPROPERTIES ('timeseries.eventTime'='mytime', 'timeseries.hierarchy'='second=1,minute=1,hour=1,day=1,month=1,year=1') as select mytime, sum(age) from mainTable group by mytime")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/timeseriestest.csv' into table mainTable")
  }

  test("test if pre-agg table is compacted with parent table minor compaction") {
    val segmentNamesSecond = sql("show segments for table maintable_agg0_second").collect().map(_.get(0).toString)
    segmentNamesSecond should equal (Array("3", "2", "1", "0.1", "0"))

    val segmentNamesMinute = sql("show segments for table maintable_agg0_minute").collect().map(_.get(0).toString)
    segmentNamesMinute should equal (Array("3", "2", "1", "0.1", "0"))

    val segmentNamesHour = sql("show segments for table maintable_agg0_hour").collect().map(_.get(0).toString)
    segmentNamesHour should equal (Array("3", "2", "1", "0.1", "0"))

    val segmentNamesday = sql("show segments for table maintable_agg0_day").collect().map(_.get(0).toString)
    segmentNamesday should equal (Array("3", "2", "1", "0.1", "0"))

    val segmentNamesmonth = sql("show segments for table maintable_agg0_month").collect().map(_.get(0).toString)
    segmentNamesmonth should equal (Array("3", "2", "1", "0.1", "0"))

    val segmentNamesyear = sql("show segments for table maintable_agg0_year").collect().map(_.get(0).toString)
    segmentNamesyear should equal (Array("3", "2", "1", "0.1", "0"))
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, isCompactionEnabled+"")
  }
}

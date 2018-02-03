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

package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SaveMode

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This example is for time series pre-aggregate tables.
 */

object TimeSeriesPreAggregateTableExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/timeseriestest.csv"
    val spark = ExampleUtils.createCarbonSession("TimeSeriesPreAggregateTableExample")

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    import scala.util.Random
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10 * 1000 )
      .map(x => ("" + 20 + "%02d".format(r.nextInt(20)) + "-" + "%02d".format(r.nextInt(11) + 1) +
        "-" + "%02d".format(r.nextInt(27) + 1) + " " + "%02d".format(r.nextInt(12)) + ":" +
        "%02d".format(r.nextInt(59)) + ":" + "%02d".format(r.nextInt(59)), "name" + x % 8,
        r.nextInt(60))).toDF("mytime", "name", "age")

    // 1. usage for time series Pre-aggregate tables creation and query
    spark.sql("drop table if exists timeSeriesTable")
    spark.sql("CREATE TABLE timeSeriesTable(mytime timestamp," +
      " name string, age int) STORED BY 'org.apache.carbondata.format'")
    spark.sql(
      s"""
         | CREATE DATAMAP agg0_hour ON TABLE timeSeriesTable
         | USING 'timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'HOUR_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM timeSeriesTable
         | GROUP BY mytime
       """.stripMargin)
    spark.sql(
      s"""
         | CREATE DATAMAP agg0_day ON TABLE timeSeriesTable
         | USING 'timeSeries'
         | DMPROPERTIES (
         | 'EVENT_TIME'='mytime',
         | 'DAY_GRANULARITY'='1')
         | AS SELECT mytime, SUM(age) FROM timeSeriesTable
         | GROUP BY mytime
       """.stripMargin)


    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")

    df.write.format("carbondata")
      .option("tableName", "timeSeriesTable")
      .option("compress", "true")
      .mode(SaveMode.Append).save()

    spark.sql(
      s"""
         select sum(age), timeseries(mytime,'hour') from timeSeriesTable group by timeseries(mytime,
         'hour')
      """.stripMargin).show()

    spark.sql(
      s"""
         select avg(age),timeseries(mytime,'year') from timeSeriesTable group by timeseries(mytime,
         'year')
      """.stripMargin).show()

    spark.sql("DROP TABLE IF EXISTS timeSeriesTable")

    spark.close()

  }
}

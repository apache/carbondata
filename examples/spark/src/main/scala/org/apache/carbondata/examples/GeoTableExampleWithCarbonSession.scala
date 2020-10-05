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

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object GeoTableExampleWithCarbonSession {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark/src/main/resources/log4j.properties")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    val spark = ExampleUtils.createCarbonSession("GeoTableExampleWithCarbonSession")
    spark.sparkContext.setLogLevel("error")
    Seq(
      "stored as carbondata",
      "using carbondata",
      "stored by 'carbondata'",
      "stored by 'org.apache.carbondata.format'"
    ).foreach { formatSyntax =>
      exampleBody(spark, formatSyntax)
    }
    spark.close()
  }

  def exampleBody(spark: SparkSession, formatSyntax: String = "stored as carbondata"): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val path = s"$rootPath/integration/spark/src/test/resources/geodata.csv"

    spark.sql("DROP TABLE IF EXISTS geoTable")

    // Create table
    spark.sql(
      s"""
        CREATE TABLE geoTable(
         | timevalue BIGINT,
         | longitude LONG,
         | latitude LONG)
         | $formatSyntax
         |  TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash',
         | 'SPATIAL_INDEX.mygeohash.type'='geohash',
         | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
         | 'SPATIAL_INDEX.mygeohash.originLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.gridSize'='50',
         | 'SPATIAL_INDEX.mygeohash.minLongitude'='115.811865',
         | 'SPATIAL_INDEX.mygeohash.maxLongitude'='116.782233',
         | 'SPATIAL_INDEX.mygeohash.minLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.maxLatitude'='40.225281',
         | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    spark.sql("select *from geoTable").show()
    val descTable = spark.sql(s"describe formatted geoTable").collect
    // Test if spatial index column is added to column schema
    descTable.find(_.get(0).toString.contains("mygeohash")) match {
      case Some(row) => assert(row.get(1).toString.contains("bigint"))
      case None => assert(false)
    }
    spark.sql(s"""LOAD DATA local inpath '$path' INTO TABLE geoTable OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
    // Test for MV creation
    spark.sql(s"CREATE MATERIALIZED VIEW view1 AS SELECT longitude, latitude FROM geoTable")
    val result = spark.sql("show materialized views on table geoTable").collectAsList()
    assert(result.get(0).get(1).toString.equalsIgnoreCase("view1"))
    spark.sql("select *from geoTable").show()
    spark.sql("DROP TABLE IF EXISTS geoTable")
  }
}

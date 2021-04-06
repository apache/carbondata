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

package org.apache.carbondata.geo

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class GeoQueryTest extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {

  val geoTable = "geoTable"
  val polygonTable = "polygonTable"
  val polylineTable = "polylineTable"

  override def beforeAll(): Unit = {
    drop()
  }

  test("test polygon list udf with select query as input") {
    createTable()
    loadData()
    createPolygonTable
    sql(s"insert into $polygonTable select 'POLYGON ((120.176433 30.327431,120.171283 30.322245," +
        s"120.181411 30.314540, 120.190509 30.321653,120.185188 30.329358,120.176433 30.327431))" +
        s"','abc','1'")
    sql(s"insert into $polygonTable select 'polygon((120.191603 30.328946,120.184179 30.327465," +
        s"120.181819 30.321464,120.190359 30.315388,120.199242 30.324464,120.191603 30.328946))'," +
        s"'abc','1'")
    sql(s"insert into $polygonTable select null,'abc','1'")
    sql(s"insert into $polygonTable select '','abc','1'")
    checkAnswer(sql(s"select longitude, latitude from $geoTable where IN_POLYGON_LIST(" +
                    s"'select polygon from $polygonTable','OR')"),
      Seq(Row(120177080, 30326882), Row(120180685, 30326327), Row(120184976, 30327105),
        Row(120176365, 30320687), Row(120179669, 30323688), Row(120181001, 30320761),
        Row(120187094, 30323540), Row(120186192, 30320132), Row(120181001, 30317316),
        Row(120189311, 30327549), Row(120193574, 30323651), Row(120190055, 30317464),
        Row(120196020, 30321651)))
  }

  test("test polygon list udf with select query and and invalid input") {
    createTable()
    loadData()
    createPolygonTable
    // verify empty data on polygon table
    assert(intercept[RuntimeException] {
      sql(s"select longitude, latitude from $geoTable where IN_POLYGON_LIST(" +
          s"'select polygon from $polygonTable','OR')").collect()
    }.getMessage.contains("polygon list need at least 2 polygons, really has 0"))
    sql(s"insert into $polygonTable select 'POLYGON ((120.176433 30.327431,120.171283 30.322245," +
        s"120.181411 30.314540, 120.190509 30.321653,120.185188 30.329358,120.176433 30.327431))" +
        s"','abc','1'")
    assert(intercept[RuntimeException] {
      sql(s"select longitude, latitude from $geoTable where IN_POLYGON_LIST(" +
          s"'select polygon from $polygonTable','OR')").collect()
    }.getMessage.contains("polygon list need at least 2 polygons, really has 1"))
    sql(s"insert into $polygonTable select 'POLYGON ((120.176433 30.327431,120.171283 30.322245," +
        s"120.181411 30.314540, 120.190509 30.321653,120.185188 30.329358,120.176433 30.327431))" +
        s"','abc','1'")
    assert(intercept[UnsupportedOperationException] {
      sql(s"select longitude, latitude from $geoTable where IN_POLYGON_LIST(" +
          s"'select polygon,poiId from $polygonTable','OR')").collect()
    }.getMessage.contains("More than one column exists in the query for Polygon List Udf"))
    assert(intercept[RuntimeException] {
      sql(s"select longitude, latitude from $geoTable where IN_POLYGON_LIST(" +
          s"'select poiId from $polygonTable','OR')").collect()
    }.getMessage.contains("polygon list need at least 2 polygons, really has 0"))
  }

  test("test polygon line udf with select query as input") {
    createTable()
    loadData()
    sql(s"""
         | CREATE TABLE polyLineTable(
         | polyline string,
         | poiType string,
         | poiId String)
         | STORED AS carbondata
            """.stripMargin)
    sql(s"insert into $polylineTable select 'linestring (120.184179 30.327465, 120.191603 " +
        s"30.328946, 120.199242 30.324464)','abc','1'")
    sql(s"insert into $polylineTable select 'linestring (120.199242 30.324464, 120.190359 " +
        s"30.315388)','abc','1'")
    sql(s"insert into $polylineTable select null,'abc','1'")
    checkAnswer(
      sql(s"select longitude, latitude from $geoTable where IN_POLYLINE_LIST(" +
          s"'select polyline from $polylineTable', 65)"),
      Seq(Row(120184976, 30327105),
        Row(120197093, 30325985),
        Row(120196020, 30321651),
        Row(120198638, 30323540)))
  }

  test("test polygon line udf with select query and invalid input") {
    createTable()
    loadData()
    sql(s"""
         | CREATE TABLE polyLineTable(
         | polyline string,
         | poiType string,
         | poiId String)
         | STORED AS carbondata
            """.stripMargin)
    sql(s"insert into $polylineTable select 'linestring (120.184179 30.327465, 120.191603 " +
        s"30.328946, 120.199242 30.324464)','abc','1'")
    intercept[UnsupportedOperationException] {
      sql(s"select longitude, latitude from $geoTable where IN_POLYLINE_LIST(" +
          s"'select polyline,poiId from $polylineTable', 65)").collect()
    }.getMessage.contains("More than one column exists in the query for PolyLine List Udf")
  }

  test("test join on spatial and polygon table with in_polygon_join udf") {
    createTable()
    loadData()
    createPolygonTable
    loadPolygonData
    val df = sql(s"select sum(t1.col1),sum(t1.col2),t2.poiId " +
                 s"from $geoTable t1 " +
                 s"inner join " +
                 s"(select polygon,poiId from $polygonTable where poitype='abc') t2 " +
                 s"on in_polygon_join(t1.mygeohash,t2.polygon) group by t2.poiId")
    checkAnswer(df, Seq(Row(64, 79, "1"), Row(39, 37, "2")))
  }

  // Exclude when running with index server, as pruning info for explain command
  // not set with index server.
  test("test block pruning with polygon join query", true) {
    createTable()
    sql(s"insert into $geoTable select 855280799612,1,2,116285807,40084087")
    sql(s"insert into $geoTable select 855283635086,1,2,116372142,40129503")
    sql(s"insert into $geoTable select 855279346102,1,2,116187332,39979316")
    sql(s"insert into $geoTable select 855282156308,1,2,116337069,39951887")
    sql(s"insert into $geoTable select 855283640154,1,2,116359102,40154684")
    sql(s"insert into $geoTable select 855282440834,1,2,116736367,39970323")
    sql(s"insert into $geoTable select 855282072206,1,2,116362699,39942444")
    sql(s"insert into $geoTable select 855282157702,1,2,116325378,39963129")
    sql(s"insert into $geoTable select 855279270226,1,2,116302895,39930753")
    sql(s"insert into $geoTable select 855279368850,1,2,116288955,39999101")
    createPolygonTable
    sql(
      s"insert into $polygonTable select 'POLYGON ((116.321011 40.123503, 116.137676 39.947911, " +
      "116.560993 39.935276, 116.321011 40.123503))','china','1'")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    val joinQuery =
      s"select t1.longitude, t1.latitude,t2.poiId from $geoTable t1 inner join (select polygon," +
      s"poiId from $polygonTable where poitype='china') t2 on in_polygon_join" +
      s"(t1.mygeohash,t2.polygon)"
    assert(sql(s"explain $joinQuery").collect()(0)
      .toString()
      .contains("- pruned by Main Index\n    - skipped: 5 blocks, 4 blocklets"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
    val joinQuery_without_joinUdf =
      s"select t1.longitude, t1.latitude,t2.poiId from $geoTable t1 inner join (select polygon," +
      s"poiId from $polygonTable where poitype='china') t2 where in_polygon('116.321011 " +
      s"40.123503,116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"
    checkAnswer(sql(joinQuery), sql(joinQuery_without_joinUdf))
    // set segments for spatial table
    try {
      sql(s"set carbon.input.segments.default.$geoTable=0,1")
      checkAnswer(sql(joinQuery), Seq(Row(116285807L, 40084087L, "1")))
    } finally {
      sql(s"set carbon.input.segments.default.$geoTable=*")
    }
  }

  test("test join on spatial and polygon table with in_polygon_join_range_list udf") {
    createTable()
    loadData()
    createPolygonTable
    sql(s"insert into $polygonTable select 'rangelist (855279368850 855279368852, 855280799610 " +
        s"855280799612,855282156300 855282157400)','xyz','1'")
    sql(s"insert into $polygonTable select 'RANGELIST (855279368848 855279368850, 855280799613 " +
        s"855280799615, 855282156301 855282157800)','xyz','2'")
    sql(s"insert into $polygonTable select 'null','xyz','2'")
    val df = sql(s"select sum(t1.col1),sum(t1.col2),t2.poiId " +
                 s"from $geoTable t1 " +
                 s"inner join " +
                 s"(select polygon,poiId from $polygonTable where poitype='xyz') t2 " +
                 s"on in_polygon_join_range_list(t1.mygeohash,t2.polygon) group by t2.poiId")
    checkAnswer(df, Seq(Row(4, 10, "1"), Row(7, 11, "2")))
  }

  test("test join on spatial table without data") {
    createTable()
    createPolygonTable
    loadPolygonData
    val df = sql(s"select sum(t1.col1),sum(t1.col2),t2.poiId " +
                 s"from $geoTable t1 " +
                 s"inner join " +
                 s"(select polygon,poiId from $polygonTable where poitype='abc') t2 " +
                 s"on in_polygon_join(t1.mygeohash,t2.polygon) group by t2.poiId")
    assert(df.count() == 0)
  }

  test("test join with invalid udf data") {
    createTable()
    loadData()
    createPolygonTable
    loadPolygonData
    intercept[UnsupportedOperationException](
      sql(s"select sum(t1.col1),sum(t1.col2),t2.poiId " +
          s"from $geoTable t1 " +
          s"inner join " +
          s"(select polygon,poiId from $polygonTable where poitype='abc') t2 " +
          s"on in_polygon_join(t2.polygon,t1.mygeohash) group by t2.poiId").collect()
    ).getMessage.contains("Join condition having left column polygon is not GeoId column")
  }

  // Exclude when running with index server, as pruning info for explain command
  // not set with index server.
  test("test block pruning on spatial and polygon table with in_polygon_join_range_list udf",
    true) {
   createTable()
    sql(s"insert into $geoTable select 855280799612,1,2,116285807,40084087")
    sql(s"insert into $geoTable select 855283635086,1,2,116372142,40129503")
    sql(s"insert into $geoTable select 855279346102,1,2,116187332,39979316")
    sql(s"insert into $geoTable select 855282156308,1,2,116337069,39951887")
    createPolygonTable
    sql(s"insert into $polygonTable select 'RANGELIST (855279368848 855279368850, 855279346102 " +
        s"855280799615, 855282156301 855282157800)','xyz','2'")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    sql(s"select sum(col1),2  from $geoTable where IN_POLYGON_RANGE_LIST" +
      s"('RANGELIST (855279368848 855279368850, 855279346102 " +
      s"855280799615, 855282156301 855282157800)','OR')")
    val joinQuery = s"select sum(t1.col1),t2.poiId " +
              s"from $geoTable t1 " +
              s"inner join " +
              s"(select polygon,poiId from $polygonTable where poitype='xyz') t2 " +
              s"on in_polygon_join_range_list(t1.mygeohash,t2.polygon) group by t2.poiId"
    if (SPARK_VERSION.startsWith("2")) {
      assert(sql(s"explain $joinQuery").collect()(0)
        .toString()
        .contains("- pruned by Main Index\n    - skipped: 2 blocks, 1 blocklets"))
    }
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
    checkAnswer(sql(joinQuery),
      sql(s"select sum(col1),'2' from $geoTable where IN_POLYGON_RANGE_LIST" +
          s"('RANGELIST (855279368848 855279368850, 855279346102 " +
          s"855280799615, 855282156301 855282157800)','OR')"))
    sql(s"insert into $polygonTable select 'RANGELIST (855279368848 855279368850, 855279346102 " +
        s"855280799615, 855282156301 855282157800)','xyz','2'")
    checkAnswer(sql(joinQuery), Seq(Row(6, "2")))
  }

  test("test toRangeList as String Udf") {
    createPolygonTable
    sql(s"insert into $polygonTable select 'POLYGON ((116.321011 40.123503, 116.320311 " +
        s"40.122503, 116.321111 40.121503, 116.321011 40.123503))','abc','1'")
    checkAnswer(sql(s"select ToRangeListAsString(polygon, 39.832277, 50) from $polygonTable"),
      Seq(Row("855280833998 855280833998,855280834020 855280834020,855280834022 855280834022")))
  }

  private def createPolygonTable = {
    sql(s"""
         | CREATE TABLE $polygonTable(
         | polygon string,
         | poiType string,
         | poiId String)
         | STORED AS carbondata
            """.stripMargin)
  }

  private def loadPolygonData = {
    sql(s"insert into $polygonTable select 'POLYGON ((120.176433 30.327431,120.171283 30.322245," +
        s"120.181411 30.314540,120.190509 30.321653,120.185188 30.329358,120.176433 30.327431))'," +
        "'abc','1'")
    sql(s"insert into $polygonTable select 'POLYGON ((120.191603 30.328946,120.184179 30.327465," +
        s"120.181819 30.321464,120.190359 30.315388,120.199242 30.324464,120.191603 30.328946))'," +
        "'abc','2'")
    sql(s"insert into $polygonTable select 'null','abcd','2'")
  }

  override def afterEach(): Unit = {
    drop()
  }
  override def afterAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
    drop()
  }

  def createTable(tableName : String = geoTable, customProperties : String = ""): Unit = {
    sql(s"""
           | CREATE TABLE $tableName(
           | col1 INT,
           | col2 INT,
           | longitude LONG,
           | latitude LONG)
           | STORED AS carbondata
           | TBLPROPERTIES ($customProperties 'SPATIAL_INDEX'='mygeohash',
           | 'SPATIAL_INDEX.mygeohash.type'='geohash',
           | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
           | 'SPATIAL_INDEX.mygeohash.originLatitude'='39.832277',
           | 'SPATIAL_INDEX.mygeohash.gridSize'='50',
           | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000',
           | 'SPATIAL_INDEX.mygeohash.class'='org.apache.carbondata.geo.GeoHashIndex')
       """.stripMargin)
  }

  def loadData(tableName : String = geoTable): Unit = {
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodata3.csv' INTO TABLE $tableName OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
  }

  def drop(): Unit = {
    sql(s"drop table if exists $geoTable")
    sql(s"drop table if exists $polygonTable")
    sql(s"drop table if exists $polylineTable")
  }
}

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

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException}
import org.apache.carbondata.core.constants.CarbonCommonConstants

class GeoTest extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val table1 = "geoTable1"
  val table2 = "geotable2"
  val dfTable1 = "dfGeoTable1"
  val dfTable2 = "dfGeoTable2"
  val result = Seq(Row(116187332, 39979316),
    Row(116362699, 39942444),
    Row(116288955, 39999101),
    Row(116325378, 39963129),
    Row(116337069, 39951887),
    Row(116285807, 40084087))

  override def beforeAll(): Unit = {
    drop()
  }

  test("Invalid spatial index property") {
    // Index name must not match with table column name.  Fails to create table.
    var exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='longitude')
      """.stripMargin))

    assert(exception.getMessage.contains(
      "index: longitude must not match with any other column name in the table"))

    // Type property is not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.SPATIAL_INDEX}.mygeohash.type property must be specified"))

    // Source columns are not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash', 'SPATIAL_INDEX.mygeohash.type'='geohash')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.SPATIAL_INDEX}.mygeohash.sourcecolumns property must be " +
      s"specified."))

    // Source columns must be present in the table. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash', 'SPATIAL_INDEX.mygeohash.type'='geohash',
         | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='unknown1, unknown2')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"Source column: unknown1 in property " +
      s"${CarbonCommonConstants.SPATIAL_INDEX}.mygeohash.sourcecolumns must be a column in the " +
      s"table."))
  }

  test("test Invalid spatial index property with dataframe") {
    val geoDf = createDf()
    // Index name must not match with table column name.  Fails to create table.
    var exception = intercept[MalformedCarbonCommandException](geoDf.write
      .format("carbondata")
      .option("tableName", s"$table1")
      .option("SPATIAL_INDEX", "longitude")
      .mode(SaveMode.Overwrite)
      .save())
    assert(exception.getMessage.contains(
      "index: longitude must not match with any other column name in the table"))
    // Type property is not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](geoDf.write
      .format("carbondata")
      .option("tableName", s"$table1")
      .option("SPATIAL_INDEX", "mygeohash")
      .mode(SaveMode.Overwrite)
      .save())
    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.SPATIAL_INDEX}.mygeohash.type property must be specified"))
    // Source columns are not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](geoDf.write
      .format("carbondata")
      .option("tableName", s"$table1")
      .option("SPATIAL_INDEX", "geo1")
      .option("SPATIAL_INDEX.geo1.type", "geohash")
      .mode(SaveMode.Overwrite)
      .save())
    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.SPATIAL_INDEX}.geo1.sourcecolumns property must be " +
      s"specified."))
    // Source columns must be present in the table. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](geoDf.write
      .format("carbondata")
      .option("tableName", s"$table1")
      .option("SPATIAL_INDEX", "mygeohash")
      .option("SPATIAL_INDEX.mygeohash.type", "geohash")
      .option("SPATIAL_INDEX.mygeohash.sourcecolumns", "unknown1, unknown2")
      .mode(SaveMode.Overwrite)
      .save())
    assert(exception.getMessage.contains(
      s"Source column: unknown1 in property " +
      s"${CarbonCommonConstants.SPATIAL_INDEX}.mygeohash.sourcecolumns must be a column in the " +
      s"table."))
  }

  test("test geo table with invalid table properties") {
    var exception = intercept[MalformedCarbonCommandException](
      createTable(table1, " 'RANGE_COLUMN'='timevalue', 'COLUMN_META_CACHE' = 'mygeohash', "))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): column_meta_cache"))

    exception = intercept[MalformedCarbonCommandException](
      createTable(table1, " 'NO_INVERTED_INDEX'='mygeohash', "))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): no_inverted_index"))

    exception = intercept[MalformedCarbonCommandException](
      createTable(table1,
        " 'SORT_COLUMNS'='mygeohash, timevalue ', 'INVERTED_INDEX'='mygeohash', "))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): inverted_index"))

    exception = intercept[MalformedCarbonCommandException](
      createTable(table1, " 'RANGE_COLUMN'='mygeohash', "))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): range_column"))

    exception = intercept[MalformedCarbonCommandException](
      createTable(table1, " 'BUCKET_NUMBER'='10', 'BUCKET_COLUMNS'='mygeohash', "))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): bucket_columns"))
  }

  test("test alter table with invalid table properties") {
    createTable()
    var exception = intercept[RuntimeException](
      sql(s"ALTER TABLE $table1 SET TBLPROPERTIES('SORT_COLUMNS'='mygeohash, timevalue ', " +
          s"'INVERTED_INDEX'='mygeohash')"))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): inverted_index"))

    exception = intercept[RuntimeException](
      sql(s"ALTER TABLE $table1 SET TBLPROPERTIES('NO_INVERTED_INDEX'='mygeohash')"))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): no_inverted_index"))

    exception = intercept[RuntimeException](
      sql(s"ALTER TABLE $table1 SET TBLPROPERTIES('COLUMN_META_CACHE' = 'mygeohash')"))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): column_meta_cache"))
  }

  test("test UDF's with invalid values") {
    createTable()
    val exception1 = intercept[RuntimeException](sql(
      s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
      s"'RANGELIST (855279368848 855279368850, 855279368849 855279368852)', 45)").collect())
    assert(exception1.getMessage.contains("Unsupported operation type 45"))

    var exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select longitude, latitude from $table1 where IN_POLYLINE_LIST(" +
          s"'linestring (120.184179 30.327465, 120.191603 30.328946, 120.199242 30.324464, " +
          s"120.190359 30.315388)', 'x')").collect())
    assert(exception2.getMessage.contains("Expect buffer size to be of float type"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select longitude, latitude from $table1 where IN_POLYLINE_LIST(" +
          s"'linestring (120.184179 30.327465, 120.191603 30.328946, 120.199242 30.324464, " +
          s"120.190359 30.315388)', -1)").collect())
    assert(exception2.getMessage.contains("Expect buffer size to be a positive value"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select LatLngToGeoId(39930753, 116302895, 39.832277, -50) as geoId").collect())
    assert(exception2.getMessage.contains("Expect grid size to be a positive integer"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select GeoIdToLatLng(855279270226, 39.832277, -50) as LatitudeAndLongitude").collect())
    assert(exception2.getMessage.contains("Expect grid size to be a positive integer"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select ToRangeList('116.321011 40.123503, 116.320311 40.122503,116.321111 40.121503, " +
          s"116.321011 40.123503', 39.832277, 0) as rangeList")
        .collect())
    assert(exception2.getMessage.contains("Expect grid size to be a positive integer"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select GeoIdToGridXy('X') as GridXY").collect())
    assert(exception2.getMessage.contains("Expect geoId to be of long type"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select LatLngToGeoId('X', 'X', 'X', 'X') as geoId").collect())
    assert(exception2.getMessage.contains("Expect latitude to be of long type"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select GeoIdToLatLng('X', 'X', 'X') as LatitudeAndLongitude").collect())
    assert(exception2.getMessage.contains("Expect geoId to be of long type"))

    exception2 = intercept[MalformedCarbonCommandException](
      sql(s"select ToUpperLayerGeoId('X') as upperLayerGeoId").collect())
    assert(exception2.getMessage.contains("Expect geoId to be of long type"))
  }

  test("test materialized view with spatial column") {
    createTable()
    val exception = intercept[MalformedCarbonCommandException](sql(
      s"CREATE MATERIALIZED VIEW view1 AS SELECT longitude, mygeohash FROM $table1"))
    assert(exception.getMessage.contains(
      s"mygeohash is a spatial index column and is not allowed for " +
      s"the option(s): MATERIALIZED VIEW"))
  }

  test("test geo table create index on spatial column") {
    createTable()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      val exception = intercept[MalformedIndexCommandException](sql(
        s"""
           | CREATE INDEX bloom_index ON TABLE $table (mygeohash)
           | AS 'bloomfilter'
           | PROPERTIES('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')
      """.stripMargin))
      assert(exception.getMessage.contains(
        s"Spatial Index column is not supported, column 'mygeohash' is spatial column"))
    })
  }

  test("test geo table create with spark session and check describe formatted") {
    createTable()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      // Test if spatial index column is added as a sort column
      val descTable = sql(s"describe formatted $table").collect
      descTable.find(_.get(0).toString.contains("Sort Scope")) match {
        case Some(row) => assert(row.get(1).toString.contains("LOCAL_SORT"))
        case None => assert(false)
      }
      descTable.find(_.get(0).toString.contains("Sort Columns")) match {
        case Some(row) => assert(row.get(1).toString.contains("mygeohash"))
        case None => assert(false)
      }
      // Test if spatial index column is added to column schema
      descTable.find(_.get(0).toString.contains("mygeohash")) match {
        case Some(row) => assert(row.get(1).toString.contains("bigint"))
        case None => assert(false)
      }
    })
  }

  test("test create geo table with spark session having syntax: using carbondata") {
    sql(
      s"""
         | CREATE TABLE $table1(
         | timevalue BIGINT,
         | longitude LONG,
         | latitude LONG)
         | using carbondata
         | options ('SPATIAL_INDEX'='mygeohash',
         | 'SPATIAL_INDEX.mygeohash.type'='geohash',
         | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
         | 'SPATIAL_INDEX.mygeohash.originLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.gridSize'='50',
         | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    val descTable = sql(s"describe formatted $table1").collect
    // Test if spatial index column is added to column schema
    descTable.find(_.get(0).toString.contains("mygeohash")) match {
      case Some(row) => assert(row.get(1).toString.contains("bigint"))
      case None => assert(false)
    }
  }

  test("test geo table drop spatial index column") {
    createTable()
    val exception = intercept[MalformedCarbonCommandException](
      sql(s"alter table $table1 drop columns(mygeohash)"))
    assert(exception.getMessage.contains(
      s"Columns present in ${ CarbonCommonConstants.SPATIAL_INDEX } " +
      s"table property cannot be altered/updated"))
  }

  test("test geo table alter spatial index column") {
    createTable()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      val exception = intercept[MalformedCarbonCommandException](
        sql(s"update $table set (mygeohash)=(111111) where longitude=116285807 "))
      assert(exception.getMessage.contains(
        s"Columns present in ${ CarbonCommonConstants.SPATIAL_INDEX } " +
        s"table property cannot be altered/updated"))
    })
  }

  test("test geo table filter by geo spatial index column") {
    createTable()
    loadData()
    checkAnswer(sql(s"select *from $table1 where mygeohash = '855282156308'"),
      Seq(Row(855282156308L, 1575428400000L, 116337069, 39951887)))
  }

  test("test polygon query") {
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('116.321011 40.123503, " +
            s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
        result)
    })
  }

  test("test insert into table select from another table") {
    val sourceTable = table1;
    val targetTable = table2;
    createTable(sourceTable)
    loadData(sourceTable)
    createTable(targetTable)
    sql(s"insert into  $targetTable select * from $sourceTable")
    checkAnswer(
      sql(s"select longitude, latitude from $targetTable where IN_POLYGON('116.321011 40.123503, " +
          s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      result)
  }

  test("insert into table select from another geo table with different properties") {
    val sourceTable = table1;
    val targetTable = table2;
    sql(
      s"""
         | CREATE TABLE $sourceTable(
         | timevalue BIGINT,
         | longitude LONG,
         | latitude LONG) COMMENT "This is a GeoTable"
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='spatial',
         | 'SPATIAL_INDEX.spatial.type'='geohash',
         | 'SPATIAL_INDEX.spatial.sourcecolumns'='longitude, latitude',
         | 'SPATIAL_INDEX.spatial.originLatitude'='39.832277',
         | 'SPATIAL_INDEX.spatial.gridSize'='60',
         | 'SPATIAL_INDEX.spatial.conversionRatio'='1000000')
       """.stripMargin)
    loadData(sourceTable)
    createTable(targetTable)
    // INSERT INTO will keep SPATIAL_INDEX column from sourceTable instead of generating internally
    sql(s"insert into  $targetTable select * from $sourceTable")
    checkAnswer(sql(s"select *from $targetTable where mygeohash = '233137655761'"),
      Seq(Row(233137655761L, 1575428400000L, 116337069, 39951887)))
  }

  test("test insert into non-geo table select from geo table") {
    val sourceTable = table1;
    val targetTable = table2;
    createTable(sourceTable)
    loadData(sourceTable)
    sql(
      s"""
          CREATE TABLE IF NOT EXISTS $targetTable
          (spatial Long, time Bigint, longitude Long, latitude Long)
          STORED AS carbondata
        """)
    sql(s"insert into  $targetTable select * from $sourceTable")
    checkAnswer(
      sql(s"select * from $targetTable where spatial='855282156308'"),
      Seq(Row(855282156308L, 1575428400000L, 116337069, 39951887)))
  }

  test("test insert into table select from another table with target table sort scope as global") {
    val sourceTable = table1;
    val targetTable = table2;
    createTable(sourceTable)
    loadData(sourceTable)
    createTable(targetTable, "'SORT_SCOPE'='GLOBAL_SORT',")
    sql(s"insert into  $targetTable select * from $sourceTable")
    checkAnswer(
      sql(s"select longitude, latitude from $targetTable where IN_POLYGON('116.321011 40.123503, " +
          s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      result)
  }

  // Exclude when running with index server as it uses UNKNOWN expression to prune.
  test("test block pruning for polygon query", true) {
    createTable()
    sql(s"insert into $table1 select 855280799612,1575428400000,116285807,40084087")
    sql(s"insert into $table1 select 855283635086,1575428400000,116372142,40129503")
    sql(s"insert into $table1 select 855279346102,1575428400000,116187332,39979316")
    sql(s"insert into $table1 select 855282156308,1575428400000,116337069,39951887")
    sql(s"insert into $table1 select 855283640154,1575428400000,116359102,40154684")
    sql(s"insert into $table1 select 855282440834,1575428400000,116736367,39970323")
    sql(s"insert into $table1 select 855282072206,1575428400000,116362699,39942444")
    sql(s"insert into $table1 select 855282157702,1575428400000,116325378,39963129")
    sql(s"insert into $table1 select 855279270226,1575428400000,116302895,39930753")
    sql(s"insert into $table1 select 855279368850,1575428400000,116288955,39999101")
    val df = sql(s"select * from $table1 where IN_POLYGON('116.321011 " +
                 s"40.123503, 116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')")
    assert(df.rdd.getNumPartitions == 6)
    checkAnswer(df, Seq(Row(855279346102L, 1575428400000L, 116187332, 39979316),
      Row(855282072206L, 1575428400000L, 116362699, 39942444),
      Row(855279368850L, 1575428400000L, 116288955, 39999101),
      Row(855282157702L, 1575428400000L, 116325378, 39963129),
      Row(855282156308L, 1575428400000L, 116337069, 39951887),
      Row(855280799612L, 1575428400000L, 116285807, 40084087)))
  }

  test("test insert into on table partitioned by timevalue column") {
    sql(
      s"""
         | CREATE TABLE $table1(
         | longitude LONG,
         | latitude LONG) COMMENT "This is a GeoTable" PARTITIONED BY (timevalue BIGINT)
         | STORED AS carbondata
         | TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash',
         | 'SPATIAL_INDEX.mygeohash.type'='geohash',
         | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
         | 'SPATIAL_INDEX.mygeohash.originLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.gridSize'='50',
         | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    sql(s"insert into $table1 select 0, 116337069, 39951887, 1575428400000")
    checkAnswer(
      sql(s"select * from $table1 where mygeohash = '0'"),
      Seq(Row(0, 116337069, 39951887, 1575428400000L)))
  }

  test("test polygon query on table partitioned by timevalue column") {
    sql(s"""
           | CREATE TABLE $table1(
           | longitude LONG,
           | latitude LONG) COMMENT "This is a GeoTable" PARTITIONED BY (timevalue BIGINT)
           | STORED AS carbondata
           | TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash',
           | 'SPATIAL_INDEX.mygeohash.type'='geohash',
           | 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
           | 'SPATIAL_INDEX.mygeohash.originLatitude'='39.832277',
           | 'SPATIAL_INDEX.mygeohash.gridSize'='50',
           | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    loadData()
    val geodf = createDf()
    geodf.write
      .format("carbondata")
      .option("tableName", s"$dfTable1")
      .option("partitionColumns", "timevalue")
      .option("SPATIAL_INDEX", "mygeohash")
      .option("SPATIAL_INDEX.mygeohash.type", "geohash")
      .option("SPATIAL_INDEX.mygeohash.sourcecolumns", "longitude, latitude")
      .option("SPATIAL_INDEX.mygeohash.originLatitude", "39.832277")
      .option("SPATIAL_INDEX.mygeohash.gridSize", "50")
      .option("SPATIAL_INDEX.mygeohash.conversionRatio", "1000000")
      .option("SPATIAL_INDEX.mygeohash.class", "org.apache.carbondata.geo.GeoHashIndex")
      .mode(SaveMode.Overwrite)
      .save()
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('116.321011 40.123503, " +
            s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
        result)
    })
  }

  test("test insert into geo table with customized spatial index and polygon query") {
    createTable()
    sql(s"insert into $table1 select 855279346102,1575428400000,116187332,39979316")
    sql(s"insert into $table1 select 855282072206,1575428400000,116362699,39942444")
    sql(s"insert into $table1 select 855279368850,1575428400000,116288955,39999101")
    sql(s"insert into $table1 select 855282157702,1575428400000,116325378,39963129")
    sql(s"insert into $table1 select 855280799612,1575428400000,116285807,40084087")
    sql(s"insert into $table1 select 0, 1575428400000, 116337069, 39951887")
    checkAnswer(
      sql(s"select * from $table1 where longitude = '116337069'"),
      Seq(Row(0, 1575428400000L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 40.123503, " +
        s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      Seq(Row(116187332, 39979316),
        Row(116362699, 39942444),
        Row(116288955, 39999101),
        Row(116325378, 39963129),
        Row(116285807, 40084087)))
  }

  test("test load data with customized correct spatial index to geo table and polygon query") {
    createTable()
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodataWithCorrectSpatialIndex.csv'
           |INTO TABLE $table1 OPTIONS ('DELIMITER'= ',')""".stripMargin)
    checkAnswer(
      sql(s"select * from $table1 where longitude = '116337069'"),
      Seq(Row(855282156308L, 1575428400000L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 40.123503, " +
        s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      result)
  }

  test("test load data with customized error spatial index to geo table and polygon query") {
    createTable()
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodataWithErrorSpatialIndex.csv'
           |INTO TABLE $table1 OPTIONS ('DELIMITER'= ',')""".stripMargin)
    checkAnswer(
      sql(s"select * from $table1 where longitude = '116337069'"),
      Seq(Row(0, 1575428400000L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 40.123503, " +
        s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      Seq())
  }

  test("test polygon list query: union of two polygons which are intersected") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431')"),
        Seq(Row(120177080, 30326882),
          Row(120180685, 30326327),
          Row(120184976, 30327105),
          Row(120176365, 30320687),
          Row(120179669, 30323688),
          Row(120181001, 30320761),
          Row(120187094, 30323540),
          Row(120186192, 30320132),
          Row(120181001, 30317316)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.191603 30.328946,120.184179 30.327465,120.181819 30.321464," +
            s"120.190359 30.315388,120.199242 30.324464,120.191603 30.328946')"),
        Seq(Row(120184976, 30327105),
          Row(120189311, 30327549),
          Row(120187094, 30323540),
          Row(120193574, 30323651),
          Row(120186192, 30320132),
          Row(120190055, 30317464),
          Row(120196020, 30321651)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON_LIST(" +
            s"'POLYGON ((120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431)), " +
            s"POLYGON ((120.191603 30.328946,120.184179 30.327465,120.181819 30.321464," +
            s"120.190359 30.315388,120.199242 30.324464,120.191603 30.328946))', " +
            s"'OR')"),
        Seq(Row(120177080, 30326882),
          Row(120180685, 30326327),
          Row(120184976, 30327105),
          Row(120176365, 30320687),
          Row(120179669, 30323688),
          Row(120181001, 30320761),
          Row(120187094, 30323540),
          Row(120186192, 30320132),
          Row(120181001, 30317316),
          Row(120189311, 30327549),
          Row(120193574, 30323651),
          Row(120190055, 30317464),
          Row(120196020, 30321651)))
    })
  }

  test("test polygon list query: intersection of two polygons which are intersected") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431')"),
        Seq(Row(120177080, 30326882),
          Row(120180685, 30326327),
          Row(120184976, 30327105),
          Row(120176365, 30320687),
          Row(120179669, 30323688),
          Row(120181001, 30320761),
          Row(120187094, 30323540),
          Row(120186192, 30320132),
          Row(120181001, 30317316)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.191603 30.328946,120.184179 30.327465,120.181819 30.321464," +
            s"120.190359 30.315388,120.199242 30.324464,120.191603 30.328946')"),
        Seq(Row(120184976, 30327105),
          Row(120189311, 30327549),
          Row(120187094, 30323540),
          Row(120193574, 30323651),
          Row(120186192, 30320132),
          Row(120190055, 30317464),
          Row(120196020, 30321651)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON_LIST(" +
            s"'polygon ((120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431)), " +
            s"POLYGON ((120.191603 30.328946,120.184179 30.327465,120.181819 30.321464," +
            s"120.190359 30.315388,120.199242 30.324464,120.191603 30.328946))', " +
            s"'AND')"),
        Seq(Row(120184976, 30327105),
          Row(120187094, 30323540),
          Row(120186192, 30320132)))
    })
  }

  test("test polygon list query: intersection of two polygons which are not intersected") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431')"),
        Seq(Row(120177080, 30326882),
          Row(120180685, 30326327),
          Row(120184976, 30327105),
          Row(120176365, 30320687),
          Row(120179669, 30323688),
          Row(120181001, 30320761),
          Row(120187094, 30323540),
          Row(120186192, 30320132),
          Row(120181001, 30317316)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.164492 30.326279,120.160629 30.318870,120.172259 30.315351,120.164492 " +
            s"30.326279')"),
        Seq(Row(120164563, 30322243),
          Row(120168211, 30318057)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON_LIST(" +
            s"'POLYGON ((120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431)), " +
            s"POLYGON ((120.164492 30.326279,120.160629 30.318870,120.172259 30.315351," +
            s"120.164492 30.326279))', " +
            s"'AND')"),
        Seq())
    })
  }

  test("test polygon list query: intersection of two polygons when second polygon " +
    "is completely in first polygon") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431')"),
        Seq(Row(120177080, 30326882),
          Row(120180685, 30326327),
          Row(120184976, 30327105),
          Row(120176365, 30320687),
          Row(120179669, 30323688),
          Row(120181001, 30320761),
          Row(120187094, 30323540),
          Row(120186192, 30320132),
          Row(120181001, 30317316)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON('" +
            s"120.179442 30.325205,120.177253 30.322242,120.180944 30.319426," +
            s"120.186094 30.321834,120.179442 30.325205')"),
        Seq(Row(120179669, 30323688),
          Row(120181001, 30320761)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYGON_LIST(" +
            s"'POLYGON ((120.176433 30.327431,120.171283 30.322245,120.181411 30.314540," +
            s"120.190509 30.321653,120.185188 30.329358,120.176433 30.327431)), " +
            s"POLYGON ((120.179442 30.325205,120.177253 30.322242,120.180944 30.319426," +
            s"120.186094 30.321834,120.179442 30.325205))', " +
            s"'AND')"),
        Seq(Row(120179669, 30323688),
          Row(120181001, 30320761)))
    })
  }

  test("test one polyline query") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      val df = sql(s"select longitude, latitude from $table where IN_POLYLINE_LIST(" +
                   s"'LINESTRING (120.184179 30.327465, 120.191603 30.328946, 120.199242 " +
                   s"30.324464, 120.190359 30.315388)', 65)")
      checkAnswer(df, Seq(Row(120184976, 30327105),
        Row(120197093, 30325985),
        Row(120196020, 30321651),
        Row(120198638, 30323540)))
      checkAnswer(sql(s"select longitude, latitude from $table where IN_POLYLINE_LIST(" +
                      s"'LINESTRING(120.184179 30.327465, 120.191603 30.328946, 120.199242 " +
                      s"30.324464, 120.190359 30.315388)', 65)"), df)
    })
  }

  test("test polyline list query, result is union of two polylines") {
    createTable()
    loadData2()
    createTableWithDf(createDf2(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYLINE_LIST(" +
            s"'LINESTRING (120.184179 30.327465, 120.191603 30.328946, 120.199242 30.324464)', " +
            s"65)"), Seq(Row(120184976, 30327105), Row(120197093, 30325985)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYLINE_LIST(" +
            s"'LINESTRING (120.199242 30.324464, 120.190359 30.315388)', 65)"),
        Seq(Row(120196020, 30321651),
          Row(120198638, 30323540)))
      checkAnswer(
        sql(s"select longitude, latitude from $table where IN_POLYLINE_LIST(" +
            s"'linestring (120.184179 30.327465, 120.191603 30.328946, 120.199242 30.324464), " +
            s"linestring (120.199242 30.324464, 120.190359 30.315388)', 65)"),
        Seq(Row(120184976, 30327105),
          Row(120197093, 30325985),
          Row(120196020, 30321651),
          Row(120198638, 30323540)))
    })
  }

  test("test one range list query which have no overlapping range") {
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST(855279368848 855279368850, 855280799610 855280799612)', 'OR')"),
        Seq(Row(855279368850L, 116288955, 39999101),
          Row(855280799612L, 116285807, 40084087)))
    })
  }

  test("test one range list query which have overlapping range") {
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST (855279368848 855279368850, 855279368849 855279368852)', 'OR')"),
        Seq(Row(855279368850L, 116288955, 39999101)))
    })
  }

  test("test one range list query when one range contains another range") {
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST (855279368848 855279368856, 855279368849 855279368852)', 'OR')"),
        Seq(Row(855279368850L, 116288955, 39999101)))
    })
  }

  test("test two range lists query: union of two range lists which are intersected") {
    createTable()
    loadData()
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400)', 'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855280799612L, 116285807, 40084087),
        Row(855282156308L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
        s"855282156301 855282157800)', 'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855282156308L, 116337069, 39951887),
        Row(855282157702L, 116325378, 39963129)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'rangelist (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400), " +
        s"RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
        s"855282156301 855282157800)', " +
        s"'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855280799612L, 116285807, 40084087),
        Row(855282156308L, 116337069, 39951887),
        Row(855282157702L, 116325378, 39963129)))
  }

  test("test two range lists query: intersection of two range lists which are intersected") {
    createTable()
    loadData()
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400)', 'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855280799612L, 116285807, 40084087),
        Row(855282156308L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
        s"855282156301 855282157800)', 'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855282156308L, 116337069, 39951887),
        Row(855282157702L, 116325378, 39963129)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400), " +
        s"RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
        s"855282156301 855282157800)', " +
        s"'AND')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855282156308L, 116337069, 39951887)))
  }

  test("test two range lists query: intersection of two range lists which are not intersected") {
    createTable()
    loadData()
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400)', 'OR')"),
      Seq(Row(855279368850L, 116288955, 39999101),
        Row(855280799612L, 116285807, 40084087),
        Row(855282156308L, 116337069, 39951887)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368830 855279368840, 855280799613 855280799615, " +
        s"855282157700 855282157800)', 'OR')"),
      Seq(Row(855282157702L, 116325378, 39963129)))
    checkAnswer(
      sql(s"select mygeohash, longitude, latitude from $table1 where IN_POLYGON_RANGE_LIST(" +
        s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
        s"855282156300 855282157400), " +
        s"RANGELIST (855279368830 855279368840, 855280799613 855280799615, " +
        s"855282157700 855282157800)', " +
        s"'AND')"),
      Seq())
  }

  test("test two range lists query: intersection of two range lists when second range list " +
      "is completely in first range list") {
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST (855279368850 855279368852, 855280799610 855280799612, " +
            s"855282156300 855282157400)', 'OR')"),
        Seq(Row(855279368850L, 116288955, 39999101),
          Row(855280799612L, 116285807, 40084087),
          Row(855282156308L, 116337069, 39951887)))
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
            s"855282156301 855282157000)', 'OR')"),
        Seq(Row(855279368850L, 116288955, 39999101),
          Row(855282156308L, 116337069, 39951887)))
      checkAnswer(
        sql(s"select mygeohash, longitude, latitude from $table where IN_POLYGON_RANGE_LIST(" +
            s"'RANGELIST (855279368840 855279368852, 855280799610 855280799620, " +
            s"855282156300 855282157400), " +
            s"RANGELIST (855279368848 855279368850, 855280799613 855280799615, " +
            s"855282156301 855282157000)', " +
            s"'AND')"),
        Seq(Row(855279368850L, 116288955, 39999101),
          Row(855282156308L, 116337069, 39951887)))
    })
  }

  test("test transforming GeoId to GridXY") {
    checkAnswer(
      sql(s"select GeoIdToGridXy(855279270226) as GridXY"),
      Seq(Row(Seq(613089, 722908))))
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude, mygeohash, GeoIdToGridXy(mygeohash) as GridXY " +
            s"from $table where mygeohash = 855279270226"),
        Seq(Row(116302895, 39930753, 855279270226L, Seq(613089, 722908))))
    })
  }

  test("test transforming latitude and longitude to GeoId") {
    checkAnswer(
      sql(s"select LatLngToGeoId(39930753, 116302895, 39.832277, 50) as geoId"),
      Seq(Row(855279270226L)))
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude, mygeohash, " +
            s"LatLngToGeoId(latitude, longitude, 39.832277, 50) as geoId " +
            s"from $table where mygeohash = 855279270226"),
        Seq(Row(116302895, 39930753, 855279270226L, 855279270226L)))
    })
  }

  test("test transforming GeoId to latitude and longitude") {
    checkAnswer(
      sql(s"select GeoIdToLatLng(855279270226, 39.832277, 50) as LatitudeAndLongitude"),
      Seq(Row(Seq(39.930529, 116.303093))))
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude, mygeohash, " +
            s"GeoIdToLatLng(mygeohash, 39.832277, 50) as LatitudeAndLongitude " +
            s"from $table where mygeohash = 855279270226"),
        Seq(Row(116302895, 39930753, 855279270226L, Seq(39.930529, 116.303093))))
    })
  }

  test("test transforming to upper layer geoId") {
    checkAnswer(
      sql(s"select ToUpperLayerGeoId(855279270226) as upperLayerGeoId"),
      Seq(Row(213819817556L)))
    createTable()
    loadData()
    createTableWithDf(createDf(), dfTable1)
    List(table1, dfTable1).foreach(table => {
      checkAnswer(
        sql(s"select longitude, latitude, mygeohash, " +
            s"ToUpperLayerGeoId(mygeohash) as upperLayerGeoId " +
            s"from $table where mygeohash = 855279270226"),
        Seq(Row(116302895, 39930753, 855279270226L, 213819817556L)))
    })
  }

  test("test transforming polygon string to rangeList") {
    checkAnswer(
      sql(s"select ToRangeList('116.321011 40.123503, 116.320311 40.122503, " +
        s"116.321111 40.121503, 116.321011 40.123503', 39.832277, 50) as rangeList"),
      Seq(Row(mutable.WrappedArray.make(Array(
        mutable.WrappedArray.make(Array(855280833998L, 855280833998L)),
        mutable.WrappedArray.make(Array(855280834020L, 855280834020L)),
        mutable.WrappedArray.make(Array(855280834022L, 855280834022L))))))
    )
  }

  test("test insert with autogenerated geoid") {
    createTable()
    // insert without geoid
    sql(s"insert into $table1 select 1575428400000,116285807,40084087")
    // insert with customized geoid
    sql(s"insert into $table1 select 0,1575428400000,116285807,40084087")
    checkAnswer(sql(s"select *from $table1"),
      Seq(Row(855280799612L, 1575428400000L, 116285807, 40084087),
        Row(0, 1575428400000L, 116285807, 40084087)))
  }

  override def afterEach(): Unit = {
    drop()
  }
  override def afterAll(): Unit = {
    drop()
  }

  def drop(): Unit = {
    sql(s"drop table if exists $table1")
    sql(s"drop table if exists $table2")
    sql(s"drop table if exists $dfTable1")
    sql(s"drop table if exists $dfTable2")
  }

  def createTable(tableName : String = table1, customProperties : String = ""): Unit = {
    sql(s"""
           | CREATE TABLE $tableName(
           | timevalue BIGINT,
           | longitude LONG,
           | latitude LONG) COMMENT "This is a GeoTable"
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

  def loadData(tableName : String = table1): Unit = {
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodata.csv' INTO TABLE $tableName OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
  }

  def loadData2(tableName : String = table1): Unit = {
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodata2.csv' INTO TABLE $tableName OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
  }

  def createDf(): DataFrame = {
    val geoSchema = StructType(Seq(StructField("timevalue", LongType, nullable = true),
      StructField("longitude", LongType, nullable = false),
      StructField("latitude", LongType, nullable = false)))
    sqlContext.read.option("delimeter", ",").option("header", "true").schema(geoSchema)
      .csv(s"$resourcesPath/geodata.csv")
  }

  def createDf2(): DataFrame = {
    val geoSchema = StructType(Seq(StructField("timevalue", LongType, nullable = true),
      StructField("longitude", LongType, nullable = false),
      StructField("latitude", LongType, nullable = false)))
    sqlContext.read.option("delimeter", ",").option("header", "true").schema(geoSchema)
      .csv(s"$resourcesPath/geodata2.csv")
  }

  def createTableWithDf(geoDf: DataFrame, tableName: String = dfTable1): Unit = {
    geoDf.write
      .format("carbondata")
      .option("tableName", s"$tableName")
      .option("SPATIAL_INDEX", "mygeohash")
      .option("SPATIAL_INDEX.mygeohash.type", "geohash")
      .option("spatial_index.MyGeoHash.sourcecolumns", "longitude, latitude")
      .option("SPATIAL_INDEX.MyGeoHash.originLatitude", "39.832277")
      .option("SPATIAL_INDEX.mygeohash.gridSize", "50")
      .option("spatial_index.mygeohash.conversionRatio", "1000000")
      .option("spatial_index.mygeohash.CLASS", "org.apache.carbondata.geo.GeoHashIndex")
      .mode(SaveMode.Overwrite)
      .save()
  }
}

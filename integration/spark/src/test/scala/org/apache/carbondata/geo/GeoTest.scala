package org.apache.carbondata.geo

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants

class GeoTest extends QueryTest with BeforeAndAfterAll with BeforeAndAfterEach {
  val table1 = "geoTable1"
  val table2 = "geotable2"
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

  test("test geo table create and load and check describe formatted") {
    createTable()
    loadData()
    // Test if spatial index column is added as a sort column
    val descTable = sql(s"describe formatted $table1").collect
    descTable.find(_.get(0).toString.contains("Sort Scope")) match {
      case Some(row) => assert(row.get(1).toString.contains("LOCAL_SORT"))
      case None => assert(false)
    }
    descTable.find(_.get(0).toString.contains("Sort Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("mygeohash"))
      case None => assert(false)
    }
  }

  test("test polygon query") {
    createTable()
    loadData()
    checkAnswer(
      sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 40.123503, " +
          s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      result)
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

  test("test block pruning for polygon query") {
    createTable()
    sql(s"insert into $table1 select 1575428400000,116285807,40084087")
    sql(s"insert into $table1 select 1575428400000,116372142,40129503")
    sql(s"insert into $table1 select 1575428400000,116187332,39979316")
    sql(s"insert into $table1 select 1575428400000,116337069,39951887")
    sql(s"insert into $table1 select 1575428400000,116359102,40154684")
    sql(s"insert into $table1 select 1575428400000,116736367,39970323")
    sql(s"insert into $table1 select 1575428400000,116362699,39942444")
    sql(s"insert into $table1 select 1575428400000,116325378,39963129")
    sql(s"insert into $table1 select 1575428400000,116302895,39930753")
    sql(s"insert into $table1 select 1575428400000,116288955,39999101")
    val df = sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 " +
                 s"40.123503, 116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')")
    assert(df.rdd.getNumPartitions == 6)
    checkAnswer(df, result)
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
           | 'SPATIAL_INDEX.mygeohash.minLongitude'='115.811865',
           | 'SPATIAL_INDEX.mygeohash.maxLongitude'='116.782233',
           | 'SPATIAL_INDEX.mygeohash.minLatitude'='39.832277',
           | 'SPATIAL_INDEX.mygeohash.maxLatitude'='40.225281',
           | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    loadData()
    checkAnswer(
      sql(s"select longitude, latitude from $table1 where IN_POLYGON('116.321011 40.123503, " +
          s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      result)
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
           | 'SPATIAL_INDEX.mygeohash.minLongitude'='115.811865',
           | 'SPATIAL_INDEX.mygeohash.maxLongitude'='116.782233',
           | 'SPATIAL_INDEX.mygeohash.minLatitude'='39.832277',
           | 'SPATIAL_INDEX.mygeohash.maxLatitude'='40.225281',
           | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
  }

  def loadData(tableName : String = table1): Unit = {
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodata.csv' INTO TABLE $tableName OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
  }
}


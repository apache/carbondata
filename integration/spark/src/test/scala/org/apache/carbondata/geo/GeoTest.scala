package org.apache.carbondata.geo

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedIndexCommandException}
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
    val exception = intercept[MalformedIndexCommandException](sql(
      s"""
         | CREATE INDEX bloom_index ON TABLE $table1 (mygeohash)
         | AS 'bloomfilter'
         | PROPERTIES('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')
      """.stripMargin))
    assert(exception.getMessage.contains(
      s"Spatial Index column is not supported, column 'mygeohash' is spatial column"))
  }

  test("test geo table create with spark session and check describe formatted") {
    createTable()
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
    // Test if spatial index column is added to column schema
    descTable.find(_.get(0).toString.contains("mygeohash")) match {
      case Some(row) => assert(row.get(1).toString.contains("bigint"))
      case None => assert(false)
    }
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
         | 'SPATIAL_INDEX.mygeohash.minLongitude'='115.811865',
         | 'SPATIAL_INDEX.mygeohash.maxLongitude'='116.782233',
         | 'SPATIAL_INDEX.mygeohash.minLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.maxLatitude'='40.225281',
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
    val exception = intercept[MalformedCarbonCommandException](
      sql(s"update $table1 set (mygeohash)=(111111) where longitude=116285807 "))
    assert(exception.getMessage.contains(
      s"Columns present in ${ CarbonCommonConstants.SPATIAL_INDEX } " +
      s"table property cannot be altered/updated"))
  }

  test("test geo table filter by geo spatial index column") {
    createTable()
    loadData()
    checkAnswer(sql(s"select *from $table1 where mygeohash = '2196036'"),
      Seq(Row(2196036, 1575428400000L, 116337069, 39951887)))
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
         | 'SPATIAL_INDEX.spatial.minLongitude'='115.811865',
         | 'SPATIAL_INDEX.spatial.maxLongitude'='116.782233',
         | 'SPATIAL_INDEX.spatial.minLatitude'='39.832277',
         | 'SPATIAL_INDEX.spatial.maxLatitude'='40.225281',
         | 'SPATIAL_INDEX.spatial.conversionRatio'='1000000')
       """.stripMargin)
    loadData(sourceTable)
    createTable(targetTable)
    sql(s"insert into  $targetTable select * from $sourceTable")
    checkAnswer(sql(s"select *from $targetTable where mygeohash = '2196036'"),
      Seq(Row(2196036, 1575428400000L, 116337069, 39951887)))
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
      sql(s"select * from $targetTable where spatial='2196036'"),
      Seq(Row(2196036, 1575428400000L, 116337069, 39951887)))
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
    sql(s"insert into $table1 select 0,1575428400000,116285807,40084087")
    sql(s"insert into $table1 select 0,1575428400000,116372142,40129503")
    sql(s"insert into $table1 select 0,1575428400000,116187332,39979316")
    sql(s"insert into $table1 select 0,1575428400000,116337069,39951887")
    sql(s"insert into $table1 select 0,1575428400000,116359102,40154684")
    sql(s"insert into $table1 select 0,1575428400000,116736367,39970323")
    sql(s"insert into $table1 select 0,1575428400000,116362699,39942444")
    sql(s"insert into $table1 select 0,1575428400000,116325378,39963129")
    sql(s"insert into $table1 select 0,1575428400000,116302895,39930753")
    sql(s"insert into $table1 select 0,1575428400000,116288955,39999101")
    val df = sql(s"select * from $table1 where IN_POLYGON('116.321011 " +
                 s"40.123503, 116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')")
    assert(df.rdd.getNumPartitions == 6)
    checkAnswer(df, Seq(Row(733215, 1575428400000L, 116187332, 39979316),
      Row(2160019, 1575428400000L, 116362699, 39942444),
      Row(2170151, 1575428400000L, 116288955, 39999101),
      Row(2174509, 1575428400000L, 116325378, 39963129),
      Row(2196036, 1575428400000L, 116337069, 39951887),
      Row(2361256, 1575428400000L, 116285807, 40084087)))
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
         | 'SPATIAL_INDEX.mygeohash.minLongitude'='115.811865',
         | 'SPATIAL_INDEX.mygeohash.maxLongitude'='116.782233',
         | 'SPATIAL_INDEX.mygeohash.minLatitude'='39.832277',
         | 'SPATIAL_INDEX.mygeohash.maxLatitude'='40.225281',
         | 'SPATIAL_INDEX.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
    sql(s"insert into $table1 select 0, 116337069, 39951887, 1575428400000")
    checkAnswer(
      sql(s"select * from $table1 where mygeohash = '2196036'"),
      Seq(Row(2196036, 116337069, 39951887, 1575428400000L)))
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


package org.apache.carbondata.geo

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants

class GeoTest extends QueryTest with BeforeAndAfterAll with BeforeAndAfter {
  override def beforeAll(): Unit = {
    drop()
  }

  test("Invalid geo index handler property") {
    // Handler name must not match with table column name.  Fails to create table.
    var exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('INDEX_HANDLER'='longitude')
      """.stripMargin))

    assert(exception.getMessage.contains(
      "handler: longitude must not match with any other column name in the table"))

    // Type property is not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('INDEX_HANDLER'='mygeohash')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.INDEX_HANDLER}.mygeohash.type property must be specified"))

    // Source columns are not configured. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('INDEX_HANDLER'='mygeohash', 'INDEX_HANDLER.mygeohash.type'='geohash')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"${CarbonCommonConstants.INDEX_HANDLER}.mygeohash.sourcecolumns property must be " +
      s"specified."))

    // Source columns must be present in the table. Fails to create table.
    exception = intercept[MalformedCarbonCommandException](sql(
      s"""
         | CREATE TABLE malformed(timevalue BIGINT, longitude LONG, latitude LONG)
         | COMMENT "This is a malformed table"
         | STORED AS carbondata
         | TBLPROPERTIES ('INDEX_HANDLER'='mygeohash', 'INDEX_HANDLER.mygeohash.type'='geohash',
         | 'INDEX_HANDLER.mygeohash.sourcecolumns'='unknown1, unknown2')
      """.stripMargin))

    assert(exception.getMessage.contains(
      s"Source column: unknown1 in property " +
      s"${CarbonCommonConstants.INDEX_HANDLER}.mygeohash.sourcecolumns must be a column in the " +
      s"table."))
  }

  test("test geo table create and load and check describe formatted") {
    createTable()
    loadData()
    // Test if index handler column is added as a sort column
    val descTable = sql("describe formatted geotable").collect
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
      sql(s"select longitude, latitude from geotable where IN_POLYGON('116.321011 40.123503, " +
          s"116.137676 39.947911, 116.560993 39.935276, 116.321011 40.123503')"),
      Seq(Row(116187332, 39979316),
        Row(116362699, 39942444),
        Row(116288955, 39999101),
        Row(116325378, 39963129),
        Row(116337069, 39951887),
        Row(116285807, 40084087)))
  }

  after {
    drop()
  }
  override def afterAll(): Unit = {
    drop()
  }

  def drop(): Unit = {
    sql("drop table if exists geotable")
  }

  def createTable(): Unit = {
    sql(s"""
           | CREATE TABLE geotable(
           | timevalue BIGINT,
           | longitude LONG,
           | latitude LONG) COMMENT "This is a GeoTable"
           | STORED AS carbondata
           | TBLPROPERTIES ('INDEX_HANDLER'='mygeohash',
           | 'INDEX_HANDLER.mygeohash.type'='geohash',
           | 'INDEX_HANDLER.mygeohash.sourcecolumns'='longitude, latitude',
           | 'INDEX_HANDLER.mygeohash.originLatitude'='39.832277',
           | 'INDEX_HANDLER.mygeohash.gridSize'='50',
           | 'INDEX_HANDLER.mygeohash.minLongitude'='115.811865',
           | 'INDEX_HANDLER.mygeohash.maxLongitude'='116.782233',
           | 'INDEX_HANDLER.mygeohash.minLatitude'='39.832277',
           | 'INDEX_HANDLER.mygeohash.maxLatitude'='40.225281',
           | 'INDEX_HANDLER.mygeohash.conversionRatio'='1000000')
       """.stripMargin)
  }

  def loadData(): Unit = {
    sql(s"""LOAD DATA local inpath '$resourcesPath/geodata.csv' INTO TABLE geotable OPTIONS
           |('DELIMITER'= ',')""".stripMargin)
  }
}


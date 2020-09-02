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
package org.apache.carbondata.spark.testsuite.secondaryindex

import scala.collection.JavaConverters._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils
.isFilterPushedDownToSI;
import org.apache.spark.sql.{CarbonEnv, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.exception.ProcessMetaDataException

import org.apache.spark.sql.test.util.QueryTest

class TestSIWithSecondryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop index if exists si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    sql("drop table if exists table_drop_columns")
    sql("drop table if exists table_drop_columns_fail")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("create table table_WithSIAndAlter(c1 string, c2 date,c3 timestamp) STORED AS carbondata")
    sql("insert into table_WithSIAndAlter select 'xx',current_date, current_timestamp")
    sql("alter table table_WithSIAndAlter add columns(date1 date, time timestamp)")
    sql("update table_WithSIAndAlter set(date1) = (c2)").show
    sql("update table_WithSIAndAlter set(time) = (c3)").show
    sql("create index si_altercolumn on table table_WithSIAndAlter(date1,time) AS 'carbondata'")
  }

  private def isExpectedValueValid(dbName: String,
      tableName: String,
      key: String,
      expectedValue: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    if (key.equalsIgnoreCase(CarbonCommonConstants.COLUMN_META_CACHE)) {
      val value = carbonTable.getMinMaxCachedColumnsInCreateOrder.asScala.mkString(",")
      expectedValue.equals(value)
    } else {
      val value = carbonTable.getTableInfo.getFactTable.getTableProperties.get(key)
      expectedValue.equals(value)
    }
  }

  test ("test alter drop all columns of the SI table") {
    sql("create table table_drop_columns (name string, id string, country string) stored as carbondata")
    sql("insert into table_drop_columns select 'xx', '1', 'china'")
    sql("create index index_1 on table table_drop_columns(id, country) as 'carbondata'")
    // alter table to drop all the columns used in index
    sql("alter table table_drop_columns drop columns(id, country)")
    sql("insert into table_drop_columns select 'xy'")
    assert(sql("show indexes on table_drop_columns").collect().isEmpty)
  }

  test ("test alter drop few columns of the SI table") {
    sql("create table table_drop_columns_fail (name string, id string, country string) stored as carbondata")
    sql("insert into table_drop_columns_fail select 'xx', '1', 'china'")
    sql("create index index_1 on table table_drop_columns_fail(id, country) as 'carbondata'")
    // alter table to drop few columns used in index. This should fail as we are not dropping all
    // the index columns
    assert(intercept[ProcessMetaDataException](sql(
      "alter table table_drop_columns_fail drop columns(id)")).getMessage
      .contains("Alter table drop column operation failed:"))
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from si_altercolumn")
      ,Seq(Row(1)))
  }

  test("test create secondary index when all records are deleted from table") {
    sql("drop table if exists delete_records")
    sql("create table delete_records (a string,b string) STORED AS carbondata")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("delete from delete_records where a='k'").show()
    sql("alter table delete_records compact 'minor'")
    sql("create index index1 on table delete_records(b) AS 'carbondata'")
    checkAnswer(sql("select count(*) from index1"), Row(0))
    sql("drop table if exists delete_records")
  }

  test("test secondary index data after parent table rename") {
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
    sql("create table maintable (a string,b string, c int) STORED AS carbondata")
    sql("insert into maintable values('k','x',2)")
    sql("insert into maintable values('k','r',1)")
    sql("create index index21 on table maintable(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable where c>1"), Seq(Row("k","x",2)))
    sql("ALTER TABLE maintable RENAME TO maintableeee")
    checkAnswer(sql("select * from maintableeee where c>1"), Seq(Row("k","x",2)))
  }

  test("validate column_meta_cache and cache_level on SI table") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) STORED AS carbondata")
    sql("create index indexCache on table column_meta_cache(c2,c1) AS 'carbondata' PROPERTIES('COLUMN_meta_CachE'='c2','cache_level'='BLOCK')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", "c2"))
    assert(isExpectedValueValid("default", "indexCache", "cache_level", "BLOCK"))
    // set invalid values for SI table for column_meta_cache and cache_level and verify
    intercept[MalformedCarbonCommandException] {
      sql("create index indexCache1 on table column_meta_cache(c2) AS 'carbondata' PROPERTIES('COLUMN_meta_CachE'='abc')")
    }
    intercept[MalformedCarbonCommandException] {
      sql("create index indexCache1 on table column_meta_cache(c2) AS 'carbondata' PROPERTIES('cache_level'='abc')")
    }
    intercept[Exception] {
      sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='abc')")
    }
    intercept[Exception] {
      sql("Alter table indexCache SET TBLPROPERTIES('CACHE_LEVEL'='abc')")
    }
    // alter table to unset these properties on SI table
    sql("Alter table indexCache UNSET TBLPROPERTIES('column_meta_cache')")
    var descResult = sql("describe formatted indexCache")
    checkExistence(descResult, false, "COLUMN_META_CACHE")
    sql("Alter table indexCache UNSET TBLPROPERTIES('cache_level')")
    descResult = sql("describe formatted indexCache")
    checkExistence(descResult, true, "Min/Max Index Cache Level")
    //alter SI table to set the properties again
    sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='c1')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", "c1"))
    // set empty value for column_meta_cache
    sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", ""))
    // set cache_level to blocklet
    sql("Alter table indexCache SET TBLPROPERTIES('cache_level'='BLOCKLET')")
    assert(isExpectedValueValid("default", "indexCache", "cache_level", "BLOCKLET"))
  }

  test("test parallel load of SI to main table") {
    sql("drop table if exists uniqdata")
    sql("CREATE table uniqdata (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    sql("create index index1 on table uniqdata (workgroupcategoryname) AS 'carbondata'")
    val indexTable = CarbonEnv.getCarbonTable(Some("default"), "index1")(sqlContext.sparkSession)
    val carbontable = CarbonEnv.getCarbonTable(Some("default"), "uniqdata")(sqlContext.sparkSession)
    val details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
    val failSegments = List("3","4")
    sql(s"""set carbon.si.repair.limit = 2""")
    var loadMetadataDetailsList = Array[LoadMetadataDetails]()
    details.foreach{detail =>
      if(failSegments.contains(detail.getLoadName)){
        val loadmetadatadetail = detail
        loadmetadatadetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
        loadMetadataDetailsList +:= loadmetadatadetail
      } else {
        loadMetadataDetailsList +:= detail
      }
    }

    SegmentStatusManager.writeLoadDetailsIntoFile(
      indexTable.getMetadataPath + CarbonCommonConstants.FILE_SEPARATOR +
      CarbonTablePath.TABLE_STATUS_FILE,
      loadMetadataDetailsList)

    sql(s"""ALTER TABLE default.index1 SET
           |SERDEPROPERTIES ('isSITableEnabled' = 'false')""".stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdata OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    val count1 = sql("select * from uniqdata where workgroupcategoryname = 'developer'").count()
    val df1 = sql("select * from uniqdata where workgroupcategoryname = 'developer'").queryExecution.sparkPlan
    sql(s"""ALTER TABLE default.index1 SET
           |SERDEPROPERTIES ('isSITableEnabled' = 'false')""".stripMargin)
    val count2 = sql("select * from uniqdata where workgroupcategoryname = 'developer'").count()
    val df2 = sql("select * from uniqdata where workgroupcategoryname = 'developer'").queryExecution.sparkPlan
    sql(s"""set carbon.si.repair.limit = 1""")
    assert(count1 == count2)
    assert(isFilterPushedDownToSI(df1))
    assert(!isFilterPushedDownToSI(df2))
  }

  test("test drop table on index table") {
    sql("drop table if exists uniqdataTable")
    sql("CREATE table uniqdataTable (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql(
      "create index uniqdataindex1 on table uniqdataTable (workgroupcategoryname) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE uniqdataTable OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
    val errorMessage = intercept[Exception] {
      sql("drop table uniqdataindex1")
    }.getMessage
    assert(errorMessage.contains("Drop table is not permitted on Index Table"))
  }

  test("test SI creation on two tables with the same name") {
    sql("drop table if exists uniqdataTable1")
    sql("drop table if exists uniqdataTable2")
    sql("CREATE table uniqdataTable1 (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql(
      "create index uniqdataidxtable on table uniqdataTable1 (workgroupcategoryname) AS 'carbondata'")

    sql("CREATE table uniqdataTable2 (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    val errorMessage = intercept[Exception] {
      sql(
        "create index uniqdataidxtable on table uniqdataTable2 (workgroupcategoryname) AS 'carbondata'")
    }.getMessage
    assert(errorMessage.contains("Index [uniqdataidxtable] already exists under database [default]"))
  }

  test("test date type with SI table") {
    sql("drop table if exists maintable")
    sql("CREATE TABLE maintable (id int,name string,salary float,dob date,address string) STORED AS carbondata")
    sql("insert into maintable values(1,'aa',23423.334,'2009-09-06','df'),(1,'aa',23423.334,'2009-09-07','df')")
    sql("insert into maintable select 2,'bb',4454.454,'2009-09-09','bang'")
    sql("drop index if exists index_date on maintable")
    sql("create index index_date on table maintable(dob) AS 'carbondata'")
    val df = sql("select id,name,dob from maintable where dob = '2009-09-07'")
    assert(isFilterPushedDownToSI(df.queryExecution.sparkPlan))
    checkAnswer(df, Seq(Row(1,"aa", java.sql.Date.valueOf("2009-09-07"))))
    sql("drop table if exists maintable")
  }

  override def afterAll {
    sql("drop index si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
    sql("drop table if exists column_meta_cache")
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdataTable")
    sql("drop table if exists table_drop_columns")
    sql("drop table if exists table_drop_columns_fail")
  }
}

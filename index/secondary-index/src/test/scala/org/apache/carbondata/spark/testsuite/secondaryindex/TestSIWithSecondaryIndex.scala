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

import java.sql.Date

import mockit.{Mock, MockUp}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import scala.collection.JavaConverters._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.AbstractCarbonLock
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

class TestSIWithSecondaryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropIndexAndTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("create table table_WithSIAndAlter(c1 string, c2 date,c3 timestamp) STORED AS carbondata")
    sql("insert into table_WithSIAndAlter select 'xx',current_date, current_timestamp")
    sql("alter table table_WithSIAndAlter add columns(date1 date, time timestamp)")
    sql("update table_WithSIAndAlter set(date1) = (c2)").collect()
    sql("update table_WithSIAndAlter set(time) = (c3)").collect()
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

  test("test alter drop columns of the SI table") {
    sql("create table table_drop_columns_fail (" +
        "name string, id string, country string) stored as carbondata")
    sql("ALTER TABLE table_drop_columns_fail ADD COLUMNS(arr1 array<string>)")
    sql("insert into table_drop_columns_fail values( 'xx', '1', 'china', array('hello', 'world') )")
    sql("drop index if exists tdcf_index_1 on table_drop_columns_fail")
    sql("create index tdcf_index_1 on table table_drop_columns_fail(arr1,country) as 'carbondata'")
    sql("drop index if exists tdcf_index_2 on table_drop_columns_fail")
    sql("create index tdcf_index_2 on table table_drop_columns_fail(id,name) as 'carbondata'")
    val exception1 = intercept[Exception] {
      sql("ALTER TABLE table_drop_columns_fail DROP COLUMNS(arr1) ")
    }
    val exceptionMessage1 =
      "operation failed for default.table_drop_columns_fail: Alter table drop column operation " +
      "failed: The provided column(s) are present in index table. Please drop the index table " +
      "[tdcf_index_1] first and then retry the drop column operation"
    assert(exception1.getMessage.contains(exceptionMessage1))
    val exception2 = intercept[Exception] {
      sql("ALTER TABLE table_drop_columns_fail DROP COLUMNS(id,name) ")
    }
    val exceptionMessage2 =
      "operation failed for default.table_drop_columns_fail: Alter table drop column operation " +
      "failed: The provided column(s) are present in index table. Please drop the index table " +
      "[tdcf_index_2] first and then retry the drop column operation"
    assert(exception2.getMessage.contains(exceptionMessage2))
    val columns = sql("desc table table_drop_columns_fail").collect()
    assert(columns.size == 4)
  }

  test("test create secondary index global sort after insert") {
    sql("create table table1 (name string, id string, country string) stored as carbondata")
    sql("insert into table1 select 'xx', '2', 'china' union all select 'xx', '1', 'india'")
    sql("create index table1_index on table table1(id, country) as 'carbondata' properties" +
        "('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    checkAnswer(sql("select id, country from table1_index"),
      Seq(Row("1", "india"), Row("2", "china")))
    // check for valid sort_scope
    checkExistence(sql("describe formatted table1_index"), true, "Sort Scope global_sort")
    // check the invalid sort scope
    assert(intercept[MalformedCarbonCommandException](sql(
      "create index index_2 on table table1(id, country) as 'carbondata' properties" +
      "('sort_scope'='tim_sort', 'Global_sort_partitions'='3')"))
      .getMessage
      .contains("Invalid SORT_SCOPE tim_sort"))
    // check for invalid global_sort_partitions
    assert(intercept[MalformedCarbonCommandException](sql(
      "create index index_2 on table table1(id, country) as 'carbondata' properties" +
      "('sort_scope'='global_sort', 'Global_sort_partitions'='-1')"))
      .getMessage
      .contains("Table property global_sort_partitions : -1 is invalid"))
  }

  test ("test SI with column meta cache") {
    val timeStampFormat = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")
    try {
      sql("drop table if exists uniqdataTable")
      sql("CREATE table uniqdataTable (empno int, empname String, designation String, " +
        "doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        "deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        " attendance int, utilization int,salary int) STORED AS carbondata " +
        "TBLPROPERTIES('COLUMN_META_CACHE'='projectjoindate')")
      sql("create index uniqdataindex1 on table uniqdataTable (projectjoindate) AS 'carbondata'")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdataTable " +
        "OPTIONS('DELIMITER'=',')")

      checkAnswer(sql("select max(to_date(projectjoindate)),min(to_date(projectjoindate))," +
        "count(to_date(projectjoindate)) from uniqdataTable where to_date(projectjoindate)=" +
        "'2007-02-17' or to_date(projectjoindate)='2011-01-29' union select " +
        "max(to_date(projectjoindate)), min(to_date(projectjoindate))," +
        "count(to_date(projectjoindate)) from uniqdataTable where to_date(projectjoindate)=" +
        "'2007-02-17' or to_date(projectjoindate)='2011-01-29'"), Seq(Row(Date
        .valueOf("2011-01-29"), Date.valueOf("2007-02-17"), 2)))
      sql("drop table if exists uniqdataTable")
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timeStampFormat)
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
          CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    }
  }

  test("test create secondary index global sort before insert") {
    sql("create table table11 (name string, id string, country string) stored as carbondata")
    sql("create index table11_index on table table11(id, country) as 'carbondata' properties" +
        "('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    sql("insert into table11 select 'xx', '2', 'china' union all select 'xx', '1', 'india'")
    checkAnswer(sql("select id, country from table11_index"),
      Seq(Row("1", "india"), Row("2", "china")))
    // check for valid sort_scope
    checkExistence(sql("describe formatted table11_index"), true, "Sort Scope global_sort")
  }

  test("test create secondary index global sort on partition table") {
    sql("create table partition_carbon_table (" +
        "name string, id string, country string) PARTITIONED BY(dateofjoin " +
      "string) stored as carbondata")
    // create SI before the inserting the data
    sql("create index partition_carbon_table_index on table partition_carbon_table(" +
        "id, country) as 'carbondata' properties" +
        "('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    sql("insert into partition_carbon_table select 'xx', '2', 'china', '2020' " +
        "union all select 'xx', '1', 'india', '2021'")
    checkAnswer(sql("select id, country from partition_carbon_table_index"),
      Seq(Row("1", "india"), Row("2", "china")))
    // check for valid sort_scope
    checkExistence(sql("describe formatted partition_carbon_table_index"),
      true, "Sort Scope global_sort")
    sql("drop index partition_carbon_table_index on partition_carbon_table")
    // create SI after the inserting the data
    sql("create index partition_carbon_table_index on table partition_carbon_table(" +
        "id, country) as 'carbondata' properties" +
        "('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    checkAnswer(sql("select id, country from partition_carbon_table_index"),
      Seq(Row("1", "india"), Row("2", "china")))
    // check for valid sort_scope
    checkExistence(sql("describe formatted partition_carbon_table_index"),
      true,
      "Sort Scope global_sort")
  }

  test("test array<string> and string as index columns on secondary index with global sort") {
    sql(
      "create table complextable (id string, country array<string>, name string) stored as " +
      "carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b' union all select 2, array" +
        "('pak', 'india', 'china'), 'v' ")
    sql("drop index if exists complextable_index_1 on complextable")
    sql("create index complextable_index_1 on table complextable(country, name) " +
        "as 'carbondata' properties('sort_scope'='global_sort', 'Global_sort_partitions'='3')")
    checkAnswer(sql("select country,name from complextable_index_1"),
      Seq(Row("china", "b"), Row("china", "v"), Row("india", "v"), Row("pak", "v"), Row("us", "b")))
    // check for valid sort_scope
    checkExistence(sql("describe formatted complextable_index_1"), true, "Sort Scope global_sort")
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from si_altercolumn"), Seq(Row(1)))
  }

  test("test create secondary index when all records are deleted from table") {
    sql("create table delete_records (a string,b string) STORED AS carbondata")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("delete from delete_records where a='k'").collect()
    sql("alter table delete_records compact 'minor'")
    sql("create index dr_index1 on table delete_records(b) AS 'carbondata'")
    checkAnswer(sql("select count(*) from dr_index1"), Row(0))
  }

  test("test secondary index data after parent table rename") {
    sql("drop index if exists m_index21 on maintable")
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string, c int) STORED AS carbondata")
    sql("insert into maintable values('k','x',2)")
    sql("insert into maintable values('k','r',1)")
    sql("create index m_index21 on table maintable(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable where c>1"), Seq(Row("k", "x", 2)))
    sql("ALTER TABLE maintable RENAME TO maintableeee")
    checkAnswer(sql("select * from maintableeee where c>1"), Seq(Row("k", "x", 2)))
  }

  test("test secondary index with cache_level as blocklet") {
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata")
    sql("insert into maintable2 values('k','x',2)")
    sql("create index m_indextable2 on table maintable2(b) AS 'carbondata'")
    sql("ALTER TABLE maintable2 SET TBLPROPERTIES('CACHE_LEVEL'='BLOCKLET')")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
  }

  test("test secondary index with cache_level as blocklet on partitioned table") {
    sql("create table partitionTable (" +
        "a string,b string) partitioned by (c int) STORED AS carbondata")
    sql("insert into partitionTable values('k','x',2)")
    sql("create index p_indextable on table partitionTable(b) AS 'carbondata'")
    sql("ALTER TABLE partitionTable SET TBLPROPERTIES('CACHE_LEVEL'='BLOCKLET')")
    checkAnswer(sql("select * from partitionTable where b='x'"), Seq(Row("k", "x", 2)))
    sql("drop table partitionTable")
  }

  test("validate column_meta_cache and cache_level on SI table") {
    sql("create table column_meta_cache(" +
        "c1 String, c2 String, c3 int, c4 double) STORED AS carbondata")
    sql("create index cmc_indexCache on table column_meta_cache(c2,c1) " +
        "AS 'carbondata' PROPERTIES('COLUMN_meta_CachE'='c2','cache_level'='BLOCK')")
    assert(isExpectedValueValid("default", "cmc_indexCache", "column_meta_cache", "c2"))
    assert(isExpectedValueValid("default", "cmc_indexCache", "cache_level", "BLOCK"))
    // set invalid values for SI table for column_meta_cache and cache_level and verify
    intercept[MalformedCarbonCommandException] {
      sql("create index cmc_indexCache1 on table column_meta_cache(c2) " +
          "AS 'carbondata' PROPERTIES('COLUMN_meta_CachE'='abc')")
    }
    intercept[MalformedCarbonCommandException] {
      sql("create index cmc_indexCache1 on table column_meta_cache(c2) " +
          "AS 'carbondata' PROPERTIES('cache_level'='abc')")
    }
    intercept[Exception] {
      sql("Alter table cmc_indexCache SET TBLPROPERTIES('column_meta_cache'='abc')")
    }
    intercept[Exception] {
      sql("Alter table cmc_indexCache SET TBLPROPERTIES('CACHE_LEVEL'='abc')")
    }
    // alter table to unset these properties on SI table
    sql("Alter table cmc_indexCache UNSET TBLPROPERTIES('column_meta_cache')")
    var descResult = sql("describe formatted cmc_indexCache")
    checkExistence(descResult, false, "COLUMN_META_CACHE")
    sql("Alter table cmc_indexCache UNSET TBLPROPERTIES('cache_level')")
    descResult = sql("describe formatted cmc_indexCache")
    checkExistence(descResult, true, "Min/Max Index Cache Level")
    // alter SI table to set the properties again
    sql("Alter table cmc_indexCache SET TBLPROPERTIES('column_meta_cache'='c1')")
    assert(isExpectedValueValid("default", "cmc_indexCache", "column_meta_cache", "c1"))
    // set empty value for column_meta_cache
    sql("Alter table cmc_indexCache SET TBLPROPERTIES('column_meta_cache'='')")
    assert(isExpectedValueValid("default", "cmc_indexCache", "column_meta_cache", ""))
    // set cache_level to blocklet
    sql("Alter table cmc_indexCache SET TBLPROPERTIES('cache_level'='BLOCKLET')")
    assert(isExpectedValueValid("default", "cmc_indexCache", "cache_level", "BLOCKLET"))
  }

  test("test parallel load of SI to main table") {
    sql("CREATE table uniqdata (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql("create index ud_index1 on table uniqdata (workgroupcategoryname) AS 'carbondata'")
    val indexTable = CarbonEnv.getCarbonTable(Some("default"), "ud_index1")(sqlContext.sparkSession)
    val carbontable = CarbonEnv.getCarbonTable(Some("default"), "uniqdata")(sqlContext.sparkSession)
    val details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
    val failSegments = List("3", "4")
    sql(s"""set carbon.si.repair.limit = 2""")
    var loadMetadataDetailsList = Array[LoadMetadataDetails]()
    details.foreach{detail =>
      if (failSegments.contains(detail.getLoadName)) {
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
    sql(s"CLEAN FILES FOR TABLE ud_index1  OPTIONS('stale_inprogress'='true','force'='true')")
        .show()
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED)

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdata " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    val count1 = sql("select * from uniqdata where workgroupcategoryname = 'developer'").count()
    val df1 = sql("select * from uniqdata where workgroupcategoryname = 'developer'")
      .queryExecution.sparkPlan
    val count2 = sql("select * from uniqdata where NI(workgroupcategoryname = 'developer')").count()
    val df2 = sql("select * from uniqdata where NI(workgroupcategoryname = 'developer')")
      .queryExecution.sparkPlan
    sql(s"""set carbon.si.repair.limit = 1""")
    assert(count1 == count2)
    assert(isFilterPushedDownToSI(df1))
    assert(!isFilterPushedDownToSI(df2))
  }

  test("test drop table on index table") {
    sql("CREATE table uniqdataTable (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql(
      "create index uniqdataindex1 on table uniqdataTable (workgroupcategoryname) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE uniqdataTable " +
        "OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    val errorMessage = intercept[Exception] {
      sql("drop table uniqdataindex1")
    }.getMessage
    assert(errorMessage.contains("Drop table is not permitted on Index Table"))
  }

  test("test SI creation on two tables with the same name") {
    sql("CREATE table uniqdataTable1 (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql("create index uniqdataidxtable on table uniqdataTable1 (" +
        "workgroupcategoryname) AS 'carbondata'")

    sql("CREATE table uniqdataTable2 (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    val errorMessage = intercept[Exception] {
      sql("create index uniqdataidxtable on table uniqdataTable2 (" +
          "workgroupcategoryname) AS 'carbondata'")
    }.getMessage
    assert(errorMessage.contains(
      "Index [uniqdataidxtable] already exists under database [default]"))
  }

  test("test date type with SI table") {
    sql("drop index if exists m_index_date on maintable3")
    sql("drop table if exists maintable3")
    sql("CREATE TABLE maintable3 (" +
        "id int,name string,salary float,dob date,address string) STORED AS carbondata")
    sql("insert into maintable3 values(" +
        "1,'aa',23423.334,'2009-09-06','df'),(1,'aa',23423.334,'2009-09-07','df')")
    sql("insert into maintable3 select 2,'bb',4454.454,'2009-09-09','bang'")
    sql("create index m_index_date on table maintable3(dob) AS 'carbondata'")
    val df = sql("select id,name,dob from maintable3 where dob = '2009-09-07'")
    assert(isFilterPushedDownToSI(df.queryExecution.sparkPlan))
    checkAnswer(df, Seq(Row(1, "aa", java.sql.Date.valueOf("2009-09-07"))))
  }

  test("test SI order by limit push down") {
    sql("drop index if exists table2_index1 on table2")
    sql("drop index if exists table2_index2 on table2")
    sql("drop table if exists table2")
    sql("CREATE TABLE `table2` (`imsi` STRING, `carno` STRING, `longitude` STRING, `city` " +
      "STRING, `starttime` BIGINT, `endtime` BIGINT) STORED AS carbondata TBLPROPERTIES" +
      "('sort_scope'='global_sort','sort_columns'='starttime')")
    sql("create index table2_index1 on table table2(carno, longitude, starttime) as 'carbondata'")
    sql("create index table2_index2 on table table2(city) as 'carbondata'")
    sql("insert into table2 select 'aa','ka14','ll','abc',23,24 ")
    sql("insert into table2 select 'aa','ka14','ll','xyz',25,26 ")

    // Allow order by and limit pushdown as all the filter and order by column is in SI
    // a. For selected projections
    var plan = sql(
      "explain SELECT imsi FROM table2 WHERE  CARNO = 'ka14' AND LONGITUDE is not null  ORDER BY " +
      "STARTTIME LIMIT 1")
      .collect()(0)
      .toString()
    assert(StringUtils.countMatches(plan, "TakeOrderedAndProject") == 2)

    // b. For all projections
    plan = sql(
      "explain SELECT * FROM table2 WHERE  CARNO = 'ka14' AND LONGITUDE is not null  ORDER BY " +
      "STARTTIME LIMIT 1")
      .collect()(0)
      .toString()
    assert(StringUtils.countMatches(plan, "TakeOrderedAndProject") == 2)

    // Don't allow orderby and limit pushdown as order by column is not an SI column
    plan = sql(
      "explain SELECT * FROM table2 WHERE  CARNO = 'ka14' AND LONGITUDE is not null  ORDER BY " +
      "endtime LIMIT 1")
      .collect()(0)
      .toString()
    assert(StringUtils.countMatches(plan, "TakeOrderedAndProject") == 1)

    // Don't allow orderby and limit pushdown as filter column is not an SI column
    plan = sql(
      "explain SELECT * FROM table2 WHERE  imsi = 'aa' AND LONGITUDE is not null  ORDER BY " +
      "STARTTIME LIMIT 1")
      .collect()(0)
      .toString()
    assert(StringUtils.countMatches(plan, "TakeOrderedAndProject") == 1)

    // just NotEqual to should not be pushed down to SI without order by
    plan = sql(
      "explain SELECT * FROM table2 WHERE  CARNO != 'ka14' ")
      .collect()(0)
      .toString()
    assert(!plan.contains("table2_index1"))

    // NotEqual to should not be pushed down to SI without order by in case of multiple tables also
    plan = sql(
      "explain SELECT * FROM table2 WHERE  CARNO = 'ka14' and CITY != 'ddd' ")
      .collect()(0)
      .toString()
    assert(!plan.contains("table2_index2") && plan.contains("table2_index1"))

    sql("drop table table2")
  }

  test("test SI creation with special char column") {
    sql("create table special_char(`i#d` string, `nam(e` string,`ci)&#@!ty` string," +
        "`a\be` int, `ag!e` float, `na^me1` Decimal(8,4)) stored as carbondata")
    sql("create index special_char_index on table special_char(`nam(e`) as 'carbondata'")
    sql("insert into special_char values('1','joey','hud', 2, 2.2, 2.3456)")
    val plan =
      sql("explain select * from special_char where `nam(e` = 'joey'").collect()(0).toString()
    assert(plan.contains("special_char_index"))
    val df = sql("describe formatted special_char_index").collect()
    assert(df.exists(_.get(0).toString.contains("nam(e")))
  }

  test("test alter table set streaming for table having SI") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata ")
    sql("insert into maintable2 values('k','x',2)")
    sql("create index m_indextable on table maintable2(b) AS 'carbondata'")
    var exeption = intercept[RuntimeException] {
      sql("ALTER TABLE maintable2 SET TBLPROPERTIES('streaming'='true')")
    }
    assert(exeption.getMessage.contains("Set streaming table is " +
      "not allowed for tables which are having index(s)."))

    exeption = intercept[RuntimeException] {
      sql("ALTER TABLE m_indextable SET TBLPROPERTIES('streaming'='true')")
    }
    assert(exeption.getMessage.contains("Set streaming table is not allowed on the index table."))
    sql("drop table if exists maintable2")
  }

  test("test change data type from string to long string of SI column") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int) STORED AS carbondata ")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    sql("insert into maintable values('k','x',2)")
    val exception = intercept[RuntimeException] {
      sql("ALTER TABLE maintable SET TBLPROPERTIES('long_String_columns'='b')")
    }
    assert(exception.getMessage.contains("Cannot Alter column b to " +
      "Long_string_column, as the column exists in a secondary index with name " +
      "indextable. LONG_STRING_COLUMNS is not allowed on secondary index."))
    sql("drop table if exists maintable")
  }

  test("test SI on fact table with columnDrift enabled") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int,d int) STORED AS carbondata ")
    sql("insert into maintable values('k','d',2,3)")
    sql("alter table maintable set tblproperties('sort_columns'='c,d','sort_scope'='local_sort')")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    sql("insert into maintable values('k','x',2,4)")
    val dataFrame = sql("select * from maintable where b='x'")
    checkAnswer(dataFrame, Seq(Row("k", "x", 2, 4)))
    TestSecondaryIndexUtils.isFilterPushedDownToSI(dataFrame.queryExecution.sparkPlan)
    sql("drop table if exists maintable")
  }

  test("test SI with change of sort column") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata " +
      "TBLPROPERTIES('sort_scope'='global_sort','sort_columns'='b,a')")
    sql("insert into maintable2 values('k','x',2)")
    sql("create index m_indextable2 on table maintable2(b) AS 'carbondata'")
    sql("ALTER TABLE maintable2 SET TBLPROPERTIES('sort_columns'='b')")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    sql("ALTER TABLE m_indextable2 SET " +
      "TBLPROPERTIES('sort_scope'='global_sort','sort_columns'='b')")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    sql("drop table if exists maintable2")
  }

  test("test SI with change subset of sort column on which SI is created") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata " +
      "TBLPROPERTIES('sort_scope'='global_sort','sort_columns'='b')")
    sql("insert into maintable2 values('k','x',2)")
    sql("create index m_indextable2 on table maintable2(b, a) AS 'carbondata'")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    sql("ALTER TABLE m_indextable2 SET " +
      "TBLPROPERTIES('sort_scope'='global_sort','sort_columns'='a')")
    sql("insert into maintable2 values('k1','c',3)")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    sql("drop table if exists maintable2")
  }

  test("test SI with MV on the same column") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata ")
    sql("drop materialized view if exists view1")
    sql(s"CREATE MATERIALIZED VIEW view1 AS SELECT b, c FROM maintable2")
    sql("create index m_indextable on table maintable2(b) AS 'carbondata'")
    sql("insert into maintable2 values('k','x',2)")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    val result = sql("show materialized views on table maintable2").collectAsList()
    assert(result.get(0).get(1).toString.equalsIgnoreCase("view1"))
    sql("drop table if exists maintable2")
  }

  test("test SI with measure column when include in sort columns") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata " +
      "TBLPROPERTIES('sort_scope'='global_sort','sort_columns'='c')")
    sql("insert into maintable2 values('k','x',2)")
    sql("create index m_indextable2 on table maintable2(c) AS 'carbondata'")
    checkAnswer(sql("select c from m_indextable2"), Seq(Row(2)))
    sql("drop table if exists maintable2")
  }

  test("test SI with lucene and bloom on the same column") {
    createAndInsertDataIntoTable()
    sql("create index m_indextable on table maintable2(b) AS 'carbondata'")
    sql("create index m_bloomindex on table maintable2(b) AS 'bloomfilter'")
    sql("create index m_luceneindex on table maintable2(b) AS 'lucene'")
    checkAnswer(sql("select * from maintable2 where b='x'"), Seq(Row("k", "x", 2)))
    checkExistence(sql("show indexes on table maintable2"),
      true, "m_indextable", "m_bloomindex", "m_luceneindex")
    sql("drop index m_luceneindex on maintable2")
    sql("drop index m_bloomindex on maintable2")
    sql("drop index m_indextable on maintable2")
    sql("drop table if exists maintable2")
  }

  test("test si with limit with index on all filter column") {
    createAndInsertDataIntoTable()
    sql("create index m_indextable on table maintable2(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable2 where b='x' limit 1"), Seq(Row("k", "x", 2)))
    checkAnswer(sql("select a, c from maintable2 where b='x' limit 1"), Seq(Row("k", 2)))
    sql("insert into maintable2 values('ab','cd',20)")
    sql("delete from maintable2 where b='x'")
    checkAnswer(sql("select * from maintable2 where b='cd' limit 1"), Seq(Row("ab", "cd", 20)))
    checkAnswer(sql("select a, c from maintable2 where b='cd' limit 1"), Seq(Row("ab", 20)))
    sql("drop table if exists maintable2")
  }

  test("test SI with add column and filter on default value") {
    createAndInsertDataIntoTable()
    sql("alter table maintable2 add columns (stringfield string) " +
      "TBLPROPERTIES('DEFAULT.VALUE.stringfield'='val')")
    sql("insert into maintable2 values('ab','cd',3,'ef')")
    sql("create index m_indextable on table maintable2(stringfield) AS 'carbondata'")
    checkAnswer(sql("select stringfield from m_indextable"), Seq(Row("val"), Row("ef")))
    sql("drop table if exists maintable2")
  }

  test ("test drop column on SI table") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c string) STORED AS carbondata ")
    sql("create index m_indextable on table maintable2(b,c) AS 'carbondata'")
    val errorMessage = intercept[Exception] {
      sql("alter table m_indextable drop columns(c)")
    }
    assert(errorMessage.getMessage.contains("alter table drop column " +
      "is not supported for index table"))
    sql("drop table if exists maintable2")
  }

  test("test SI to prune when parent and child table" +
      " segments are not in sync with spark plan rewrite") {
    createTableWithIndexAndLoadData("false")
    val df = sql("select * from maintable where b='cd'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df))
    sql("drop table if exists maintable")
  }

  def createTableWithIndexAndLoadData(coarseGrainIndex: String): Unit = {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
        .addProperty(CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX, coarseGrainIndex)
    try {
      sql("drop table if exists maintable")
      sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
      sql("create index m_indextable on table maintable(b,c) AS 'carbondata'")
      sql("insert into maintable values('ab','cd','ef')")
      sql("insert into maintable values('bc','cedfd','fg')")
      sql("insert into maintable values('bcde','cd','fgh')")
      sql("DELETE FROM TABLE m_indextable WHERE SEGMENT.ID IN(0,1)")
      sql("clean files for table m_indextable options('force'='true')")
      assert(sql("show segments on m_indextable").collect().length == 1)
      checkExistence(sql("show indexes on maintable"),
        true, "m_indextable", "enabled")
      checkAnswer(sql("select * from maintable where b='cd'"),
        Seq(Row("ab", "cd", "ef"), Row("bcde", "cd", "fgh")))
    } finally {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
          .addProperty(CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX,
            CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX_DEFAULT)
    }
  }

  test("test SI to prune when parent and child table" +
      " segments are not in sync with SI as datamap") {
    createTableWithIndexAndLoadData("true")
    sql("drop table if exists maintable")
  }

  test("test multiple SI to prune with non-default database " +
      "when parent and child table segments are not in sync with SI plan rewrite") {
    tableWithMultipleSIAndSegmentMissmatch("false");
    val df = sql("select * from test.table1 where c2='a1' and c3='b'").queryExecution.sparkPlan
    assert(isFilterPushedDownToSI(df))
    sql("drop database if exists test cascade")
  }

  test("test multiple SI to prune with non-default database " +
      "when parent and child table segments are not in sync with SI as datamap") {
    tableWithMultipleSIAndSegmentMissmatch("true");
    sql("drop database if exists test cascade")
  }

  def tableWithMultipleSIAndSegmentMissmatch(coarseGrainIndex: String): Unit = {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED, "true")
        .addProperty(CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX, coarseGrainIndex)
    sql("drop database if exists test cascade")
    sql("create database test")
    sql("use test")
    try {
      sql("drop table if exists table1")
      sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata")
      for (i <- 0 until 5) {
        sql(s"insert into table1 values(${i + 1},'a$i','b')")
      }
      sql("update table1 set (c2) = ('a1') where c1=3")
      sql("create index idx1 on table table1(c3) as 'carbondata'")
      sql("create index idx2 on table table1(c2) as 'carbondata'")
      sql("DELETE FROM TABLE idx1 WHERE SEGMENT.ID IN(0,1)")
      sql("DELETE FROM TABLE idx2 WHERE SEGMENT.ID IN(3,4,0)")
      sql("clean files for table idx1 options('force'='true')")
      sql("clean files for table idx2 options('force'='true')")
      assert(sql("show segments on idx1").collect().length == 3)
      assert(sql("show segments on idx2").collect().length == 2)
      checkAnswer(sql("select * from table1 where c2='a1' and c3='b'"),
        Seq(Row(2, "a1", "b"), Row(3, "a1", "b")))
    } finally {
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED,
            CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED_DEFAULT)
          .addProperty(CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX,
            CarbonCommonConstants.CARBON_COARSE_GRAIN_SECONDARY_INDEX_DEFAULT)
      sql("use default")
    }
  }

  test("test SI when carbon data handler will through exception") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c string) STORED AS carbondata ")
    sql("insert into maintable2 values('ab','cd','ef')")
    val mock = TestSecondaryIndexUtils.mockDataHandler()
    val ex = intercept[Exception] {
      sql("create index m_indextable on table maintable2(b,c) AS 'carbondata'")
    }
    mock.tearDown()
    assert(ex.getMessage.contains("Problem loading data while creating secondary index:"))
  }

  test("test SI with carbon.use.local.dir as false") {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR, "false")
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c string) STORED AS carbondata ")
    sql("create index m_indextable on table maintable2(b) AS 'carbondata'")
    sql("insert into maintable2 values('ab','cd','ef')")
    checkAnswer(sql("select * from maintable2 where b='cd'"), Row("ab", "cd", "ef"))
    sql("drop table if exists maintable2")
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR,
          CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR_DEFAULT)
  }

  test("test SI when segment lock fail") {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c string) STORED AS carbondata ")
    sql("insert into maintable2 values('ab','cd','ef')")
    val mock: MockUp[AbstractCarbonLock] = new MockUp[AbstractCarbonLock]() {
      @Mock
      def lockWithRetries(retries: Int, retryInterval: Int): Boolean = {
        if (retries == 1 && retryInterval == 0) {
          false
        } else {
          true
        }
      }
    }
    sql("create index m_indextable on table maintable2(b,c) AS 'carbondata'")
    mock.tearDown()
    checkExistence(sql("show indexes on table maintable2"),
      true, "m_indextable", "enabled")
    assert(sql("show segments on m_indextable").collect().isEmpty)
    sql("drop table if exists maintable2")
  }

  test("test SI creation with different value of si creation thread") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
    sql("insert into maintable values ('aa', 'bb', 'cc')")
    // number of threads are more than max value
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS, "51")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable where b='bb'"), Row("aa", "bb", "cc"))
    sql("drop index if exists indextable on maintable")

    // number of threads are less than default value
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS, "0")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable where b='bb'"), Row("aa", "bb", "cc"))
    sql("drop index if exists indextable on maintable")

    // invalid number for number of threads
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS, "invalid")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    checkAnswer(sql("select * from maintable where b='bb'"), Row("aa", "bb", "cc"))
    sql("drop index if exists indextable on maintable")
    sql("drop table if exists maintable")
  }

  test("test SI with donot push down not equal to filter with Cast") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
    sql("insert into maintable values ('aa', '3', 'cc')")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    val df1 = sql("select * from maintable where b!=2")
    val df2 = sql("select * from maintable where b!='2'")
    if (isFilterPushedDownToSI(df1.queryExecution.sparkPlan) &&
        isFilterPushedDownToSI(df2.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    sql("drop table if exists maintable")
  }

  def createAndInsertDataIntoTable(): Unit = {
    sql("drop table if exists maintable2")
    sql("create table maintable2 (a string,b string,c int) STORED AS carbondata ")
    sql("insert into maintable2 values('k','x',2)")
  }

  override def afterAll {
    dropIndexAndTable()
  }

  private def dropIndexAndTable(): Unit = {
    sql("drop index if exists si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    sql("drop index if exists tdc_index_1 on table_drop_columns")
    sql("drop table if exists table_drop_columns")
    sql("drop index if exists tdcf_index_1 on table_drop_columns_fail")
    sql("drop table if exists table_drop_columns_fail")
    sql("drop index if exists table1_index on table1")
    sql("drop table if exists table1")
    sql("drop index if exists table11_index on table11")
    sql("drop table if exists table11")
    sql("drop index if exists partition_carbon_table_index on partition_carbon_table")
    sql("drop table if exists partition_carbon_table")
    sql("drop index if exists complextable_index_1 on complextable")
    sql("drop table if exists complextable")
    sql("drop index if exists dr_index1 on delete_records")
    sql("drop table if exists delete_records")
    sql("drop index if exists m_index21 on maintable")
    sql("drop table if exists maintableeee")
    sql("drop index if exists m_indextable2 on maintable2")
    sql("drop table if exists maintable2")
    sql("drop index if exists p_indextable on partitionTable")
    sql("drop table if exists partitionTable")
    sql("drop index if exists cmc_indexCache on column_meta_cache")
    sql("drop index if exists cmc_indexCache1 on column_meta_cache")
    sql("drop table if exists column_meta_cache")
    sql("drop index if exists ud_index1 on uniqdata")
    sql("drop table if exists uniqdata")
    sql("drop index if exists uniqdataindex1 on uniqdataTable")
    sql("drop table if exists uniqdataTable")
    sql("drop index if exists uniqdataidxtable on uniqdataTable1")
    sql("drop table if exists uniqdataTable1")
    sql("drop table if exists uniqdataTable2")
    sql("drop index if exists m_index_date on maintable3")
    sql("drop table if exists maintable3")
    sql("drop index if exists table2_index1 on table2")
    sql("drop index if exists table2_index2 on table2")
    sql("drop table if exists table2")
    sql("drop index if exists special_char_index on special_char")
    sql("drop table if exists special_char")
  }
}

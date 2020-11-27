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

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.format.TableInfo

/**
 * test cases for testing create index table
 */
class TestCreateIndexTable extends QueryTest with BeforeAndAfterAll {

  var path: String = s"/tmp/parquet${UUID.randomUUID().toString}"

  override def beforeAll {
    dropIndexAndTable()
    sql("CREATE table carbon (empno string, empname String, " +
        "designation String, doj Timestamp, workgroupcategory string, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata")
    sql("CREATE TABLE stream_si(c1 string,c2 int,c3 string,c5 string) " +
        "STORED AS carbondata TBLPROPERTIES ('streaming' = 'true')")
    sql("CREATE TABLE part_si(c1 string,c2 int,c3 string,c5 string) PARTITIONED BY (c6 string)" +
        "STORED AS carbondata ")
  }

  test("test create index table with no parent table") {
    try {
      sql("create index index_without_parentTable on table carbon_dummy (empname) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    } finally {
      sql("drop index if exists index_without_parentTable on carbon")
    }
  }

  test("test create index table on measure column") {
    try {
      sql("create index index_on_measure on table carbon (salary) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.equalsIgnoreCase(
          "Secondary Index is not supported for measure column : salary"))
    } finally {
      sql("drop index if exists index_on_measure on carbon")
    }
  }

  test("test create index table on dimension,measure column") {
    try {
      sql("create index index_on_measure on table carbon (empname, salary) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.equalsIgnoreCase(
          "Secondary Index is not supported for measure column : salary"))
    } finally {
      sql("drop index if exists index_on_measure on carbon")
    }

  }

  test("Test case insensitive create & drop index command") {
    sql("drop INDEX if exists index_case_insensitive ON dEfaUlt.caRbon")
    sql("CREATE INDEX index_case_insensitive ON TABLE dEfaUlt.cArBon (" +
        "workgroupcategory) AS 'carbondata'")
    sql("drop INDEX index_case_insensitive ON CarBOn")
  }

  test("test create index table with indextable col size > parent table key col size") {
    try {
      sql("drop table if exists parentTable")
      sql("create table parentTable(a string, b String) STORED AS carbondata")
      sql("create index indexOnCarbon on table parentTable (a,b,positionid) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.equalsIgnoreCase("Number of columns in " +
          "Index table cannot be more than number of key columns in Source table"))
    } finally {
      sql("drop index if exists indexOnCarbon on parentTable")
    }
  }

  test("test create index table with duplicate column") {
    try {
      sql("create index index_on_measure on table carbon (empno,empname,designation,doj," +
          "workgroupcategory,empno) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.equalsIgnoreCase("Duplicate column name found : empno"))
    } finally {
      sql("drop index if exists index_on_measure on carbon")
    }
  }

  test("test create index table on more than one column") {
    try {
      sql("drop index if exists index_more_columns on carbon")
      sql("create index index_more_columns on table carbon (" +
          "doj,designation,deptname) AS 'carbondata'")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    } finally {
      sql("drop index if exists index_more_columns on carbon")
    }
  }

  test("test create index table with invalid column") {
    try {
      sql("drop index if exists index_with_invalid_column on carbon")
      sql("create index index_with_invalid_column on table carbon (abc) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("one or more specified index cols either " +
          "does not exist or not a key column or complex column in table default.carbon"))
    } finally {
      sql("drop index if exists index_with_invalid_column on carbon")
    }
  }

  test("test create index table with index table name containing invalid characters") {
    try {
      sql("create index #$%^^!@#$*() on table carbon (abc) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    }
  }

  test("test create index table with index table name same as fact table") {
    try {
      sql("create index carbon on table carbon (empname) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    }
  }

  test("test create index table on first dimension column of fact table") {
    try {
      sql("drop index if exists index_first_column on carbon")
      sql("create index index_first_column on table carbon (empno) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    } finally {
      sql("drop index if exists index_first_column on carbon")
    }
  }

  test("test create index table") {
    try {
      sql("drop index if exists index_1 on carbon")
      sql("create index index_1 on table carbon (workgroupcategory) AS 'carbondata'")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    } finally {
      sql("drop index if exists index_1 on carbon")
    }
  }

  test("test 2 create index with same name") {
    try {
      sql("drop index if exists index_1 on carbon")
      sql("create index index_1 on table carbon (workgroupcategory) AS 'carbondata'")
      sql("create index index_1 on table carbon (workgroupcategory) AS 'carbondata'")
      assert(false)
    } catch {
      case ex: Exception =>
        assert(true)
    } finally {
      sql("drop index if exists index_1 on carbon")
    }
  }


  /* test("test secondary index with delete from tablename -> should fail") {
    sql("create index indexdelete on table carbon (deptno) AS 'carbondata'")
    try {
      sql("delete from carbon where empname='ayushi'")
      assert(false)
    } catch  {
      case ex: Exception => assert(ex.getMessage.equalsIgnoreCase(
          "Delete is not permitted on table that contains secondary index [default.carbon]. Drop
          all indexes and retry"))
    }
    sql("drop index if exists indexdelete on carbon")
  } */

  /* test("test secondary index with update tablename -> should fail") {
    sql("create index indexupdate on table carbon (deptno) AS 'carbondata'")
    try {
      sql("update carbon set (salary) = (salary + 1)")
      assert(false)
    } catch  {
      case ex: Exception => assert(ex.getMessage.equalsIgnoreCase(
          "Update is not permitted on table that contains secondary index [default.carbon]. Drop
          all indexes and retry"))
    }
    sql("drop index if exists indexupdate on carbon")
  } */

  test("test secondary index with insert into tablename -> should fail") {
    try {
      sql("drop table if exists TCarbonSource")
      sql("drop table if exists TCarbon")
      // scalastyle:off lineLength
      sql("create table TCarbonSource (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
      sql(s"LOAD DATA INPATH '$resourcesPath/100_olap.csv' INTO table TCarbonSource options ('DELIMITER'=',', 'FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointDescription,gamePointId,contractNumber', 'bad_records_logger_enable'='false','bad_records_action'='FORCE')")
      sql("create table TCarbon (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY Decimal(30,10), Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string,gamePointId double,contractNumber BigInt) STORED AS carbondata")
      // scalastyle:on lineLength
      sql("create index index_on_insert on table TCarbon (deviceColor) AS 'carbondata'")
      sql("insert into index_on_insert select * from TCarbonSource")
      assert(false)
    } catch {
      case ex: Exception => assert(true)
    } finally {
      sql("drop index if exists index_on_insert on TCarbon")
    }
  }

  test("test create one index and compare the results") {
    sql("drop table if exists carbontable")
    sql("CREATE table carbontable (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS CARBONDATA")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
        "TABLE carbontable OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', " +
        "'BAD_RECORDS_ACTION'='FORCE')")
    val withoutIndex =
      sql("select empno from carbontable where empname = 'ayushi' or " +
          "empname = 'krithin' or empname = 'madhan'").collect().toSeq
    sql("create index empnameindex on table carbontable (" +
      "workgroupcategoryname,empname) AS 'carbondata'")

    checkAnswer(sql("select empno from carbontable where empname = 'ayushi' or " +
                    "empname = 'krithin' or empname = 'madhan'"), withoutIndex)
    sql("drop index if exists empnameindex on carbontable")
    sql("drop table if exists carbontable")
  }

  test("test create table with column name as positionID") {
    try {
      sql("CREATE table carbontable (empno int, positionID String, " +
          "designation String, doj Timestamp, workgroupcategory int, " +
          "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
          "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
          "utilization int,salary int) STORED AS CARBONDATA " +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='empname')")
    } catch {
      case ex: Exception => assert(true)
    }
  }

  test("test create table with column name as positionReference") {
    try {
      sql("CREATE table carbontable (empno int, positionReference String, " +
          "designation String, doj Timestamp, workgroupcategory int, " +
          "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
          "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
          "utilization int,salary int) STORED AS CARBONDATA " +
          "TBLPROPERTIES('DICTIONARY_EXCLUDE'='empname')")
    } catch {
      case ex: Exception => assert(true)
    }
  }

  test("create index on temp table") {
    emptyParquetFolder()
    sql(
      "CREATE temporary table createindextemptable(id int,name string,city string,age int) using " +
      s"parquet options(path='${path}')")
    sql("insert into createindextemptable values(1,'string','string',3)")
    sql("insert into createindextemptable values(1,'string','string',3)")
    sql("insert into createindextemptable values(1,'string','string',3)")

    try {
      sql(
        "create index empnameindex on table createindextemptable (city) AS 'carbondata'")
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Operation not allowed because either table " +
          "createindextemptable doesn't exist or not a carbon table."))
    }
  }

  test("create index on temp table when carbon table exists") {
    sql(s"CREATE DATABASE if not exists temptablecheckDB")
    sql("USE temptablecheckDB")
    sql("CREATE TABLE createindextemptable1(" +
        "id int, name string, city string, age int) STORED AS CARBONDATA ")
    emptyParquetFolder()
    sql(
      "CREATE temporary table createindextemptable1(id int,name string,city string,age int) using" +
      s" parquet options(path='${path}')")
    sql("insert into createindextemptable1 values(1,'string','string',3)")
    sql("insert into createindextemptable1 values(1,'string','string',3)")
    sql("insert into createindextemptable1 values(1,'string','string',3)")

    try {
      sql("drop index if exists empnameindex on temptablecheckDB.createindextemptable1")
      sql(
        "create index empnameindex on table createindextemptable1 (city) AS 'carbondata'")
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Operation not allowed because either table " +
          "createindextemptable1 doesn't exist or not a carbon table."))
    }
    sql("insert into temptablecheckDB.createindextemptable1 select 1,'string','string',3")
    sql("insert into temptablecheckDB.createindextemptable1 select 1,'string','string',3")
    sql("insert into temptablecheckDB.createindextemptable1 select 1,'string','string',3")
    try {
      sql("create index empnameindex on table temptablecheckDB.createindextemptable1 (" +
          "city) AS 'carbondata'")
      assert(true)
    } catch {
      case e: Exception =>
        assert(false)
    } finally {
      sql("drop index if exists empnameindex on temptablecheckDB.createindextemptable1")
      sql("USE default")
    }
  }

  test("index creation on long string columns") {
    sql("drop table if exists si_table")
    sql(
      s"""
         | CREATE TABLE si_table(
         | name STRING,
         | longstr STRING
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         | 'LONG_STRING_COLUMNS'='longstr')
         | """.stripMargin)

    sql("drop index if exists temp_ind on si_table")
    val thrown = intercept[Exception] {
      sql("create index temp_ind on table si_table (longstr) AS 'carbondata'")
    }
    assert(thrown.getMessage
      .contains(
        "one or more index columns specified contains long string or binary column in table" +
          " default.si_table. SI cannot be created on long string or binary columns."))
  }

  test("drop index on temp table") {
    emptyParquetFolder()
    sql(
      "CREATE temporary table dropindextemptable(id int,name string,city string,age int) using " +
      s"parquet options(path='${path}')")
    sql("insert into dropindextemptable values(1,'string','string',3)")
    sql("insert into dropindextemptable values(1,'string','string',3)")
    sql("insert into dropindextemptable values(1,'string','string',3)")

    try {
      sql("drop index if exists empnameindex on dropindextemptable")
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  test("drop index on temp table when carbon table exists") {
    sql(s"CREATE DATABASE if not exists temptablecheckDB")
    sql("USE temptablecheckDB")
    sql("CREATE TABLE dropindextemptable1(" +
        "id int, name string, city string, age int) STORED AS CARBONDATA")
    emptyParquetFolder()
    sql(
      "CREATE temporary table dropindextemptable1(id int,name string,city string,age int) using " +
      s"parquet options(path='${path}')")
    sql("insert into dropindextemptable1 values(1,'string','string',3)")
    sql("insert into dropindextemptable1 values(1,'string','string',3)")
    sql("insert into dropindextemptable1 values(1,'string','string',3)")
    sql("insert into temptablecheckDB.dropindextemptable1 select 1,'string','string',3")
    sql("insert into temptablecheckDB.dropindextemptable1 select 1,'string','string',3")
    sql("insert into temptablecheckDB.dropindextemptable1 select 1,'string','string',3")
    sql("create index empnaindex on table temptablecheckDB.dropindextemptable1 (" +
        "city) AS 'carbondata'")
    try {
      sql("drop index if exists empnaindex on temptablecheckDB.dropindextemptable1")
      assert(true)
    } catch {
      case e: Exception =>
        assert(false)
    }
    finally {
      sql("USE default")

    }
  }

  test("test creation of index table 2 times with same name, on error drop " +
       "and create with same name again") {
    sql("DROP TABLE IF EXISTS carbon_si_same_name_test")
    sql("DROP INDEX IF EXISTS si_drop_i1 on carbon_si_same_name_test")
    // create table
    sql(
      "CREATE table carbon_si_same_name_test (empno int, empname String, designation String) " +
      "STORED AS CARBONDATA")
    // insert data
    sql("insert into carbon_si_same_name_test select 11,'arvind','lead'")
    sql("insert into carbon_si_same_name_test select 12,'krithi','TA'")
    // create index
    sql(
      "create index si_drop_i1 on table carbon_si_same_name_test (designation) AS 'carbondata'")
    val ex = intercept[Exception] {
      sql(
        "create index si_drop_i1 on table carbon_si_same_name_test (designation) AS 'carbondata'")
    }
    assert(ex.getMessage.contains("Index [si_drop_i1] already exists under database [default]"))
    sql("DROP INDEX IF EXISTS si_drop_i1 on carbon_si_same_name_test")
    sql(
      "create index si_drop_i1 on table carbon_si_same_name_test (designation) AS 'carbondata'")
    checkAnswer(sql("select designation from si_drop_i1"),
      sql("select designation from carbon_si_same_name_test"))
  }

  test("test blocking secondary Index on streaming table") {
    sql("use default")
    val ex = intercept[RuntimeException] {
      sql("""create index streamin_index on table stream_si(c3) AS 'carbondata'""").collect()
    }
    assert(ex.getMessage.contains("Parent Table  default.stream_si " +
      "is Streaming Table and Secondary index on Streaming table is not supported"))
  }

  test("test SI creation on table which doesn't exist") {
    val exception = intercept[RuntimeException] {
      sql("""create index indextable on table unknown(c) AS 'carbondata'""").show()
    }
    assert(exception.getMessage.contains("Operation not allowed because either table " +
    "unknown doesn't exist or not a carbon table."))
  }

  test("test SI creation on binary data type") {
    sql("use default")
    sql("drop table if exists carbontable")
    sql("CREATE table carbontable (empno int, empname String, " +
      "designation String, binarycol binary) STORED AS CARBONDATA")
    val exception = intercept[RuntimeException] {
      sql("CREATE INDEX indextable on carbontable(binarycol) as 'carbondata'")
    }
    assert(exception.getMessage.contains("one or more index columns specified " +
      "contains long string or binary column in table default.carbontable. " +
      "SI cannot be created on long string or binary columns."))
    sql("drop table if exists carbontable")
  }

  test("test table creation with like for index table") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int) STORED AS carbondata ")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    sql("insert into maintable values('k','x',2)")
    sql("drop table if exists targetTable")
    val exception = intercept[MalformedCarbonCommandException] {
      sql("create table targetTable like indextable")
    }
    assert(exception.getMessage.contains("Unsupported operation on SI table or MV."))
    sql("drop table if exists maintable")
  }

  test("test create index on partition column") {
    sql("insert into part_si values('dsa',1,'def','asd','fgh')")
    val exception = intercept[UnsupportedOperationException] {
      sql("create index index_on_partitionTable on table part_si (c6) AS 'carbondata'")
    }
    assert(exception.getMessage.contains("Secondary Index cannot be " +
      "created on a partition column."))
  }

  test("test create index on spatial index column") {
    sql("drop table if exists maintable")
    sql(s"""
           | CREATE TABLE maintable(
           | timevalue BIGINT,
           | longitude LONG,
           | latitude LONG) COMMENT "This is a GeoTable"
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
    val exception = intercept[RuntimeException] {
      sql("create index index_on_spatial_col on table maintable (mygeohash) AS 'carbondata'")
    }
    assert(exception.getMessage.contains("Secondary Index is not supported for Spatial " +
      "index column: mygeohash"))
    sql("drop table if exists maintable")
  }

  test("test create index table on already selected column") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int) STORED AS carbondata ")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    val exception = intercept[RuntimeException] {
      sql("create index indextable2 on table maintable(b) AS 'carbondata'")
    }
    assert(exception.getMessage.contains("Index Table with selected columns already exist"))
    sql("drop table if exists maintable")
  }

  test("test SI creation when other data modification operation is in progress") {
    sql("use default")
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int) STORED AS carbondata ")
    val mock = TestSecondaryIndexUtils.mockTableLock()
    val ex = intercept[RuntimeException] {
      sql("create index indextable on table maintable(b) AS 'carbondata'")
    }
    assert(ex.getMessage.contains("Not able to acquire lock. Another Data Modification operation " +
      "is already in progress for either default.maintable or default or indextable."))
    mock.tearDown()
    sql("drop table if exists maintable")
  }

  test("test SI creation with deferred refresh") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c int) STORED AS carbondata ")
    val ex = intercept[UnsupportedOperationException] {
      sql("create index indextable on table maintable(b) AS 'carbondata' with deferred refresh")
    }
    assert(ex.getMessage.contains("DEFERRED REFRESH is not supported"))
    sql("drop table if exists maintable")
  }

  test("test create index table when indexes are present in stale state") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    val mock = TestSecondaryIndexUtils.mockGetSecondaryIndexFromCarbon()
    val ex = intercept[RuntimeException] {
      sql("create index indextable1 on table maintable(b, c) AS 'carbondata'")
    }
    mock.tearDown()
    assert(ex.getMessage.contains("Index with [indextable1] under database [default] is present " +
        "in stale state. Please use drop index if exists command to delete the index table"))
    val mock2 = TestSecondaryIndexUtils.mockIsFileExists()
    val exception = intercept[RuntimeException] {
      sql("create index indextable1 on table maintable(b, c) AS 'carbondata'")
    }
    mock2.tearDown()
    assert(exception.getMessage.contains("Index with [indextable1] under database [default] " +
        "is present in stale state. Please use drop index " +
        "if exists command to delete the index table"))
    sql("drop table if exists maintable")
  }

  test("test index creation on index table") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
    sql("create index indextable on table maintable(b) AS 'carbondata'")
    val ex = intercept[RuntimeException] {
      sql("create index indextable1 on table indextable(b) AS 'carbondata'")
    }
    assert(ex.getMessage.contains("Table [indextable] under database " +
        "[default] is already an index table"))
  }

  test("test SI creation when create table will throw exception") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string,b string,c string) STORED AS carbondata ")
    val mock = TestSecondaryIndexUtils.mockCreateTable()
    val ex = intercept[IOException] {
      sql("create index indextable on table maintable(b) AS 'carbondata'")
    }
    mock.tearDown()
    assert(ex.getMessage.contains("An exception occurred while creating index table."))
  }

  object CarbonMetastore {

    import org.apache.carbondata.core.reader.ThriftReader

    def readSchemaFileToThriftTable(schemaFilePath: String): TableInfo = {
      val createTBase = new ThriftReader.TBaseCreator() {
        override def create(): org.apache.thrift.TBase[TableInfo, TableInfo._Fields] = {
          new TableInfo()
        }
      }
      val thriftReader = new ThriftReader(schemaFilePath, createTBase)
      var tableInfo: TableInfo = null
      try {
        thriftReader.open()
        tableInfo = thriftReader.read().asInstanceOf[TableInfo]
      } finally {
        thriftReader.close()
      }
      tableInfo
    }
  }

  def emptyParquetFolder(): Unit = {
    val file = new File(path)
    FileFactory.deleteAllFilesOfDir(file)
    file.mkdir()
  }

  override def afterAll: Unit = {
    dropIndexAndTable()
    FileFactory.deleteAllFilesOfDir(new File(path))
  }

  private def dropIndexAndTable(): Unit = {
    sql("drop index if exists index_case_insensitive on carbon")
    sql("drop table if exists carbon")
    sql("drop table if exists carbontable")
    sql("drop table if exists createindextemptable")
    sql("drop index if exists empnameindex on temptablecheckDB.createindextemptable1")
    sql("drop table if exists temptablecheckDB.createindextemptable1")
    sql("drop table if exists dropindextemptable")
    sql("drop table if exists temptablecheckDB.dropindextemptable1")
    sql("drop index if exists si_drop_i1 on carbon_si_same_name_test")
    sql("drop table if exists carbon_si_same_name_test")
    sql(s"DROP DATABASE if exists temptablecheckDB cascade")

    sql("drop index if exists t_ind1 on test1")
    sql("drop table if exists test1")
    sql("drop table if exists stream_si")
    sql("drop table if exists part_si")
  }
}

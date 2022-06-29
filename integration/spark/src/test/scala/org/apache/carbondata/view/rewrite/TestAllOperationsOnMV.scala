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

package org.apache.carbondata.view.rewrite

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedMVCommandException}
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 * Test Class for MV materialized view to verify all scenerios
 */
class TestAllOperationsOnMV extends QueryTest with BeforeAndAfterEach {
  // scalastyle:off lineLength
  override def beforeEach(): Unit = {
    sql("set carbon.enable.mv = true")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop table IF EXISTS testtable")
    sql("create table testtable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table testtable select 'abc',21,2000")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select name,sum(price) " +
        "from maintable group by name")
    sql("refresh materialized view dm1")
    checkResult()
  }

  private def checkResult(): Unit = {
    checkAnswer(sql("select name,sum(price) from maintable group by name"),
      sql("select name,sum(price) from maintable group by name"))
  }

  override def afterEach(): Unit = {
    sql("set carbon.enable.mv = false")
    sql("drop materialized view if exists dm_mv ")
    sql("drop materialized view if exists dm_pre ")
    sql("drop table IF EXISTS maintable")
    sql("drop table IF EXISTS testtable")
    sql("drop table if exists par_table")
  }

  test("test alter add column on maintable") {
    sql("alter table maintable add columns(d int)")
    sql("insert into table maintable select 'abc',21,2000,30")
    sql("refresh materialized view dm1")
    checkResult()
  }

  test("test alter add column on MV table") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1 add columns(d int)")
    }.getMessage.contains("Cannot add columns in a materialized view table default.dm1")
  }

  test("test drop column on maintable") {
    // check drop column not present in materialized view table
    sql("alter table maintable drop columns(c_code)")
    checkResult()
    // check drop column present in materialized view table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable drop columns(name)")
    }.getMessage.contains("Column name cannot be dropped because it exists in mv materialized view: dm1")
  }

  test("test alter drop column on MV table") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1 drop columns(maintable_name)")
    }.getMessage.contains("Cannot drop columns present in a materialized view table default.dm1")
  }

  test("test rename column on maintable") {
    // check rename column not present in materialized view table
    sql("alter table maintable change c_code d_code int")
    checkResult()
    // check rename column present in mv materialized view table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change name name1 string")
    }.getMessage.contains(
      "Column name exists in a MV materialized view. Drop MV materialized view to continue")
  }

  test("test alter rename column on MV table") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1 change sum_price sum_cost int")
    }.getMessage.contains("Cannot change data type or rename column for columns " +
                          "present in mv materialized view table default.dm1")
  }

  test("test alter rename table") {
    // check rename maintable
    intercept[MalformedCarbonCommandException] {
      sql("alter table maintable rename to maintable_rename")
    }.getMessage.contains("alter rename is not supported for materialized view table or for tables which have child materialized view")
    // check rename MV
    intercept[MalformedCarbonCommandException] {
      sql("alter table dm1 rename to dm11")
    }.getMessage.contains("alter rename is not supported for materialized view table or for tables which have child materialized view")
  }

  test("test alter change datatype") {
    // change datatype for column
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change price price bigint")
    }.getMessage.contains("Column price exists in a MV materialized view. Drop MV materialized view to continue")
    // change datatype for column not present in materialized view table
    sql("alter table maintable change c_code c_code bigint")
    checkResult()
    // change datatype for column present in materialized view table
    intercept[ProcessMetaDataException] {
      sql("alter table dm1 change sum_price sum_price bigint")
    }.getMessage.contains("Cannot change data type or rename column for columns " +
                          "present in mv materialized view table default.dm1")
  }

  test("test properties") {
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh properties" +
        "('LOCAL_DICTIONARY_ENABLE'='false') as select name,price from maintable")
    checkExistence(sql("describe formatted dm1"), true, "Local Dictionary Enabled false")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh properties('TABLE_BLOCKSIZE'='256 MB') " +
        "as select name,price from maintable")
    checkExistence(sql("describe formatted dm1"), true, "Table Block Size  256 MB")
  }

  test("test table properties") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('LOCAL_DICTIONARY_ENABLE'='false')")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select name,price from maintable")
    checkExistence(sql("describe formatted dm1"), true, "Local Dictionary Enabled false")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('TABLE_BLOCKSIZE'='256 MB')")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select name,price from maintable")
    checkExistence(sql("describe formatted dm1"), true, "Table Block Size  256 MB")
  }

  test("test delete segment by id on main table") {
    sql("drop materialized view if exists dm1")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("create materialized view dm1 with deferred refresh as select name,sum(price) " +
        "from maintable group by name")
    sql("refresh materialized view dm1")
    intercept[UnsupportedOperationException] {
      sql("Delete from table maintable where segment.id in (1)")
    }.getMessage.contains("Delete segment operation is not supported on tables which have mv materialized view")
    intercept[UnsupportedOperationException] {
      sql("Delete from table dm1 where segment.id in (0)")
    }.getMessage.contains("Delete segment operation is not supported on mv table")
  }

  test("test delete segment by date on main table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select name,sum(price) " +
        "from maintable group by name")
    sql("refresh materialized view dm1")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE maintable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }.getMessage.contains("Delete segment operation is not supported on tables which have mv materialized view")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE dm1 WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }.getMessage.contains("Delete segment operation is not supported on mv table")
  }

  test("test direct load to mv materialized view table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select name " +
        "from maintable")
    sql("refresh materialized view dm1")
    intercept[UnsupportedOperationException] {
      sql("insert into dm1 select 2")
    }.getMessage.contains("Cannot insert data directly into MV table")
    sql("drop table IF EXISTS maintable")
  }

  test("test drop materialized view with tablename") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1")
    sql("create materialized view dm1 with deferred refresh as select price " +
        "from maintable")
    sql("refresh materialized view dm1")
    checkAnswer(sql("select price from maintable"), Seq(Row(2000)))
    checkExistence(sql("show materialized views on table maintable"), true, "dm1")
    sql("drop materialized view dm1")
    checkExistence(sql("show materialized views on table maintable"), false, "dm1")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv with attribute having qualifier") {
    sql("drop table if exists maintable")
    sql("create table maintable (product string) partitioned by (amount int) STORED AS carbondata ")
    sql("insert into maintable values('Mobile',2000)")
    sql("drop materialized view if exists p")
    sql("Create materialized view p  as Select p.product, p.amount from maintable p where p.product = 'Mobile'")
    sql("refresh materialized view p")
    checkAnswer(sql("Select p.product, p.amount from maintable p where p.product = 'Mobile'"), Seq(Row("Mobile", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test mv with non-carbon table") {
    sql("drop table if exists noncarbon")
    sql("create table noncarbon (product string,amount int) stored as parquet")
    sql("insert into noncarbon values('Mobile',2000)")
    sql("drop materialized view if exists p")
    sql("Create materialized view p as Select product from noncarbon")
    sql("drop materialized view p")
    sql("drop table if exists noncarbon")
  }

  // Test show materialized view
  test("test materialized view status with single table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1 ")
    sql("create materialized view dm1 with deferred refresh as select price from maintable")
    checkExistence(sql("show materialized views on table maintable"), true, "DISABLED")
    sql("refresh materialized view dm1")
    var result = sql("show materialized views on table maintable").collectAsList()
    assert(result.get(0).get(0).toString.equalsIgnoreCase("default"))
    assert(result.get(0).get(1).toString.equalsIgnoreCase("dm1"))
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(3).toString.equalsIgnoreCase("incremental"))
    assert(result.get(0).get(4).toString.equalsIgnoreCase("on_manual"))
    assert(result.get(0).get(5).toString.contains("'mv_related_tables'='maintable'"))
    sql("insert into table maintable select 'abc',21,2000")
    checkExistence(sql("show materialized views on table maintable"), true, "DISABLED")
    sql("refresh materialized view dm1")
    result = sql("show materialized views on table maintable").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("'mv_related_tables'='maintable'"))
    sql("drop materialized view if exists dm1 ")
    sql("drop table IF EXISTS maintable")
  }

  test("test materialized view status with multiple tables") {
    sql("drop table if exists products")
    sql("create table products (product string, amount int) STORED AS carbondata ")
    sql(s"load data INPATH '$resourcesPath/products.csv' into table products")
    sql("drop table if exists sales")
    sql("create table sales (product string, quantity int) STORED AS carbondata")
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales")
    sql("drop materialized view if exists innerjoin")
    sql(
      "Create materialized view innerjoin with deferred refresh as Select p.product, p.amount, " +
      "s.quantity, s.product from " +
      "products p, sales s where p.product=s.product")
    checkExistence(sql("show materialized views on table products"), true, "DISABLED")
    checkExistence(sql("show materialized views on table sales"), true, "DISABLED")
    sql("refresh materialized view innerjoin")
    var result = sql("show materialized views on table products").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(3).toString.equalsIgnoreCase("full"))
    assert(result.get(0).get(4).toString.equalsIgnoreCase("on_manual"))
    assert(result.get(0).get(5).toString.contains("'mv_related_tables'='products,sales'"))
    result = sql("show materialized views on table sales").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("'mv_related_tables'='products,sales'"))
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales")
    checkExistence(sql("show materialized views on table products"), true, "DISABLED")
    checkExistence(sql("show materialized views on table sales"), true, "DISABLED")
    sql("refresh materialized view innerjoin")
    result = sql("show materialized views on table sales").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("'mv_related_tables'='products,sales'"))
    sql("drop materialized view if exists innerjoin ")
    sql(
      "Create materialized view innerjoin as Select p.product, p.amount, " +
      "s.quantity, s.product from " +
      "products p, sales s where p.product=s.product")
    checkExistence(sql("show materialized views on table products"), true, "ENABLED")
    checkExistence(sql("show materialized views on table sales"), true, "ENABLED")
    sql("show materialized views").collect()
    result = sql("show materialized views on table products").collectAsList()
    assert(result.get(0).get(2).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(3).toString.equalsIgnoreCase("full"))
    assert(result.get(0).get(4).toString.equalsIgnoreCase("on_commit"))
    sql("drop materialized view if exists innerjoin ")

    sql("drop table if exists products")
    sql("drop table if exists sales")
  }

  test("directly drop materialized view table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1 ")
    sql("create materialized view dm1 with deferred refresh as select price from maintable")
    intercept[ProcessMetaDataException] {
      sql("drop table dm1")
    }.getMessage.contains("Child table which is associated with materialized view cannot be dropped, use DROP materialized view command to drop")
    sql("drop table IF EXISTS maintable")
  }

  test("create materialized view on child table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1 ")
    sql("create materialized view dm1  as select name, price from maintable")
    intercept[Exception] {
      sql("create materialized view dm_agg  as select maintable_name, sum(maintable_price) from dm1 group by maintable_name")
    }.getMessage.contains("Cannot create materialized view on child table default.dm1")
  }

  test("create materialized view if already exists") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm1 ")
    sql("create materialized view dm1  as select name from maintable")
    intercept[Exception] {
      sql("create materialized view dm1  as select price from maintable")
    }.getMessage.contains("materialized view with name dm1 already exists in storage")
    checkAnswer(sql("select name from maintable"), Seq(Row("abc")))
  }

  test("test create materialized view with select query having 'like' expression") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("select name from maintable where name like '%b%'").collect()
    sql("drop materialized view if exists dm_like ")
    sql("create materialized view dm_like  as select name from maintable where name like '%b%'")
    checkAnswer(sql("select name from maintable where name like '%b%'"), Seq(Row("abc")))
    sql("drop table IF EXISTS maintable")
  }

  test("test materialized view with streaming dmproperty") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    intercept[MalformedCarbonCommandException] {
      sql("create materialized view dm properties('STREAMING'='true') as select name from maintable")
    }.getMessage.contains("Materialized view does not support streaming")
    sql("drop table IF EXISTS maintable")
  }

  test("test set streaming after creating materialized view table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select name from maintable")
    intercept[MalformedCarbonCommandException] {
      sql("ALTER TABLE dm SET TBLPROPERTIES('streaming'='true')")
    }.getMessage.contains("materialized view table does not support set streaming property")
    sql("drop table IF EXISTS maintable")
  }

  test("test block complex data types") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code array<int>, price struct<b:int>,type map<string, string>) STORED AS carbondata")
    sql("insert into table maintable values('abc', array(21), named_struct('b', 2000), map('ab','type1'))")
    sql("drop materialized view if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select c_code from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : c_code")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select price from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : price")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select type from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : type")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select price.b from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype child columns and complex datatype return types of function : price")
    sql("drop table IF EXISTS maintable")
  }

  test("validate properties") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    intercept[MalformedCarbonCommandException] {
      sql("create materialized view dm properties('dictionary_include'='name', 'sort_columns'='name') as select name from maintable")
    }.getMessage.contains("Properties dictionary_include,sort_columns are not allowed for this materialized view")
  }

  test("test todate UDF function with mv") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata")
    sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11 01:00:03.0','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select max(to_date(dob)) , min(to_date(dob)) from maintable where to_date(dob)='1975-06-11' or to_date(dob)='1975-06-23'")
    checkExistence(sql("select max(to_date(dob)) , min(to_date(dob)) from maintable where to_date(dob)='1975-06-11' or to_date(dob)='1975-06-23'"), true, "1975-06-11 1975-06-11")
    sql("drop materialized view if exists dm2 ")
    sql("create materialized view dm2 as select to_date(DOB) from maintable where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =120 or DECIMAL_COLUMN1 = 4.34 or Double_COLUMN1 =12345  or INTEGER_COLUMN1 IS NULL")
    checkExistence(sql("select to_date(DOB) from maintable where CUST_ID IS NULL or dob IS NOT NULL or BIGINT_COLUMN1 =120 or DECIMAL_COLUMN1 = 4.34 or Double_COLUMN1 =12345  or INTEGER_COLUMN1 IS NULL"), true, "1975-06-11")
    val df = sql("select to_date(dob) from maintable where CUST_ID IS NULL or dob IS NOT NULL or BIGINT_COLUMN1 =120 or DECIMAL_COLUMN1 = 4.34 or Double_COLUMN1 =12345  or INTEGER_COLUMN1 IS NULL")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "dm2")
    sql("drop table IF EXISTS maintable")
  }

  test("test preagg and mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm_mv ")
    sql("create materialized view dm_mv  as select name, sum(price) from maintable group by name")
    sql("drop materialized view if exists dm_pre ")
    sql("insert into table maintable select 'abcd',21,20002")
    checkAnswer(sql("select count(*) from dm_mv"), Seq(Row(2)))
    sql("drop table IF EXISTS maintable")
  }

  test("test inverted index  & no-inverted index inherited from parent table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('sort_columns'='name', 'inverted_index'='name','sort_scope'='local_sort')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm"), true, "Inverted Index Columns maintable_name")
    checkAnswer(sql("select name, sum(price) from maintable group by name"), Seq(Row("abc", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test column compressor on preagg and mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('carbon.column.compressor'='zstd')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm_mv ")
    sql("create materialized view dm_mv as select name, sum(price) from maintable group by name")
    val mvTable = CarbonMetadata.getInstance().getCarbonTable(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "dm_mv")
    assert(mvTable.getTableInfo.getFactTable.getTableProperties.get(CarbonCommonConstants.COMPRESSOR).equalsIgnoreCase("zstd"))
    sql("drop table IF EXISTS maintable")
  }

  test("test sort_scope if sort_columns are provided") {
    CarbonProperties.getInstance().removeProperty("carbon.load.sort.scope")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('sort_columns'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("create materialized view dm_mv as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_mv"), true, "Sort Scope LOCAL_SORT")
    sql("drop table IF EXISTS maintable")
  }

  test("test inverted_index if sort_scope is provided") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('sort_scope'='no_sort','sort_columns'='name', 'inverted_index'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    checkExistence(sql("describe formatted maintable"), true, "Inverted Index Columns name")
    sql("create materialized view dm_mv as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_mv"), true, "Inverted Index Columns maintable_name")
    sql("drop table IF EXISTS maintable")
  }

  test("test sort column") {
    sql("drop table IF EXISTS maintable")
    intercept[MalformedCarbonCommandException] {
      sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('sort_scope'='local_sort','sort_columns'='')")
    }.getMessage.contains("Cannot set SORT_COLUMNS as empty when SORT_SCOPE is LOCAL_SORT")
  }

  test("test delete on materialized view table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata tblproperties('sort_scope'='no_sort','sort_columns'='name', 'inverted_index'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("create materialized view dm_mv as select name, sum(price) from maintable group by name")
    intercept[UnsupportedOperationException] {
      sql("delete from dm_mv where maintable_name='abc'")
    }.getMessage.contains("Delete operation is not supported for materialized view table")
    sql("drop table IF EXISTS maintable")
  }

  test("test drop/show meta cache directly on mv materialized view table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select name, sum(price) from maintable group by name")
    sql("select name, sum(price) from maintable group by name").collect()
    intercept[UnsupportedOperationException] {
      sql("show metacache on table dm").collect()
    }.getMessage.contains("Operation not allowed on child table.")
    intercept[UnsupportedOperationException] {
      sql("drop metacache on table dm").collect()
    }.getMessage.contains("Operation not allowed on child table.")
  }

  test("test count(*) with filter") {
    sql("drop table if exists maintable")
    sql("create table maintable(id int, name string, id1 string, id2 string, dob timestamp, doj " +
        "timestamp, v1 bigint, v2 bigint, v3 decimal(30,10), v4 decimal(20,10), v5 double, v6 " +
        "double ) STORED AS carbondata")
    sql("insert into maintable values(1, 'abc', 'id001', 'id002', '2017-01-01 00:00:00','2017-01-01 " +
        "00:00:00', 234, 2242,12.4,23.4,2323,455 )")
    checkAnswer(sql("select count(*) from maintable where  id1 < id2"), Seq(Row(1)))
    sql("drop table if exists maintable")
  }

  test("test mv with filter instance of expression") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB date, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata")
    sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select dob from maintable where (dob='1975-06-11' or cust_id=2)")
    val df = sql("select dob from maintable where (dob='1975-06-11' or cust_id=2)")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "dm")
    sql("drop table IF EXISTS maintable")
  }

  test("test histogram_numeric, collect_set & collect_list functions") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata")
    sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11 01:00:03.0','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop materialized view if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select histogram_numeric(1,2) from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : histogram_numeric( 1, 2)")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select collect_set(cust_id) from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : collect_set(cust_id)")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm  as select collect_list(cust_id) from maintable")
    }.getMessage.contains("MV materialized view is not supported for complex datatype columns and complex datatype return types of function : collect_list(cust_id)")
    sql("drop table IF EXISTS maintable")
  }

  test("test query aggregation on mv materialized view ") {
    sql("drop table if exists maintable")
    sql("create table maintable(name string, age int, add string) STORED AS carbondata")
    sql("insert into maintable values('abc',1,'a'),('def',2,'b'),('ghi',3,'c')")
    val res = sql("select sum(age) from maintable")
    val res1 = sql("select age,sum(age) from maintable group by age")
    sql("drop materialized view if exists mv3")
    sql("create materialized view mv3 as select age,sum(age) from maintable group by age")
    val df = sql("select sum(age) from maintable")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv3")
    checkAnswer(res, df)
    sql("drop materialized view if exists mv3")
    sql("create materialized view mv3 as select age as a,sum(age) as b from maintable group by age")
    val df1 = sql("select age,sum(age) from maintable group by age")
    TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mv3")
    checkAnswer(res1, df1)
    sql("drop table if exists maintable")
  }

  test("test order by columns not given in projection") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    val res = sql("select name from maintable order by c_code")
    sql("drop materialized view if exists dm1")
    intercept[UnsupportedOperationException] {
      sql("create materialized view dm1  as select name from maintable order by c_code")
    }.getMessage.contains("Order by column `c_code` must be present in project columns")
    sql("create materialized view dm1  as select name,c_code from maintable order by c_code")
    val df = sql("select name from maintable order by c_code")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "dm1")
    checkAnswer(res, df)
    intercept[Exception] {
      sql("alter table maintable drop columns(c_code)")
    }.getMessage.contains("Column name cannot be dropped because it exists in mv materialized view: dm1")
    sql("insert into table maintable select 'mno',20,2000")
    checkAnswer(sql("select name from maintable order by c_code"), Seq(Row("mno"), Row("abc")))
    sql("drop table if exists maintable")
  }

  test("test query on mv with limit") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'bcd',22,2000")
    sql("insert into table maintable select 'def',22,2000")
    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1  as select a.name,a.price from maintable a")
    var dataFrame = sql("select a.name,a.price from maintable a limit 1")
    assert(dataFrame.collect().length == 1)
    TestUtil.verifyMVHit(dataFrame.queryExecution.optimizedPlan, "mv1")
    dataFrame = sql("select a.name,a.price from maintable a order by a.name limit 1")
    assert(dataFrame.collect().length == 1)
    TestUtil.verifyMVHit(dataFrame.queryExecution.optimizedPlan, "mv1")
    sql("drop table if exists maintable")
  }

  test("test horizontal comapction on mv for more than two update") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable values('abc',21,2000),('mno',24,3000)")
    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1  as select name,c_code from maintable")
    sql("update maintable set(name) = ('aaa') where c_code = 21").collect()
    var df = sql("select name,c_code from maintable")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1")
    checkAnswer(df, Seq(Row("aaa", 21), Row("mno", 24)))
    sql("update maintable set(name) = ('mmm') where c_code = 24").collect()
    df = sql("select name,c_code from maintable")
    TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1")
    checkAnswer(df, Seq(Row("aaa", 21), Row("mmm", 24)))
    sql("drop table IF EXISTS maintable")
  }

  test("test duplicate column name in mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable values('abc',21,2000),('mno',24,3000)")
    sql("drop materialized view if exists mv1")
    val res1 = sql("select name,sum(c_code) from maintable group by name")
    val res2 = sql("select name, name,sum(c_code),sum(c_code) from maintable  group by name")
    val res3 = sql("select c_code,price from maintable")
    sql("create materialized view mv1 as select name,sum(c_code) from maintable group by name")
    val df1 = sql("select name,sum(c_code) from maintable group by name")
    TestUtil.verifyMVHit(df1.queryExecution.optimizedPlan, "mv1")
    checkAnswer(res1, df1)
    val df2 = sql("select name, name,sum(c_code),sum(c_code) from maintable  group by name")
    TestUtil.verifyMVHit(df2.queryExecution.optimizedPlan, "mv1")
    checkAnswer(df2, res2)
    sql("drop materialized view if exists mv2")
    sql("create materialized view mv2 as select c_code,price from maintable")
    val df3 = sql("select c_code,price from maintable")
    TestUtil.verifyMVHit(df3.queryExecution.optimizedPlan, "mv2")
    checkAnswer(res3, df3)
    val df4 = sql("select c_code,price,price,c_code from maintable")
    TestUtil.verifyMVHit(df4.queryExecution.optimizedPlan, "mv2")
    checkAnswer(df4, Seq(Row(21, 2000, 2000, 21), Row(24, 3000, 3000, 24)))
    sql("drop table IF EXISTS maintable")
  }

  test("test duplicate column with different alias name") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable values('abc',21,2000),('mno',24,3000)")
    sql("drop materialized view if exists mv1")
    intercept[MalformedMVCommandException] {
      sql("create materialized view mv1 as select name,sum(c_code),sum(c_code) as a from maintable group by name")
    }.getMessage.contains("Cannot create mv having duplicate column with different alias name: sum(CAST(maintable.`c_code` AS BIGINT)) AS `a`")
    intercept[MalformedMVCommandException] {
      sql("create materialized view mv1 as select name,name as a from maintable")
    }.getMessage.contains("Cannot create mv having duplicate column with different alias name: maintable.`name` AS `a`")
    intercept[MalformedMVCommandException] {
      sql("create materialized view mv1 as select name as a,name as b from maintable")
    }.getMessage.contains("Cannot create mv having duplicate column with different alias name: maintable.`name` AS `b`")
    sql("drop table IF EXISTS maintable")
  }

  test("test case sensitive issues with implicit cast type expressions") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB " +
      "timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 " +
      "decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
      "double,INTEGER_COLUMN1 int) STORED AS carbondata")
    sql("insert into maintable select 1,'abc','2001','2017-09-01 00:00:00','2017-09-03 00:00:00',1234567,1234564,'1234.456','1234.4567',1.123455,1.123455,null")
    sql("drop materialized view if exists mv1")
    sql("create materialized view mv1 as select length(CUST_NAME) from maintable where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1=1234567 or" +
        " DECIMAL_COLUMN1=1234.456 or Double_COLUMN1=1.123455 or INTEGER_COLUMN1 IS NULL")
    val withUpperCase = sql(
      "select length(CUST_NAME) from maintable where CUST_ID IS NULL or DOB IS NOT NULL or " +
      "BIGINT_COLUMN1=1234567 or DECIMAL_COLUMN1=1234.456 or Double_COLUMN1=1.123455 or INTEGER_COLUMN1 IS NULL")
    val withLowerCase = sql(
      "select length(cust_name) from maintable where cust_id IS NULL or dob IS NOT NULL or " +
      "bigint_column1=1234567 or decimal_column1=1234.456 or double_column1=1.123455 or integer_column1 IS NULL")
    checkAnswer(withUpperCase, Seq(Row(3)))
    checkAnswer(withLowerCase, Seq(Row(3)))
    TestUtil.verifyMVHit(withUpperCase.queryExecution.optimizedPlan, "mv1")
    TestUtil.verifyMVHit(withLowerCase.queryExecution.optimizedPlan, "mv1")
    sql("drop table IF EXISTS maintable")
  }

  test("test refresh mv which does not exists") {
    intercept[MalformedMVCommandException] {
      sql("refresh materialized view does_not_exist")
    }.getMessage.contains("Materialized view default.does_not_exist does not exist")
  }

  test("test create table like maintable having mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("drop materialized view if exists mv_table ")
    sql("create materialized view mv_table  as select name, sum(price) from maintable group by name")
    sql("drop table if exists new_Table")
    sql("create table new_Table like maintable")
    sql("insert into table new_Table select 'abc',21,2000")
    checkAnswer(sql("select * from new_Table"), Seq(Row("abc", 21, 2000)))
    intercept[MalformedCarbonCommandException] {
      sql("create table new_Table1 like mv_table")
    }.getMessage.contains("Unsupported operation on SI table or MV.")
    sql("drop table if exists new_Table")
    sql("drop table IF EXISTS maintable")
  }

  test("drop meta cache on mv materialized view table") {
    defaultConfig()
    sql("set carbon.enable.mv = true")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) STORED AS carbondata")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop materialized view if exists dm ")
    sql("create materialized view dm  as select name, sum(price) from maintable group by name")
    sql("select name, sum(price) from maintable group by name").collect()
    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql("drop metacache on table maintable").collect()

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier("maintable", Some("default"))
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val dbPath = CarbonEnv
      .getDatabaseLocation(tableIdentifier.database.get, sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    val mvPath = FileFactory.getUpdatedFilePath(dbPath) + CarbonCommonConstants.FILE_SEPARATOR +
                 "dm" + CarbonCommonConstants.FILE_SEPARATOR

    // Check if table index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(tablePath)))

    // check if cache does not have any more table index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(tablePath)))

    // Check if mv index entries are dropped
    assert(droppedCacheKeys.asScala.exists(key => key.startsWith(mvPath)))

    // check if cache does not have any more mv index entries
    assert(!cacheAfterDrop.asScala.exists(key => key.startsWith(mvPath)))
  }

  def clone(oldSet: util.Set[String]): util.HashSet[String] = {
    val newSet = new util.HashSet[String]
    newSet.addAll(oldSet)
    newSet
  }
  // scalastyle:on lineLength
}

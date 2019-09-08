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

package org.apache.carbondata.mv.rewrite

import scala.collection.JavaConverters._
import java.util

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 * Test Class for MV Datamap to verify all scenerios
 */
class TestAllOperationsOnMV extends CarbonQueryTest with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop table IF EXISTS testtable")
    sql("create table testtable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table testtable select 'abc',21,2000")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    checkResult()
  }

  private def checkResult(): Unit = {
    checkAnswer(sql("select name,sum(price) from maintable group by name"),
      sql("select name,sum(price) from maintable group by name"))
  }

  override def afterEach(): Unit = {
    sql("drop table IF EXISTS maintable")
    sql("drop table IF EXISTS testtable")
    sql("drop table if exists par_table")
  }

  test("test alter add column on maintable") {
    sql("alter table maintable add columns(d int)")
    sql("insert into table maintable select 'abc',21,2000,30")
    sql("rebuild datamap dm1")
    checkResult()
  }

  test("test alter add column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table add columns(d int)")
    }.getMessage.contains("Cannot add columns in a DataMap table default.dm1_table")
  }

  test("test drop column on maintable") {
    // check drop column not present in datamap table
    sql("alter table maintable drop columns(c_code)")
    checkResult()
    // check drop column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable drop columns(name)")
    }.getMessage.contains("Column name cannot be dropped because it exists in mv datamap: dm1")
  }

  test("test alter drop column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table drop columns(maintable_name)")
    }.getMessage.contains("Cannot drop columns present in a datamap table default.dm1_table")
  }

  test("test rename column on maintable") {
    // check rename column not present in datamap table
    sql("alter table maintable change c_code d_code int")
    checkResult()
    // check rename column present in mv datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change name name1 string")
    }.getMessage.contains("Column name exists in a MV datamap. Drop MV datamap to continue")
  }

  test("test alter rename column on datamaptable") {
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table change sum_price sum_cost int")
    }.getMessage.contains("Cannot change data type or rename column for columns " +
                          "present in mv datamap table default.dm1_table")
  }

  test("test alter rename table") {
    //check rename maintable
    intercept[MalformedCarbonCommandException] {
      sql("alter table maintable rename to maintable_rename")
    }.getMessage.contains("alter rename is not supported for datamap table or for tables which have child datamap")
    //check rename datamaptable
    intercept[MalformedCarbonCommandException] {
      sql("alter table dm1_table rename to dm11_table")
    }.getMessage.contains("alter rename is not supported for datamap table or for tables which have child datamap")
  }

  test("test alter change datatype") {
    //change datatype for column
    intercept[ProcessMetaDataException] {
      sql("alter table maintable change price price bigint")
    }.getMessage.contains("Column price exists in a MV datamap. Drop MV datamap to continue")
    //change datatype for column not present in datamap table
    sql("alter table maintable change c_code c_code bigint")
    checkResult()
    //change datatype for column present in datamap table
    intercept[ProcessMetaDataException] {
      sql("alter table dm1_table change sum_price sum_price bigint")
    }.getMessage.contains("Cannot change data type or rename column for columns " +
                          "present in mv datamap table default.dm1_table")
  }

  test("test dmproperties") {
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' WITH DEFERRED REBUILD dmproperties" +
        "('LOCAL_DICTIONARY_ENABLE'='false') as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Local Dictionary Enabled false")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 on table maintable using 'mv' WITH DEFERRED REBUILD dmproperties('TABLE_BLOCKSIZE'='256 MB') " +
        "as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Table Block Size  256 MB")
  }

  test("test table properties") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('LOCAL_DICTIONARY_ENABLE'='false')")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1  using 'mv' WITH DEFERRED REBUILD as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Local Dictionary Enabled false")
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('TABLE_BLOCKSIZE'='256 MB')")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1  using 'mv' WITH DEFERRED REBUILD as select name,price from maintable")
    checkExistence(sql("describe formatted dm1_table"), true, "Table Block Size  256 MB")
  }

  test("test delete segment by id on main table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("Delete from table maintable where segment.id in (1)")
    }.getMessage.contains("Delete segment operation is not supported on tables which have mv datamap")
    intercept[UnsupportedOperationException] {
      sql("Delete from table dm1_table where segment.id in (0)")
    }.getMessage.contains("Delete segment operation is not supported on mv table")
  }

  test("test delete segment by date on main table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("insert into table maintable select 'abc',21,2000")
    sql("Delete from table maintable where segment.id in (0)")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1  using 'mv' WITH DEFERRED REBUILD as select name,sum(price) " +
        "from maintable group by name")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE maintable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }.getMessage.contains("Delete segment operation is not supported on tables which have mv datamap")
    intercept[UnsupportedOperationException] {
      sql("DELETE FROM TABLE dm1_table WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06'")
    }.getMessage.contains("Delete segment operation is not supported on mv table")
  }

  test("test direct load to mv datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select name " +
        "from maintable")
    sql("rebuild datamap dm1")
    intercept[UnsupportedOperationException] {
      sql("insert into dm1_table select 2")
    }.getMessage.contains("Cannot insert/load data directly into pre-aggregate/child table")
    sql("drop table IF EXISTS maintable")
  }


  test("test drop datamap with tablename") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1 on table maintable")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select price " +
        "from maintable")
    sql("rebuild datamap dm1")
    checkAnswer(sql("select price from maintable"), Seq(Row(2000)))
    checkExistence(sql("show datamap on table maintable"), true, "dm1")
    sql("drop datamap dm1 on table maintable")
    checkExistence(sql("show datamap on table maintable"), false, "dm1")
    sql("drop table IF EXISTS maintable")
  }

  test("test mv with attribute having qualifier") {
    sql("drop table if exists maintable")
    sql("create table maintable (product string) partitioned by (amount int) stored by 'carbondata' ")
    sql("insert into maintable values('Mobile',2000)")
    sql("drop datamap if exists p")
    sql("Create datamap p using 'mv' as Select p.product, p.amount from maintable p where p.product = 'Mobile'")
    sql("rebuild datamap p")
    checkAnswer(sql("Select p.product, p.amount from maintable p where p.product = 'Mobile'"), Seq(Row("Mobile", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test mv with non-carbon table") {
    sql("drop table if exists noncarbon")
    sql("create table noncarbon (product string,amount int)")
    sql("insert into noncarbon values('Mobile',2000)")
    sql("drop datamap if exists p")
    intercept[MalformedCarbonCommandException] {
      sql("Create datamap p using 'mv' as Select product from noncarbon")
    }.getMessage.contains("Non-Carbon table does not support creating MV datamap")
    sql("drop table if exists noncarbon")
  }

  //Test show datamap
  test("test datamap status with single table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1 ")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select price from maintable")
    checkExistence(sql("show datamap on table maintable"), true, "DISABLED")
    sql("rebuild datamap dm1")
    var result = sql("show datamap on table maintable").collectAsList()
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("{\"default.maintable\":\"0\""))
    sql("insert into table maintable select 'abc',21,2000")
    checkExistence(sql("show datamap on table maintable"), true, "DISABLED")
    sql("rebuild datamap dm1")
    result = sql("show datamap on table maintable").collectAsList()
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("{\"default.maintable\":\"1\""))
    sql("drop table IF EXISTS maintable")
  }

  test("test datamap status with multiple tables") {
    sql("drop table if exists products")
    sql("create table products (product string, amount int) stored by 'carbondata' ")
    sql(s"load data INPATH '$resourcesPath/products.csv' into table products")
    sql("drop table if exists sales")
    sql("create table sales (product string, quantity int) stored by 'carbondata'")
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales")
    sql("drop datamap if exists innerjoin")
    sql(
      "Create datamap innerjoin using 'mv'  with deferred rebuild as Select p.product, p.amount, " +
      "s.quantity, s.product from " +
      "products p, sales s where p.product=s.product")
    checkExistence(sql("show datamap on table products"), true, "DISABLED")
    checkExistence(sql("show datamap on table sales"), true, "DISABLED")
    sql("rebuild datamap innerjoin")
    var result = sql("show datamap on table products").collectAsList()
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("\"default.products\":\"0\",\"default.sales\":\"0\"}"))
    result = sql("show datamap on table sales").collectAsList()
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("\"default.products\":\"0\",\"default.sales\":\"0\"}"))
    sql(s"load data INPATH '$resourcesPath/sales_data.csv' into table sales")
    checkExistence(sql("show datamap on table products"), true, "DISABLED")
    checkExistence(sql("show datamap on table sales"), true, "DISABLED")
    sql("rebuild datamap innerjoin")
    result = sql("show datamap on table sales").collectAsList()
    assert(result.get(0).get(4).toString.equalsIgnoreCase("ENABLED"))
    assert(result.get(0).get(5).toString.contains("\"default.products\":\"0\",\"default.sales\":\"1\"}"))
    sql("drop table if exists products")
    sql("drop table if exists sales")
  }

  test("directly drop datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1 ")
    sql("create datamap dm1 using 'mv' WITH DEFERRED REBUILD as select price from maintable")
    intercept[ProcessMetaDataException] {
      sql("drop table dm1_table")
    }.getMessage.contains("Child table which is associated with datamap cannot be dropped, use DROP DATAMAP command to drop")
    sql("drop table IF EXISTS maintable")
  }

  test("create datamap on child table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1 ")
    sql("create datamap dm1 using 'mv' as select name, price from maintable")
    intercept[Exception] {
      sql("create datamap dm_agg on table dm1_table using 'preaggregate' as select maintable_name, sum(maintable_price) from dm1_table group by maintable_name")
    }.getMessage.contains("Cannot create DataMap on child table default.dm1_table")
    intercept[Exception] {
      sql("create datamap dm_agg using 'mv' as select maintable_name, sum(maintable_price) from dm1_table group by maintable_name")
    }.getMessage.contains("Cannot create DataMap on child table default.dm1_table")
  }

  test("create datamap if already exists") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm1 ")
    sql("create datamap dm1 using 'mv' as select name from maintable")
    intercept[Exception] {
      sql("create datamap dm1 using 'mv' as select price from maintable")
    }.getMessage.contains("DataMap with name dm1 already exists in storage")
    checkAnswer(sql("select name from maintable"), Seq(Row("abc")))
  }

  test("test create datamap with select query having 'like' expression") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("select name from maintable where name like '%b%'").show(false)
    sql("drop datamap if exists dm_like ")
    sql("create datamap dm_like using 'mv' as select name from maintable where name like '%b%'")
    checkAnswer(sql("select name from maintable where name like '%b%'"), Seq(Row("abc")))
    sql("drop table IF EXISTS maintable")
  }

  test("test datamap with streaming dmproperty") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    intercept[MalformedCarbonCommandException] {
      sql("create datamap dm using 'mv' dmproperties('STREAMING'='true') as select name from maintable")
    }.getMessage.contains("MV datamap does not support streaming")
    sql("drop table IF EXISTS maintable")
  }

  test("test set streaming after creating datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select name from maintable")
    intercept[MalformedCarbonCommandException] {
      sql("ALTER TABLE dm_table SET TBLPROPERTIES('streaming'='true')")
    }.getMessage.contains("Datamap table does not support set streaming property")
    sql("drop table IF EXISTS maintable")
  }

  test("test block complex data types") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code array<int>, price struct<b:int>,type map<string, string>) stored by 'carbondata'")
    sql("insert into table maintable values('abc', array(21), named_struct('b', 2000), map('ab','type1'))")
    sql("drop datamap if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select c_code from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : c_code")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select price from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : price")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select type from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : type")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select price.b from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype child columns and complex datatype return types of function : price")
    sql("drop table IF EXISTS maintable")
  }

  test("validate dmproperties") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    intercept[MalformedCarbonCommandException] {
      sql("create datamap dm using 'mv' dmproperties('dictionary_include'='name', 'sort_columns'='name') as select name from maintable")
    }.getMessage.contains("DMProperties dictionary_include,sort_columns are not allowed for this datamap")
  }

  test("test todate UDF function with mv") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'")
  sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11 01:00:03.0','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select max(to_date(dob)) , min(to_date(dob)) from maintable where to_date(dob)='1975-06-11' or to_date(dob)='1975-06-23'")
    checkExistence(sql("select max(to_date(dob)) , min(to_date(dob)) from maintable where to_date(dob)='1975-06-11' or to_date(dob)='1975-06-23'"), true, "1975-06-11 1975-06-11")
  }

  test("test global dictionary inherited from parent table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('dictionary_include'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_table"), true, "Global Dictionary maintable_name")
    checkAnswer(sql("select name, sum(price) from maintable group by name"), Seq(Row("abc", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test global dictionary inherited from parent table - Preaggregate") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('dictionary_include'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted maintable_dm"), true, "Global Dictionary maintable_name")
    checkAnswer(sql("select name, sum(price) from maintable group by name"), Seq(Row("abc", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test preagg and mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm_mv ")
    sql("create datamap dm_mv using 'mv' as select name, sum(price) from maintable group by name")
    sql("drop datamap if exists dm_pre ")
    sql("create datamap dm_pre on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    sql("insert into table maintable select 'abcd',21,20002")
    checkAnswer(sql("select count(*) from dm_mv_table"), Seq(Row(2)))
    checkAnswer(sql("select count(*) from maintable_dm_pre"), Seq(Row(2)))
    sql("drop table IF EXISTS maintable")
  }

  test("test inverted index  & no-inverted index inherited from parent table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_columns'='name', 'inverted_index'='name','sort_scope'='local_sort')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_table"), true, "Inverted Index Columns maintable_name")
    checkAnswer(sql("select name, sum(price) from maintable group by name"), Seq(Row("abc", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test inverted index & no-inverted index inherited from parent table - Preaggregate") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_columns'='name', 'inverted_index'='name','sort_scope'='local_sort')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted maintable_dm"), true, "Inverted Index Columns maintable_name")
    checkAnswer(sql("select name, sum(price) from maintable group by name"), Seq(Row("abc", 2000)))
    sql("drop table IF EXISTS maintable")
  }

  test("test column compressor on preagg and mv") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('carbon.column.compressor'='zstd')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm_pre ")
    sql("create datamap dm_pre on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    var dataMapTable = CarbonMetadata.getInstance().getCarbonTable(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "maintable_dm_pre")
    assert(dataMapTable.getTableInfo.getFactTable.getTableProperties.get(CarbonCommonConstants.COMPRESSOR).equalsIgnoreCase("zstd"))
    sql("drop datamap if exists dm_mv ")
    sql("create datamap dm_mv on table maintable using 'mv' as select name, sum(price) from maintable group by name")
    dataMapTable = CarbonMetadata.getInstance().getCarbonTable(CarbonCommonConstants.DATABASE_DEFAULT_NAME, "dm_mv_table")
    assert(dataMapTable.getTableInfo.getFactTable.getTableProperties.get(CarbonCommonConstants.COMPRESSOR).equalsIgnoreCase("zstd"))
    sql("drop table IF EXISTS maintable")
  }

  test("test sort_scope if sort_columns are provided") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_columns'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm_pre ")
    sql("create datamap dm_pre on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted maintable_dm_pre"), true, "Sort Scope LOCAL_SORT")
    sql("create datamap dm_mv on table maintable using 'mv' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_mv_table"), true, "Sort Scope LOCAL_SORT")
    sql("drop table IF EXISTS maintable")
  }

  test("test inverted_index if sort_scope is provided") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_scope'='no_sort','sort_columns'='name', 'inverted_index'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    checkExistence(sql("describe formatted maintable"), true, "Inverted Index Columns name")
    sql("drop datamap if exists dm_pre ")
    sql("create datamap dm_pre on table maintable using 'preaggregate' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted maintable_dm_pre"), true, "Inverted Index Columns maintable_name")
    sql("create datamap dm_mv on table maintable using 'mv' as select name, sum(price) from maintable group by name")
    checkExistence(sql("describe formatted dm_mv_table"), true, "Inverted Index Columns maintable_name")
    sql("drop table IF EXISTS maintable")
  }

  test("test sort column") {
    sql("drop table IF EXISTS maintable")
    intercept[MalformedCarbonCommandException] {
      sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_scope'='local_sort','sort_columns'='')")
    }.getMessage.contains("Cannot set SORT_COLUMNS as empty when SORT_SCOPE is LOCAL_SORT")
  }

  test("test delete on datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata' tblproperties('sort_scope'='no_sort','sort_columns'='name', 'inverted_index'='name')")
    sql("insert into table maintable select 'abc',21,2000")
    sql("create datamap dm_mv on table maintable using 'mv' as select name, sum(price) from maintable group by name")
    intercept[UnsupportedOperationException] {
      sql("delete from dm_mv_table where maintable_name='abc'")
    }.getMessage.contains("Delete operation is not supported for datamap table")
    sql("drop table IF EXISTS maintable")
  }

  test("test drop/show meta cache directly on mv datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select name, sum(price) from maintable group by name")
    sql("select name, sum(price) from maintable group by name").collect()
    intercept[UnsupportedOperationException] {
      sql("show metacache on table dm_table").show(false)
    }.getMessage.contains("Operation not allowed on child table.")
    intercept[UnsupportedOperationException] {
      sql("drop metacache on table dm_table").show(false)
    }.getMessage.contains("Operation not allowed on child table.")
  }

  test("test count(*) with filter") {
    sql("drop table if exists maintable")
    sql("create table maintable(id int, name string, id1 string, id2 string, dob timestamp, doj " +
        "timestamp, v1 bigint, v2 bigint, v3 decimal(30,10), v4 decimal(20,10), v5 double, v6 " +
        "double ) stored by 'carbondata'")
    sql("insert into maintable values(1, 'abc', 'id001', 'id002', '2017-01-01 00:00:00','2017-01-01 " +
        "00:00:00', 234, 2242,12.4,23.4,2323,455 )")
    checkAnswer(sql("select count(*) from maintable where  id1 < id2"), Seq(Row(1)))
    sql("drop table if exists maintable")
  }

  test("test mv with filter instance of expression") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB date, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'")
    sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop datamap if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select dob from maintable where (dob='1975-06-11' or cust_id=2)")
    }.getMessage.contains("Group by/Filter columns must be present in project columns")
    sql("drop table IF EXISTS maintable")
  }

  test("test histogram_numeric, collect_set & collect_list functions") {
    sql("drop table IF EXISTS maintable")
    sql("CREATE TABLE maintable (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'")
    sql("insert into maintable values(1, 'abc', 'abc001', '1975-06-11 01:00:03.0','1975-06-11 02:00:03.0', 120, 1234,4.34,24.56,12345, 2464, 45)")
    sql("drop datamap if exists dm ")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select histogram_numeric(1,2) from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : histogram_numeric( 1, 2)")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select collect_set(cust_id) from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : collect_set(cust_id)")
    intercept[UnsupportedOperationException] {
      sql("create datamap dm using 'mv' as select collect_list(cust_id) from maintable")
    }.getMessage.contains("MV datamap is not supported for complex datatype columns and complex datatype return types of function : collect_list(cust_id)")
    sql("drop table IF EXISTS maintable")
  }

  test("drop meta cache on mv datamap table") {
    sql("drop table IF EXISTS maintable")
    sql("create table maintable(name string, c_code int, price int) stored by 'carbondata'")
    sql("insert into table maintable select 'abc',21,2000")
    sql("drop datamap if exists dm ")
    sql("create datamap dm using 'mv' as select name, sum(price) from maintable group by name")
    sql("select name, sum(price) from maintable group by name").collect()
    val droppedCacheKeys = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())

    sql("drop metacache on table maintable").show(false)

    val cacheAfterDrop = clone(CacheProvider.getInstance().getCarbonCache.getCacheMap.keySet())
    droppedCacheKeys.removeAll(cacheAfterDrop)

    val tableIdentifier = new TableIdentifier("maintable", Some("default"))
    val carbonTable = CarbonEnv.getCarbonTable(tableIdentifier)(sqlContext.sparkSession)
    val dbPath = CarbonEnv
      .getDatabaseLocation(tableIdentifier.database.get, sqlContext.sparkSession)
    val tablePath = carbonTable.getTablePath
    val mvPath = dbPath + CarbonCommonConstants.FILE_SEPARATOR + "dm_table" +
                 CarbonCommonConstants.FILE_SEPARATOR

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
  
}


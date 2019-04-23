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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 * Test Class for MV Datamap to verify all scenerios
 */
class TestAllOperationsOnMV extends QueryTest with BeforeAndAfterEach {

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

  test("test partition table with mv") {
    sql("drop table if exists par_table")
    sql("CREATE TABLE par_table(id INT, name STRING, age INT) PARTITIONED BY(city string) STORED BY 'carbondata'")
    sql("insert into par_table values(1,'abc',3,'def')")
    sql("drop datamap if exists p1")
    sql("create datamap p1 using 'mv' WITH DEFERRED REBUILD as select city, id from par_table")
    sql("rebuild datamap p1")
    intercept[MalformedCarbonCommandException] {
      sql("alter table par_table drop partition (city='def')")
    }.getMessage.contains("Drop Partition is not supported for datamap table or for tables which have child datamap")
    sql("drop datamap if exists p1")
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

}


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

package org.apache.carbondata.spark.testsuite.datamap

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties

class TestDataMapCommand extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll {
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
    sql("create table datamaptest (a string, b string, c string) stored by 'carbondata'")
  }


  test("test datamap create") {
    sql("create datamap datamap1 on table datamaptest using 'new.class'")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 1)
    assert(dataMapSchemaList.get(0).getDataMapName.equals("datamap1"))
    assert(dataMapSchemaList.get(0).getClassName.equals("new.class"))
  }

  test("test datamap create with dmproperties") {
    sql("create datamap datamap2 on table datamaptest using 'new.class' dmproperties('key'='value')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
    assert(dataMapSchemaList.get(1).getDataMapName.equals("datamap2"))
    assert(dataMapSchemaList.get(1).getClassName.equals("new.class"))
    assert(dataMapSchemaList.get(1).getProperties.get("key").equals("value"))
  }

  test("test datamap create with existing name") {
    intercept[Exception] {
      sql(
        "create datamap datamap2 on table datamaptest using 'new.class' dmproperties('key'='value')")
    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
  }

  test("test datamap create with preagg") {
    sql("drop datamap if exists datamap3 on table datamaptest")
    sql(
      "create datamap datamap3 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
    assert(dataMapSchemaList.get(2).getDataMapName.equals("datamap3"))
    assert(dataMapSchemaList.get(2).getProperties.get("key").equals("value"))
    assert(dataMapSchemaList.get(2).getChildSchema.getTableName.equals("datamaptest_datamap3"))
  }

  test("check hivemetastore after drop datamap") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          "true")
      sql("drop datamap if exists datamap_hiveMetaStoreTable on table hiveMetaStoreTable")
      sql("drop table if exists hiveMetaStoreTable")
      sql("create table hiveMetaStoreTable (a string, b string, c string) stored by 'carbondata'")

      sql(
        "create datamap datamap_hiveMetaStoreTable on table hiveMetaStoreTable using 'preaggregate' dmproperties('key'='value') as select count(a) from hiveMetaStoreTable")
      checkExistence(sql("show datamap on table hiveMetaStoreTable"), true, "datamap_hiveMetaStoreTable")

      sql("drop datamap datamap_hiveMetaStoreTable on table hiveMetaStoreTable")
      checkExistence(sql("show datamap on table hiveMetaStoreTable"), false, "datamap_hiveMetaStoreTable")

    }
    finally {
      sql("drop table hiveMetaStoreTable")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    }
  }

  test("drop the table having pre-aggregate"){
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          "true")
      sql("drop datamap if exists datamap_hiveMetaStoreTable_1 on table hiveMetaStoreTable_1")
      sql("drop table if exists hiveMetaStoreTable_1")
      sql("create table hiveMetaStoreTable_1 (a string, b string, c string) stored by 'carbondata'")

      sql(
        "create datamap datamap_hiveMetaStoreTable_1 on table hiveMetaStoreTable_1 using 'preaggregate' dmproperties('key'='value') as select count(a) from hiveMetaStoreTable_1")

      checkExistence(sql("show datamap on table hiveMetaStoreTable_1"),
        true,
        "datamap_hiveMetaStoreTable_1")

      sql("drop table hiveMetaStoreTable_1")

      checkExistence(sql("show tables"), false, "datamap_hiveMetaStoreTable_1")
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    }
  }

  test("test datamap create with preagg with duplicate name") {
    intercept[Exception] {
      sql(
        "create datamap datamap2 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }

  test("test datamap drop with preagg") {
    intercept[Exception] {
      sql("drop table datamap3")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }

  test("test show datamap without preaggregate") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap datamap1 on table datamapshowtest using 'new.class' dmproperties('key'='value')")
    sql("create datamap datamap2 on table datamapshowtest using 'new.class' dmproperties('key'='value')")
    checkExistence(sql("show datamap on table datamapshowtest"), true, "datamap1", "datamap2", "(NA)", "new.class")
  }

  test("test show datamap with preaggregate") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
    sql("create datamap datamap2 on table datamapshowtest using 'new.class' dmproperties('key'='value')")
    val frame = sql("show datamap on table datamapshowtest")
    assert(frame.collect().length == 2)
    checkExistence(frame, true, "datamap1", "datamap2", "(NA)", "new.class", "default.datamapshowtest_datamap1")
  }

  test("test show datamap with no datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    assert(sql("show datamap on table datamapshowtest").collect().length == 0)
  }

  test("test show datamap after dropping datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
    sql("create datamap datamap2 on table datamapshowtest using 'new.class' dmproperties('key'='value')")
    sql("drop datamap datamap1 on table datamapshowtest")
    val frame = sql("show datamap on table datamapshowtest")
    assert(frame.collect().length == 1)
    checkExistence(frame, true, "datamap2", "(NA)", "new.class")
  }

  test("test if preaggregate load is successfull for hivemetastore") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE, "true")
    sql("DROP TABLE IF EXISTS maintable")
    sql(
      """
        | CREATE TABLE maintable(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    sql(
      s"""create datamap preagg_sum on table maintable using 'preaggregate' as select id,sum(age) from maintable group by id"""
        .stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table maintable")
    checkAnswer(sql(s"select * from maintable_preagg_sum"),
      Seq(Row(1, 31), Row(2, 27), Row(3, 70), Row(4, 55)))
  }


  override def afterAll {
    sql("DROP TABLE IF EXISTS maintable")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
  }
}

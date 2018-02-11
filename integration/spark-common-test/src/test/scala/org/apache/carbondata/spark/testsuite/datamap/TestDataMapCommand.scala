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

import java.io.{File, FilenameFilter}

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestDataMapCommand extends QueryTest with BeforeAndAfterAll {

  val testData = s"$resourcesPath/sample.csv"

  override def beforeAll {
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
    sql("drop table if exists uniqdata")
    sql("create table datamaptest (a string, b string, c string) stored by 'carbondata'")
  }

  val newClass = "org.apache.spark.sql.CarbonSource"

  test("test datamap create: don't support using class, only support short name") {
    intercept[MalformedDataMapCommandException] {
      sql(s"CREATE DATAMAP datamap1 ON TABLE datamaptest USING '$newClass'")
      val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
      assert(table != null)
      val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
      assert(dataMapSchemaList.size() == 1)
      assert(dataMapSchemaList.get(0).getDataMapName.equals("datamap1"))
      assert(dataMapSchemaList.get(0).getClassName.equals(newClass))
    }
  }

  test("test datamap create with dmproperties: don't support using class") {
    intercept[MalformedDataMapCommandException] {
      sql(s"CREATE DATAMAP datamap2 ON TABLE datamaptest USING '$newClass' DMPROPERTIES('key'='value')")
      val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
      assert(table != null)
      val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
      assert(dataMapSchemaList.size() == 2)
      assert(dataMapSchemaList.get(1).getDataMapName.equals("datamap2"))
      assert(dataMapSchemaList.get(1).getClassName.equals(newClass))
      assert(dataMapSchemaList.get(1).getProperties.get("key").equals("value"))
    }
  }

  test("test datamap create with existing name: don't support using class") {
    intercept[MalformedDataMapCommandException] {
      sql(
        s"CREATE DATAMAP datamap2 ON TABLE datamaptest USING '$newClass' DMPROPERTIES('key'='value')")
      val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
      assert(table != null)
      val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
      assert(dataMapSchemaList.size() == 2)
    }
  }

  test("test datamap create with preagg") {
    sql("drop datamap if exists datamap3 on table datamaptest")
    sql(
      "create datamap datamap3 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 1)
    assert(dataMapSchemaList.get(0).getDataMapName.equals("datamap3"))
    assert(dataMapSchemaList.get(0).getProperties.get("key").equals("value"))
    assert(dataMapSchemaList.get(0).getChildSchema.getTableName.equals("datamaptest_datamap3"))
  }

  test("check hivemetastore after drop datamap") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          "true")
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

  test("drop the table having pre-aggregate") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          "true")
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
        s"""
           | CREATE DATAMAP datamap2 ON TABLE datamaptest
           | USING 'preaggregate'
           | DMPROPERTIES('key'='value')
           | AS SELECT COUNT(a) FROM datamaptest
         """.stripMargin)
      sql(
        s"""
           | CREATE DATAMAP datamap2 ON TABLE datamaptest
           | USING 'preaggregate'
           | DMPROPERTIES('key'='value')
           | AS SELECT COUNT(a) FROM datamaptest
         """.stripMargin)
    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
  }

  test("test datamap drop with preagg") {
    intercept[Exception] {
      sql("drop table datamap3")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default", "datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
  }

  test("test show datamap without preaggregate: don't support using class") {
    intercept[MalformedDataMapCommandException] {
      sql("drop table if exists datamapshowtest")
      sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
      sql(s"CREATE DATAMAP datamap1 ON TABLE datamapshowtest USING '$newClass' DMPROPERTIES('key'='value')")
      sql(s"CREATE DATAMAP datamap2 ON TABLE datamapshowtest USING '$newClass' DMPROPERTIES('key'='value')")
      checkExistence(sql("SHOW DATAMAP ON TABLE datamapshowtest"), true, "datamap1", "datamap2", "(NA)", newClass)
    }
  }

  test("test show datamap with preaggregate: don't support using class") {
    intercept[MalformedDataMapCommandException] {
      sql("drop table if exists datamapshowtest")
      sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
      sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
      sql(s"CREATE DATAMAP datamap2 ON TABLE datamapshowtest USING '$newClass' DMPROPERTIES('key'='value')")
      val frame = sql("show datamap on table datamapshowtest")
      assert(frame.collect().length == 2)
      checkExistence(frame, true, "datamap1", "datamap2", "(NA)", newClass, "default.datamapshowtest_datamap1")
    }
  }

  test("test show datamap with no datamap") {
    sql("drop table if exists datamapshowtest")
    sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
    assert(sql("show datamap on table datamapshowtest").collect().length == 0)
  }

  test("test show datamap after dropping datamap: don't support using class") {
    intercept[MalformedDataMapCommandException] {
      sql("drop table if exists datamapshowtest")
      sql("create table datamapshowtest (a string, b string, c string) stored by 'carbondata'")
      sql("create datamap datamap1 on table datamapshowtest using 'preaggregate' as select count(a) from datamapshowtest")
      sql(s"CREATE DATAMAP datamap2 ON TABLE datamapshowtest USING '$newClass' DMPROPERTIES('key'='value')")
      sql("drop datamap datamap1 on table datamapshowtest")
      val frame = sql("show datamap on table datamapshowtest")
      assert(frame.collect().length == 1)
      checkExistence(frame, true, "datamap2", "(NA)", newClass)
    }
  }

  test("test if preaggregate load is successfull for hivemetastore") {
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE, "true")
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
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    }
  }

  test("test preaggregate load for decimal column for hivemetastore") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE, "true")
    sql("CREATE TABLE uniqdata(CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp,DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'")
    sql("insert into uniqdata select 9000,'CUST_NAME_00000','ACTIVE_EMUI_VERSION_00000','1970-01-01 01:00:03','1970-01-01 02:00:03',123372036854,-223372036854,12345678901.1234000000,22345678901.1234000000,11234567489.7976000000,-11234567489.7976000000,1")
    sql("create datamap uniqdata_agg on table uniqdata using 'preaggregate' as select min(DECIMAL_COLUMN1) from uniqdata group by DECIMAL_COLUMN1")
    checkAnswer(sql("select * from uniqdata_uniqdata_agg"), Seq(Row(12345678901.1234000000, 12345678901.1234000000)))
    sql("drop datamap if exists uniqdata_agg on table uniqdata")
  }

  test("create pre-agg table with path") {
    sql("drop table if exists main_preagg")
    sql("drop table if exists main ")
    val warehouse = s"$metastoredb/warehouse"
    val path = warehouse + "/" + System.nanoTime + "_preAggTestPath"
    sql(
      s"""
         | create table main(
         |     year int,
         |     month int,
         |     name string,
         |     salary int)
         | stored by 'carbondata'
         | tblproperties('sort_columns'='month,year,name')
      """.stripMargin)
    sql("insert into main select 10,11,'amy',12")
    sql("insert into main select 10,11,'amy',14")
    sql(
      s"""
         | create datamap preagg
         | on table main
         | using 'preaggregate'
         | dmproperties ('path'='$path')
         | as select name,avg(salary)
         |    from main
         |    group by name
       """.stripMargin)
    assertResult(true)(new File(path).exists())
    assertResult(true)(new File(s"${CarbonTablePath.getSegmentPath(path, "0")}")
      .list(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.contains(CarbonCommonConstants.FACT_FILE_EXT)
        }
      }).length > 0)
    checkAnswer(sql("select name,avg(salary) from main group by name"), Row("amy", 13.0))
    checkAnswer(sql("select * from main_preagg"), Row("amy", 26, 2))
    sql("drop datamap preagg on table main")
    assertResult(false)(new File(path).exists())
    sql("drop table main")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS maintable")
    sql("drop table if exists uniqdata")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
      CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
    sql("drop table if exists datamaptest")
    sql("drop table if exists datamapshowtest")
  }
}

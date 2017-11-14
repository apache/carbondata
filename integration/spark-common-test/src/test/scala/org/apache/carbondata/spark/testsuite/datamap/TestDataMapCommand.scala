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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata

class TestDataMapCommand extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists datamaptest")
    sql("create table datamaptest (a string, b string, c string) stored by 'carbondata'")
  }


  test("test datamap create") {
    sql("create datamap datamap1 on table datamaptest using 'new.class'")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 1)
    assert(dataMapSchemaList.get(0).getDataMapName.equals("datamap1"))
    assert(dataMapSchemaList.get(0).getClassName.equals("new.class"))
  }

  test("test datamap create with dmproperties") {
    sql("create datamap datamap2 on table datamaptest using 'new.class' dmproperties('key'='value')")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
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
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 2)
  }

  test("test datamap create with preagg") {
    sql("drop datamap if exists datamap3 on table datamaptest")
    sql(
      "create datamap datamap3 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
    assert(dataMapSchemaList.get(2).getDataMapName.equals("datamap3"))
    assert(dataMapSchemaList.get(2).getProperties.get("key").equals("value"))
    assert(dataMapSchemaList.get(2).getChildSchema.getTableName.equals("datamaptest_datamap3"))
  }

  test("test datamap create with preagg with duplicate name") {
    intercept[Exception] {
      sql(
        "create datamap datamap2 on table datamaptest using 'preaggregate' dmproperties('key'='value') as select count(a) from datamaptest")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }

  test("test datamap drop with preagg") {
    intercept[Exception] {
      sql("drop table datamap3")

    }
    val table = CarbonMetadata.getInstance().getCarbonTable("default_datamaptest")
    assert(table != null)
    val dataMapSchemaList = table.getTableInfo.getDataMapSchemaList
    assert(dataMapSchemaList.size() == 3)
  }


  override def afterAll {
    sql("drop table if exists datamaptest")
  }
}

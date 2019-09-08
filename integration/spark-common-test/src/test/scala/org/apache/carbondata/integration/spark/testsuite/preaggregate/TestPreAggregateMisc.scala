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
package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants

@Ignore
class TestPreAggregateMisc extends CarbonQueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
  }

  test("test PreAggregate With Set Segments property") {
    sql("create datamap agg1 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql("SET carbon.input.segments.default.mainTable=0")
    checkAnswer(
      sql("select sum(age) from mainTable"),
      Seq(Row(183.0)))
    sqlContext.sparkSession.catalog.clearCache()
    sql("RESET")
    sql("drop datamap agg1 on table mainTable")

  }

  test("check preagg tbl properties sort columns inherit from main tbl") {
    sql("drop table if exists y ")
    sql(
      "create table y(year int,month int,name string,salary int) stored by 'carbondata' " +
      "tblproperties('NO_INVERTED_INDEX'='name','sort_scope'='Global_sort'," +
      "'table_blocksize'='23','Dictionary_include'='month','Dictionary_exclude'='year,name'," +
      "'sort_columns'='month,year,name')")
    sql("insert into y select 10,11,'babu',12")
    sql(
      "create datamap y1_sum1 on table y using 'preaggregate' as select year,month,name,sum" +
      "(salary) from y group by year,month,name")

    val carbonTable = CarbonEnv.getCarbonTable(Some("default"), "y")(sqlContext.sparkSession)
    val datamaptable = CarbonEnv
      .getCarbonTable(Some("default"), "y_y1_sum1")(sqlContext.sparkSession)

    val sortcolumns = datamaptable.getTableInfo.getFactTable.getTableProperties
      .get(CarbonCommonConstants.SORT_COLUMNS)
    val sortcolummatch = sortcolumns != null && sortcolumns.equals("y_month,y_year,y_name")

    val sortscope = datamaptable.getTableInfo.getFactTable.getTableProperties.get("sort_scope")
    val sortscopematch = sortscope != null && sortscope.equals(
      carbonTable.getTableInfo.getFactTable.getTableProperties.get("sort_scope"))
    val blockSize = datamaptable.getTableInfo.getFactTable.getTableProperties
      .get(CarbonCommonConstants.TABLE_BLOCKSIZE)
    val blocksizematch = blockSize != null &&
                         blockSize.equals(carbonTable.getTableInfo.getFactTable.getTableProperties.
                           get(CarbonCommonConstants.TABLE_BLOCKSIZE))
    assert(sortcolummatch && sortscopematch && blocksizematch)
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS mainTable")
    sql("DROP TABLE IF EXISTS y")
  }
}

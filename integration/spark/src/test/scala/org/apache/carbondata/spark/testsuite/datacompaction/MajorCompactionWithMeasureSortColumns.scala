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
package org.apache.carbondata.spark.testsuite.datacompaction

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class MajorCompactionWithMeasureSortColumns extends QueryTest with BeforeAndAfterAll {

  val csvFilePath = s"$resourcesPath/compaction/nodictionary_compaction.csv"
  val backupDateFormat = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  override def beforeAll: Unit = {
    sql("drop table if exists store")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  }

  override def afterAll {
    sql("drop table if exists  store")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, backupDateFormat)
  }

  test("test major compaction with measure sort columns") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE, "1024")

    val createStoreTableSql =
      s"""
         | CREATE TABLE IF NOT EXISTS store(
         | code1 STRING,
         | code2 STRING,
         | country_code STRING,
         | category_id INTEGER,
         | product_id LONG,
         | date DATE,
         | count1 LONG,
         | count2 LONG,
         | count3 LONG
         | )
         | STORED AS carbondata
         | TBLPROPERTIES(
         | 'SORT_COLUMNS'='code1, code2, country_code, date, category_id, product_id',
         | 'SORT_SCOPE'='LOCAL_SORT',
         | 'CACHE_LEVEL'='BLOCKLET'
         | )
      """.stripMargin
    sql(createStoreTableSql)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvFilePath'
         | INTO TABLE store
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin).show(false)

    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvFilePath'
         | INTO TABLE store
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin).show(false)

    val csvRows = sqlContext.sparkSession.read.option("header", "true")
      .csv(csvFilePath).orderBy("code1")

    sql("ALTER TABLE store COMPACT 'MAJOR'")

    val answer = sql("select * from store ").orderBy("code1")
    assert(answer.except(csvRows).count() == 0)
    sql("drop table store")
  }

}

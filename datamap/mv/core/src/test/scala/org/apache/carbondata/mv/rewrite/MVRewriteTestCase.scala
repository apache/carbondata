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

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVRewriteTestCase extends QueryTest with BeforeAndAfterAll {


  override def beforeAll(): Unit = {
    drop
    sql("create table region(l4id string,l4name string) using carbondata")
    sql(s"""create table data_table(
        |starttime int, seq long,succ long,LAYER4ID string,tmp int)
        |using carbondata""".stripMargin)
  }

  def drop(): Unit ={
    sql("drop table if exists region")
    sql("drop table if exists data_table")
  }

  test("test mv count and case when expression") {
    sql("drop materialized view if exists data_table_mv")
    sql(s"""create materialized view data_table_mv as
        | SELECT STARTTIME,LAYER4ID,
        |  SUM(seq) AS seq_c,
        |  SUM(succ)  AS succ_c
        | FROM data_table
        | GROUP BY STARTTIME,LAYER4ID""".stripMargin)

    sql("refresh materialized view data_table_mv")

    val frame =
      sql(s"""SELECT  MT.`3600` AS `3600`,
          | MT.`2250410101` AS `2250410101`,
          | (CASE WHEN (SUM(COALESCE(seq_c, 0))) = 0 THEN NULL
          |   ELSE
          |   (CASE WHEN (CAST((SUM(COALESCE(seq_c, 0))) AS int)) = 0 THEN 0
          |     ELSE ((CAST((SUM(COALESCE(succ_c, 0))) AS double))
          |     / (CAST((SUM(COALESCE(seq_c, 0))) AS double)))
          |     END) * 100
          |   END) AS rate
          | FROM (
          |   SELECT sum_result.*, H_REGION.`2250410101` FROM
          |   (SELECT cast(floor((starttime + 28800) / 3600) * 3600 - 28800 as int) AS `3600`,
          |     LAYER4ID,
          |     COALESCE(SUM(seq), 0) AS seq_c,
          |     COALESCE(SUM(succ), 0) AS succ_c
          |       FROM data_table
          |       WHERE STARTTIME >= 1549866600 AND STARTTIME < 1549899900
          |       GROUP BY cast(floor((STARTTIME + 28800) / 3600) * 3600 - 28800 as int),LAYER4ID
          |   )sum_result
          |   LEFT JOIN
          |   (SELECT l4id AS `225040101`,
          |     l4name AS `2250410101`,
          |     l4name AS NAME_2250410101
          |       FROM region
          |       GROUP BY l4id, l4name) H_REGION
          |   ON sum_result.LAYER4ID = H_REGION.`225040101`
          | WHERE H_REGION.NAME_2250410101 IS NOT NULL
          | ) MT
          | GROUP BY MT.`3600`, MT.`2250410101`
          | ORDER BY `3600` ASC LIMIT 5000""".stripMargin)

    assert(TestUtil.verifyMVDataMap(frame.queryExecution.optimizedPlan, "data_table_mv"))
  }

  override def afterAll(): Unit = {
    drop
  }
}

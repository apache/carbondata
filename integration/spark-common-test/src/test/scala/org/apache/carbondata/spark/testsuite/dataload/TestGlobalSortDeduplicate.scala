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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

/**
 * TestGlobalSortDeduplicate
 */
class TestGlobalSortDeduplicate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable

    sql(
      """
        | CREATE TABLE carbon_deduplicate_1(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS' = 'name', 'DEDUPLICATE_BY'='id')
      """.stripMargin)

    sql(
      """
        | CREATE TABLE carbon_deduplicate_2(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata TBLPROPERTIES('SORT_SCOPE'='GLOBAL_SORT', 'SORT_COLUMNS' = 'name')
      """.stripMargin)

    prepareBaseTable("carbon_deduplicate_view_1", 1000, 10)
    prepareBaseTable("carbon_deduplicate_view_2", 10000, 100)
  }

  def prepareBaseTable(tableName: String, totalRows: Int, duplicateRows: Int): Unit = {
    val data1 = (1 to totalRows).map { index =>
      Row(index, s"name_$index", s"city_$index", index % 100)
    }
    val rdd = sqlContext.sparkContext.parallelize(data1 ++ data1.take(duplicateRows), 1)
    val schema = StructType(
      StructField("id", IntegerType, nullable = false) ::
      StructField("name", StringType, nullable = false) ::
      StructField("city", StringType, nullable = false) ::
      StructField("age", IntegerType, nullable = false) :: Nil)

    sqlContext.createDataFrame(rdd, schema).createOrReplaceTempView(tableName)
  }

  override def afterAll(): Unit = {
    dropTable
  }

  private def dropTable: Unit = {
    sql("DROP TABLE IF EXISTS carbon_deduplicate_1")
    sql("DROP TABLE IF EXISTS carbon_deduplicate_2")
    sql("DROP VIEW IF EXISTS carbon_deduplicate_view_1")
    sql("DROP VIEW IF EXISTS carbon_deduplicate_view_2")
  }


  test("deduplicate during data loading") {
    checkAnswer(
      sql("select count(*) from carbon_deduplicate_view_1"),
      Seq(Row(1010))
    )
    sql("insert into table carbon_deduplicate_1 select * from carbon_deduplicate_view_1")
    checkAnswer(
      sql("select count(*) from carbon_deduplicate_1"),
      Seq(Row(1000))
    )
  }

  test("deduplicate during compaction") {
    sql("alter table carbon_deduplicate_2 set tblproperties('DEDUPLICATE_BY'='id')")

    sql("insert into table carbon_deduplicate_2 select * from carbon_deduplicate_view_1")
    sql("insert into table carbon_deduplicate_2 select * from carbon_deduplicate_view_2")
    sql("insert into table carbon_deduplicate_2 select * from carbon_deduplicate_view_1")
    checkAnswer(
      sql("select count(*) from carbon_deduplicate_2"),
      Seq(Row(1000 + 10000 + 1000))
    )
    sql("alter table carbon_deduplicate_2 compact 'custom' where segment.id in (0, 1, 2)")
    checkAnswer(
      sql("select count(*) from carbon_deduplicate_2"),
      Seq(Row(10000))
    )
    checkAnswer(
      sql("select count(distinct id) from carbon_deduplicate_2"),
      Seq(Row(10000))
    )
  }


}

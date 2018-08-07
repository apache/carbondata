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

package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example is for pre-aggregate tables.
 */

object MVDataMapExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("MVDataMapExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark: SparkSession): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/sample.csv"

    // 1. simple usage for Pre-aggregate tables creation and query
    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS dimtable")
    spark.sql(
      """
        | CREATE TABLE mainTable
        | (id Int,
        | name String,
        | city String,
        | age Int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    spark.sql(
      """
        | CREATE TABLE dimtable
        | (name String,
        | address String)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)

    spark.sql(s"""LOAD DATA LOCAL INPATH '$testData' into table mainTable""")

    spark.sql(s"""insert into dimtable select name, concat(city, ' street1') as address from
           |mainTable group by name, address""".stripMargin)


    // 1. create simple sub projection MV datamap

    // sub projections to be hit
    spark.sql(s"""create datamap simple_sub_projection using 'mv' as
         | select id,name from mainTable"""
        .stripMargin)
    spark.sql(s"""rebuild datamap simple_sub_projection""")

    // Check the physical plan which uses MV table simple_sub_projection_table instead of mainTable
    spark.sql(s"""select id from mainTable""").explain(true)

    // Check the physical plan which uses MV table simple_sub_projection_table instead of mainTable
    spark.sql(s"""select sum(id) from mainTable""").explain(true)

    // aggregate functions to be hit
    spark.sql(
      s"""create datamap simple_agg using 'mv' as
         | select id,sum(age) from mainTable group by id""".stripMargin)

    spark.sql(s"""rebuild datamap simple_agg""")

    // Check the physical plan which uses MV table simple_agg_table instead of mainTable
    spark.sql(s"""select id,sum(age) from mainTable group by id""").explain(true)

    // Check the physical plan of subquery which uses MV table simple_agg_table instead of mainTable
    spark.sql(s"""select sub.id from (select id ,sum(age) from mainTable group by id) sub where sub
           |.id = 4""".stripMargin).explain(true)



    // join with another table and aggregate functions to be hit
    spark.sql(s"""create datamap simple_agg_with_join using 'mv' as
         | select id,address, sum(age) from mainTable inner join dimtable on mainTable
         | .name=dimtable.name group by id ,address""".stripMargin)
    spark.sql(s"""rebuild datamap simple_agg_with_join""")

    // Check the physical plan which uses MV table simple_agg_with_join_table instead of
    // mainTable and dimtable.
    spark.sql(s"""select id,address, sum(age) from mainTable inner join dimtable on mainTable
           |.name=dimtable.name group by id ,address""".stripMargin).explain(true)

    // Check the physical plan which uses MV table simple_agg_with_join_table instead of
    // mainTable and dimtable.
    spark.sql(s"""select id,address, sum(age) from mainTable inner join dimtable on mainTable
                 |.name=dimtable.name where id =1 group by id ,address""".stripMargin).explain(true)

    // Show datamaps
    spark.sql("show datamap").show(false)

    // Drop datamap
    spark.sql("drop datamap if exists simple_agg_with_join")

    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS dimtable")
  }
}

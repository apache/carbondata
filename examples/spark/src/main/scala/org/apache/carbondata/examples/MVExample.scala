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
import java.util.Random

import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example is for Materialized View.
 */

object MVExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("MVDataMapExample")
    exampleBody(spark)
    performanceTest(spark)
    spark.close()
  }

  def exampleBody(spark: SparkSession): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/sample.csv"

    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS dimtable")
    spark.sql(
      """
        | CREATE TABLE mainTable
        | (id Int,
        | name String,
        | city String,
        | age Int)
        | STORED AS carbondata
      """.stripMargin)

    spark.sql(
      """
        | CREATE TABLE dimtable
        | (name String,
        | address String)
        | STORED AS carbondata
      """.stripMargin)

    spark.sql(s"""LOAD DATA LOCAL INPATH '$testData' into table mainTable""")

    spark.sql(s"""insert into dimtable select name, concat(city, ' street1') as address from
           |mainTable group by name, address""".stripMargin)


    // 1. create simple sub projection MV

    // sub projections to be hit
    spark.sql(s"""create materialized view simple_sub_projection as
         | select id,name from mainTable"""
        .stripMargin)
    spark.sql(s"""refresh materialized view simple_sub_projection""")

    // Check the physical plan which uses MV table simple_sub_projection_table instead of mainTable
    spark.sql(s"""select id from mainTable""").explain(true)

    // Check the physical plan which uses MV table simple_sub_projection_table instead of mainTable
    spark.sql(s"""select sum(id) from mainTable""").explain(true)

    // 2. aggregate functions to be hit
    spark.sql(
      s"""create materialized view simple_agg as
         | select id,sum(age) from mainTable group by id""".stripMargin)

    spark.sql(s"""refresh materialized view simple_agg""")

    // Check the physical plan which uses MV table simple_agg_table instead of mainTable
    spark.sql(s"""select id,sum(age) from mainTable group by id""").explain(true)

    // Check the physical plan of subquery which uses MV table simple_agg_table instead of mainTable
    spark.sql(s"""select sub.id from (select id ,sum(age) from mainTable group by id) sub where sub
           |.id = 4""".stripMargin).explain(true)

    // 3.join with another table and aggregate functions to be hit
    spark.sql(s"""create materialized view simple_agg_with_join as
         | select id,address, sum(age) from mainTable inner join dimtable on mainTable
         | .name=dimtable.name group by id ,address""".stripMargin)
    spark.sql(s"""refresh materialized view simple_agg_with_join""")

    // Check the physical plan which uses MV table simple_agg_with_join_table instead of
    // mainTable and dimtable.
    spark.sql(s"""select id,address, sum(age) from mainTable inner join dimtable on mainTable
           |.name=dimtable.name group by id ,address""".stripMargin).explain(true)

    // Check the physical plan which uses MV table simple_agg_with_join_table instead of
    // mainTable and dimtable.
    spark.sql(s"""select id,address, sum(age) from mainTable inner join dimtable on mainTable
                 |.name=dimtable.name where id =1 group by id ,address""".stripMargin).explain(true)

    // Show datamaps
    spark.sql("show materialized views").show(false)

    // Drop datamap
    spark.sql("drop materialized view if exists simple_agg_with_join")

    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS dimtable")
  }

  private def performanceTest(spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS employee_salary")
    spark.sql("DROP TABLE IF EXISTS employee_salary_without_mv")
    spark.sql("DROP TABLE IF EXISTS emp_address")

    createFactTable(spark, "employee_salary")
    createFactTable(spark, "employee_salary_without_mv")

    spark.sql(
      """
        | CREATE TABLE emp_address
        | (name String,
        | address String)
        | STORED AS carbondata
      """.stripMargin)

    spark.sql(
      s"""insert into emp_address select name, concat(city, ' street1') as address from
         |employee_salary group by name, address""".stripMargin)

    spark.sql(
      s"""create datamap simple_agg_employee using 'mv' as
         | select id,sum(salary) from employee_salary group by id""".stripMargin)
    spark.sql(s"""rebuild datamap simple_agg_employee""")

    // Test performance of aggregate queries with mv datamap
    val timeWithOutMv = time(spark
      .sql("select id, name, sum(salary) from employee_salary_without_mv group by id,name")
      .collect())
    val timeWithMv = time(spark
      .sql("select id,name,sum(salary) from employee_salary group by id,name").collect())
    // scalastyle:off
    println("Time of table with MV is : " + timeWithMv + " time withoutmv : " + timeWithOutMv)
    // scalastyle:on
    val timeWithOutMvFilter = time(spark
      .sql(
        "select id, name, sum(salary) from employee_salary_without_mv where name='name10' group " +
        "by id,name")
      .collect())
    val timeWithMvFilter = time(spark
      .sql("select id,name,sum(salary) from employee_salary where name='name10' group by id,name")
      .collect())
    // scalastyle:off
    println("Time of table with MV with filter is : " + timeWithMvFilter + " time withoutmv : " +
            timeWithOutMvFilter)
    // scalastyle:on

    // Tests performance of aggregate with join queries.
    spark.sql(
      s"""create datamap simple_join_agg_employee using 'mv' as
         | select id,address, sum(salary) from employee_salary f join emp_address d
         | on f.name=d.name group by id,address""".stripMargin)
    spark.sql(s"""rebuild datamap simple_join_agg_employee""")

    val timeWithMVJoin =
      time(spark.sql(
        s"""select id,address, sum(salary) from employee_salary f join emp_address d
           | on f.name=d.name group by id,address""".stripMargin).collect())
    val timeWithOutMVJoin =
      time(spark.sql(
        s"""select id,address, sum(salary) from employee_salary_without_mv f
           |join emp_address d on f.name=d.name group by id,address""".stripMargin).collect())
    // scalastyle:off
    println("Time of table with MV with join is : " + timeWithMVJoin + " time withoutmv : " +
            timeWithOutMVJoin)
    // scalastyle:on

    spark.sql("DROP TABLE IF EXISTS employee_salary")
    spark.sql("DROP TABLE IF EXISTS emp_address")
    spark.sql("DROP TABLE IF EXISTS employee_salary_without_mv")
  }

  private def createFactTable(spark: SparkSession, tableName: String): Unit = {
    import spark.implicits._
    val rand = new Random()
    // Create fact table with datamap
    val df = spark.sparkContext.parallelize(1 to 1000000)
      .map(x => (x % 1000, "name" + x % 1000, "city" + x % 100, rand.nextInt()))
      .toDF("id", "name", "city", "salary")

    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .save()
  }

  // define time function
  private def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }
}

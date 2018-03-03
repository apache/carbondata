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

import org.apache.spark.sql.SaveMode

/**
 * This example is for pre-aggregate tables.
 */

object PreAggregateTableExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/sample.csv"
    val spark = ExampleUtils.createCarbonSession("PreAggregateTableExample")

    spark.sparkContext.setLogLevel("ERROR")

    // 1. simple usage for Pre-aggregate tables creation and query
    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS mainTable_other")
    spark.sql("""
                | CREATE TABLE mainTable
                | (id Int,
                | name String,
                | city String,
                | age Int)
                | STORED BY 'org.apache.carbondata.format'
              """.stripMargin)

    spark.sql("""
                | CREATE TABLE mainTable_other
                | (id Int,
                | name String,
                | city String,
                | age Int)
                | STORED BY 'org.apache.carbondata.format'
              """.stripMargin)

    spark.sql(s"""
       LOAD DATA LOCAL INPATH '$testData' into table mainTable
       """)

    spark.sql(s"""
       LOAD DATA LOCAL INPATH '$testData' into table mainTable_other
       """)

    spark.sql(
      s"""create datamap preagg_sum on table mainTable using 'preaggregate' as
         | select id,sum(age) from mainTable group by id"""
        .stripMargin)
    spark.sql(
      s"""create datamap preagg_avg on table mainTable using 'preaggregate' as
         | select id,avg(age) from mainTable group by id"""
        .stripMargin)

    spark.sql(
      s"""create datamap preagg_count_age on table mainTable using 'preaggregate' as
         | select id,count(age) from mainTable group by id"""
        .stripMargin)

    spark.sql(
      s"""create datamap preagg_min on table mainTable using 'preaggregate' as
         | select id,min(age) from mainTable group by id"""
        .stripMargin)

    spark.sql(
      s"""create datamap preagg_max on table mainTable using 'preaggregate' as
         | select id,max(age) from mainTable group by id"""
        .stripMargin)

    spark.sql(
      s"""
         | create datamap preagg_case on table mainTable using 'preaggregate' as
         | select name,sum(case when age=35 then id else 0 end) from mainTable group by name
         | """.stripMargin)

    spark.sql(
      s"""create datamap preagg_count on table maintable using 'preaggregate' as
         | select name, count(*) from maintable group by name""".stripMargin)

    spark.sql("show datamap on table maintable").show
    
    spark.sql(
      s"""
         | SELECT id,max(age)
         | FROM mainTable group by id
      """.stripMargin).show()

    spark.sql(
      s"""
         | select name, count(*) from
         | mainTable group by name
      """.stripMargin).show()

    spark.sql(
      s"""
         | select name as NewName,
         | sum(case when age=35 then id else 0 end) as sum
         | from mainTable group by name order by name
      """.stripMargin).show()

    spark.sql(
      s"""
         | select t1.name,t1.city from mainTable_other t1 join
         | (select name as newnewname,sum(age) as sum
         | from mainTable group by name )t2 on t1.name=t2.newnewname
      """.stripMargin).show()

    // 2.compare the performance : with pre-aggregate VS main table

    // build test data, if set the data is larger than 100M, it will take 10+ mins.
    import spark.implicits._

    import scala.util.Random
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10 * 1000 * 1000)
      .map(x => ("No." + r.nextInt(100000), "name" + x % 8, "city" + x % 50, x % 60))
      .toDF("ID", "name", "city", "age")

    // Create table with pre-aggregate table
    df.write.format("carbondata")
      .option("tableName", "personTable")
      .option("compress", "true")
      .mode(SaveMode.Overwrite).save()

    // Create table without pre-aggregate table
    df.write.format("carbondata")
      .option("tableName", "personTableWithoutAgg")
      .option("compress", "true")
      .mode(SaveMode.Overwrite).save()

    // Create pre-aggregate table
    spark.sql("""
       CREATE datamap preagg_avg on table personTable using 'preaggregate' as
       | select id,avg(age) from personTable group by id
              """.stripMargin)

    // define time function
    def time(code: => Unit): Double = {
      val start = System.currentTimeMillis()
      code
      // return time in second
      (System.currentTimeMillis() - start).toDouble / 1000
    }

    val time_without_aggTable = time {
      spark.sql(
        s"""
           | SELECT id, avg(age)
           | FROM personTableWithoutAgg group by id
      """.stripMargin).count()
    }

    val time_with_aggTable = time {
      spark.sql(
        s"""
           | SELECT id, avg(age)
           | FROM personTable group by id
      """.stripMargin).count()
    }
    // scalastyle:off
    println("time for query on table with pre-aggregate table:" + time_with_aggTable.toString)
    println("time for query on table without pre-aggregate table:" + time_without_aggTable.toString)
    // scalastyle:on

    // 3. if avg function is defined for a column, sum also can be used on that;but not other way
    // round
    val time_without_aggTable_sum = time {
      spark.sql(
        s"""
           | SELECT id, sum(age)
           | FROM personTableWithoutAgg group by id
      """.stripMargin).count()
    }

    val time_with_aggTable_sum = time {
      spark.sql(
        s"""
           | SELECT id, sum(age)
           | FROM personTable group by id
      """.stripMargin).count()
    }
    // scalastyle:off
    println("time for query with function sum on table with pre-aggregate table:" +
      time_with_aggTable_sum.toString)
    println("time for query with function sum on table without pre-aggregate table:" +
      time_without_aggTable_sum.toString)
    // scalastyle:on

    spark.sql("DROP TABLE IF EXISTS mainTable")
    spark.sql("DROP TABLE IF EXISTS mainTable_other")
    spark.sql("DROP TABLE IF EXISTS personTable")
    spark.sql("DROP TABLE IF EXISTS personTableWithoutAgg")

    spark.close()

  }
}

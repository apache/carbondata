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

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.examples.util.ExampleUtils


/**
 * This example is for lucene datamap.
 */

object LuceneDataMapExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("LuceneDataMapExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {

    // build the test data, please increase the data for more obvious comparison.
    // if set the data is larger than 100M, it will take 10+ mins.
    import scala.util.Random

    import spark.implicits._
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10 * 10 * 1000)
      .map(x => ("which test" + r.nextInt(10000) + " good" + r.nextInt(10),
      "who and name" + x % 8, "city" + x % 50, x % 60))
      .toDF("id", "name", "city", "age")

    spark.sql("DROP TABLE IF EXISTS personTable")
    df.write.format("carbondata")
      .option("tableName", "personTable")
      .option("compress", "true")
      .mode(SaveMode.Overwrite).save()

    // create lucene datamap on personTable
    spark.sql(
      s"""
         | CREATE DATAMAP IF NOT EXISTS dm ON TABLE personTable
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='id , name')
      """.stripMargin)

    // 1. Compare the performance:

    def time(code: => Unit): Double = {
      val start = System.currentTimeMillis()
      code
      // return time in second
      (System.currentTimeMillis() - start).toDouble / 1000
    }

    val time_without_lucenedatamap = time {

      spark.sql(
        s"""
           | SELECT count(*)
           | FROM personTable where id like '% test1 %'
      """.stripMargin).show()

    }

    val time_with_lucenedatamap = time {

      spark.sql(
        s"""
           | SELECT count(*)
           | FROM personTable where TEXT_MATCH('id:test1')
      """.stripMargin).show()

    }

    // scalastyle:off
    println("time for query on table with lucene datamap table:" + time_with_lucenedatamap.toString)
    println("time for query on table without lucene datamap table:" + time_without_lucenedatamap.toString)
    // scalastyle:on

    // 2. Search for word "test1" and not "good" in the id field
    spark.sql(
      s"""
         | SELECT id,name
         | FROM personTable where TEXT_MATCH('id:test1 -id:good1')
      """.stripMargin).show(100)

     // 3. TEXT_MATCH_WITH_LIMIT usage:
    spark.sql(
      s"""
         | SELECT id,name
         | FROM personTable where TEXT_MATCH_WITH_LIMIT('id:test1',10)
      """.stripMargin).show()

    spark.sql("DROP TABLE IF EXISTS personTable")
  }
}

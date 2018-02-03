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

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * This example is dynamic partition, same as spark partition.
 */

object StandardPartitionExample {

  def main(args: Array[String]) {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/partition_data.csv"
    val spark = ExampleUtils.createCarbonSession("StandardPartitionExample")

    spark.sparkContext.setLogLevel("ERROR")

    // 1. simple usage for StandardPartition
    spark.sql("DROP TABLE IF EXISTS partitiontable0")
    spark.sql("""
                | CREATE TABLE partitiontable0
                | (id Int,
                | vin String,
                | phonenumber Long,
                | area String,
                | salary Int)
                | PARTITIONED BY (country String)
                | STORED BY 'org.apache.carbondata.format'
                | TBLPROPERTIES('SORT_COLUMNS'='id,vin')
              """.stripMargin)

    spark.sql(s"""
       LOAD DATA LOCAL INPATH '$testData' into table partitiontable0
       """)

    spark.sql(
      s"""
         | SELECT country,id,vin,phonenumber,area,salary
         | FROM partitiontable0
      """.stripMargin).show()

    spark.sql("UPDATE partitiontable0 SET (salary) = (88888) WHERE country='UK'").show()
    spark.sql(
      s"""
         | SELECT country,id,vin,phonenumber,area,salary
         | FROM partitiontable0
      """.stripMargin).show()

    // 2.compare the performance : with partition VS without partition

    // build test data, if set the data is larger than 100M, it will take 10+ mins.
    import scala.util.Random
    import spark.implicits._
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 10 * 100 * 1000)
      .map(x => ("No." + r.nextInt(1000), "country" + x % 8, "city" + x % 50, x % 300))
      .toDF("ID", "country", "city", "population")

    // Create table without partition
    df.write.format("carbondata")
      .option("tableName", "withoutpartition")
      .option("compress", "true")
      .mode(SaveMode.Overwrite).save()

    // Create table with partition
    spark.sql("DROP TABLE IF EXISTS withpartition")
    spark.sql("""
                | CREATE TABLE withpartition
                | (ID String,
                | city String,
                | population Int)
                | PARTITIONED BY (country String)
                | STORED BY 'org.apache.carbondata.format'
              """.stripMargin)

    df.write.format("carbondata")
      .option("tableName", "withpartition")
      .option("compress", "true")
      .mode(SaveMode.Overwrite).save()

    // define time function
    def time(code: => Unit): Double = {
      val start = System.currentTimeMillis()
      code
      // return time in second
      (System.currentTimeMillis() - start).toDouble / 1000
    }

    val time_without_partition = time {
      spark.sql(
        s"""
           | SELECT *
           | FROM withoutpartition WHERE country='country3'
      """.stripMargin).count()
    }

    val time_with_partition = time {
      spark.sql(
        s"""
           | SELECT *
           | FROM withpartition WHERE country='country3'
      """.stripMargin).count()
    }
    // scalastyle:off
    println("time of without partition:" + time_without_partition.toString)
    println("time of with partition:" + time_with_partition.toString)
    // scalastyle:on

    spark.sql("DROP TABLE IF EXISTS partitiontable0")
    spark.sql("DROP TABLE IF EXISTS withoutpartition")
    spark.sql("DROP TABLE IF EXISTS withpartition")

    spark.close()

  }

}

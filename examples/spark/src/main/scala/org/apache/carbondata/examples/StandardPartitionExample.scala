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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * This example is for standard partition, same as hive and spark partition
 */

object StandardPartitionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createSparkSession("StandardPartitionExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val testData = s"$rootPath/integration/spark/src/test/resources/" +
                   s"partition_data_example.csv"
    /**
     * 1. Partition basic usages
     */

    spark.sql("DROP TABLE IF EXISTS origintable")
    spark.sql(
      """
        | CREATE TABLE origintable
        | (id Int,
        | vin String,
        | logdate Date,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | STORED AS carbondata
      """.stripMargin)

    spark.sql(
      s"""
       LOAD DATA LOCAL INPATH '$testData' into table origintable
       """)

    spark.sql("select * from origintable").show(false)

    // create partition table with logdate as partition column

    spark.sql("DROP TABLE IF EXISTS partitiontable0")
    spark.sql(
      """
        | CREATE TABLE partitiontable0
        | (id Int,
        | vin String,
        | phonenumber Long,
        | country String,
        | area String,
        | salary Int)
        | PARTITIONED BY (logdate Date)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='id,vin')
      """.stripMargin)

    // load data and build partition with logdate value

    spark.sql(
      s"""
       LOAD DATA LOCAL INPATH '$testData' into table partitiontable0
       """)

    spark.sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0 where logdate = cast('2016-02-12' as date)
      """.stripMargin).show(100, false)

    spark.sql("show partitions default.partitiontable0").show()

    // insert data to table partitiontable0 and build partition with static value '2018-02-15'
    spark.sql("insert into table partitiontable0 partition(logdate='2018-02-15') " +
              "select id,vin,phonenumber,country,area,salary from origintable")

    spark.sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0
      """.stripMargin).show(100, false)

    // insert overwrite data to table partitiontable0

    spark.sql("UPDATE origintable SET (salary) = (88888)").show()

    spark.sql("insert overwrite table partitiontable0 partition(logdate='2018-02-15') " +
              "select id,vin,phonenumber,country,area,salary from origintable")

    spark.sql(
      s"""
         | SELECT logdate,id,vin,phonenumber,country,area,salary
         | FROM partitiontable0
      """.stripMargin).show(100, false)

    /**
     * 2.Compare the performance : with partition VS without partition
     */

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
    spark.sql(
      """
        | CREATE TABLE withpartition
        | (ID String,
        | city String,
        | population Int)
        | PARTITIONED BY (country String)
        | STORED AS carbondata
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
    println("----time of without partition----:" + time_without_partition.toString)
    println("----time of with partition----:" + time_with_partition.toString)
    // scalastyle:on

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    spark.sql("DROP TABLE IF EXISTS partitiontable0")
    spark.sql("DROP TABLE IF EXISTS withoutpartition")
    spark.sql("DROP TABLE IF EXISTS withpartition")
    spark.sql("DROP TABLE IF EXISTS origintable")
  }
}

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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.api.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants

object CarbonPartitionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val testData = s"$rootPath/integration/spark-common-test/src/test/resources/partition_data.csv"

    CarbonProperties.getInstance().addProperty("carbon.timestamp.format", "yyyy/MM/dd")
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonPartitionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation)

    spark.sparkContext.setLogLevel("WARN")

    // range partition with bucket defined
    spark.sql("DROP TABLE IF EXISTS t0")
    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t0
                | (
                | id Int,
                | vin String,
                | phonenumber Long,
                | country String,
                | area String,
                | salary Int
                | )
                | PARTITIONED BY (logdate Timestamp)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='RANGE',
                | 'RANGE_INFO'='2014/01/01, 2015/01/01, 2016/01/01',
                | 'BUCKETNUMBER'='3',
                | 'BUCKETCOLUMNS'='vin')
              """.stripMargin)

    // none partition table
    spark.sql("DROP TABLE IF EXISTS t1")
    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t1
                | (
                | id Int,
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String
                | )
                | STORED BY 'carbondata'
              """.stripMargin)

    // list partition
    spark.sql("DROP TABLE IF EXISTS t2")
    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t2
                | (
                | id Int,
                | vin String,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | salary Int
                | )
                | PARTITIONED BY (area String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='LIST',
                | 'LIST_INFO'='Asia, America, Europe', 'DICTIONARY_EXCLUDE' ='area')
              """.stripMargin)

    // hash partition
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("""
                | CREATE TABLE IF NOT EXISTS t3
                | (
                | id Int,
                | logdate Timestamp,
                | phonenumber Long,
                | country String,
                | area String,
                | salary Int
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)

    // list partition
    spark.sql("DROP TABLE IF EXISTS t5")
    spark.sql("""
       | CREATE TABLE IF NOT EXISTS t5
       | (
       | id Int,
       | vin String,
       | logdate Timestamp,
       | phonenumber Long,
       | area String,
       | salary Int
       |)
       | PARTITIONED BY (country String)
       | STORED BY 'carbondata'
       | TBLPROPERTIES('PARTITION_TYPE'='LIST',
       | 'LIST_INFO'='(China, US),UK ,Japan,(Canada,Russia, Good, NotGood), Korea ')
       """.stripMargin)

    // load data into partition table
    spark.sql(s"""
       LOAD DATA LOCAL INPATH '$testData' into table t0 options('BAD_RECORDS_ACTION'='FORCE')
       """)
    spark.sql(s"""
       LOAD DATA LOCAL INPATH '$testData' into table t5 options('BAD_RECORDS_ACTION'='FORCE')
       """)

    // alter list partition table t5 to add a partition
    spark.sql(s"""Alter table t5 add partition ('OutSpace')""".stripMargin)
    // alter list partition table t5 to split partition 4 into 3 independent partition
    spark.sql(
      s"""
         Alter table t5 split partition(4) into ('Canada', 'Russia', '(Good, NotGood)')
       """.stripMargin)

    spark.sql("""select * from t5 where country = 'Good' """).show(100, false)

    spark.sql("select * from t0 order by salary ").show(100, false)
    spark.sql("select * from t5 order by salary ").show(100, false)

    // hive partition table
    spark.sql("DROP TABLE IF EXISTS t7")
    spark.sql("""
       | create table t7(id int, name string) partitioned by (city string)
       | row format delimited fields terminated by ','
       """.stripMargin)
    spark.sql("alter table t7 add partition (city = 'Hangzhou')")

    // not default db partition table
    try {
      spark.sql(s"DROP TABLE IF EXISTS partitionDB.t9")
    } catch {
      case ex: NoSuchDatabaseException => LOGGER.error(ex.getMessage())
    }
    spark.sql(s"DROP DATABASE IF EXISTS partitionDB")
    spark.sql(s"CREATE DATABASE partitionDB")
    spark.sql(s"""
                | CREATE TABLE IF NOT EXISTS partitionDB.t9(
                | id Int,
                | logdate Timestamp,
                | phonenumber Int,
                | country String,
                | area String
                | )
                | PARTITIONED BY (vin String)
                | STORED BY 'carbondata'
                | TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='5')
                """.stripMargin)

    // show tables
    spark.sql("SHOW TABLES").show()

    // show partitions
    try {
      spark.sql("""SHOW PARTITIONS t1""").show(100, false)
    } catch {
      case ex: AnalysisException => LOGGER.error(ex.getMessage())
    }
    spark.sql("""SHOW PARTITIONS t0""").show(100, false)
    spark.sql("""SHOW PARTITIONS t3""").show(100, false)
    spark.sql("""SHOW PARTITIONS t5""").show(100, false)
    spark.sql("""SHOW PARTITIONS t7""").show(100, false)
    spark.sql("""SHOW PARTITIONS partitionDB.t9""").show(100, false)

    // drop table
    spark.sql("DROP TABLE IF EXISTS t0")
    spark.sql("DROP TABLE IF EXISTS t1")
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t5")
    spark.sql("DROP TABLE IF EXISTS t7")
    spark.sql("DROP TABLE IF EXISTS partitionDB.t9")
    spark.sql(s"DROP DATABASE IF EXISTS partitionDB")

    spark.close()

  }

}

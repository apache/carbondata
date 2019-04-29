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
package org.apache.carbondata.spark.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

/**
  * Test Case for Dictionary LRU Cache.
  */
class DictionaryLRUCacheTestCase extends Spark2QueryTest with BeforeAndAfterAll {
  var spark : SparkSession = null
  var path : String = null

  def checkDictionaryAccessCount(databaseName: String, tableName: String): Unit = {
    val carbonTable = CarbonEnv.getInstance(Spark2TestQueryExecutor.spark).carbonMetaStore
      .lookupRelation(Option(databaseName), tableName)(Spark2TestQueryExecutor.spark)
      .asInstanceOf[CarbonRelation].carbonTable
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier

    val dimensions = carbonTable.getAllDimensions.asScala.toList
    dimensions.foreach { dim =>
      val columnIdentifier = dim.getColumnIdentifier
      // Check the dictionary cache access.
      val identifier: DictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
        absoluteTableIdentifier,
        columnIdentifier,
        columnIdentifier.getDataType)

      val isDictExists: Boolean = CarbonUtil.isFileExistsForGivenColumn(identifier)
      var dictionary: Dictionary = null
      if (isDictExists) {
        val dictCacheReverse: Cache[DictionaryColumnUniqueIdentifier, Dictionary]
        = CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY)
        dictionary = dictCacheReverse.get(identifier)
        assert(dictionary.getAccessCount == 1)
        CarbonUtil.clearDictionaryCache(dictionary)

        val dictCacheForward: Cache[DictionaryColumnUniqueIdentifier, Dictionary]
        = CacheProvider.getInstance().createCache(CacheType.FORWARD_DICTIONARY)
        dictionary = dictCacheForward.get(identifier)
        assert(dictionary.getAccessCount == 1)
        CarbonUtil.clearDictionaryCache(dictionary)
      }
    }
  }


  override def beforeAll {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "1")
      .addProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE, "1")

    path = s"$resourcesPath/restructure/data_2000.csv"

    sql("use default")
    sql("drop table if exists carbon_new1")
    sql("drop table if exists carbon_new2")
    sql("drop table if exists carbon_new3")
    sql("drop table if exists carbon_new4")
    sql("drop table if exists carbon_new5")
    sql("drop table if exists carbon_new6")
    sql("drop table if exists carbon_new7")
    sql("drop table if exists carbon_new8")
    sql("drop table if exists carbon_new9")
    sql("drop table if exists carbon_new10")
  }

  test("test for dictionary LRU Cache for Load Single Pass") {

    sql(
        "CREATE TABLE carbon_new1 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new1 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new1 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        "CREATE TABLE carbon_new2 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new2 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new2 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='TRUE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    checkDictionaryAccessCount("default", "carbon_new2")
  }

  test("test for dictionary LRU Cache for Load Non Single Pass") {

    sql(
        "CREATE TABLE carbon_new3 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new3 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new3 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        "CREATE TABLE carbon_new4 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new4 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new4 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    checkDictionaryAccessCount("default", "carbon_new4")
  }

  test("test for dictionary LRU Cache for Select On Table") {

    sql(
        "CREATE TABLE carbon_new5 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new5 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new5 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql("select * from carbon_new5")

    checkDictionaryAccessCount("default", "carbon_new5")


    sql(
        "CREATE TABLE carbon_new6 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new6 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new6 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql("select * from carbon_new6")

    checkDictionaryAccessCount("default", "carbon_new6")
  }

  test("test for dictionary LRU Cache for Select With Filter On Table") {

    sql(
        "CREATE TABLE carbon_new7 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new7 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new7 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql("select * from carbon_new7 where CUST_ID > 10")

    checkDictionaryAccessCount("default", "carbon_new7")


    sql(
        "CREATE TABLE carbon_new8 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new8 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new8 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql("select * from carbon_new8 where CUST_ID > 100")

    checkDictionaryAccessCount("default", "carbon_new8")
  }

  test("test for dictionary LRU Cache for Insert Into") {

    sql(
        "CREATE TABLE carbon_new9 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME, ACTIVE_EMUI_VERSION,BIGINT_COLUMN1,Double_COLUMN1, " +
        "Double_COLUMN2')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new9 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new9 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    sql("select * from carbon_new9 where CUST_ID > 10")

    checkDictionaryAccessCount("default", "carbon_new9")


    sql(
        "CREATE TABLE carbon_new10 (CUST_ID INT,CUST_NAME STRING,ACTIVE_EMUI_VERSION STRING, DOB " +
        "TIMESTAMP, DOJ TIMESTAMP, BIGINT_COLUMN1 BIGINT,BIGINT_COLUMN2 BIGINT,DECIMAL_COLUMN1 " +
        "decimal(30,10), DECIMAL_COLUMN2 DECIMAL(36,10),Double_COLUMN1 double, Double_COLUMN2 " +
        "double,INTEGER_COLUMN1 INT) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES " +
        "('dictionary_include'='CUST_NAME')")

    sql("insert into carbon_new10 select * from carbon_new9")

    checkDictionaryAccessCount("default", "carbon_new10")

    sql(
        s"LOAD DATA INPATH '$path' INTO TABLE carbon_new10 OPTIONS" +
        "('DELIMITER'=',' , 'QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','SINGLE_PASS'='FALSE'," +
        "'FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1," +
        "BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2, " +
        "INTEGER_COLUMN1')")

    checkDictionaryAccessCount("default", "carbon_new10")

  }



  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE,
        CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT)
      .addProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE,
        CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT)

    sql("drop table if exists carbon_new1")
    sql("drop table if exists carbon_new2")
    sql("drop table if exists carbon_new3")
    sql("drop table if exists carbon_new4")
    sql("drop table if exists carbon_new5")
    sql("drop table if exists carbon_new6")
    sql("drop table if exists carbon_new7")
    sql("drop table if exists carbon_new8")
    sql("drop table if exists carbon_new9")
    sql("drop table if exists carbon_new10")
  }
}

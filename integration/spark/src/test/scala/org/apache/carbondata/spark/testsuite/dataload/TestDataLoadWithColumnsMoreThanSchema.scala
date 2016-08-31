/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * This class will test data load in which number of columns in data are more than
 * the number of columns in schema
 */
class TestDataLoadWithColumnsMoreThanSchema extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS char_test")
    sql("DROP TABLE IF EXISTS hive_char_test")
    sql("CREATE TABLE char_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE hive_char_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)row format delimited fields terminated by ','")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/character_carbon.csv' into table char_test")
    sql("LOAD DATA local inpath './src/test/resources/character_hive.csv' INTO table hive_char_test")
  }

  test("test count(*) to check for data loss") {
    checkAnswer(sql("select count(*) from char_test"),
      sql("select count(*) from hive_char_test"))
  }

  test("test for invalid value of maxColumns") {
    sql("DROP TABLE IF EXISTS max_columns_test")
    sql("CREATE TABLE max_columns_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED BY 'org.apache.carbondata.format'")
    try {
      sql("LOAD DATA LOCAL INPATH './src/test/resources/character_carbon.csv' into table max_columns_test options('MAXCOLUMNS'='avfgd')")
      assert(false)
    } catch {
      case _ => assert(true)
    }
  }

  test("test for valid value of maxColumns") {
    sql("DROP TABLE IF EXISTS valid_max_columns_test")
    sql("CREATE TABLE valid_max_columns_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED BY 'org.apache.carbondata.format'")
    try {
      sql("LOAD DATA LOCAL INPATH './src/test/resources/character_carbon.csv' into table valid_max_columns_test options('MAXCOLUMNS'='400')")
      checkAnswer(sql("select count(*) from valid_max_columns_test"),
        sql("select count(*) from hive_char_test"))
    } catch {
      case _ => assert(false)
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS char_test")
    sql("DROP TABLE IF EXISTS hive_char_test")
  }
}

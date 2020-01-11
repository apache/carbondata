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

package org.apache.spark.carbondata.deletetable

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test cases for drop table
 */
class DeleteTableTestCase extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists IS")
  }

  test("drop table IS with load") {
    sql("drop table if exists IS")
    sql(
      "CREATE TABLE IS (imei string,age int,task bigint,name string,country string," +
      "city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp," +
      "enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql(
      s"LOAD DATA INPATH '$resourcesPath/big_int_Decimal.csv'  INTO TABLE IS " +
      "options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$'," +
      "'COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')")
    sql("drop table IS")
    sql("drop table if exists IS")
    sql(
      "CREATE TABLE IS (imei string,age int,task bigint,name string,country string," +
      "city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp," +
      "enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql("drop table if exists IS")
  }

  test("drop table IS without load") {
    sql("drop table if exists IS")
    sql(
      "CREATE TABLE IS (imei string,age int,task bigint,name string,country string," +
      "city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp," +
      "enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql("drop table if exists IS")
  }
  override def afterAll {
    sql("drop table if exists IS")
  }
}

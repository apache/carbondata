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

package org.carbondata.integration.spark.testsuite.complexType

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
 * Test class of creating and loading for carbon table with double
 *
 */
class TestComplexPrimitiveTimestampDirectDictionary extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists complexcarbontimestamptable")
    sql("drop table if exists complexhivetimestamptable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSS")
    sql("CREATE TABLE complexcarbontimestamptable (empno string,workdate Timestamp,punchinout array<Timestamp>, worktime struct<begintime:Timestamp, endtime:Timestamp>, salary double) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/datasamplecomplex.csv' INTO TABLE complexcarbontimestamptable OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='empno,workdate,punchinout,worktime,salary')");
    sql("CREATE TABLE complexhivetimestamptable (empno string,workdate Timestamp,punchinout array<Timestamp>, worktime struct<begintime:Timestamp, endtime:Timestamp>, salary double)row format delimited fields terminated by ',' collection items terminated by '$'")
    sql("LOAD DATA local inpath './src/test/resources/datasamplecomplex.csv' INTO TABLE complexhivetimestamptable")
  }

  test("select * query") {
     checkAnswer(sql("select * from complexcarbontimestamptable"),
     sql("select * from complexhivetimestamptable"))
  }
  
  test("timestamp complex type in the middle of complex types") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSS")
    sql("CREATE TABLE testtimestampcarbon(imei string,rat array<string>, sid array<int>, end_time array<Timestamp>, probeid array<double>, contact struct<name:string, id:string>)STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/timestampdata.csv' INTO TABLE testtimestampcarbon options('DELIMITER'=',', 'QUOTECHAR'='\"','COMPLEX_DELIMITER_LEVEL_1'='$', 'FILEHEADER'='imei,rat,sid,end_time,probeid,contact')")
    sql("CREATE TABLE testtimestamphive(imei string,rat array<string>, sid array<int>, end_time array<Timestamp>, probeid array<double>, contact struct<name:string, id:string>)row format delimited fields terminated by ',' collection items terminated by '$'")
    sql("LOAD DATA local inpath './src/test/resources/timestampdata.csv' INTO TABLE testtimestamphive")
    checkAnswer(sql("select * from testtimestampcarbon"), sql("select * from testtimestamphive"))
    sql("drop table if exists testtimestampcarbon")
    sql("drop table if exists testtimestamphive")
  }
  
  override def afterAll {
	  sql("drop table if exists complexcarbontimestamptable")
    sql("drop table if exists complexhivetimestamptable")
  }
}

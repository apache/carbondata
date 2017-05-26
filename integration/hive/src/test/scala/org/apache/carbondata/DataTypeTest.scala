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
package org.apache.carbondata

import java.io.File

import org.apache.spark.sql.SparkSession
import org.scalatest._

import org.apache.carbondata.common.logging.LogServiceFactory
import server.HiveTestingServer
import scala.collection.JavaConversions._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class DataTypeTest extends FunSuite with BeforeAndAfterAll {

  var hiveEmbeddedServer2: HiveTestingServer = _
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val store = s"$rootPath/integration/hive/target/store"
  val warehouse = s"$rootPath/integration/hive/target/warehouse"
  val metaStore_Db = s"$rootPath/integration/hive/target/carbon_metaStore_db"
  val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  import org.apache.spark.sql.CarbonSession._

  System.setProperty("hadoop.home.dir", "/")

  val carbon = SparkSession
    .builder()
    .master("local")
    .appName("HiveExample")
    .config("carbon.sql.warehouse.dir", warehouse).enableHiveSupport()
    .getOrCreateCarbonSession(
      store, metaStore_Db)

  override def beforeAll() = {
    carbon.sql("DROP TABLE IF EXISTS ALLDATATYPETEST")
    CarbonProperties.getInstance().addProperty("carbon.direct.dictionary", "true")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    carbon.sql(
      "CREATE TABLE IF NOT EXISTS ALLDATATYPETEST(ID INT,NAME STRING,SALARY DECIMAL,MARKS " +
      "DOUBLE,JOININGDATE DATE,LEAVINGDATE TIMESTAMP)STORED BY 'CARBONDATA' ")
    carbon.sql(
      s"""
           LOAD DATA LOCAL INPATH '$rootPath/integration/hive/src/test/resources/alldatatypetest.csv' INTO
           TABLE ALLDATATYPETEST
           """)
    hiveEmbeddedServer2 = new HiveTestingServer
    hiveEmbeddedServer2.start()
    hiveEmbeddedServer2
      .execute(s"CREATE TABLE ALLDATATYPETEST(ID INT,NAME STRING,SALARY DECIMAL,MARKS DOUBLE,JOININGDATE DATE,LEAVINGDATE TIMESTAMP) ROW FORMAT SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe' STORED AS INPUTFORMAT 'org.apache.carbondata.hive.MapredCarbonInputFormat' OUTPUTFORMAT 'org.apache.carbondata.hive.MapredCarbonOutputFormat' TBLPROPERTIES ('spark.sql.sources.provider'='org.apache.spark.sql.CarbonSource')")


    hiveEmbeddedServer2
      .execute(s"ALTER TABLE ALLDATATYPETEST SET LOCATION 'file:///$rootPath/integration/hive/target/store/default/alldatatypetest' ")
  }

  override def afterAll() = {
    carbon.stop()
    hiveEmbeddedServer2.stop()
  }


  test("testing for all possible data types with selecting the all columns") {

    val expectedResult = List("1,'ANUBHAV',200000,100.0,1970-01-01,48255-05-28 04:00:00.0",
      "2,'LIANG',200000,100.0,1970-01-01,48255-05-28 04:00:00.0")
    val actualResult = hiveEmbeddedServer2.execute("SELECT * FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)

  }
  test("testing for all possible data types with selecting the individual ID column") {

    val expectedResult = List("1","2")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT ID FROM ALLDATATYPETEST")

    assert((actualResult.toList diff expectedResult).isEmpty)


  }
  test("testing for all possible data types with selecting the individual NAME column") {

    val expectedResult = List("'ANUBHAV'", "'LIANG'")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT NAME FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)


  }
  test("testing for all possible data types with selecting the individual SALARY column") {

    val expectedResult = List("200000", "200000")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT SALARY FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)


  }
  test("testing for all possible data types with selecting the individual MARKS column") {

    val expectedResult = List("100.0", "100.0")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT MARKS FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)


  }
  test("testing for all possible data types with selecting the individual joiningDate column") {

    val expectedResult = List("1970-01-01", "1970-01-01")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT JOININGDATE FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)


  }
  test("testing for all possible data types with selecting the leavingDate column") {

    val expectedResult = List("2016-04-14 15:00:09.0", "2016-04-14 15:00:09.0")
    val actualResult =  hiveEmbeddedServer2.execute("SELECT LEAVINGDATE FROM ALLDATATYPETEST")
    assert((actualResult.toList diff expectedResult).isEmpty)


  }

}



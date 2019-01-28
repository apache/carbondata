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

package org.apache.carbondata.presto.integrationtest

import java.io.File
import java.util

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer


class PrestoAllDataTypeLocalDictTest extends FunSuiteLike with BeforeAndAfterAll {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoAllDataTypeLocalDictTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val systemPath = s"$rootPath/integration/presto/target/system"
  private val prestoServer = new PrestoServer

  // Table schema:
  // +-------------+----------------+-------------+------------+
  // | Column name | Data type      | Column type | Dictionary |
  // +-------------+----------------+--------------+-----------+
  // | id          | string         | dimension   | yes        |
  // +-------------+----------------+-------------+------------+
  // | date        | date           | dimension   | yes        |
  // +-------------+----------------+-------------+------------+
  // | country     | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | name        | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | phonetype   | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | serialname  | string         | dimension   | true       |
  // +-------------+----------------+-------------+-------------
  // | bonus       |short decimal   | measure     | false      |
  // +-------------+----------------+-------------+-------------
  // | monthlyBonus| longdecimal    | measure     | false      |
  // +-------------+----------------+-------------+-------------
  // | dob         | timestamp      | dimension   | true       |
  // +-------------+----------------+-------------+------------+
  // | shortField  | shortfield     | measure     | true       |
  // +-------------+----------------+-------------+-------------
  // |isCurrentEmp | boolean        | measure     | true       |
  // +-------------+----------------+-------------+------------+

  override def beforeAll: Unit = {
    import org.apache.carbondata.presto.util.CarbonDataStoreCreator
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
      systemPath)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")

    prestoServer.startServer("testdb", map)
    prestoServer.execute("drop table if exists testdb.testtable")
    prestoServer.execute("drop schema if exists testdb")
    prestoServer.execute("create schema testdb")
    prestoServer.execute("create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary double, bonus decimal(10,4), monthlyBonus decimal(18,4), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='CARBON') ")
    CarbonDataStoreCreator
      .createCarbonStore(storePath,
        s"$rootPath/integration/presto/src/test/resources/alldatatype.csv", true)
    logger.info(s"\nCarbon store is created at location: $storePath")
    cleanUp
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

  test("select string type with order by clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE ORDER BY NAME")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "akash"),
      Map("NAME" -> "anubhav"),
      Map("NAME" -> "bhavya"),
      Map("NAME" -> "geetika"),
      Map("NAME" -> "jatin"),
      Map("NAME" -> "jitesh"),
      Map("NAME" -> "liang"),
      Map("NAME" -> "prince"),
      Map("NAME" -> "ravindra"),
      Map("NAME" -> "sahil"),
      Map("NAME" -> null)
    )
    assert(actualResult.equals(expectedResult))
  }

  test("test and filter clause with greater than expression") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>1234 AND ID>2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 4,
      "NAME" -> "prince",
      "BONUS" -> java.math.BigDecimal.valueOf(9999.9990).setScale(4),
      "DATE" -> "2015-07-26",
      "SALARY" -> 15003.0,
      "SERIALNAME" -> "ASD66902",
      "COUNTRY" -> "china",
      "PHONETYPE" -> "phone2435"),
      Map("ID" -> 5,
        "NAME" -> "bhavya",
        "BONUS" -> java.math.BigDecimal.valueOf(5000.999).setScale(4),
        "DATE" -> "2015-07-27",
        "SALARY" -> 15004.0,
        "SERIALNAME" -> "ASD90633",
        "COUNTRY" -> "china",
        "PHONETYPE" -> "phone2441"))
    assert(actualResult.toString() equals expectedResult.toString())


  }

  test("test and filter clause with greater than equal to expression") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>=1234.444 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1,
      "NAME" -> "anubhav",
      "BONUS" -> java.math.BigDecimal.valueOf(1234.4440).setScale(4),
      "DATE" -> "2015-07-23",
      "SALARY" -> "5000000.0",
      "SERIALNAME" -> "ASD69643",
      "COUNTRY" -> "china",
      "PHONETYPE" -> "phone197"),
      Map("ID" -> 2,
        "NAME" -> "jatin",
        "BONUS" -> java.math.BigDecimal.valueOf(1234.5555).setScale(4)
        ,
        "DATE" -> "2015-07-24",
        "SALARY" -> java.math.BigDecimal.valueOf(150010.9990).setScale(3),
        "SERIALNAME" -> "ASD42892",
        "COUNTRY" -> "china",
        "PHONETYPE" -> "phone756"),
      Map("ID" -> 4,
        "NAME" -> "prince",
        "BONUS" -> java.math.BigDecimal.valueOf(9999.9990).setScale(4),
        "DATE" -> "2015-07-26",
        "SALARY" -> java.math.BigDecimal.valueOf(15003.0).setScale(1),
        "SERIALNAME" -> "ASD66902",
        "COUNTRY" -> "china",
        "PHONETYPE" -> "phone2435"),
      Map("ID" -> 5,
        "NAME" -> "bhavya",
        "BONUS" -> java.math.BigDecimal.valueOf(5000.9990).setScale(4),
        "DATE" -> "2015-07-27",
        "SALARY" -> java.math.BigDecimal.valueOf(15004.0).setScale(1),
        "SERIALNAME" -> "ASD90633",
        "COUNTRY" -> "china",
        "PHONETYPE" -> "phone2441"))
    assert(actualResult.toString() equals expectedResult.toString())
  }
  test("test and filter clause with less than equal to expression") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS<=1234.444 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID LIMIT 2")

    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1,
      "NAME" -> "anubhav",
      "BONUS" -> java.math.BigDecimal.valueOf(1234.4440).setScale(4),
      "DATE" -> "2015-07-23",
      "SALARY" -> "5000000.0",
      "SERIALNAME" -> "ASD69643",
      "COUNTRY" -> "china",
      "PHONETYPE" -> "phone197"),
      Map("ID" -> 3,
        "NAME" -> "liang",
        "BONUS" -> java.math.BigDecimal.valueOf(600.7770).setScale(4),
        "DATE" -> "2015-07-25",
        "SALARY" -> java.math.BigDecimal.valueOf(15002.11).setScale(2),
        "SERIALNAME" -> "ASD37014",
        "COUNTRY" -> "china",
        "PHONETYPE" -> "phone1904"))
    assert(actualResult.toString() equals expectedResult.toString())
  }
  test("test equal to expression on decimal value") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID FROM TESTDB.TESTTABLE WHERE BONUS=1234.444")

    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1))

    assert(actualResult equals expectedResult)
  }
  test("test less than expression with and operator") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY,BONUS FROM TESTDB.TESTTABLE " +
        "WHERE BONUS>1234 AND ID<2 GROUP BY ID,DATE,COUNTRY,NAME,PHONETYPE,SERIALNAME,SALARY," +
        "BONUS ORDER BY ID")
    actualResult.foreach(println)
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1,
      "NAME" -> "anubhav",
      "BONUS" -> java.math.BigDecimal.valueOf(1234.4440).setScale(4),
      "DATE" -> "2015-07-23",
      "SALARY" -> 5000000.0,
      "SERIALNAME" -> "ASD69643",
      "COUNTRY" -> "china",
      "PHONETYPE" -> "phone197"))
    assert(actualResult.toString().equals(expectedResult.toString()))
  }
  test("test the result for in clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME from testdb.testtable WHERE PHONETYPE IN('phone1848','phone706')")
    val expectedResult: List[Map[String, Any]] = List(
      Map("NAME" -> "geetika"),
      Map("NAME" -> "ravindra"),
      Map("NAME" -> "jitesh"))

    assert(actualResult.equals(expectedResult))
  }
  test("test the result for not in clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT NAME from testdb.testtable WHERE PHONETYPE NOT IN('phone1848','phone706')")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "anubhav"),
      Map("NAME" -> "jatin"),
      Map("NAME" -> "liang"),
      Map("NAME" -> "prince"),
      Map("NAME" -> "bhavya"),
      Map("NAME" -> "akash"),
      Map("NAME" -> "sahil"))

    assert(actualResult.equals(expectedResult))
  }
  test("test for null operator on date data type") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT ID FROM TESTDB.TESTTABLE WHERE DATE IS NULL")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 9),Map("ID" -> null))
    assert(actualResult.equals(expectedResult))

  }
  test("test for not null operator on date data type") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DATE IS NOT NULL AND ID=9")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "ravindra"))
    assert(actualResult.equals(expectedResult))

  }
  test("test for not null operator on timestamp type") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DOB IS NOT NULL AND ID=9")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "ravindra"),
      Map("NAME" -> "jitesh"))
    assert(actualResult.equals(expectedResult))

  }

  test("test timestamp datatype using cast operator") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME AS RESULT FROM TESTDB.TESTTABLE WHERE DOB = CAST('2016-04-14 15:00:09' AS TIMESTAMP)")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> "jatin"))
    assert(actualResult.equals(expectedResult))
  }

  private def cleanUp(): Unit = {
    FileFactory.deleteFile(s"$storePath/Fact", FileType.LOCAL)
    FileFactory
      .createDirectoryAndSetPermission(s"$storePath/_system",
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory
      .createDirectoryAndSetPermission(s"$storePath/.DS_Store",
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    FileFactory.createNewFile(s"$storePath/testdb/.DS_STORE",FileType.LOCAL)
  }

}
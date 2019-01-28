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
import java.sql.Timestamp
import java.util

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer


class PrestoAllDataTypeTest extends FunSuiteLike with BeforeAndAfterAll {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoAllDataTypeTest].getCanonicalName)

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
        s"$rootPath/integration/presto/src/test/resources/alldatatype.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    cleanUp
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

  test("test the result for count(*) in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(*) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 11))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count() clause with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    assert(actualResult.equals(expectedResult))

  }
  test("test the result for sum()in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT SUM(ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 54))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for sum() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT SUM(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 45))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for avg() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT AVG(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 5))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for min() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT MIN(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 1))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for max() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT MAX(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count()clause with distinct operator on decimal column in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count()clause with out  distinct operator on decimal column in presto")
  {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT COUNT(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for sum()with out distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT SUM(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 20774.6475))
    assert(actualResult.toString().equals(expectedResult.toString()))
  }
  test("test the result for sum() with distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT SUM(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 20774.6475))
    assert(
      actualResult.head("RESULT").toString.toDouble ==
      expectedResult.head("RESULT").toString.toDouble)
  }
  test("test the result for avg() with distinct operator on decimal on presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT AVG(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 2077.4648))
    assert(actualResult.toString.equals(expectedResult.toString))
  }

  test("test the result for min() with distinct operator in decimalType of presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT MIN(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "RESULT" -> java.math.BigDecimal.valueOf(500.414).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }

  test("test the result for max() with distinct operator in decimalType of presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT MAX(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "RESULT" -> java.math.BigDecimal.valueOf(9999.999).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }
  test("select decimal data type with ORDER BY  clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT DISTINCT BONUS FROM TESTDB.TESTTABLE ORDER BY BONUS limit 3 ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "BONUS" -> java.math.BigDecimal.valueOf(500.414).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.59).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.88).setScale(4)))
    assert(actualResult.equals(expectedResult))
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
  test("select DATE type with order by clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT DATE FROM TESTDB.TESTTABLE ORDER BY DATE")
    val expectedResult: List[Map[String, Any]] = List(Map("DATE" -> "2015-07-18"),
      Map("DATE" -> "2015-07-23"),
      Map("DATE" -> "2015-07-24"),
      Map("DATE" -> "2015-07-25"),
      Map("DATE" -> "2015-07-26"),
      Map("DATE" -> "2015-07-27"),
      Map("DATE" -> "2015-07-28"),
      Map("DATE" -> "2015-07-29"),
      Map("DATE" -> "2015-07-30"),
      Map("DATE" -> null),
      Map("DATE" -> null)
    )

    assert(actualResult.filterNot(_.get("DATE") == null).zipWithIndex.forall {
      case (map, index) => map.get("DATE").toString
        .equals(expectedResult(index).get("DATE").toString)
    })
    assert(actualResult.reverse.head("DATE") == null)
  }
  test("select int type with order by clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT DISTINCT ID FROM TESTDB.TESTTABLE ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1),
      Map("ID" -> 2),
      Map("ID" -> 3),
      Map("ID" -> 4),
      Map("ID" -> 5),
      Map("ID" -> 6),
      Map("ID" -> 7),
      Map("ID" -> 8),
      Map("ID" -> 9),
      Map("ID" -> null)
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
  test("test for null operator on timestamp type") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DOB IS NULL AND ID=1")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "anubhav"))
    assert(actualResult.equals(expectedResult))

  }
  test("test the result for short datatype with order by clause") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT DISTINCT SHORTFIELD from testdb.testtable ORDER BY SHORTFIELD ")
    val expectedResult: List[Map[String, Any]] = List(Map("SHORTFIELD" -> 1),
      Map("SHORTFIELD" -> 4),
      Map("SHORTFIELD" -> 8),
      Map("SHORTFIELD" -> 10),
      Map("SHORTFIELD" -> 11),
      Map("SHORTFIELD" -> 12),
      Map("SHORTFIELD" -> 18),
      Map("SHORTFIELD" -> null))

    assert(actualResult.equals(expectedResult))
  }
  test("test the result for short datatype in clause where field is null") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE SHORTFIELD IS NULL ORDER BY SHORTFIELD ")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 7),Map("ID" -> null))

    assert(actualResult.equals(expectedResult))
  }
  test("test the result for short datatype with greater than operator") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE SHORTFIELD>11 ")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 6), Map("ID" -> 9))

    assert(actualResult.equals(expectedResult))
  }

  test("test longDecimal type of presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE bonus = DECIMAL '1234.5555'")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 2))

    assert(actualResult.equals(expectedResult))
  }

  test("test shortDecimal type of presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE monthlyBonus = 15.13")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 2))

    assert(actualResult.equals(expectedResult))
  }

  test("test timestamp datatype using cast operator") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT NAME AS RESULT FROM TESTDB.TESTTABLE WHERE DOB = CAST('2016-04-14 15:00:09' AS TIMESTAMP)")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> "jatin"))
    assert(actualResult.equals(expectedResult))
  }

  test("test timestamp datatype using cast and in operator") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT ID AS RESULT FROM TESTDB.TESTTABLE WHERE DOB in (cast('2016-04-14 " +
                    "15:00:09' as timestamp),cast('2015-10-07' as timestamp),cast('2015-10-07 01:00:03' as timestamp))")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> "2"))
    assert(actualResult.toString() equals expectedResult.toString())
  }
  test("test the boolean data type") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT isCurrentEmployee AS RESULT FROM TESTDB.TESTTABLE WHERE ID=1")
    assert(actualResult.head("RESULT").toString.toBoolean)
  }
  test("test the boolean data type for null value") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT id AS RESULT FROM TESTDB.TESTTABLE WHERE isCurrentEmployee is null")
    assert(actualResult.head("RESULT").toString.toInt==2)
  }
  test("test the boolean data type for not null value with filter ") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT id AS RESULT FROM TESTDB.TESTTABLE WHERE isCurrentEmployee is NOT null AND ID>8")
    assert(actualResult.head("RESULT").toString.toInt==9)
  }
  test("test the is null operator when null is included in string data type dictionary_include"){
    // See CARBONDATA-2155
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT SERIALNAME  FROM TESTDB.TESTTABLE WHERE SERIALNAME IS NULL")
    assert(actualResult equals List(Map("SERIALNAME" -> null)))
  }
  test("test the min function when null is included in string data type with dictionary_include"){
    // See CARBONDATA-2152
    val actualResult = prestoServer.executeQuery("SELECT MIN(SERIALNAME) FROM TESTDB.TESTTABLE")
    val expectedResult = List(Map("_col0" -> "ASD14875"))

    assert(actualResult.equals(expectedResult))
  }


  test("test the show schemas result"){
   val actualResult = prestoServer.executeQuery("SHOW SCHEMAS")
    assert(actualResult.equals(List(Map("Schema" -> "information_schema"), Map("Schema" -> "testdb"))))
  }
  test("test the show tables"){
  val actualResult = prestoServer.executeQuery("SHOW TABLES")
  assert(actualResult.equals(List(Map("Table" -> "testtable"))))
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

  test("test the OR operator on same column"){
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT BONUS FROM TESTDB.TESTTABLE WHERE" +
      " BONUS < 600 OR BONUS > 5000 ORDER BY BONUS")
    val expectedResult: List[Map[String, Any]] = List(
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.4140).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.5900).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.8800).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.9900).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(5000.9990).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(9999.9990).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }

  test("test the AND, OR operator on same column"){
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT SHORTFIELD FROM TESTDB.TESTTABLE WHERE" +
      " SHORTFIELD > 4 AND (SHORTFIELD < 10 or SHORTFIELD > 15) ORDER BY SHORTFIELD")
    val expectedResult: List[Map[String, Any]] = List(
      Map("SHORTFIELD" -> 8),
      Map("SHORTFIELD" -> 18))
    assert(actualResult.equals(expectedResult))
  }

  test("test the OR operator with multiple AND on same column"){
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT SHORTFIELD FROM TESTDB.TESTTABLE WHERE" +
      " (SHORTFIELD > 1 AND SHORTFIELD < 5) OR (SHORTFIELD > 10 AND SHORTFIELD < 15) ORDER BY SHORTFIELD")
    val expectedResult: List[Map[String, Any]] = List(
      Map("SHORTFIELD" -> 4),
      Map("SHORTFIELD" -> 11),
      Map("SHORTFIELD" -> 12))
    assert(actualResult.equals(expectedResult))
  }

  test("test the OR, AND operator with on Different column"){
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT SHORTFIELD FROM TESTDB.TESTTABLE WHERE" +
      " ID < 7 AND (SHORTFIELD < 5 OR SHORTFIELD > 15) ORDER BY SHORTFIELD")
    val expectedResult: List[Map[String, Any]] = List(
      Map("SHORTFIELD" -> 4),
      Map("SHORTFIELD" -> 18))
    assert(actualResult.equals(expectedResult))
  }

  test("test the Timestamp greaterthan expression"){
    val actualResult: List[Map[String, Any]] = prestoServer.executeQuery("SELECT DOB FROM TESTDB.TESTTABLE" +
                                                                         " WHERE DOB > timestamp '2016-01-01 00:00:00.0' order by DOB")
    val expectedResult: List[Map[String, Any]] = List(
      Map("DOB" -> new Timestamp(new java.util.Date(2016-1900,1-1,14,15,7,9).getTime)),
      Map("DOB" -> new Timestamp(new java.util.Date(2016-1900,4-1,14,15,0,9).getTime)))
    assert(actualResult.equals(expectedResult))
  }


}
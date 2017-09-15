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

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.presto.server.PrestoServer


class PrestoAllDataTypeTest extends FunSuiteLike with BeforeAndAfterAll {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoAllDataTypeTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"

  override def beforeAll: Unit = {
    import org.apache.carbondata.presto.util.CarbonDataStoreCreator
    CarbonDataStoreCreator
      .createCarbonStore(storePath,
        s"$rootPath/integration/presto/src/test/resources/alldatatype.csv")
    logger.info(s"\nCarbon store is created at location: $storePath")
    PrestoServer.startServer(storePath)
  }

  override def afterAll(): Unit = {
    PrestoServer.stopServer()
  }

  test("test the result for count(*) in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(*) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count() clause with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    assert(actualResult.equals(expectedResult))

  }
  test("test the result for sum()in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 54))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for sum() wiTh distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 45))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for avg() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT AVG(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 5))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for min() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MIN(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 1))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for max() with distinct operator in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MAX(DISTINCT ID) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 9))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count()clause with distinct operator on decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for count()clause with out  distinct operator on decimal column in presto")
  {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT COUNT(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 10))
    assert(actualResult.equals(expectedResult))
  }
  test("test the result for sum()with out distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 20774.6475))
    assert(actualResult.toString().equals(expectedResult.toString()))
  }
  test("test the result for sum() with distinct operator for decimal column in presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT SUM(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 20774.6475))
    assert(
      actualResult.head("RESULT").toString.toDouble ==
      expectedResult.head("RESULT").toString.toDouble)
  }
  test("test the result for avg() with distinct operator on decimal on presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT AVG(DISTINCT BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map("RESULT" -> 2077.4648))
    assert(actualResult.toString.equals(expectedResult.toString))
  }

  test("test the result for min() with distinct operator in decimalType of presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MIN(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "RESULT" -> java.math.BigDecimal.valueOf(500.414).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }

  test("test the result for max() with distinct operator in decimalType of presto") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT MAX(BONUS) AS RESULT FROM TESTDB.TESTTABLE ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "RESULT" -> java.math.BigDecimal.valueOf(9999.999).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }
  test("select decimal data type with ORDER BY  clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DISTINCT BONUS FROM TESTDB.TESTTABLE ORDER BY BONUS limit 3 ")
    val expectedResult: List[Map[String, Any]] = List(Map(
      "BONUS" -> java.math.BigDecimal.valueOf(500.414).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.59).setScale(4)),
      Map("BONUS" -> java.math.BigDecimal.valueOf(500.88).setScale(4)))
    assert(actualResult.equals(expectedResult))
  }
  test("select string type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
      Map("NAME" -> "sahil"))
    assert(actualResult.equals(expectedResult))
  }
  test("select DATE type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
      Map("DATE" -> null))

    assert(actualResult.filterNot(_.get("DATE") == null).zipWithIndex.forall {
      case (map, index) => map.get("DATE").toString
        .equals(expectedResult(index).get("DATE").toString)
    })
    assert(actualResult.reverse.head("DATE") == null)
  }
  test("select int type with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT DISTINCT ID FROM TESTDB.TESTTABLE ORDER BY ID")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1),
      Map("ID" -> 2),
      Map("ID" -> 3),
      Map("ID" -> 4),
      Map("ID" -> 5),
      Map("ID" -> 6),
      Map("ID" -> 7),
      Map("ID" -> 8),
      Map("ID" -> 9))

    assert(actualResult.equals(expectedResult))

  }

  test("test and filter clause with greater than expression") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID FROM TESTDB.TESTTABLE WHERE BONUS=1234.444")

    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 1))

    assert(actualResult equals expectedResult)
  }
  test("test less than expression with and operator") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME from testdb.testtable WHERE PHONETYPE IN('phone1848','phone706')")
    val expectedResult: List[Map[String, Any]] = List(
      Map("NAME" -> "geetika"),
      Map("NAME" -> "ravindra"),
      Map("NAME" -> "jitesh"))

    assert(actualResult.equals(expectedResult))
  }
  test("test the result for not in clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT ID FROM TESTDB.TESTTABLE WHERE DATE IS NULL")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 9))
    assert(actualResult.equals(expectedResult))

  }
  test("test for not null operator on date data type") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DATE IS NOT NULL AND ID=9")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "ravindra"))
    assert(actualResult.equals(expectedResult))

  }
  test("test for not null operator on timestamp type") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DOB IS NOT NULL AND ID=9")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "ravindra"),
      Map("NAME" -> "jitesh"))
    assert(actualResult.equals(expectedResult))

  }
  test("test for null operator on timestamp type") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery("SELECT NAME FROM TESTDB.TESTTABLE WHERE DOB IS NULL AND ID=1")
    val expectedResult: List[Map[String, Any]] = List(Map("NAME" -> "anubhav"))
    assert(actualResult.equals(expectedResult))

  }
  test("test the result for short datatype with order by clause") {
    val actualResult: List[Map[String, Any]] = PrestoServer
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
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE SHORTFIELD IS NULL ORDER BY SHORTFIELD ")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 7))

    assert(actualResult.equals(expectedResult))
  }
  test("test the result for short datatype with greater than operator") {
    val actualResult: List[Map[String, Any]] = PrestoServer
      .executeQuery(
        "SELECT ID from testdb.testtable WHERE SHORTFIELD>11 ")
    val expectedResult: List[Map[String, Any]] = List(Map("ID" -> 6), Map("ID" -> 9))

    assert(actualResult.equals(expectedResult))
  }
}
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
import java.sql.{DriverManager, ResultSet, Statement}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

// scalastyle:off println
object HiveExample {

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  def main(args: Array[String]) {
    val carbonSession = ExampleUtils.createCarbonSession("HiveExample")
    exampleBody(carbonSession, CarbonProperties.getStorePath
      + CarbonCommonConstants.FILE_SEPARATOR
      + CarbonCommonConstants.DATABASE_DEFAULT_NAME)
    carbonSession.stop()

    System.exit(0)
  }

  def exampleBody(carbonSession: SparkSession, store: String): Unit = {
    val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath

    carbonSession.sql("""DROP TABLE IF EXISTS HIVE_CARBON_EXAMPLE""".stripMargin)

    carbonSession.sql(
      s"""
         | CREATE TABLE HIVE_CARBON_EXAMPLE
         | (ID int,NAME string,SALARY double)
         | STORED BY 'carbondata'
       """.stripMargin)

    carbonSession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$rootPath/examples/spark2/src/main/resources/sample.csv'
         | INTO TABLE HIVE_CARBON_EXAMPLE
       """.stripMargin)

    carbonSession.sql("SELECT * FROM HIVE_CARBON_EXAMPLE").show()

    try {
      Class.forName(driverName)
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        classNotFoundException.printStackTrace()
    }

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2()
    hiveEmbeddedServer2.start()
    val port = hiveEmbeddedServer2.getFreePort
    val connection = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
    val statement: Statement = connection.createStatement

    logger.info(s"============HIVE CLI IS STARTED ON PORT $port ==============")

    statement.execute(
      s"""
         | CREATE TABLE IF NOT EXISTS HIVE_CARBON_EXAMPLE
         | (ID int, NAME string,SALARY double)
         | ROW FORMAT SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe'
         | WITH SERDEPROPERTIES ('mapreduce.input.carboninputformat.databaseName'='default',
         | 'mapreduce.input.carboninputformat.tableName'='HIVE_CARBON_EXAMPLE')
       """.stripMargin)

    statement.execute(
      s"""
         | ALTER TABLE HIVE_CARBON_EXAMPLE
         | SET FILEFORMAT
         | INPUTFORMAT \"org.apache.carbondata.hive.MapredCarbonInputFormat\"
         | OUTPUTFORMAT \"org.apache.carbondata.hive.MapredCarbonOutputFormat\"
         | SERDE \"org.apache.carbondata.hive.CarbonHiveSerDe\"
       """.stripMargin)

    statement
      .execute(
        "ALTER TABLE HIVE_CARBON_EXAMPLE SET LOCATION " +
          s"'file:///$store/hive_carbon_example' ")

    val resultSet: ResultSet = statement.executeQuery("SELECT * FROM HIVE_CARBON_EXAMPLE")

    var rowsFetched = 0
    var resultId = ""
    var resultName = ""
    var resultSalary = ""

    while (resultSet.next) {
      if (rowsFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| ID|" + "| NAME |" + "| SALARY        |")

        println("+---+" + "+-------+" + "+--------------+")

        resultId = resultSet.getString("id")
        resultName = resultSet.getString("name")
        resultSalary = resultSet.getString("salary")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary  |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        resultId = resultSet.getString("ID")
        resultName = resultSet.getString("NAME")
        resultSalary = resultSet.getString("SALARY")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary   |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      rowsFetched = rowsFetched + 1
    }
    println(s"******Total Number Of Rows Fetched ****** $rowsFetched")
    assert(rowsFetched == 2)

    logger.info("Fetching the Individual Columns ")

    // fetching the separate columns
    var individualColRowsFetched = 0

    val resultIndividualCol = statement.executeQuery("SELECT NAME FROM HIVE_CARBON_EXAMPLE")

    while (resultIndividualCol.next) {
      if (individualColRowsFetched == 0) {
        println("+--------------+")
        println("| NAME         |")

        println("+---++---------+")

        resultName = resultIndividualCol.getString("name")

        println(s"| $resultName    |")
        println("+---+" + "+---------+")
      }
      else {
        resultName = resultIndividualCol.getString("NAME")

        println(s"| $resultName      |")
        println("+---+" + "+---------+")
      }
      individualColRowsFetched = individualColRowsFetched + 1
    }
    println(" ********** Total Rows Fetched When Quering The Individual Columns **********" +
      s"$individualColRowsFetched")
    assert(individualColRowsFetched == 2)

    logger.info("Fetching the Out Of Order Columns ")

    val resultOutOfOrderCol = statement
      .executeQuery("SELECT SALARY,ID,NAME FROM HIVE_CARBON_EXAMPLE")
    var outOfOrderColFetched = 0

    while (resultOutOfOrderCol.next()) {
      if (outOfOrderColFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| Salary|" + "| ID |" + "| NAME        |")

        println("+---+" + "+-------+" + "+--------------+")

        resultId = resultOutOfOrderCol.getString("id")
        resultName = resultOutOfOrderCol.getString("name")
        resultSalary = resultOutOfOrderCol.getString("salary")

        println(s"| $resultSalary |" + s"| $resultId |" + s"| $resultName  |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        resultId = resultOutOfOrderCol.getString("ID")
        resultName = resultOutOfOrderCol.getString("NAME")
        resultSalary = resultOutOfOrderCol.getString("SALARY")

        println(s"| $resultSalary |" + s"| $resultId |" + s"| $resultName   |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      outOfOrderColFetched = outOfOrderColFetched + 1
    }
    println(" ********** Total Rows Fetched When Quering The Out Of Order Columns **********" +
      s"$outOfOrderColFetched")
    assert(outOfOrderColFetched == 2)

    hiveEmbeddedServer2.stop()
  }
}

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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hive.test.server.HiveEmbeddedServer2

// scalastyle:off println
object HiveExample {

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val projectLoc = s"$rootPath/examples/spark"
  System.setProperty("derby.system.home", s"$projectLoc")
  val metaStoreLoc = s"$projectLoc/metastore_db"
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def main(args: Array[String]) {
    val sparkSession = ExampleUtils.createSparkSession("HiveExample")
    createCarbonTable(sparkSession)
    readFromHive
    System.exit(0)
  }

  def createCarbonTable(sparkSession: SparkSession): Unit = {

    sparkSession.sql("""DROP TABLE IF EXISTS HIVE_CARBON_EXAMPLE""".stripMargin)

    sparkSession.sql(
      s"""
         | CREATE TABLE HIVE_CARBON_EXAMPLE
         | (ID int,NAME string,SALARY double)
         | STORED AS carbondata
       """.stripMargin)

    var inputPath = FileFactory
      .getUpdatedFilePath(s"$rootPath/examples/spark/src/main/resources/sample.csv")

    sparkSession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputPath'
         | INTO TABLE HIVE_CARBON_EXAMPLE
       """.stripMargin)

    sparkSession.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$inputPath'
         | INTO TABLE HIVE_CARBON_EXAMPLE
       """.stripMargin)

    sparkSession.sql("SELECT * FROM HIVE_CARBON_EXAMPLE").show()

    sparkSession.sql("DROP TABLE IF EXISTS TEST_BOUNDARY")

    sparkSession
      .sql(
        s"""CREATE TABLE TEST_BOUNDARY (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,30),
           |c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string)
           |STORED AS carbondata""".stripMargin)

    inputPath = FileFactory
      .getUpdatedFilePath(s"$rootPath/examples/spark/src/main/resources/Test_Data1.csv")

    sparkSession
      .sql(
        s"LOAD DATA INPATH '$inputPath' INTO table TEST_BOUNDARY OPTIONS('DELIMITER'=','," +
        "'QUOTECHAR'='\"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='c1_int,c2_Bigint," +
        "c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc')")

    sparkSession.sql("""DROP TABLE IF EXISTS complexMap""".stripMargin)

    sparkSession.sql("create table complexMap(name map<string,string>) STORED AS carbondata")

    sparkSession
      .sql(
        "insert into complexMap values(map('Manish','Nalla','Shardul','Singh','Vishal','Kumar'," +
        "'EmptyVal','','NullVal', 'null'))")

    // delete the already existing lock on metastore so that new derby instance
    // for HiveServer can run on the same metastore
    checkAndDeleteDBLock
  }

  def checkAndDeleteDBLock: Unit = {
    val dbLockPath = FileFactory.getUpdatedFilePath(s"$metaStoreLoc/db.lck")
    val dbexLockPath = FileFactory.getUpdatedFilePath(s"$metaStoreLoc/dbex.lck")
    if(FileFactory.isFileExist(dbLockPath)) {
      FileFactory.deleteFile(dbLockPath)
    }
    if(FileFactory.isFileExist(dbexLockPath)) {
      FileFactory.deleteFile(dbexLockPath)
    }
  }


  def readFromHive: Unit = {
    try {
      Class.forName(driverName)
    }
    catch {
      case classNotFoundException: ClassNotFoundException =>
        classNotFoundException.printStackTrace()
    }

    // make HDFS writable
    val path = new Path(projectLoc)
    val fileSys = path.getFileSystem(FileFactory.getConfiguration)
    fileSys.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))

    val hiveEmbeddedServer2 = new HiveEmbeddedServer2()
    hiveEmbeddedServer2.start(projectLoc)
    val port = hiveEmbeddedServer2.getFreePort
    val connection = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
    val statement: Statement = connection.createStatement

    LOGGER.info(s"============HIVE CLI IS STARTED ON PORT $port ==============")

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
    assert(rowsFetched == 4)

    LOGGER.info("Fetching the Individual Columns ")

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
    assert(individualColRowsFetched == 4)

    LOGGER.info("Fetching the Out Of Order Columns ")

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
    assert(outOfOrderColFetched == 4)

    val resultAggQuery = statement
      .executeQuery(
        "SELECT min(c3_Decimal) as min, max(c3_Decimal) as max, " +
        "sum(c3_Decimal) as sum FROM TEST_BOUNDARY")

    var resultAggQueryFetched = 0

    var resultMin = ""
    var resultMax = ""
    var resultSum = ""

    while (resultAggQuery.next) {
      if (resultAggQueryFetched == 0) {
        println("+-----+" + "+-------------------+" + "+--------------------------------+")
        println("| min |" + "| max               |" + "| sum                            |")

        println("+-----+" + "+-------------------+" + "+--------------------------------+")

        resultMin = resultAggQuery.getString("min")
        resultMax = resultAggQuery.getString("max")
        resultSum = resultAggQuery.getString("sum")

        println(s"| $resultMin   |" + s"| $resultMax               |" + s"| $resultSum|")
        println("+-----+" + "+-------------------+" + "+--------------------------------+")
      }
      resultAggQueryFetched = resultAggQueryFetched + 1
    }
    println(" ********** Total Rows Fetched When Aggregate Query **********" +
            s"$resultAggQueryFetched")
    assert(resultAggQueryFetched == 1)

    val resultComplexQuery = statement
      .executeQuery(
        "SELECT name FROM complexMap")

    var resultComplex = 0

    var name = ""

    while (resultComplexQuery.next) {
      if (resultComplex == 0) {
        println("+------------------------------------------------------------------------------" +
                "------+")
        println("| name                                                                          " +
                "     |")

        println("+-------------------------------------------------------------------------------" +
                "-----+")

        name = resultComplexQuery.getString("name")

        println(s"|$name|")
        println("+-------------------------------------------------------------------------------" +
                "-----+")      }
      resultComplex = resultComplex + 1
    }
    println(" ********** Total Rows Fetched When Complex Query **********" +
            s"$resultComplex")
    assert(resultComplex == 1)

    hiveEmbeddedServer2.stop()
  }
}

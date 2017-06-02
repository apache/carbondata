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
package org.apache.carbondata.hiveexample

import java.io.File
import java.sql.{DriverManager, ResultSet, SQLException, Statement}

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.hive.server.HiveEmbeddedServer2

// scalastyle:off println
object HiveExample {

  private val driverName: String = "org.apache.hive.jdbc.HiveDriver"

  /**
   * @param args
   * @throws SQLException
   */
  @throws[SQLException]
  def main(args: Array[String]) {
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

    val carbonHadoopJarPath = s"$rootPath/assembly/target/scala-2.11/carbondata_2.11-1.1" +
      ".0-incubating-SNAPSHOT-shade-hadoop2.7.2.jar"

    val carbon_DefaultHadoopVersion_JarPath =
      s"$rootPath/assembly/target/scala-2.11/carbondata_2.11-1.1" +
        ".0-incubating-SNAPSHOT-shade-hadoop2.2.0.jar"

    val hiveJarPath = s"$rootPath/integration/hive/target/carbondata-hive-1.1" +
      ".0-incubating-SNAPSHOT.jar"

    carbon.sql("""DROP TABLE IF EXISTS HIVE_CARBON_EXAMPLE""".stripMargin)

    carbon
      .sql(
        """CREATE TABLE HIVE_CARBON_EXAMPLE (ID int,NAME string,SALARY double) STORED BY
          |'CARBONDATA' """
          .stripMargin)

    carbon.sql(
      s"""
           LOAD DATA LOCAL INPATH '$rootPath/integration/hive/src/main/resources/data.csv' INTO
           TABLE
         HIVE_CARBON_EXAMPLE
           """)
    carbon.sql("SELECT * FROM HIVE_CARBON_EXAMPLE").show()

    carbon.stop()

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
    val con = DriverManager.getConnection(s"jdbc:hive2://localhost:$port/default", "", "")
    val stmt: Statement = con.createStatement

    logger.info(s"============HIVE CLI IS STARTED ON PORT $port ==============")

    try {
      stmt
        .execute(s"ADD JAR $carbonHadoopJarPath")
    }
    catch {
      case exception: Exception =>
        logger.warn(s"Jar Not Found $carbonHadoopJarPath" + "Looking For hadoop 2.2.0 version jar")
        try {
          stmt
            .execute(s"ADD JAR $carbon_DefaultHadoopVersion_JarPath")
        }
        catch {
          case exception: Exception => logger
            .error("Exception Occurs:Neither One of Jar is Found" +
                   s"$carbon_DefaultHadoopVersion_JarPath,$carbonHadoopJarPath" +
                   "Atleast One Should Be Build")
            hiveEmbeddedServer2.stop()
            System.exit(0)
        }
    }
    try {
      stmt
        .execute(s"ADD JAR $hiveJarPath")
    }
    catch {
      case exception: Exception => logger.error(s"Exception Occurs:Jar Not Found $hiveJarPath")
        hiveEmbeddedServer2.stop()
        System.exit(0)

    }
    stmt.execute("set hive.mapred.supports.subdirectories=true")
    stmt.execute("set mapreduce.input.fileinputformat.input.dir.recursive=true")


    stmt.execute("CREATE TABLE IF NOT EXISTS " + "HIVE_CARBON_EXAMPLE " +
                 " (ID int, NAME string,SALARY double)")
    stmt
      .execute(
        "ALTER TABLE HIVE_CARBON_EXAMPLE SET FILEFORMAT INPUTFORMAT \"org.apache.carbondata." +
        "hive.MapredCarbonInputFormat\"OUTPUTFORMAT \"org.apache.carbondata.hive." +
        "MapredCarbonOutputFormat\"SERDE \"org.apache.carbondata.hive." +
        "CarbonHiveSerDe\" ")

    stmt
      .execute(
        "ALTER TABLE HIVE_CARBON_EXAMPLE SET LOCATION " +
        s"'file:///$store/default/hive_carbon_example' ")


    val sql = "SELECT * FROM HIVE_CARBON_EXAMPLE"

    val res: ResultSet = stmt.executeQuery(sql)

    var rowsFetched = 0

    while (res.next) {
      if (rowsFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| ID|" + "| NAME |" + "| SALARY        |")

        println("+---+" + "+-------+" + "+--------------+")

        val resultId = res.getString("id")
        val resultName = res.getString("name")
        val resultSalary = res.getString("salary")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary  |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        val resultId = res.getString("ID")
        val resultName = res.getString("NAME")
        val resultSalary = res.getString("SALARY")

        println(s"| $resultId |" + s"| $resultName |" + s"| $resultSalary   |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      rowsFetched = rowsFetched + 1
    }
    println(s"******Total Number Of Rows Fetched ****** $rowsFetched")

    logger.info("Fetching the Individual Columns ")

    // fetching the separate columns
    var individualColRowsFetched = 0

    val resultIndividualCol = stmt.executeQuery("SELECT NAME FROM HIVE_CARBON_EXAMPLE")

    while (resultIndividualCol.next) {
      if (individualColRowsFetched == 0) {
        println("+--------------+")
        println("| NAME         |")

        println("+---++---------+")

        val resultName = resultIndividualCol.getString("name")

        println(s"| $resultName    |")
        println("+---+" + "+---------+")
      }
      else {
        val resultName = resultIndividualCol.getString("NAME")

        println(s"| $resultName      |")
        println("+---+" + "+---------+")
      }
      individualColRowsFetched = individualColRowsFetched + 1
    }
    println(" ********** Total Rows Fetched When Quering The Individual Column **********" +
            s"$individualColRowsFetched")

    logger.info("Fetching the Out Of Order Columns ")

    val resultOutOfOrderCol = stmt.executeQuery("SELECT SALARY,ID,NAME FROM HIVE_CARBON_EXAMPLE")
    var outOfOrderColFetched = 0
    while (resultOutOfOrderCol.next()) {
      if (outOfOrderColFetched == 0) {
        println("+---+" + "+-------+" + "+--------------+")
        println("| Salary|" + "| ID |" + "| NAME        |")

        println("+---+" + "+-------+" + "+--------------+")

        val resultId = resultOutOfOrderCol.getString("id")
        val resultName = resultOutOfOrderCol.getString("name")
        val resultSalary = resultOutOfOrderCol.getString("salary")

        println(s"| $resultSalary |" + s"| $resultId |" + s"| $resultName  |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      else {
        val resultId = resultOutOfOrderCol.getString("ID")
        val resultName = resultOutOfOrderCol.getString("NAME")
        val resultSalary = resultOutOfOrderCol.getString("SALARY")

        println(s"| $resultSalary |" + s"| $resultId |" + s"| $resultName   |")
        println("+---+" + "+-------+" + "+--------------+")
      }
      outOfOrderColFetched = outOfOrderColFetched + 1
    }
    hiveEmbeddedServer2.stop()
    System.exit(0)
  }

}

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

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonSessionExample {

  val holders: ArrayBuffer[TestHolder] = new ArrayBuffer[TestHolder]()
  val path: String = "/home/root1/carbon/carbondata/integration/spark-common-cluster-test/src/test/scala/org/apache/carbondata/cluster/sdv/generated"
  val tableMapping = new java.util.HashMap[String, String]()
  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")
    val selectQuery = ExcelFeed
      .inputFeed("/home/root1/Downloads/21Machine_Query.xls", "Queries_combined2.1", 4)

    var i = 0
    while (i < selectQuery.size()) {
      val strings = selectQuery.get(i)
      if (strings(2) != null) {
        generateHiveCreateQuery(strings, spark)
        generateHiveLoadQuery(strings)
        generateSelectHiveQuery(spark, strings)
        generateDropQuery(strings)
      }
      i = i + 1
    }
    holders.foreach { h=>
      h.write()
      h.close()
    }

    spark.stop()
  }

  private def generateSelectHiveQuery(spark: SparkSession, strings: Array[String]) = {
    var hiveQuery: String = null
    if (strings(2).toLowerCase.startsWith("select")) {
      try {
        val logical = spark.sql(strings(2)).queryExecution.logical
        val set = new java.util.HashSet[String]()
        logical.collect {
          case l: UnresolvedRelation =>
            val tableName = l.tableIdentifier.table
            set.add(tableName)
        }
        hiveQuery = strings(2)
        set.asScala.foreach { name =>
          hiveQuery = hiveQuery.replaceAll(name, name + "_hive")
        }
        if (strings(3).equalsIgnoreCase("yes")) {
          addSelectQuery(set.asScala.toSeq, strings(2), hiveQuery, strings(0), true)
        } else {
          addSelectQuery(set.asScala.toSeq, strings(2), null, strings(0), false)
        }
      } catch {
        case e: Exception =>
          addSelectQuery(Seq("ErrorQueries"), strings(2), hiveQuery, strings(0), false)
      }
    }
    hiveQuery
  }

  def generateCompareTest(testId: String, carbonQuery: String, hiveQuery: String, tag: String): String = {
    val l: String = "s\"\"\""+carbonQuery+"\"\"\""
    val r: String = "s\"\"\""+hiveQuery+"\"\"\""
      s"""
         |//$testId
         |test("$testId", $tag) {
         |  checkAnswer($l,
         |    $r)
         |}
       """.stripMargin
  }

  def generateNormalTest(testId: String, carbonQuery: String, tag: String): String = {
    val s: String = "s\"\"\""+carbonQuery+"\"\"\""
    s"""
       |//$testId
       |test("$testId", $tag) {
       |  sql($s).collect
       |}
       """.stripMargin
  }

  def generateNormalTest(testId: String, carbonQuery: String, hiveQuery: String, tag: String): String = {
    val l: String = "sql(s\"\"\""+carbonQuery+"\"\"\").collect\n"
    val r: String = "sql(s\"\"\""+hiveQuery+"\"\"\").collect\n"

    s"""
       |//$testId
       |test("${testId}", $tag) {
       |  $l
       |  $r
       |}
       """.stripMargin
  }

  def generateSql(testId: String, carbonQuery: String, tag: String): String = {
    val s: String = "s\"\"\""+carbonQuery+"\"\"\""
    s"""
       |//$testId
       |  sql($s).collect
       """.stripMargin
  }

  def generateSql(testId: String, carbonQuery: String, hiveQuery: String, tag: String): String = {
    val l: String = "sql(s\"\"\""+carbonQuery+"\"\"\").collect\n"
    val r: String = "sql(s\"\"\""+hiveQuery+"\"\"\").collect\n"

    s"""
       |//$testId
       |  $l
       |  $r
       """.stripMargin
  }


  private def generateHiveCreateQuery(strings: Array[String], sparkSession: SparkSession = null) = {
    if (strings(2).trim.toLowerCase.startsWith("create table")) {
      var hiveQuery: String = null
      var start = 0
      if (strings(2).toLowerCase.indexOf("if not exists") > 0) {
        start = strings(2).toLowerCase.indexOf("exists") + 6
      } else {
        start = strings(2).toLowerCase.indexOf("table") + 5
      }
      val index = strings(2).indexOf("(")
      val tableName = strings(2).substring(start, index).trim
      val storeIndex = strings(2).toLowerCase().indexOf("stored by")
      if (storeIndex > 0) {
        if (sparkSession != null) {
          sparkSession.sql(s"""DROP table if exists $tableName""")
          sparkSession.sql(strings(2))
        }
        hiveQuery = strings(2).substring(0, storeIndex) +
                        " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
        hiveQuery = hiveQuery.replaceAll(tableName, tableName + "_hive")
      }
      addCreateQuery(tableName, strings(2), hiveQuery, strings(0))
    }
  }

  private def generateHiveLoadQuery(strings: Array[String]) = {
    if (strings(2).trim.toLowerCase.startsWith("load data")) {
      strings(2) = strings(2).replaceAll("HDFS_URL/BabuStore", "resourcesPath")
      val of = strings(2).indexOf("resourcesPath")
      val of1 = strings(2).indexOf("'", of)
      var path = strings(2)
      if (of <= 0 || of1 <= 0) {
        println(strings(2))
      } else {
        path = strings(2).substring(of, of1)
        strings(2) = strings(2).substring(0, of) + "$resourcesPath" + strings(2).substring(of+"resourcesPath".length, strings(2).length)
      }

      var start = strings(2).toLowerCase.indexOf("into table") + 10
      var index = strings(2).toLowerCase.indexOf("options", start)
      if (index <= 0) {
        index = strings(2).length
      }
      val tableName = strings(2).substring(start, index).trim
      println(tableName+" : "+path)
      val optionIndex = strings(2).toLowerCase.indexOf("options", start)
      var hiveQuery: String = null
      if (optionIndex > 0) {
        hiveQuery = strings(2).substring(0, optionIndex)
        hiveQuery = hiveQuery.replaceAll(tableName, tableName + "_hive")
      }
      addLoadQuery(tableName, strings(2), hiveQuery, strings(0))
    }
  }

  private def generateDropQuery(strings: Array[String]) = {
    if (strings(2).trim.toLowerCase.startsWith("drop table")) {
      var hiveQuery: String = null
      var start = 0
      if (strings(2).toLowerCase.indexOf("if exists") > 0) {
        start = strings(2).toLowerCase.indexOf("exists") + 6
      } else {
        start = strings(2).toLowerCase.indexOf("table") + 5
      }
      val index = strings(2).length
      val tableName = strings(2).substring(start, index).trim
      val storeIndex = strings(2).toLowerCase().indexOf("stored by")
      hiveQuery = strings(2).replaceAll(tableName, tableName + "_hive")
      addCreateQuery(tableName, strings(2), hiveQuery, strings(0))
    }
  }


  def addCreateQuery(tableName: String, carbon: String, hive: String, testId: String) = {
    val th = findHolder(Seq(tableName), holders)
    th.addCreate(carbon, hive, testId: String, tableName)
  }

  def addLoadQuery(tableName: String, carbon: String, hive: String, testId: String) = {
    val th = findHolder(Seq(tableName), holders)
    th.addLoad(carbon, hive, testId: String)
  }

  def addSelectQuery(tableName: Seq[String], carbon: String, hive: String, testId: String, compare: Boolean) = {

    val th = findHolder(tableName, holders)
    th.addSelect(carbon, hive, testId: String, compare)
  }

  def findHolder(tableName: Seq[String], holders: ArrayBuffer[TestHolder]): TestHolder = {
    val map: Seq[TestHolder] = tableName.map { table =>
      val updatedName = tableMapper(table)
      holders.find(_.name.equalsIgnoreCase(updatedName)) match {
        case Some(t@TestHolder(_)) => t
        case _ =>
          null
      }
    }
    var testHolder: TestHolder = null
    map.foreach { holder =>
      if (testHolder != null) {
        if (holder != null) {
          if(testHolder.merge(holder)) {
            tableMapping.put(holder.name, testHolder.name)
            holders -= holder
          }
        }
      } else {
        testHolder = holder
      }
    }
    if (testHolder == null) {
      testHolder = TestHolder(tableMapper(tableName.head))
      holders += testHolder
    }
    testHolder
  }

  def tableMapper(name: String): String = {
    var uname = name
    if (tableMapping.get(uname) != null) {
      return tableMapping.get(uname)
    }
    if (uname.indexOf(".") > 0) {
      uname = uname.split("\\.")(1)
    }
    if (uname.toLowerCase.startsWith("sequential")) {
      return "sequential"
    }
    if(isStartWithNumber(uname)) {
      return "MoreRecords"
    }
    uname = uname.replaceAll("_", "")
    return uname.replaceAll("-", "")
  }

  def isStartWithNumber(name: String): Boolean = {
    val at: String = name.charAt(0)+""
    try {
      Integer.parseInt(at)
      return true
    } catch {
      case e: Exception =>
        return false
    }
  }

  case class TestHolder(name: String) {
//    val create: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
//    val load: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
    val select: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
    val tableName: ArrayBuffer[String] = new ArrayBuffer[String]()

    val include = {
      if (name.equals("ErrorQueries")) {
        "Exclude"
      } else {
        "Include"
      }
    }

    def addCreate(carbon: String, hive: String, tesId: String, table: String): Unit = {
      select += QueryTuple(carbon, hive, tesId: String, false)
      tableName += table
    }

    def addDrop(carbon: String, hive: String, tesId: String, table: String): Unit = {
      select += QueryTuple(carbon, hive, tesId: String, false)
      tableName += table
    }

    def addLoad(carbon: String, hive: String, tesId: String): Unit = {
      select += QueryTuple(carbon, hive, tesId: String, false)
    }

    def addSelect(carbon: String, hive: String, tesId: String, compare: Boolean): Unit = {
      select += QueryTuple(carbon, hive, tesId: String, compare)
    }

    def close(): Unit = {

    }
    def write(): Unit = {
      val fileWriter = new BufferedWriter(new FileWriter(path+"/"+name.toUpperCase+"TestCase.scala"))
      val header =
        s"""
           |/*
           | * Licensed to the Apache Software Foundation (ASF) under one or more
           | * contributor license agreements.  See the NOTICE file distributed with
           | * this work for additional information regarding copyright ownership.
           | * The ASF licenses this file to You under the Apache License, Version 2.0
           | * (the "License"); you may not use this file except in compliance with
           | * the License.  You may obtain a copy of the License at
           | *
           | *    http://www.apache.org/licenses/LICENSE-2.0
           | *
           | * Unless required by applicable law or agreed to in writing, software
           | * distributed under the License is distributed on an "AS IS" BASIS,
           | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
           | * See the License for the specific language governing permissions and
           | * limitations under the License.
           | */
           |
           |package org.apache.carbondata.spark.testsuite.sdv.generated
           |
           |import org.apache.spark.sql.common.util._
           |import org.scalatest.BeforeAndAfterAll
           |
           |/**
           | * Test Class for $name to verify all scenerios
           | */
           |
           |class ${name.toUpperCase+"TestCase"} extends QueryTest with BeforeAndAfterAll {
         """.stripMargin
      fileWriter.write(header)
      fileWriter.newLine()


//      val beforeAll = s"""
//           |override def beforeAll {
//         """.stripMargin
//      fileWriter.write(beforeAll)
//      fileWriter.newLine()
//      create.foreach { q =>
//        if (q.hive != null) {
//          fileWriter.write(generateSql(q.testId, q.carbon, q.hive, include))
//        } else {
//          fileWriter.write(generateSql(q.testId, q.carbon, include))
//        }
//        fileWriter.newLine()
//      }
//      load.foreach { q =>
//        if (q.hive != null) {
//          fileWriter.write(generateSql(q.testId, q.carbon, q.hive, include))
//        } else {
//          fileWriter.write(generateSql(q.testId, q.carbon, include))
//        }
//        fileWriter.newLine()
//      }
//      fileWriter.write("}")
//      fileWriter.newLine()

      val unique = new java.util.LinkedHashSet[QueryTuple]()
      select.foreach(unique.add)

      unique.asScala.foreach { q =>
        if (q.compare) {
          fileWriter.write(generateCompareTest(q.testId, q.carbon, q.hive, include))
        } else {
          if (q.hive != null) {
            fileWriter.write(generateNormalTest(q.testId, q.carbon, q.hive, include))
          } else {
            fileWriter.write(generateNormalTest(q.testId, q.carbon, include))
          }
        }
        fileWriter.newLine()
      }

      fileWriter.write("override def afterAll {")
      fileWriter.newLine()
      tableName.toSet[String].foreach {t =>
        fileWriter.write("sql(\"drop table if exists "+t+"\")")
        fileWriter.newLine()
        fileWriter.write("sql(\"drop table if exists "+t+"_hive"+"\")")
        fileWriter.newLine()
       }
      fileWriter.write("}")
      fileWriter.newLine()
      fileWriter.write("}")
      fileWriter.close()
    }

    def merge(testHolder: TestHolder): Boolean = {
      if (testHolder.name.equalsIgnoreCase(name)) {
        return false
      }
      tableName ++= testHolder.tableName
//      create ++= testHolder.create
//      load ++= testHolder.load
      select ++= testHolder.select
      return true
    }

    override def equals(obj: scala.Any): Boolean =
      obj.asInstanceOf[TestHolder].name.equalsIgnoreCase(name)
  }

  case class QueryTuple(carbon: String, hive: String, testId: String, compare: Boolean) {
    override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[QueryTuple].testId.equals(testId)


    override def hashCode(): Int = testId.hashCode
  }
}

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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object TestCaseGeneratorNewLoadSheet {
  val sheetName = "DataLoading"
  val className = sheetName.replaceAll("_", "") +"TestCase"
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
      .inputFeed("/home/root1/Downloads/queries-internal/Query_1500_2.1.xls", sheetName, 9)
    val className = sheetName.replaceAll("_", "") +"TestCase.scala"
    var i = 0
    while (i < selectQuery.size()) {
      val strings = selectQuery.get(i)
      if (strings(3) != null) {
        if (strings(8) != null) {
          strings(2) = strings(2) + "\n" + strings(3)
          strings(3) = strings(8)
        }
        generateHiveCreateQuery(strings, spark)
        generateHiveLoadQuery(strings)
        generateSelectHiveQuery(spark, strings)
        generateDropQuery(strings)
        generateAlterTable(strings)
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
    val query = strings(3).replaceAll("\\n", "")
    val hiveQuery = getValidationRowString(strings(6))
    val comparisionNeeded = strings(5) != null && strings(5).equalsIgnoreCase("yes")
    if (query.toLowerCase.startsWith("select")) {
      try {
        val logical = spark.sql(query).queryExecution.logical
        val set = new java.util.HashSet[String]()
        logical.collect {
          case l: UnresolvedRelation =>
            val tableName = l.tableIdentifier.table
            set.add(tableName)
        }

        addSelectQuery(set.asScala.toSeq, query, hiveQuery, strings(0),strings(1), comparisionNeeded, strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
      } catch {
        case e: Exception =>
          addSelectQuery(Seq("ErrorQueries"), query, hiveQuery, strings(0),strings(1), comparisionNeeded, strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
      }
    }
  }

  def getValidationRowString(validation: String): String = {
    if (validation == null) {
      return ""
    }
    val strings = validation.split("\\r?\\n")
    if (strings.length < 2) {
      return ""
    }
    var i = 0
    val build = new StringBuilder("Seq(")
    strings.foreach {colunm =>
      if (i != 0) {
        build.append("Row(")
        val fields = colunm.split(",")
        var k = 0
        fields.foreach { field =>
          if (isNumber(field) || field.equals("null")) {
            if (java.lang.Double.parseDouble(field) > Integer.MAX_VALUE) {
              build.append(field).append("L")
            } else {
              build.append(field)
            }

          } else {
            build.append(s""""$field"""")
          }
          if (k < fields.length-1) {
            build.append(",")
          }
          k = k+1
        }
        build.append(")")
        if (i < strings.length-1) {
          build.append(",")
        }
      }
      i = i+1
    }
    return build.append(")").toString()
  }

  def isNumber(value: String): Boolean =  {
    try {
      java.lang.Double.parseDouble(value)
    } catch {
      case _ => return false
    }
    return true
  }



  def generateCompareTest(testId: String, desc: String, carbonQuery: String, hiveQuery: String, tag: String, preCondition: String, postCondition: String): String = {
    val l: String = "s\"\"\""+carbonQuery+"\"\"\""
    var r: String = hiveQuery
    if (!hiveQuery.startsWith("Seq(Row(")) {
      r = "s\"\"\""+hiveQuery+"\"\"\""
    }
      s"""
         |//$desc
         |test("$testId", $tag) {
         |  ${ getCondition(preCondition) }
         |  checkAnswer($l,
         |    $r, "${className + "_" + testId}")
         |  ${ getCondition(postCondition) }
         |}
       """.stripMargin
  }

  def getCondition(condition: String): String = {
    if (condition == null || condition.equalsIgnoreCase("NA")) {
      return ""
    } else {
      val strings = condition.split("\\r?\\n")
      val buffer = new StringBuilder
      var i = 0
      var boolean = false
      strings.foreach { con =>
        if (!con.trim.equals("")) {
          buffer.append(" sql(s\"\"\"" + con + "\"\"\").collect")
          boolean = true
        }
        if (boolean && i < strings.length-1) {
          buffer.append("\n")
        }
        i = i+1
      }
      buffer.toString()
    }
  }

  def generateNormalTest(testId: String, desc: String, carbonQuery: String, tag: String, preCondition: String, postCondition: String, expectResult: Boolean): String = {
    val s: String = "s\"\"\""+carbonQuery+"\"\"\""
    if (expectResult) {
      s"""
         |//$desc
         |test("$testId", $tag) {
         |  ${ getCondition(preCondition) }
         |  sql($s).collect
         |  ${ getCondition(postCondition) }
         |}
       """.stripMargin
    } else {
      s"""
         |//$desc
         |test("$testId", $tag) {
         |  try {
         |    ${ getCondition(preCondition) }
         |    sql($s).collect
         |    assert(false)
         |  } catch {
         |    case _ => assert(true)
         |  }
         |  ${ getCondition(postCondition) }
         |}
       """.stripMargin
    }
  }

  def generateNormalTest(testId: String, desc: String, carbonQuery: String, hiveQuery: String, tag: String, preCondition: String, postCondition: String, expectResult: Boolean): String = {
    val l: String = "sql(s\"\"\""+carbonQuery+"\"\"\").collect"
    var r: String = ""
    if (!(hiveQuery.equalsIgnoreCase("NA") || hiveQuery.equalsIgnoreCase(""))) {
      r = "sql(s\"\"\""+hiveQuery+"\"\"\").collect"
    }

    if (expectResult) {
      s"""
         |//$desc
         |test("${testId}", $tag) {
         |  ${getCondition(preCondition)}
         |  $l
         |  $r
         |  ${getCondition(postCondition)}
         |}
       """.stripMargin

    } else {
      s"""
         |//$desc
         |test("${testId}", $tag) {
         |  try {
         |  ${getCondition(preCondition)}
         |    $l
         |    $r
         |    assert(false)
         |  } catch {
         |    case _ => assert(true)
         |  }
         |  ${getCondition(postCondition)}
         |}
       """.stripMargin
    }
  }


  private def generateHiveCreateQuery(strings: Array[String], sparkSession: SparkSession = null) = {
    val query = strings(3)
    if (query.trim.toLowerCase.startsWith("create table")) {
      var hiveQuery: String = null
      var start = 0
      if (query.toLowerCase.indexOf("if not exists") > 0) {
        start = query.toLowerCase.indexOf("exists") + 6
      } else {
        start = query.toLowerCase.indexOf("table") + 5
      }
      val index = query.indexOf("(")
      val tableName = query.substring(start, index).trim
      val storeIndex = query.toLowerCase().indexOf("stored by")
      try {
        if (storeIndex > 0) {
          if (sparkSession != null) {
            sparkSession.sql(s"""DROP table if exists $tableName""")
            sparkSession.sql(query)
          }
          hiveQuery = query.substring(0, storeIndex) +
                      " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
          hiveQuery = hiveQuery.replaceAll(tableName, tableName + "_hive")
        }
      } catch {
        case _ =>
      }
      addCreateQuery(tableName, query, hiveQuery, strings(0),strings(1), strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
    }
  }

  private def generateAlterTable(strings: Array[String]): Unit = {
    if (strings(3).toLowerCase.startsWith("alter") ||
        strings(3).toLowerCase.startsWith("show") || strings(3).toLowerCase.startsWith("delete")
        || strings(3).toLowerCase.startsWith("update") ||
        strings(3).toLowerCase.startsWith("create database")) {
      val valid = getValidationRowString(strings(6))
      if (valid.length > 1) {
        addSelectQuery(Seq(), strings(3), valid, strings(0),strings(1), true, strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
      } else {
        addCreateQuery("", strings(3), valid, strings(0),strings(1), strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
      }
    }
  }


  private def generateHiveLoadQuery(strings: Array[String]) = {
    var query = strings(3)
    if (query.trim.toLowerCase.startsWith("load data")) {
      query = query.replaceAll("HDFS_URL/BabuStore", "resourcesPath")
      val of = query.indexOf("resourcesPath")
      val of1 = query.indexOf("'", of)
      var path = query
      if (of <= 0 || of1 <= 0) {
        println(query)
      } else {
        path = query.substring(of, of1)
        query = query.substring(0, of) + "$resourcesPath" + query.substring(of+"resourcesPath".length, query.length)
      }

      var start = query.toLowerCase.indexOf("into table") + 10
      var index = query.toLowerCase.indexOf("options", start)
      if (index <= 0) {
        index = query.length
      }
      val tableName = query.substring(start, index).trim
      println(tableName+" : "+path)
      val optionIndex = query.toLowerCase.indexOf("options", start)
      var hiveQuery: String = null
      if (optionIndex > 0) {
        hiveQuery = query.substring(0, optionIndex)
        hiveQuery = hiveQuery.replaceAll(tableName, tableName + "_hive")
      }
      addLoadQuery(tableName, query, hiveQuery, strings(0),strings(1), strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
    }
  }

  private def generateDropQuery(strings: Array[String]) = {
    val query = strings(3)
    if (query.trim.toLowerCase.startsWith("drop table")) {
      var hiveQuery: String = null
      var start = 0
      if (query.toLowerCase.indexOf("if exists") > 0) {
        start = query.toLowerCase.indexOf("exists") + 6
      } else {
        start = query.toLowerCase.indexOf("table") + 5
      }
      val index = query.length
      val tableName = query.substring(start, index).trim
      val storeIndex = query.toLowerCase().indexOf("stored by")
      hiveQuery = query.replaceAll(tableName, tableName + "_hive")
      addCreateQuery(tableName, query, hiveQuery, strings(0),strings(1), strings(2), strings(7), !strings(4).equalsIgnoreCase("FAIL"))
    }
  }


  def addCreateQuery(tableName: String, carbon: String, hive: String, testId: String,tesdec:String,preCondition:String, postCondition:String, failResult: Boolean) = {
    val th = findHolder(Seq(tableName), holders)
    th.addCreate(carbon, hive, testId,tesdec, tableName, preCondition, postCondition, failResult)
  }

  def addLoadQuery(tableName: String, carbon: String, hive: String, testId: String,tesdec:String,preCondition:String, postCondition:String, failResult: Boolean) = {
    val th = findHolder(Seq(tableName), holders)
    th.addLoad(carbon, hive, testId: String,tesdec, preCondition, postCondition,failResult)
  }

  def addSelectQuery(tableName: Seq[String], carbon: String, hive: String, testId: String,tesdec:String, compare: Boolean,preCondition:String, postCondition:String, failResult: Boolean) = {

    val th = findHolder(tableName, holders)
    th.addSelect(carbon, hive, testId: String,tesdec, compare, preCondition, postCondition, failResult)
  }

  def findHolder(tableName: Seq[String], holders: ArrayBuffer[TestHolder]): TestHolder = {
    var testHolder: TestHolder = null
    if (holders.length == 0) {
      testHolder = TestHolder()
      holders += testHolder
    }
    testHolder = holders(0)
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

  case class TestHolder() {
//    val create: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
//    val load: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
    val select: ArrayBuffer[QueryTuple] = new ArrayBuffer[QueryTuple]()
    val tableName: ArrayBuffer[String] = new ArrayBuffer[String]()

    val include = "Include"

    def addCreate(carbon: String, hive: String, tesId: String,tesdec:String, table: String,preCondition:String, postCondition:String, failResult: Boolean): Unit = {
      select += QueryTuple(carbon, hive, tesId,tesdec, false, preCondition, postCondition,failResult)
      tableName += table
    }

    def addDrop(carbon: String, hive: String, tesId: String,tesdec:String, table: String, preCondition:String, postCondition:String, failResult: Boolean): Unit = {
      select += QueryTuple(carbon, hive, tesId,tesdec, false, preCondition, postCondition, failResult)
      tableName += table
    }

    def addLoad(carbon: String, hive: String, tesId: String,tesdec: String, preCondition:String, postCondition:String, failResult: Boolean): Unit = {
      select += QueryTuple(carbon, hive, tesId,tesdec, false, preCondition, postCondition, failResult)
    }

    def addSelect(carbon: String, hive: String, tesId: String,tesdec:String, compare: Boolean, preCondition:String, postCondition:String, failResult: Boolean): Unit = {
      select += QueryTuple(carbon, hive, tesId,tesdec, compare, preCondition, postCondition, failResult)
    }

    def close(): Unit = {

    }
    def write(): Unit = {
      val fileWriter = new BufferedWriter(new FileWriter(path+"/"+className+".scala"))
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
           |package org.apache.carbondata.cluster.sdv.generated
           |
           |import org.apache.spark.sql.common.util._
           |import org.scalatest.BeforeAndAfterAll
           |
           |/**
           | * Test Class for $className to verify all scenerios
           | */
           |
           |class ${className} extends QueryTest with BeforeAndAfterAll {
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
          fileWriter.write(generateCompareTest(q.testId, q.testDesc, q.carbon, q.hive, include, q.preCondition, q.postCondition))
        } else {
          if (q.hive != null) {
            fileWriter.write(generateNormalTest(q.testId, q.testDesc, q.carbon, q.hive, include, q.preCondition, q.postCondition, q.failResult))
          } else {
            fileWriter.write(generateNormalTest(q.testId, q.testDesc, q.carbon, include, q.preCondition, q.postCondition, q.failResult))
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


    override def equals(obj: scala.Any): Boolean =
      true
  }

  case class QueryTuple(carbon: String, hive: String, testId: String, testDesc: String, compare: Boolean, preCondition:String, postCondition:String, failResult: Boolean) {
    override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[QueryTuple].testId.equals(testId)


    override def hashCode(): Int = testId.hashCode
  }
}

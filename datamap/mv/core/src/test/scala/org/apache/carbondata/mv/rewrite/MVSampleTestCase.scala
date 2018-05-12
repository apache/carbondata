package org.apache.carbondata.mv.rewrite

import java.io.File

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.mv.rewrite.matching.TestSQLBatch._

class MVSampleTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark-common-test/src/test/resources"
    sql("drop database if exists sample cascade")
    sql("create database sample")
    sql("use sample")

    createTables.map(sql)

  }

  def createTables: Seq[String] = {
    Seq[String](
      s"""
         |CREATE TABLE Fact (
         |  `tid`     int,
         |  `fpgid`   int,
         |  `flid`    int,
         |  `date`    timestamp,
         |  `faid`    int,
         |  `price`   double,
         |  `qty`     int,
         |  `disc`    string
         |)
         |STORED BY 'org.apache.carbondata.format'
        """.stripMargin.trim,
      s"""
         |CREATE TABLE Dim (
         |  `lid`     int,
         |  `city`    string,
         |  `state`   string,
         |  `country` string
         |)
         |STORED BY 'org.apache.carbondata.format'
        """.stripMargin.trim,
      s"""
         |CREATE TABLE Item (
         |  `i_item_id`     int,
         |  `i_item_sk`     int
         |)
         |STORED BY 'org.apache.carbondata.format'
        """.stripMargin.trim
    )
  }


  test("test create datamap with sampleTestCases case_1") {
    sql(s"drop datamap if exists datamap_sm1")
    sql(s"create datamap datamap_sm1 using 'mv' as ${sampleTestCases(0)._2}")
    sql(s"rebuild datamap datamap_sm1")
    val df = sql(sampleTestCases(0)._3)
    val analyzed = df.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap_sm1"))
    sql(s"drop datamap datamap_sm1")
  }

  test("test create datamap with sampleTestCases case_3") {
    sql(s"drop datamap if exists datamap_sm2")
    sql(s"create datamap datamap_sm2 using 'mv' as ${sampleTestCases(2)._2}")
    sql(s"rebuild datamap datamap_sm2")
    val df = sql(sampleTestCases(2)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm2"))
    sql(s"drop datamap datamap_sm2")
  }

  test("test create datamap with sampleTestCases case_4") {
    sql(s"drop datamap if exists datamap_sm3")
    sql(s"create datamap datamap_sm3 using 'mv' as ${sampleTestCases(3)._2}")
    sql(s"rebuild datamap datamap_sm3")
    val df = sql(sampleTestCases(3)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm3"))
    sql(s"drop datamap datamap_sm3")
  }

  test("test create datamap with sampleTestCases case_5") {
    sql(s"drop datamap if exists datamap_sm4")
    sql(s"create datamap datamap_sm4 using 'mv' as ${sampleTestCases(4)._2}")
    sql(s"rebuild datamap datamap_sm4")
    val df = sql(sampleTestCases(4)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm4"))
    sql(s"drop datamap datamap_sm4")
  }

  test("test create datamap with sampleTestCases case_6") {
    sql(s"drop datamap if exists datamap_sm5")
    sql(s"create datamap datamap_sm5 using 'mv' as ${sampleTestCases(5)._2}")
    sql(s"rebuild datamap datamap_sm5")
    val df = sql(sampleTestCases(5)._3)
    val analyzed = df.queryExecution.analyzed
    assert(!verifyMVDataMap(analyzed, "datamap_sm5"))
    sql(s"drop datamap datamap_sm5")
  }

  test("test create datamap with sampleTestCases case_7") {
    sql(s"drop datamap if exists datamap_sm6")
    sql(s"create datamap datamap_sm6 using 'mv' as ${sampleTestCases(6)._2}")
    sql(s"rebuild datamap datamap_sm6")
    val df = sql(sampleTestCases(6)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm6"))
    sql(s"drop datamap datamap_sm6")
  }

  test("test create datamap with sampleTestCases case_8") {
    sql(s"drop datamap if exists datamap_sm7")
    sql(s"create datamap datamap_sm7 using 'mv' as ${sampleTestCases(7)._2}")
    sql(s"rebuild datamap datamap_sm7")
    val df = sql(sampleTestCases(7)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm7"))
    sql(s"drop datamap datamap_sm7")
  }

  test("test create datamap with sampleTestCases case_9") {
    sql(s"drop datamap if exists datamap_sm8")
    sql(s"create datamap datamap_sm8 using 'mv' as ${sampleTestCases(8)._2}")
    sql(s"rebuild datamap datamap_sm8")
    val df = sql(sampleTestCases(8)._3)
    val analyzed = df.queryExecution.analyzed
    assert(verifyMVDataMap(analyzed, "datamap_sm8"))
    sql(s"drop datamap datamap_sm8")
  }


  def verifyMVDataMap(logicalPlan: LogicalPlan, dataMapName: String): Boolean = {
    val tables = logicalPlan collect {
      case l: LogicalRelation => l.catalogTable.get
    }
    tables.exists(_.identifier.table.equalsIgnoreCase(dataMapName+"_table"))
  }


  def drop(): Unit = {
    sql("use default")
    sql("drop database if exists sample cascade")
  }

  override def afterAll {
    drop()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}

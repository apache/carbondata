package org.apache.carbondata.spark.testsuite.compaction

import java.io.{BufferedWriter, File, FileWriter}

import scala.collection.mutable.ListBuffer

import au.com.bytecode.opencsv.CSVWriter
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Assert
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


class TestHybridCompaction extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  val rootPath = new File(this.getClass.getResource("/").getPath + "../../../..").getCanonicalPath

  val csvPath1 =
    s"$rootPath/integration/spark-common-test/src/test/resources/compaction/hybridCompaction1.csv"

  val csvPath2 =
    s"$rootPath/integration/spark-common-test/src/test/resources/compaction/hybridCompaction2.csv"

  val tableName = "t1"


  override def beforeAll: Unit = {
    generateCSVFiles()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "MM/dd/yyyy")
  }


  override def afterAll: Unit = {
    deleteCSVFiles()
  }


  override def beforeEach(): Unit = {
    dropTable()
    createTable()
  }


  override def afterEach(): Unit = {
        dropTable()
  }


  def generateCSVFiles(): Unit = {
    val rows1 = new ListBuffer[Array[String]]
    rows1 += Array("seq", "first", "last", "age", "city", "state", "date")
    rows1 += Array("1", "Augusta", "Nichols", "20", "Varasdo", "WA", "07/05/2003")
    rows1 += Array("2", "Luis", "Barnes", "39", "Oroaklim", "MT", "04/05/2048")
    rows1 += Array("3", "Leah", "Guzman", "54", "Culeosa", "KS", "02/23/1983")
    rows1 += Array("4", "Ian", "Ford", "61", "Rufado", "AL", "03/02/1995")
    rows1 += Array("5", "Fanny", "Horton", "37", "Rorlihbem", "CT", "05/12/1987")
    createCSV(rows1, csvPath1)

    val rows2 = new ListBuffer[Array[String]]
    rows2 += Array("seq", "first", "last", "age", "city", "state", "date")
    rows2 += Array("11", "Claudia", "Sullivan", "42", "Dilwuani", "ND", "09/01/2003")
    rows2 += Array("12", "Kate", "Adkins", "54", "Fokafrid", "WA", "10/13/2013")
    rows2 += Array("13", "Eliza", "Lynch", "23", "Bonpige", "ME", "05/02/2015")
    rows2 += Array("14", "Sarah", "Fleming", "60", "Duvugove", "IA", "04/15/2036")
    rows2 += Array("15", "Maude", "Bass", "44", "Ukozedka", "CT", "11/08/1988")
    createCSV(rows2, csvPath2)
  }


  def createCSV(rows: ListBuffer[Array[String]], csvPath: String): Unit = {
    val out = new BufferedWriter(new FileWriter(csvPath))
    val writer: CSVWriter = new CSVWriter(out)

    for (row <- rows) {
      writer.writeNext(row)
    }

    out.close()
    writer.close()
  }


  def deleteCSVFiles(): Unit = {
    try {
      FileUtils.forceDelete(new File(csvPath1))
      FileUtils.forceDelete(new File(csvPath2))
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        Assert.fail(e.getMessage)
    }
  }


  def createTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE $tableName(seq int, first string, last string,
         |   age int, city string, state string, date date)
         | STORED AS carbondata
         | TBLPROPERTIES(
         |   'sort_scope'='local_sort',
         |   'sort_columns'='state, age',
         |   'dateformat'='MM/dd/yyyy')
      """.stripMargin)
  }


  def loadUnsortedData(n : Int = 1): Unit = {
    for(_ <- 1 to n) {
      sql(
        s"""
           | LOAD DATA INPATH '$csvPath1' INTO TABLE $tableName
           | OPTIONS (
           |   'sort_scope'='no_sort')""".stripMargin)
    }
  }


  def loadSortedData(n : Int = 1): Unit = {
    for(_ <- 1 to n) {
      sql(
        s"""
           | LOAD DATA INPATH '$csvPath2' INTO TABLE $tableName
           | OPTIONS (
           |   'sort_scope'='local_sort')""".stripMargin)
    }
  }


  def dropTable(): Unit = {
    sql(s"DROP TABLE IF EXISTS $tableName")
  }


  test("SORTED LOADS") {
    loadSortedData(2)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("CT", "CT", "IA", "IA", "ME", "ME", "ND", "ND", "WA", "WA"))
  }


  test("UNSORTED LOADS") {
    loadUnsortedData(2)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("AL", "AL", "CT", "CT", "KS", "KS", "MT", "MT", "WA", "WA"))
  }


  test("MIXED LOADS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")
    val out = sql(s"SELECT state, age FROM $tableName").collect()
    out.map(_.get(0).toString) should
    equal(Array("AL", "CT", "CT", "IA", "KS", "ME", "MT", "ND", "WA", "WA"))
    out.map(_.get(1).toString) should
    equal(Array("61", "37", "44", "60", "54", "23", "39", "42", "20", "54"))
  }


  test("INSERT") {
    loadSortedData()
    loadUnsortedData()
    sql(
      s"""
         | INSERT INTO $tableName
         | VALUES('20', 'Naman', 'Rastogi', '23', 'Bengaluru', 'ZZ', '12/28/2018')
      """.stripMargin)
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("AL", "CT", "CT", "IA", "KS", "ME", "MT", "ND", "WA", "WA", "ZZ"))
  }


  test("UPDATE") {
    loadSortedData()
    loadUnsortedData()
    sql(s"UPDATE  $tableName SET (state)=('CT') WHERE seq='13'").collect()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName WHERE seq='13'").collect()
    out.map(_.get(0).toString) should equal(Array("CT"))
  }

  test("DELETE") {
    loadSortedData()
    loadUnsortedData()
    sql(s"DELETE FROM $tableName WHERE seq='13'").collect()
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT state FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("AL", "CT", "CT", "IA", "KS", "MT", "ND", "WA", "WA"))
  }


  test("RESTRUCTURE TABLE REMOVE COLUMN NOT IN SORT_COLUMNS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName DROP COLUMNS(city)")
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT age FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("61", "37", "44", "60", "54", "23", "39", "42", "20", "54"))
  }


  test("RESTRUCTURE TABLE REMOVE COLUMN IN SORT_COLUMNS") {
    loadSortedData()
    loadUnsortedData()
    sql(s"ALTER TABLE $tableName DROP COLUMNS(state)")
    sql(s"ALTER TABLE $tableName COMPACT 'major'")

    val out = sql(s"SELECT age FROM $tableName").collect()
    out.map(_.get(0).toString) should equal(
      Array("20", "23", "37", "39", "42", "44", "54", "54", "60", "61"))
  }
}

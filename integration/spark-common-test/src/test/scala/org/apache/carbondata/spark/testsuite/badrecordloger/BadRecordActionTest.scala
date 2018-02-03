package org.apache.carbondata.spark.testsuite.badrecordloger

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class BadRecordActionTest extends QueryTest with BeforeAndAfterAll {


  val csvFilePath = s"$resourcesPath/badrecords/datasample.csv"
  def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath
  val badRecordFilePath: File =new File(currentPath + "/target/test/badRecords")

  override def beforeAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, LoggerAction.FORCE.name())
        badRecordFilePath.mkdirs()
    sql("drop table if exists sales")
  }

  test("test load for bad_record_action=force") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='force', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(6)))

  }

  test("test load for bad_record_action=FORCE") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='FORCE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(6)))
  }

  test("test load for bad_record_action=fail") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='fail', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')")
    }
  }

  test("test load for bad_record_action=FAIL") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='FAIL', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')")
    }
  }

  test("test load for bad_record_action=ignore") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='ignore', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test load for bad_record_action=IGNORE") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
        "('bad_records_action'='IGNORE', 'DELIMITER'=" +
        " ',', 'QUOTECHAR'= '\"')")
    checkAnswer(sql("select count(*) from sales"),
      Seq(Row(2)))
  }

  test("test bad record REDIRECT but not having location should throw exception") {
    sql("drop table if exists sales")
    sql(
      """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
    val exMessage = intercept[Exception] {
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"', 'BAD_RECORD_PATH'='')")
    }
    assert(exMessage.getMessage.contains("Invalid bad records location."))
  }

  test("test bad record REDIRECT but not having empty location in option should throw exception") {
    val badRecordLocation = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
      CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL)
    sql("drop table if exists sales")
    try {
      sql(
        """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
      val exMessage = intercept[Exception] {
        sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
            "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
            " ',', 'QUOTECHAR'= '\"')")
      }
      assert(exMessage.getMessage.contains("Invalid bad records location."))
    }
    finally {
      CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        badRecordLocation)
    }
  }

  test("test bad record is REDIRECT with location in carbon properties should pass") {
    sql("drop table if exists sales")
      sql(
        """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"')")
  }

  test("test bad record is redirect with location in option while data loading should pass") {
    sql("drop table if exists sales")
         sql(
        """CREATE TABLE IF NOT EXISTS sales(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")
      sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE sales OPTIONS" +
          "('bad_records_action'='REDIRECT', 'DELIMITER'=" +
          " ',', 'QUOTECHAR'= '\"', 'BAD_RECORD_PATH'='" + {badRecordFilePath.getCanonicalPath} +
          "')")
      checkAnswer(sql("select count(*) from sales"),
        Seq(Row(2)))
  }

  override def afterAll() = {
    sql("drop table if exists sales")
  }

}

package org.apache.carbondata.spark.testsuite.iud

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TestShowIUDRowCount extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    dropTable("iud_rows")
  }

  override protected def beforeEach(): Unit = {
    dropTable("iud_rows")
  }

  override protected def afterEach(): Unit = {
    dropTable("iud_rows")
  }

  test("Test show load row count") {
    sql("""create table iud_rows (c1 string,c2 int,c3 string,c5 string)
        |STORED BY 'org.apache.carbondata.format'""".stripMargin)
    checkAnswer(
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_rows"""),
      Seq(Row(5)))
  }

  test("Test show insert row count") {
    sql("""create table iud_rows (c1 string,c2 int,c3 string,c5 string)
          |STORED BY 'org.apache.carbondata.format'""".stripMargin)
    checkAnswer(
      sql(s"""INSERT INTO TABLE iud_rows VALUES('f',6,'ff','fff')"""),
      Seq(Row(1)))
  }

  test("Test show update row count") {
    sql("""create table iud_rows (c1 string,c2 int,c3 string,c5 string)
          |STORED BY 'org.apache.carbondata.format'""".stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_rows""")
    checkAnswer(
      sql(s"""UPDATE iud_rows SET (c1)=('k') WHERE c2 = 4"""),
      Seq(Row(1)))
    checkAnswer(sql("SELECT c1 FROM iud_rows WHERE c2 = 4"), Seq(Row("k")))
  }

  test("Test show delete row count") {
    sql("""create table iud_rows (c1 string,c2 int,c3 string,c5 string)
          |STORED BY 'org.apache.carbondata.format'""".stripMargin)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_rows""")
    checkAnswer(
      sql(s"""DELETE FROM iud_rows WHERE c2 = 4"""),
      Seq(Row(1)))
    checkAnswer(sql("SELECT count(*) FROM iud_rows"), Seq(Row(4)))
  }

  override protected def afterAll(): Unit = {
    dropTable("iud_rows")
  }
}

package org.apache.spark.carbondata.restructure.rowreader

import java.math.BigDecimal

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class ChangeDataTypeTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("drop table if exists hivetable")
  }

  test("test change datatype on existing column and load data, insert into hive table") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(
      "CREATE TABLE hivetable(intField bigint,stringField string,charField string,timestampField " +
      "timestamp,decimalField decimal(6,2)) stored as parquet")
    sql("insert into table hivetable select * from changedatatypetest")
    afterAll
  }

  test("test datatype change and filter") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select charField from changedatatypetest where intField > 99"),
      Seq(Row("abc"), Row("abc")))
    checkAnswer(sql("select charField from changedatatypetest where intField < 99"), Seq())
    checkAnswer(sql("select charField from changedatatypetest where intField = 100"),
      Seq(Row("abc"), Row("abc")))
    afterAll
  }


  test("test change int datatype and load data") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change intField intfield bigint")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select sum(intField) from changedatatypetest"), Row(200))
    afterAll
  }

  test("test change decimal datatype and compaction") {
    beforeAll
    sql(
      "CREATE TABLE changedatatypetest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    sql("Alter table changedatatypetest change decimalField decimalField decimal(9,5)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE " +
        s"changedatatypetest options('FILEHEADER'='intField,stringField,charField,timestampField," +
        s"decimalField')")
    checkAnswer(sql("select decimalField from changedatatypetest"),
      Seq(Row(new BigDecimal("21.23").setScale(5)), Row(new BigDecimal("21.23").setScale(5))))
    sql("alter table changedatatypetest compact 'major'")
    checkExistence(sql("show segments for table changedatatypetest"), true, "0Compacted")
    checkExistence(sql("show segments for table changedatatypetest"), true, "1Compacted")
    checkExistence(sql("show segments for table changedatatypetest"), true, "0.1Success")
    afterAll
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS changedatatypetest")
    sql("drop table if exists hivetable")
  }
}

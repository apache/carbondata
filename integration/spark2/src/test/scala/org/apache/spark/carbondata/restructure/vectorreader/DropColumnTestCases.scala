package org.apache.spark.carbondata.restructure.vectorreader

import java.math.{BigDecimal, RoundingMode}

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

class DropColumnTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("drop table if exists hivetable")
  }

  test("test drop column and insert into hive table") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(
      "CREATE TABLE hivetable(intField int,stringField string,timestampField timestamp," +
      "decimalField decimal(6,2)) stored as parquet")
    sql("insert into table hivetable select * from dropcolumntest")
    checkAnswer(sql("select * from hivetable"), sql("select * from dropcolumntest"))
    afterAll
  }

  test("test drop column and load data") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    checkAnswer(sql("select count(*) from dropcolumntest"), Row(2))
    afterAll
  }

  test("test drop column and compaction") {
    beforeAll
    sql(
      "CREATE TABLE dropcolumntest(intField int,stringField string,charField string," +
      "timestampField timestamp,decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
    sql("Alter table dropcolumntest drop columns(charField)")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest" +
        s" options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
    sql("alter table dropcolumntest compact 'major'")
    checkExistence(sql("show segments for table dropcolumntest"), true, "0Compacted")
    checkExistence(sql("show segments for table dropcolumntest"), true, "1Compacted")
    checkExistence(sql("show segments for table dropcolumntest"), true, "0.1Success")
    afterAll
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("drop table if exists hivetable")
    sqlContext.setConf("carbon.enable.vector.reader", "false")
  }
}

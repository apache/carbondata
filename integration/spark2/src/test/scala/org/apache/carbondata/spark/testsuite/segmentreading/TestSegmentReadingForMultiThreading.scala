package org.apache.carbondata.spark.testsuite.segmentreading

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{CarbonUtils, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll


/**
 * Testcase for set segment in multhread env
 */
class TestSegmentReadingForMultiThreading extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_MulTI_THread")
    sql(
      "CREATE TABLE carbon_table_MulTI_THread (empno int, empname String, designation String, doj " +
      "Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data1.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data1.csv' INTO TABLE carbon_table_MulTI_THread " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')")
  }

  test("test multithreading for segment reading") {


    CarbonUtils.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1,2,3")
    val df = sql("select count(empno) from carbon_table_MulTI_THread")
    checkAnswer(df, Seq(Row(30)))

    val four = Future {
      CarbonUtils.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1,3")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(20)))
    }

    val three = Future {
      CarbonUtils.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "0,1,2")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(30)))
    }


    val one = Future {
      CarbonUtils.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "0,2")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(20)))
    }

    val two = Future {
      CarbonUtils.threadSet("carbon.input.segments.default.carbon_table_MulTI_THread", "1")
      val df = sql("select count(empno) from carbon_table_MulTI_THread")
      checkAnswer(df, Seq(Row(10)))
    }
    Await.result(Future.sequence(Seq(one, two, three, four)), Duration(300, TimeUnit.SECONDS))
  }

  override def afterAll: Unit = {
    sql("DROP TABLE IF EXISTS carbon_table_MulTI_THread")
    CarbonUtils.threadUnset("carbon.input.segments.default.carbon_table_MulTI_THread")
  }
}

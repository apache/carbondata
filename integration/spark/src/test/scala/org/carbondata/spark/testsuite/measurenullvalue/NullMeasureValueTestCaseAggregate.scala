package org.carbondata.spark.testsuite.measurenullvalue

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class NullMeasureValueTestCaseAggregate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE IF NOT EXISTS t3 (ID Int, date Timestamp, country String, name String, phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/nullmeasurevalue.csv' into table t3");
  }

  test("select count(salary) from t3") {
    checkAnswer(
      sql("select count(salary) from t3"),
      Seq(Row(0)))
  }
  test("select count(ditinct salary) from t3") {
    checkAnswer(
      sql("select count(distinct salary) from t3"),
      Seq(Row(0)))
  }
  
  test("select sum(salary) from t3") {
    checkAnswer(
      sql("select sum(salary) from t3"),
      Seq(Row(null)))
  }
  test("select avg(salary) from t3") {
    checkAnswer(
      sql("select avg(salary) from t3"),
      Seq(Row(null)))
  }
  
   test("select max(salary) from t3") {
    checkAnswer(
      sql("select max(salary) from t3"),
      Seq(Row(null)))
   }
   test("select min(salary) from t3") {
    checkAnswer(
      sql("select min(salary) from t3"),
      Seq(Row(null)))
   }
   test("select sum(distinct salary) from t3") {
    checkAnswer(
      sql("select sum(distinct salary) from t3"),
      Seq(Row(null)))
   }
   
  override def afterAll {
    sql("drop cube t3")
  }
}
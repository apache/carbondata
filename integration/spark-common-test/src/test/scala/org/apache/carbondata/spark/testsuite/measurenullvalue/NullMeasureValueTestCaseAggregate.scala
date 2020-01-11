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
package org.apache.carbondata.spark.testsuite.measurenullvalue

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

class NullMeasureValueTestCaseAggregate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table IF EXISTS t3")
    sql(
      "CREATE TABLE IF NOT EXISTS t3 (ID Int, date Timestamp, country String, name String, " +
        "phonetype String, serialname String, salary Int) STORED AS carbondata"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurevalue.csv' into table t3");
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
    sql("drop table t3")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}

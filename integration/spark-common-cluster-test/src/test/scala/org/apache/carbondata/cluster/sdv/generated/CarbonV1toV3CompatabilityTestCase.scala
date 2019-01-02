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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.{CarbonEnv, CarbonSession, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * V1 to V3 compatability test. This test has to be at last
 */
class CarbonV1toV3CompatabilityTestCase extends QueryTest with BeforeAndAfterAll {

  var localspark: CarbonSession = null
  val storeLocation = s"${
    TestQueryExecutor
      .integrationPath
  }/spark-common-test/src/test/resources/Data/v1_version/store"
  val metaLocation = s"${
    TestQueryExecutor
      .integrationPath
  }/spark-common-test/src/test/resources/Data/v1_version"

  override def beforeAll {
    sqlContext.sparkSession.stop()
    CarbonEnv.carbonEnvMap.clear()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
    import org.apache.spark.sql.CarbonSession._
    println(s"store path for CarbonV1toV3CompatabilityTestCase is $storeLocation and metastore is" +
            s" $metaLocation")
    localspark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonV1toV3CompatabilityTestCase")
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metaLocation).asInstanceOf[CarbonSession]
    println("store path : " + CarbonProperties.getStorePath)
    localspark.sparkContext.setLogLevel("WARN")
    hiveClient.runSqlHive(
      s"ALTER TABLE default.t3 SET SERDEPROPERTIES" +
      s"('tablePath'='$storeLocation/default/t3', 'dbname'='default', 'tablename'='t3')")
    localspark.sql("show tables").show()
  }

  test("test v1 to v3 compatabilty reading all with out fail") {
    val length = localspark.sql("select ID,date,country,name,phonetype,serialname,salary from t3")
      .collect().length
    assert(length == 1000)
  }

  test("test v1 to v3 compatabilty groupy by query") {
    val dataFrame = localspark
      .sql(s"SELECT country, count(salary) AS amount FROM t3 WHERE country IN ('china','france') " +
           s"GROUP BY country")
    checkAnswer(dataFrame, Seq(Row("china", 849), Row("france", 101)))
  }

  test("test v1 to v3 compatabilty filter on measure with int measure") {
    val dataFrame = localspark
      .sql(s"SELECT sum(salary) FROM t3 where salary > 15408")
    checkAnswer(dataFrame, Seq(Row(9281064)))
  }

  test("test v1 to v3 compatabilty filter on measure with decimal dimension") {
    val dataFrame = localspark
      .sql(s"SELECT sum(salary2) FROM t3 where salary2 > 15408")
    checkAnswer(dataFrame, Seq(Row(9281064)))
  }

  test("test v1 to v3 compatabilty filter on measure with double dimension") {
    val dataFrame = localspark
      .sql(s"SELECT sum(salary1) FROM t3 where salary1 > 15408")
    checkAnswer(dataFrame, Seq(Row(9281064)))
  }

  override def afterAll {
    localspark.stop()
  }
}

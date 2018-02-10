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

package org.apache.carbondata.spark.testsuite.iud

import java.text.SimpleDateFormat

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestUpdateAndDeleteWithLargeData extends QueryTest with BeforeAndAfterAll {
  var df: DataFrame = _

  override def beforeAll {
    dropTable()
    buildTestData()
  }

  private def buildTestData(): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Simulate data and write to table orders
    import sqlContext.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    df = sqlContext.sparkSession.sparkContext.parallelize(1 to 1500000)
      .map(value => (value, new java.sql.Date(sdf.parse("2015-07-" + (value % 10 + 10)).getTime),
        "china", "aaa" + value, "phone" + 555 * value, "ASD" + (60000 + value), 14999 + value,
        "ordersTable" + value))
      .toDF("o_id", "o_date", "o_country", "o_name",
        "o_phonetype", "o_serialname", "o_salary", "o_comment")
    createTable()

  }

  private def createTable(): Unit = {
    df.write
      .format("carbondata")
      .option("tableName", "orders")
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def dropTable() = {
    sql("DROP TABLE IF EXISTS orders")

  }

  test("test the update and delete delete functionality for large data") {

    sql(
      """
            update ORDERS set (o_comment) = ('yyy')""").show()
    checkAnswer(sql(
      """select o_comment from orders limit 2 """), Seq(Row("yyy"), Row("yyy")))

    sql("delete from orders where exists (select 1 from orders)")

    checkAnswer(sql(
      """
           SELECT count(*) FROM orders
           """), Row(0))
  }

}

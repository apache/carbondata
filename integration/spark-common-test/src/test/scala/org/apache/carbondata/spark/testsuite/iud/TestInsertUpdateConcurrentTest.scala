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

import scala.collection.JavaConverters._

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestInsertUpdateConcurrentTest extends QueryTest with BeforeAndAfterAll {
  var df: DataFrame = _
  private val executorService: ExecutorService = Executors.newFixedThreadPool(10)

  override def beforeAll {
    dropTable()
    buildTestData()
  }

  override def afterAll {
    executorService.shutdownNow()
    dropTable()
  }


  private def buildTestData(): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

    // Simulate data and write to table orders
    import sqlContext.implicits._

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    df = sqlContext.sparkSession.sparkContext.parallelize(1 to 150000)
      .map(value => (value, new java.sql.Date(sdf.parse("2015-07-" + (value % 10 + 10)).getTime),
        "china", "aaa" + value, "phone" + 555 * value, "ASD" + (60000 + value), 14999 + value,"ordersTable"+value))
      .toDF("o_id", "o_date", "o_country", "o_name",
        "o_phonetype", "o_serialname", "o_salary","o_comment")
      createTable("orders")
      createTable("orders_overwrite")
  }

 private def dropTable() = {
    sql("DROP TABLE IF EXISTS orders")
    sql("DROP TABLE IF EXISTS orders_overwrite")
  }

  private def createTable(tableName: String): Unit ={
    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("tempCSV", "true")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()
  }

  test("Concurrency test for Insert-Overwrite and update") {
    val tasks = new java.util.ArrayList[Callable[String]]()
    tasks.add(new QueryTask(s"insert overWrite table orders select * from orders_overwrite"))
    tasks.add(new QueryTask("update orders set (o_country)=('newCountry') where o_country='china'"))
    val futures: util.List[Future[String]] = executorService.invokeAll(tasks)
    val results = futures.asScala.map(_.get)
    assert("PASS".equals(results(0)) && "FAIL".equals(results(1)))
  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        LOGGER.info("Executing :" + query + Thread.currentThread().getName)
        sql(query).show()
      } catch {
        case _: Exception =>
          result = "FAIL"
      }
      result
    }
  }

}

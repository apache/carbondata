/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.dataretention

import java.io.File
import java.util
import java.util.concurrent.{Callable, Executors}

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * This class contains DataRetention concurrency test cases
 */
class DataRetentionConcurrencyTestCase extends QueryTest with BeforeAndAfterAll {

  val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
    .getCanonicalPath
  val resource = currentDirectory + "/src/test/resources/"
  val executorService = Executors.newFixedThreadPool(10)

  override def beforeAll {

    sql("drop table if exists concurrent")
    sql(
      "create table concurrent (ID int, date String, country String, name " +
      "String," +
      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"

    )
    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE concurrent " +
      "OPTIONS('DELIMITER' =  ',')")
  }

  override def afterAll {
    executorService.shutdownNow()
    sql("drop table if exists concurrent")
  }

  test("DataRetention_Concurrency_load_id") {

    val tasks = new util.ArrayList[Callable[String]]()
    tasks
      .add(new QueryTask("LOAD DATA LOCAL INPATH '" + resource +
                         "dataretention1.csv' INTO TABLE concurrent OPTIONS('DELIMITER' =  ',')"))
    tasks.add(new QueryTask("Delete segment 0 from table concurrent"))
    tasks.add(new QueryTask("clean files for table concurrent"))
    val results = executorService.invokeAll(tasks)
    for (i <- 0 until tasks.size()) {
      val res = results.get(i).get
      assert("PASS".equals(res))
    }
    sql("show segments for table concurrent").show()

  }

  test("DataRetention_Concurrency_load_date") {

    sql(
      "LOAD DATA LOCAL INPATH '" + resource + "dataretention1.csv' INTO TABLE concurrent " +
      "OPTIONS('DELIMITER' =  ',')")

    val tasks = new util.ArrayList[Callable[String]]()
    tasks
      .add(new QueryTask("LOAD DATA LOCAL INPATH '" + resource +
                         "dataretention1.csv' INTO TABLE concurrent OPTIONS('DELIMITER' =  ',')"))
    tasks
      .add(new QueryTask(
        "DELETE SEGMENTS FROM TABLE concurrent where STARTTIME before '2099-01-01 00:00:00'"))
    tasks.add(new QueryTask("clean files for table concurrent"))
    val results = executorService.invokeAll(tasks)
    for (i <- 0 until tasks.size()) {
      val res = results.get(i).get
      assert("PASS".equals(res))
    }
    sql("show segments for table concurrent").show()

  }

  class QueryTask(query: String) extends Callable[String] {
    override def call(): String = {
      var result = "PASS"
      try {
        LOGGER.info("Executing :" + Thread.currentThread().getName)
        sql(query)
      } catch {
        case ex: Exception =>
          result = "FAIL"
      }
      result
    }
  }

}

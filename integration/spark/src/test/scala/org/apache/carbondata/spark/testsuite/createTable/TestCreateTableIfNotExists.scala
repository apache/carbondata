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

package org.apache.carbondata.spark.testsuite.createTable

import java.util.concurrent.{Callable, Executors, ExecutorService, Future, TimeUnit}

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.AnalysisException
import org.scalatest.BeforeAndAfterAll

class TestCreateTableIfNotExists extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop table if exists test")
    sql("drop table if exists sourceTable")
    sql("drop table if exists targetTable")
  }

  test("test create table if not exists") {
    sql("create table test(a int, b string) STORED AS carbondata")
    try {
      // table creation should be successful
      sql("create table if not exists test(a int, b string) STORED AS carbondata")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  test("test create table if not exist concurrently") {

    val executorService: ExecutorService = Executors.newFixedThreadPool(10)
    var futures: List[Future[_]] = List()
    for (i <- 0 until (3)) {
      futures = futures :+ runAsync()
    }

    executorService.shutdown();
    executorService.awaitTermination(30L, TimeUnit.SECONDS)

    futures.foreach { future =>
      assertResult("PASS")(future.get.toString)
    }

    def runAsync(): Future[String] = {
      executorService.submit(new Callable[String] {
        override def call() = {
          // Create table
          var result = "PASS"
          try {
            sql("create table IF NOT EXISTS TestIfExists(name string) STORED AS carbondata")
          } catch {
            case exception: Exception =>
              result = exception.getMessage
              exception.printStackTrace()
          }
          result
        }
      })
    }
  }

  test("test create table without column specified") {
    val exception = intercept[AnalysisException] {
      sql("create table TableWithoutColumn STORED AS carbondata tblproperties('sort_columns'='')")
    }
    assert(exception.getMessage.contains("Unable to infer the schema"))
  }

  override def afterAll {
    sql("use default")
    sql("drop table if exists test")
    sql("drop table if exists sourceTable")
    sql("drop table if exists targetTable")
    sql("drop table if exists TestIfExists")
  }

}

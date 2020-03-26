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

package org.apache.spark.carbondata.query

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test ReusedExchange on carbon table
 */
class ReusedExchangeTestSuite extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    sql("drop table if exists t_reused_exchange")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists t_reused_exchange")
  }

  test("test ReusedExchange") {
    sql("create table t_reused_exchange(c1 int, c2 string) using carbondata")
    checkExistence(
      sql(
        """
          | explain
          | select c2, sum(c1) from t_reused_exchange group by c2
          | union all
          | select c2, sum(c1) from t_reused_exchange group by c2
          |""".stripMargin), true, "ReusedExchange")
  }
}

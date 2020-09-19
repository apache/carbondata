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

package org.apache.spark.sql

import org.apache.spark.sql.execution.strategy.DDLStrategy
import org.apache.spark.sql.parser.CarbonExtensionSqlParser
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CarbonExtensionSuite extends QueryTest with BeforeAndAfterAll {

  var session: SparkSession = null

  val sparkCommands = Array("select 2 > 1")

  val carbonCommands = Array("show STREAMS")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    session = sqlContext.sparkSession
  }

  test("test parser injection") {
    assert(session.sessionState.sqlParser.isInstanceOf[CarbonExtensionSqlParser])
    (carbonCommands ++ sparkCommands) foreach (command =>
      session.sql(command).show)
  }

  test("test strategy injection") {
    assert(session.sessionState.planner.strategies.filter(_.isInstanceOf[DDLStrategy]).length == 1)
    session.sql("create table if not exists table1 (column1 String) using carbondata ").show
  }
}

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
package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class InsertIntoCarbonTableSpark2TestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists OneRowTable")
  }

  test("insert select one row") {
    sql("create table OneRowTable(col1 string, col2 string, col3 int, col4 double) STORED AS carbondata")
    sql("insert into OneRowTable select '0.1', 'a.b', 1, 1.2")
    checkAnswer(sql("select * from OneRowTable"), Seq(Row("0.1", "a.b", 1, 1.2)))
  }

  override def afterAll {
    sql("drop table if exists OneRowTable")
  }
}

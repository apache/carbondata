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
package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

@Ignore
class TestPreAggregateMisc extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
  }
  test("test PreAggregate With Set Segments property") {
    sql("create datamap agg1 on table mainTable using 'preaggregate' as select name,sum(age) from mainTable group by name")
    sql("SET carbon.input.segments.default.mainTable=0")
    checkAnswer(
      sql("select sum(age) from mainTable"),
      Seq(Row(183.0)))
    sql("RESET")
    sql("drop datamap agg1 on table mainTable")

  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
  }
}

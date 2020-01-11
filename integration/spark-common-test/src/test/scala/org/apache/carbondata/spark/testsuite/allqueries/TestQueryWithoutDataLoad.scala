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

/*
 * Test Class for query without data load
 *
 */
class TestQueryWithoutDataLoad extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists no_load")
    sql("""
        CREATE TABLE no_load(imei string, age int, productdate timestamp, gamePointId double)
        STORED AS carbondata
      """)
  }

  test("test query without data load") {
    checkAnswer(
      sql("select count(*) from no_load"), Seq(Row(0))
    )
    checkAnswer(
      sql("select * from no_load"), Seq.empty
    )
    checkAnswer(
      sql("select imei, count(age) from no_load group by imei"), Seq.empty
    )
    checkAnswer(
      sql("select imei, sum(age) from no_load group by imei"), Seq.empty
    )
    checkAnswer(
      sql("select imei, avg(age) from no_load group by imei"), Seq.empty
    )
  }

  override def afterAll {
    sql("drop table no_load")
  }

}

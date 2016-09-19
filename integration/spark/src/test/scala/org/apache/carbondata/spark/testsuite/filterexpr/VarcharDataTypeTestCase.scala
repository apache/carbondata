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

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for varchar data type
  *
  */
class VarcharDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists varchar_test")
    sql("""
        CREATE TABLE varchar_test (empno int, designation varchar(10), deptno int, projectcode int,attendance int)
        STORED BY 'org.apache.carbondata.format'
      """)
    sql("""
        LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE
        varchar_test
      """)
    sql("desc varchar_test").show
  }

  test("select designation from varchar_test") {
    checkAnswer(
      sql("select designation from varchar_test"),
      Seq(Row("SE"), Row("SSE"), Row("TPL"), Row("SA"), Row("SSA"),
        Row("SE"), Row("PL"), Row("TL"), Row("PL"), Row("PM"))
    )
  }

  override def afterAll {
    sql("drop table if exists varchar_test")
  }
}

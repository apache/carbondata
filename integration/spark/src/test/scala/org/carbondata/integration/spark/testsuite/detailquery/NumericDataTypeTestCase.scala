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

package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for detailed query on Numeric datatypes
 * @author N00902756
 *
 */
class NumericDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE CUBE doubletype DIMENSIONS (utilization Numeric,salary Numeric) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE doubletype PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  }

  test("select utilization from doubletype") {
    checkAnswer(
      sql("select utilization from doubletype"),
      Seq(Row(96.2), Row(95.1), Row(99.0), Row(92.2), Row(91.5),
        Row(93.0), Row(97.45), Row(98.23), Row(91.678), Row(94.22)))
  }

  override def afterAll {
    sql("drop cube doubletype")
  }
}
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

package org.carbondata.integration.spark.testsuite.filterexpr

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter expression query on timestamp datatypes
 * @author N00902756
 *
 */
class TimestampDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE CUBE timestamptypecube DIMENSIONS (doj Timestamp, projectjoindate Timestamp, projectenddate Timestamp) OPTIONS (PARTITIONER [PARTITION_COUNT=1])");
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE timestamptypecube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }

  test("select doj from timestamptypecube") {
    checkAnswer(
      sql("select doj from timestamptypecube"),
      Seq(Row(Timestamp.valueOf("2007-01-17 00:00:00.0")),
        Row(Timestamp.valueOf("2008-05-29 00:00:00.0")),
        Row(Timestamp.valueOf("2009-07-07 00:00:00.0")),
        Row(Timestamp.valueOf("2010-12-29 00:00:00.0")),
        Row(Timestamp.valueOf("2011-11-09 00:00:00.0")),
        Row(Timestamp.valueOf("2012-10-14 00:00:00.0")),
        Row(Timestamp.valueOf("2013-09-22 00:00:00.0")),
        Row(Timestamp.valueOf("2014-08-15 00:00:00.0")),
        Row(Timestamp.valueOf("2015-05-12 00:00:00.0")),
        Row(Timestamp.valueOf("2015-12-01 00:00:00.0"))))
  }

  override def afterAll {
    sql("drop cube timestamptypecube")
  }
}
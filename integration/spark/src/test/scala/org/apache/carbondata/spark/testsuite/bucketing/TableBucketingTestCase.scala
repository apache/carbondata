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

package org.apache.carbondata.spark.testsuite.bucketing

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TableBucketingTestCase extends QueryTest with BeforeAndAfterAll{
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)
  }

  test("test create table with cluster by on measure column") {
    try {
      sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           CLUSTERED BY(ID) INTO 4 BUCKETS
           STORED BY 'carbondata'
          """)
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.equals("Bucket field must be dimension column and " +
                                   "should not be measure or complex column: id"))
    }
  }

  test("test create table with clsuter by on dimension column") {
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           CLUSTERED BY(date) INTO 4 BUCKETS
           STORED BY 'carbondata'
        """)
    sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/dataDiff.csv' into table t3
           """)
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}

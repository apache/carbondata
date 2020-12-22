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

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test functionality of create table with location
 */
class TestCreateTablePath extends QueryTest with BeforeAndAfterAll {

  test("test create table path with location") {
    val tablePath1 = (CarbonEnv.createTablePath(Some("default"), "table_with_location", "1",
      Some("/tmp/table_with_location"), isExternal = false,
      isTransactionalTable = true)(sqlContext.sparkSession))
    assert(tablePath1.startsWith("file:/"))

    sqlContext.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost")
    val tablePath2 = CarbonEnv.createTablePath(Some("default"), "table_with_location", "1",
      Some("/tmp/table_with_location"), isExternal = false,
      isTransactionalTable = true)(sqlContext.sparkSession)
    assert(tablePath2.startsWith("hdfs://localhost/"))
    sqlContext.sparkContext.hadoopConfiguration.set("fs.defaultFS", "file:///")
  }

}

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

package org.apache.carbondata.spark.testsuite.partition

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest. BeforeAndAfterAll

class TestUpdateForPartitionTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    dropTable

    sql("create table test_hive_partition_table (id int) partitioned by (name string) " +
      "STORED AS carbondata")
  }

  def dropTable = {
    sql("drop table if exists test_hive_partition_table")
  }

  test ("test update for hive(standard) partition table") {

    sql("insert into test_hive_partition_table select 1,'b' ")
    sql("update test_hive_partition_table set (name) = ('c') where id = 1").collect()
    assertResult(1)(sql("select * from test_hive_partition_table where name = 'c'").collect().length)
  }

  override def afterAll() : Unit = {
    dropTable
  }
}

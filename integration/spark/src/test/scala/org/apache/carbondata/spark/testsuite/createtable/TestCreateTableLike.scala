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

package org.apache.carbondata.spark.testsuite.createtable

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateTableLike extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists table_hive")
    sql("drop table if exists table_hive_new")
    sql("drop table if exists table_carbon")
    sql("drop table if exists table_carbon_new")
  }

  test("test hive table can be created with like table syntax") {
    try {
      sql(
        """
           CREATE TABLE table_hive(name string, age int)
        """)
      sql(
        """
           CREATE TABLE table_hive_new like table_hive
        """)
    } catch {
      case _ => assert(false)
    }
  }

  test("test when carbon table created using like table should throw error and not create it") {
    sql(
      """
          CREATE TABLE IF NOT EXISTS table_carbon
          (id Int, name String, city String)
          STORED BY 'org.apache.carbondata.format'
      """)
    try {
      sql(
        """
           CREATE TABLE table_carbon_new LIKE table_carbon
        """)
      assert(false)
    } catch {
      case e : MalformedCarbonCommandException => {
        assert(e.getMessage.equals(
          "Carbon does not support CREATE TABLE LIKE syntax"))
      }
    }
  }

  override def afterAll {
    sql("drop table if exists table_hive")
    sql("drop table if exists table_hive_new")
    sql("drop table if exists table_carbon")
    sql("drop table if exists table_carbon_new")
  }
}

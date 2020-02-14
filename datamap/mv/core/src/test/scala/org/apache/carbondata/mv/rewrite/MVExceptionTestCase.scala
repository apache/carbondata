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
package org.apache.carbondata.mv.rewrite

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException, MalformedMaterializedViewException}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVExceptionTestCase  extends QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    drop()
    sql("create table main_table (name string,age int,height int) STORED AS carbondata")
  }

  test("test mv no base table") {
    val ex = intercept[NoSuchTableException] {
      sql("create datamap main_table_mv on table main_table_error using 'mv' as select sum(age),name from main_table_error group by name")
    }
    assertResult("Table or view 'main_table_error' not found in database 'default';")(ex.getMessage())
  }

  test("test mv reduplicate mv table") {
    val ex = intercept[MalformedMaterializedViewException] {
      sql("create materialized view main_table_mv1 as select sum(age),name from main_table group by name")
      sql("create materialized view main_table_mv1 as select sum(age),name from main_table group by name")
    }
    assertResult("Materialized view with name main_table_mv1 already exists")(ex.getMessage)
  }

  test("test mv creation with limit in query") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql("create materialized view maintable_mv2 as select sum(age),name from main_table group by name limit 10")
    }
    assertResult("Materialized view does not support the query with limit")(ex.getMessage)
  }

  def drop(): Unit = {
    sql("drop table IF EXISTS main_table")
    sql("drop table if exists main_table_error")
    sql("drop materialized view if exists main_table_mv")
    sql("drop materialized view if exists main_table_mv1")
  }

  override def afterAll(): Unit = {
    drop()
  }
}

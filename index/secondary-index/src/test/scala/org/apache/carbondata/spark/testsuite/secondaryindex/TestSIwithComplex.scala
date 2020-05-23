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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

class TestSIwithComplex extends QueryTest with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    sql("drop table if exists complextable")
  }

  test("test array<string> on secondary index") {
    sql("create table complextable (country array<string>, name string) stored as carbondata")
    sql("insert into complextable select array('china', 'us'), 'b'")
    sql("insert into complextable select array('pak'), 'v'")
    sql("insert into complextable select array('china'), 'f'")
    sql("insert into complextable select array('india'),'g'")
    val result1 = sql(" select * from complextable where array_contains(country,'china')")
    val result2 = sql(" select * from complextable where country[0]='china'")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country) as 'carbondata'")
    val df1 = sql(" select * from complextable where array_contains(country,'china')")
    val df2 = sql(" select * from complextable where country[0]='china'")
    if (!isFilterPushedDownToSI(df1.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    if (!isFilterPushedDownToSI(df2.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
   val doNotHitSIDf = sql(" select * from complextable where array_contains(country,'china') and array_contains(country,'us')")
    if (isFilterPushedDownToSI(doNotHitSIDf.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result1, df1)
    checkAnswer(result2, df2)
  }

  test("test array<string> and string as index columns on secondary index") {
    sql("create table complextable (id int, country array<string>, name string) stored as carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b'")
    sql("insert into complextable select 2, array('pak'), 'v'")
    sql("insert into complextable select 3, array('china'), 'f'")
    sql("insert into complextable select 4, array('india'),'g'")
    val result =  sql(" select * from complextable where array_contains(country,'china') and name='f'")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country, name) as 'carbondata'")
    val df =  sql(" select * from complextable where array_contains(country,'china') and name='f'")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test load data with array<string> on secondary index") {
    sql("create table complextable (id int, name string, country array<string>) stored as carbondata")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable options('delimiter'=','," +
      "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$')")
    val result =  sql(" select * from complextable where array_contains(country,'china')")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country) as 'carbondata'")
    val df =  sql(" select * from complextable where array_contains(country,'china')")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test si creation with struct and map type") {
    sql("create table complextable (country struct<b:string>, name string, id Map<string, string>, arr1 array<string>, arr2 array<string>) stored as carbondata")
    intercept[RuntimeException] {
      sql("create index index_1 on table complextable(country) as 'carbondata'")
    }
    intercept[RuntimeException] {
      sql("create index index_1 on table complextable(id) as 'carbondata'")
    }
    intercept[RuntimeException] {
      sql("create index index_1 on table complextable(arr1, arr2) as 'carbondata'")
    }
  }

  test("test complex with null and empty data") {
    sql("create table complextable (id string, country array<string>, name string) stored as carbondata")
    sql("insert into complextable select 'a', array(), ''")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country) as 'carbondata'")
    checkAnswer(sql("select count(*) from index_1"), Seq(Row(1)) )
    sql("insert into complextable select 'a', array(null), 'b'")
    checkAnswer(sql("select count(*) from index_1"), Seq(Row(2)) )
  }
}

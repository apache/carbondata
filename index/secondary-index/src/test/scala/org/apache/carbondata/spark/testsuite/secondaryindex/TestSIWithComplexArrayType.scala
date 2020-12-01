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

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonCommonConstantsInternal}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.testsuite.secondaryindex.TestSecondaryIndexUtils.isFilterPushedDownToSI

class TestSIWithComplexArrayType extends QueryTest with BeforeAndAfterEach {
  // scalastyle:off lineLength
  override def beforeEach(): Unit = {
    sql("drop table if exists complextable")
    sql("drop table if exists complextable2")
    sql("drop table if exists complextable3")
    sql("drop table if exists complextable4")
    sql("drop table if exists complextable5")
  }

  override def afterEach(): Unit = {
    sql("drop index if exists index_1 on complextable")
    sql("drop table if exists complextable")
    sql("drop table if exists complextable2")
    sql("drop table if exists complextable3")
    sql("drop table if exists complextable4")
    sql("drop table if exists complextable5")
  }

  test("test array<string> on secondary index") {
    sql("create table complextable (id string, country array<string>, name string) stored as carbondata")
    sql("insert into complextable select 1,array('china', 'us'), 'b'")
    sql("insert into complextable select 2,array('pak'), 'v'")
    sql("insert into complextable select 3,array('china'), 'f'")
    sql("insert into complextable select 4,array('india'),'g'")
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
    sql("create table complextable (id string, country array<string>, name string) stored as carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b'")
    sql("insert into complextable select 2, array('pak'), 'v'")
    sql("insert into complextable select 3, array('china'), 'f'")
    sql("insert into complextable select 4, array('india'),'g'")
    val result = sql(" select * from complextable where array_contains(country,'china') and name='f'")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country, name) as 'carbondata'")
    val df = sql(" select * from complextable where array_contains(country,'china') and name='f'")
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
    val result = sql(" select * from complextable where array_contains(country,'china')")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country) as 'carbondata'")
    val df = sql(" select * from complextable where array_contains(country,'china')")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test SI global sort with si segment merge enabled for complex data types") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("create table complextable2 (id int, name string, country array<string>) stored as " +
      "carbondata tblproperties('sort_scope'='global_sort','sort_columns'='name')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable2 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    val result = sql(" select * from complextable2 where array_contains(country,'china')")
    sql("create index index_2 on table complextable2(country) as 'carbondata' properties" +
      "('sort_scope'='global_sort')")
    checkAnswer(sql("select count(*) from complextable2 where array_contains(country,'china')"),
      sql("select count(*) from complextable2 where ni(array_contains(country,'china'))"))
    val df = sql(" select * from complextable2 where array_contains(country,'china')")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
  }

  test("test SI global sort with si segment merge enabled for primitive data types") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("create table complextable3 (id int, name string, country array<string>) stored as " +
      "carbondata tblproperties('sort_scope'='global_sort','sort_columns'='name')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable3 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable3 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    val result = sql(" select * from complextable3 where name='abc'")
    sql("create index index_3 on table complextable3(name) as 'carbondata' properties" +
      "('sort_scope'='global_sort')")
    checkAnswer(sql("select count(*) from complextable3 where name='abc'"),
      sql("select count(*) from complextable3 where ni(name='abc')"))
    val df = sql(" select * from complextable3 where name='abc'")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
  }

  test("test SI global sort with si segment merge complex data types by rebuild command") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("create table complextable4 (id int, name string, country array<string>) stored as " +
      "carbondata tblproperties('sort_scope'='global_sort','sort_columns'='name')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable4 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    val result = sql(" select * from complextable4 where array_contains(country,'china')")
    sql("create index index_4 on table complextable4(country) as 'carbondata' properties" +
      "('sort_scope'='global_sort')")
    checkAnswer(sql("select count(*) from complextable4 where array_contains(country,'china')"),
      sql("select count(*) from complextable4 where ni(array_contains(country,'china'))"))
    sql("REFRESH INDEX index_4 ON TABLE complextable4")
    checkAnswer(sql("select count(*) from complextable4 where array_contains(country,'china')"),
      sql("select count(*) from complextable4 where ni(array_contains(country,'china'))"))
    val df = sql(" select * from complextable4 where array_contains(country,'china')")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test SI global sort with si segment merge primitive data types by rebuild command") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("create table complextable5 (id int, name string, country array<string>) stored as " +
      "carbondata tblproperties('sort_scope'='global_sort','sort_columns'='name')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable5 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    sql(
      s"load data inpath '$resourcesPath/secindex/array.csv' into table complextable5 options('delimiter'=','," +
        "'quotechar'='\"','fileheader'='id,name,country','complex_delimiter_level_1'='$'," +
        "'global_sort_partitions'='10')")
    val result = sql(" select * from complextable5 where name='abc'")
    sql("create index index_5 on table complextable5(name) as 'carbondata' properties" +
      "('sort_scope'='global_sort')")
    checkAnswer(sql("select count(*) from complextable5 where name='abc'"),
      sql("select count(*) from complextable5 where ni(name='abc')"))
    sql("REFRESH INDEX index_5 ON TABLE complextable5")
    checkAnswer(sql("select count(*) from complextable5 where name='abc'"),
      sql("select count(*) from complextable5 where ni(name='abc')"))
    val df = sql(" select * from complextable5 where name='abc'")
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

  test("test si creation with array") {
    sql("create table complextable (id int, name string, country array<array<string>>, add array<int>) stored as carbondata")
    sql("drop index if exists index_1 on complextable")
    intercept[RuntimeException] {
      sql("create index index_1 on table complextable(country) as 'carbondata'")
    }.getMessage.contains("SI creation with nested array complex type is not supported yet")
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

  test("test array<date> on secondary index") {
    sql("drop table if exists complextable")
    sql("create table complextable (name string, time date, projectdate array<date>) stored as carbondata")
    sql("drop index if exists index_1 on complextable")
    sql("insert into complextable select 'b', '2017-02-01',array('2017-02-01','2018-02-01')")
    val result = sql(" select * from complextable where array_contains(projectdate,cast('2017-02-01' as date))")
    sql("create index index_1 on table complextable(projectdate) as 'carbondata'")
    checkAnswer(sql("select count(*) from index_1"), Seq(Row(2)))
    val df = sql(" select * from complextable where array_contains(projectdate,cast('2017-02-01' as date))")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test array<timestamp> on secondary index") {
    sql("drop table if exists complextable")
    sql("create table complextable (name string, time date, projectdate array<timestamp>) stored as carbondata")
    sql("drop index if exists index_1 on complextable")
    sql("insert into complextable select 'b', '2017-02-01',array('2017-02-01 00:01:00','2018-02-01 02:00:00')")
    val result = sql(" select * from complextable where array_contains(projectdate,cast('2017-02-01 00:01:00' as timestamp))")
    sql("create index index_1 on table complextable(projectdate) as 'carbondata'")
    checkAnswer(sql("select count(*) from index_1"), Seq(Row(2)))
    val df = sql(" select * from complextable where array_contains(projectdate,cast('2017-02-01 00:01:00' as timestamp))")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test array<varchar> and varchar as index columns on secondary index") {
    sql("create table complextable (id string, country array<varchar(10)>, name string) stored as carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b'")
    sql("insert into complextable select 2, array('pak'), 'v'")
    sql("insert into complextable select 3, array('china'), 'f'")
    sql("insert into complextable select 4, array('india'),'g'")
    val result = sql(" select * from complextable where array_contains(country,'china') and name='f'")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country, name) as 'carbondata'")
    val df = sql(" select * from complextable where array_contains(country,'china') and name='f'")
    if (!isFilterPushedDownToSI(df.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result, df)
  }

  test("test multiple SI with array and primitive type") {
    sql("create table complextable (id string, country array<varchar(10)>, name string, addr string) stored as carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b', 'b1'")
    sql("insert into complextable select 2, array('pak', 'india'), 'v', 'v'")
    val result1 = sql("select * from complextable where addr='v' and array_contains(country,'pak')")
    val result2 = sql("select * from complextable where array_contains(country,'pak') and addr='v'")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country, name) as 'carbondata'")
    sql("drop index if exists index_2 on complextable")
    sql("create index index_2 on table complextable(addr) as 'carbondata'")
    val df1 = sql("select * from complextable where addr='v' and array_contains(country,'pak')")
    val df2 = sql("select * from complextable where array_contains(country,'pak') and addr='v'")
    if (!isFilterPushedDownToSI(df1.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result1, df1)
    if (!isFilterPushedDownToSI(df2.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result2, df2)
  }

  test("test SI complex with multiple array contains") {
    sql("create table complextable (id string, country array<varchar(10)>, name string, addr string) stored as carbondata")
    sql("insert into complextable select 1, array('china', 'us'), 'b', 'b1'")
    sql("insert into complextable select 2, array('pak', 'india'), 'v', 'v'")
    val result1 = sql("select * from complextable where array_contains(country,'india') and array_contains(country,'pak')")
    sql("drop index if exists index_1 on complextable")
    sql("create index index_1 on table complextable(country, name) as 'carbondata'")
    val df1 = sql("select * from complextable where array_contains(country,'india') and array_contains(country,'pak')")
    if (isFilterPushedDownToSI(df1.queryExecution.sparkPlan)) {
      assert(false)
    } else {
      assert(true)
    }
    checkAnswer(result1, df1)
  }
  // scalastyle:on lineLength
}

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

package org.apache.spark.carbondata.query

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class SubQueryJoinTestSuite extends QueryTest with BeforeAndAfterAll {

  test("test to check if 2nd level subquery gives correct result") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")

    sql("create table t1 (s1 string) STORED AS carbondata ")
    sql("insert into t1 select 'abcd' ")
    sql("insert into t1 select 'efgh' ")
    sql("insert into t1 select 'ijkl' ")
    sql("insert into t1 select 'MNOP' ")

    sql("create table t2 (t2 string) STORED AS carbondata")
    sql("insert into t2 select 'ef' ")
    sql("insert into t2 select 'mnop' ")
    sql("insert into t2 select '4' ")

    // substring
    checkAnswer(sql(
      "select a.ch from (select substring(s1,1,2) as ch from t1) a  join t2 h on (a.ch = h.t2)"),
      Seq(Row("ef")))

    // lower
    checkAnswer(sql(
      "select a.ch from (select lower(s1) as ch from t1) a  join t2 h on (a.ch = h.t2)"),
      Seq(Row("mnop")))

    // length
    checkAnswer(sql(
      "select a.ch from (select length(s1) as ch from t1) a  join t2 h on (a.ch = h.t2)"),
      Seq(Row(4), Row(4), Row(4), Row(4)))

    sql("drop table t1")
    sql("drop table t2")
  }

  test("test join with dictionary include with udf") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql(
      "create table t1 (m_month smallint, hs_code string, country smallint, dollar_value double, " +
      "quantity double, unit smallint, b_country smallint, imex int, y_year smallint) " +
      "STORED AS carbondata tblproperties('sort_columns'='y_year,m_month,country,b_country,imex')")
    sql("create table t2(id bigint, hs string, hs_cn string, hs_en string) STORED AS carbondata ")
    checkAnswer(sql(
      "select a.hs,count(*) tb from (select substring(hs_code,1,2) as hs,count(*) v2000 from t1 " +
      "group by substring(hs_code,1,2),y_year) a left join t2 h on (a.hs=h.hs) group by a.hs"),
      Seq())
    sql("drop table if exists t1")
    sql("drop table if exists t2")
  }
}

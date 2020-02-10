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
import org.scalatest.BeforeAndAfterAll

/**
 * Test class to test CTAS on both carbon & non-carbon tables from
 * carbon table having Secondary index column
 */
class TestCTASWithSecondaryIndex extends QueryTest with BeforeAndAfterAll{

  override def beforeAll: Unit = {
    dropTables()
    sql(
      "create table carbon_table (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment varchar(20), c_comment string) STORED as carbondata")
    sql(s"""load data  inpath '${ resourcesPath }/secindex/firstunique.csv' into table carbon_table options('DELIMITER'='|','QUOTECHAR'='','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    sql(s"""load data  inpath '${ resourcesPath }/secindex/secondunique.csv' into table carbon_table options('DELIMITER'='|','QUOTECHAR'='','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    sql("drop index if exists sc_indx1 on carbon_table")
    sql("drop index if exists sc_indx2 on carbon_table")
    sql(
      "create index sc_indx1 on table carbon_table(c_phone,c_mktsegment) as 'carbondata'")
    sql("create index sc_indx2 on table carbon_table(c_phone) as 'carbondata'")
    sql("drop table if exists udfValidation")
    sql(
      "CREATE table udfValidation (empno int, empname String,designation String, doj Timestamp, workgroupcategory int, " +
      "workgroupcategoryname String, deptno string, deptname String, projectcode int,projectjoindate Timestamp, projectenddate Timestamp, attendance int,utilization int,salary int) STORED as carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE udfValidation OPTIONS('DELIMITER'=',','BAD_RECORDS_LOGGER_ENABLE'='FALSE','BAD_RECORDS_ACTION'='FORCE')")
    sql("create index ind_i1 on table udfValidation (deptname, empname) AS 'carbondata'")
    sql("create index ind_i2 on table udfValidation (designation) AS 'carbondata'")
    sql(
      "create index ind_i3 on table udfValidation (workgroupcategoryname) AS 'carbondata'")
    sql("create index ind_i4 on table udfValidation (deptno) AS 'carbondata'")
    sql("create index ind_i5 on table udfValidation (deptname) AS 'carbondata'")
  }

  def dropTables(): Unit = {
    sql("drop index if exists sc_indx1 on carbon_table")
    sql("drop index if exists sc_indx2 on carbon_table")
    sql("drop table if exists carbon_table")
    sql("drop index if exists ind_i1 on udfValidation")
    sql("drop index if exists ind_i2 on udfValidation")
    sql("drop index if exists ind_i3 on udfValidation")
    sql("drop index if exists ind_i4 on udfValidation")
    sql("drop index if exists ind_i5 on udfValidation")
    sql("drop table if exists udfValidation")
  }

  override def afterAll(): Unit = {
    dropTables()
  }

  test("test ctas with parquet table") {
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("drop table if exists parquet_table")
    sql("create table parquet_table using parquet as select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from parquet_table"), Seq(Row("25-989-741-2988")))
    sql("drop table if exists parquet_table")
    sql("create table parquet_table stored as parquet as select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from parquet_table"), Seq(Row("25-989-741-2988")))
    sql("drop table if exists parquet_table")
  }

  test("test ctas with hive table") {
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("drop table if exists hive_table")
    sql("create table hive_table row format delimited fields terminated by ',' as " +
        "select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from hive_table"), Seq(Row("25-989-741-2988")))
  }

  test("test ctas with spark table") {
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("drop table if exists spark_table")
    sql("create table spark_table as select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'").collect()
    checkAnswer(sql("select * from spark_table"), Seq(Row("25-989-741-2988")))
    // USING org.apache.spark.sql.CarbonSource
    sql("drop table if exists spark_table")
    sql("create table spark_table USING org.apache.spark.sql.CarbonSource as " +
        "select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from spark_table"), Seq(Row("25-989-741-2988")))
  }

  test("test ctas with carbon table") {
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("drop table if exists carbon_table_1")
    // spark file format
    sql("create table carbon_table_1 using carbon as select c_phone from carbon_table where c_phone='25-989-741-2988' and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from carbon_table_1"), Seq(Row("25-989-741-2988")))
    // using carbondata syntax
    sql("drop table if exists carbon_table1")
    sql("create table carbon_table1 using carbondata as select c_phone from carbon_table where c_phone='25-989-741-2988'and c_mktsegment = 'AUTOMOBILE'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2988")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having and filter") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2989' and c_mktsegment='BUILDING'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone from carbon_table where c_phone='25-989-741-2989' and c_mktsegment='BUILDING'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having or filter") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select c_phone from carbon_table where c_phone='25-989-741-2989' OR c_mktsegment='BUILDING'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone from carbon_table where c_phone='25-989-741-2989' OR c_mktsegment='BUILDING'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having OR and AND filter") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select c_phone from carbon_table where (c_phone='25-989-741-2989' OR c_phone='25-989-741-2990') AND c_mktsegment='BUILDING'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone from carbon_table where (c_phone='25-989-741-2989' OR c_phone='25-989-741-2990') AND c_mktsegment='BUILDING'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having StartsWith Filter") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select c_phone,c_mktsegment from carbon_table where c_mktsegment like 'BU%'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone,c_mktsegment from carbon_table where c_mktsegment like 'BU%'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989","BUILDING")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having first_value udf") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select first_value(c_phone), first_value(c_mktsegment) as a from carbon_table where c_mktsegment like 'BU%'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select first_value(c_phone) as a,first_value(c_mktsegment) as b from carbon_table where c_mktsegment like 'BU%'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989","BUILDING")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having last_value udf") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select last_value(c_phone),last_value(c_mktsegment) as a from carbon_table where c_mktsegment like 'BU%'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select last_value(c_phone) as a ,last_value(c_mktsegment) as b from carbon_table where c_mktsegment like 'BU%'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989","BUILDING")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having first udf") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select first(c_phone),first(c_mktsegment) as a from carbon_table where c_mktsegment like 'BU%'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select first(c_phone) as a,first(c_mktsegment) as b from carbon_table where c_mktsegment like 'BU%'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989","BUILDING")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having last udf") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select last(c_phone),last(c_mktsegment) as a from carbon_table where c_mktsegment like 'BU%'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select last(c_phone) as a,last(c_mktsegment) as b from carbon_table where c_mktsegment like 'BU%'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989","BUILDING")))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having all UDF functions") {
    sql("drop table if exists carbon_table1")
    val query = "select corr(deptno, empno) as a, covar_pop(deptno, empno) as b, covar_samp" +
                "(deptno, empno) as c, mean(empno) as d,skewness(empno) as e, stddev(empno) as f, stddev_pop(empno) as g, stddev_samp(empno) as h," +
                "var_samp(empno) as i, COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as j from udfValidation where empname ='pramod' or deptname = 'network' or designation='TL' " +
                "group by designation, deptname, empname with ROLLUP"
    val df = sql(s"explain extended $query").collect()
    df(0).getString(0).contains("default.ind_i1 ")
    sql(s"create table carbon_table1 stored as carbondata as $query")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(15)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having cast of UDF functions") {
    sql("drop table if exists carbon_table1")
    val query =   "select cast(approx_count_distinct(empname) as string) as c1, cast(approx_count_distinct(deptname) as string) as c2,cast(corr(deptno, empno) as string) as c5, cast(covar_pop(deptno, empno) as string) as c6, " +
                  "cast(covar_samp(deptno, empno) as string) as c7, cast(grouping(designation) as string) as c8, cast(grouping(deptname) as string) as c9, cast(mean(deptno) as string) as c10, cast(mean" +
                  "(empno) as string) as c11,cast(skewness(deptno) as string) as c12, cast(skewness(empno) as string) as c13, cast(stddev(deptno) as string) as c14, cast(stddev(empno) as string) as c15, cast(stddev_pop" +
                  "(deptno) as string) as c16, cast(stddev_pop(empno) as string) as c17, cast(stddev_samp(deptno) as string) as c18, cast(stddev_samp(empno) as string) as c19, cast(var_pop(deptno) as string) as c20, " +
                  "cast(var_pop(empno) as string) as c21, cast(var_samp(deptno) as string) as c22, cast(var_samp(empno) as string) as c23, cast(variance(deptno) as string) as c24, cast(variance(empno) as string) as c25, " +
                  "COALESCE(CONV(substring(empname, 3, 2), 16, 10), '') as c26, COALESCE(CONV(substring(deptname, 3," +
                  " 2), 16, 10), '') as c27 from udfValidation where empname = 'pramod' or deptname = 'network' or " +
                  "designation='TL' group by designation, deptname, empname with ROLLUP"
    val df = sql(s"explain extended $query").collect()
    df(0).getString(0).contains("default.ind_i1 ")
    sql(s"create table carbon_table1 stored as carbondata as $query")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(15)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having concat function") {
    sql("drop table if exists carbon_table1")
    val df = sql(s"explain extended select concat_ws(deptname) as c1 from udfValidation where concat_ws(deptname) IS NOT NULL or concat_ws(deptname) is null").collect()
    df(0).getString(0).contains("default.ind_i5")
    sql("create table carbon_table1 stored as carbondata as select concat_ws(deptname) as c1 from udfValidation where concat_ws(deptname) IS NOT NULL or concat_ws(deptname) is null")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(10)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having find_in_set function") {
    sql("drop table if exists carbon_table1")
    val df = sql(s"explain extended select find_in_set(deptname,'o') as c1 from udfValidation where find_in_set(deptname,'o') =0 or find_in_set(deptname,'a') is null").collect()
    df(0).getString(0).contains("default.ind_i5")
    sql("create table carbon_table1 stored as carbondata as select find_in_set(deptname,'o') as c1 from udfValidation where find_in_set(deptname,'o') =0 or find_in_set(deptname,'a') is null")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(10)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having agg function") {
    sql("drop table if exists carbon_table1")
    val df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 or length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 or length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having arithmetic expressions in filter") {
    // arithmetic + in filter
    sql("drop table if exists carbon_table1")
    var df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 + length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 + length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // arithmetic - in filter
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 - length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 - length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // arithmetic / in filter
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 / length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 / length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // arithmetic * in filter
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 * length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname)=6 * length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    sql("drop table if exists carbon_table1")
  }

  test("test ctas with carbon table with SI having comparsion operators in filter") {
    // comparison operator >
    sql("drop table if exists carbon_table1")
    var df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) > length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) > length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // comparison operator <
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) < length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) < length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // comparison operator >=
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) >= length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) >= length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    // comparison operator >=
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) <= length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) <= length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    sql("drop table if exists carbon_table1")
    // comparison operator <>
    sql("drop table if exists carbon_table1")
    df = sql(s"explain extended select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) <> length(empname) is NULL").collect()
    df(0).getString(0).contains("default.ind_i1")
    sql("create table carbon_table1 stored as carbondata as select max(length(deptname)),min(length(designation)),avg(length(empname)),count(length(empname)),sum(length(deptname)),variance(length(designation)) from udfValidation where length(empname) <> length(empname) is NULL")
    checkAnswer(sql("select count(*) from carbon_table1"), Seq(Row(1)))
    sql("drop table if exists carbon_table1")
  }

  test("test Ctas with Carbon Table having SI with Union and Unionall") {
    sql("drop table if exists carbon_table1")
    var df = sql("explain extended select c_phone from carbon_table where c_phone = '25-989-741-2989' union select c_phone from carbon_table  where c_phone = '25-989-741-2989'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone from carbon_table where c_phone = '25-989-741-2989' union select c_phone from carbon_table  where c_phone = '25-989-741-2989'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989")))
    sql("drop table if exists carbon_table1")
    df = sql("explain extended select c_phone from carbon_table where c_phone = '25-989-741-2989' union all select c_phone from carbon_table  where c_phone = '25-989-741-2989'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select c_phone from carbon_table where c_phone = '25-989-741-2989' union all select c_phone from carbon_table  where c_phone = '25-989-741-2989'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989"),Row("25-989-741-2989")))
    sql("drop table if exists carbon_table1")
  }

  test("test CTAS with Carbon table having SI with join") {
    sql("drop table if exists carbon_table1")
    val df = sql("explain extended select t1.c_phone as a,t2.c_phone as b from carbon_table t1, carbon_table t2 where t1.c_phone=t2.c_phone and t1.c_phone = '25-989-741-2989'").collect()
    df(0).getString(0).contains("default.sc_indx1")
    sql("create table carbon_table1 stored as carbondata as select t1.c_phone as a,t2.c_phone as b from carbon_table t1, carbon_table t2 where t1.c_phone=t2.c_phone and t1.c_phone = '25-989-741-2989'")
    checkAnswer(sql("select * from carbon_table1"), Seq(Row("25-989-741-2989", "25-989-741-2989")))
    sql("drop table if exists carbon_table1")
  }
}

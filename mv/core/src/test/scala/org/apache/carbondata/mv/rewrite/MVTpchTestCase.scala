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

import java.io.File

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class MVTpchTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    drop()
    val projectPath = new File(this.getClass.getResource("/").getPath + "../../../../")
      .getCanonicalPath.replaceAll("\\\\", "/")
    val integrationPath = s"$projectPath/integration"
    val resourcesPath = s"$integrationPath/spark/src/test/resources"

    sql(s"""create table if not exists LINEITEM(  L_SHIPDATE date,  L_SHIPMODE string,  L_SHIPINSTRUCT string,  L_RETURNFLAG string,  L_RECEIPTDATE date,  L_ORDERKEY INT ,  L_PARTKEY INT ,  L_SUPPKEY   string,  L_LINENUMBER int,  L_QUANTITY double,  L_EXTENDEDPRICE double,  L_DISCOUNT double,  L_TAX double,  L_LINESTATUS string,  L_COMMITDATE date,  L_COMMENT  string) STORED AS carbondata""")
    sql(s"""create table if not exists ORDERS(  O_ORDERDATE date,  O_ORDERPRIORITY string,  O_ORDERSTATUS string,  O_ORDERKEY int,  O_CUSTKEY string,  O_TOTALPRICE double,  O_CLERK string,  O_SHIPPRIORITY int,  O_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists CUSTOMER(  C_MKTSEGMENT string,  C_NATIONKEY string,  C_CUSTKEY string,  C_NAME string,  C_ADDRESS string,  C_PHONE string,  C_ACCTBAL double,  C_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists REGION(  R_NAME string,  R_REGIONKEY string,  R_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists NATION (  N_NAME string,  N_NATIONKEY string,  N_REGIONKEY string,  N_COMMENT  string) STORED AS carbondata""")
    sql(s"""create table if not exists SUPPLIER(S_COMMENT string,S_SUPPKEY string,S_NAME string, S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double) STORED AS carbondata""")

    sql(s"""load data inpath "$resourcesPath/tpch/lineitem.csv" into table lineitem options('DELIMITER'='|','FILEHEADER'='L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/orders.csv" into table ORDERS options('DELIMITER'='|','FILEHEADER'='O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/customers.csv" into  table CUSTOMER options('DELIMITER'='|','FILEHEADER'='C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/region.csv" into table REGION options('DELIMITER'='|','FILEHEADER'='R_REGIONKEY,R_NAME,R_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/nation.csv" into table NATION options('DELIMITER'='|','FILEHEADER'='N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/supplier.csv" into table SUPPLIER options('DELIMITER'='|','FILEHEADER'='S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')""")


    sql(s"""create table if not exists LINEITEM1(  L_SHIPDATE date,  L_SHIPMODE string,  L_SHIPINSTRUCT string,  L_RETURNFLAG string,  L_RECEIPTDATE date,  L_ORDERKEY INT ,  L_PARTKEY INT ,  L_SUPPKEY   string,  L_LINENUMBER int,  L_QUANTITY double,  L_EXTENDEDPRICE double,  L_DISCOUNT double,  L_TAX double,  L_LINESTATUS string,  L_COMMITDATE date,  L_COMMENT  string) STORED AS carbondata""")
    sql(s"""create table if not exists ORDERS1(  O_ORDERDATE date,  O_ORDERPRIORITY string,  O_ORDERSTATUS string,  O_ORDERKEY int,  O_CUSTKEY string,  O_TOTALPRICE double,  O_CLERK string,  O_SHIPPRIORITY int,  O_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists CUSTOMER1(  C_MKTSEGMENT string,  C_NATIONKEY string,  C_CUSTKEY string,  C_NAME string,  C_ADDRESS string,  C_PHONE string,  C_ACCTBAL double,  C_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists REGION1(  R_NAME string,  R_REGIONKEY string,  R_COMMENT string) STORED AS carbondata""")
    sql(s"""create table if not exists NATION1 (  N_NAME string,  N_NATIONKEY string,  N_REGIONKEY string,  N_COMMENT  string) STORED AS carbondata""")
    sql(s"""create table if not exists SUPPLIER1(S_COMMENT string,S_SUPPKEY string,S_NAME string, S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double) STORED AS carbondata""")

    sql(s"""load data inpath "$resourcesPath/tpch/lineitem.csv" into table lineitem1 options('DELIMITER'='|','FILEHEADER'='L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/orders.csv" into table ORDERS1 options('DELIMITER'='|','FILEHEADER'='O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/customers.csv" into  table CUSTOMER1 options('DELIMITER'='|','FILEHEADER'='C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/region.csv" into table REGION1 options('DELIMITER'='|','FILEHEADER'='R_REGIONKEY,R_NAME,R_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/nation.csv" into table NATION1 options('DELIMITER'='|','FILEHEADER'='N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT')""")
    sql(s"""load data inpath "$resourcesPath/tpch/supplier.csv" into table SUPPLIER1 options('DELIMITER'='|','FILEHEADER'='S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')""")
  }

  test("test create materialized view with tpch1") {
    sql(s"drop materialized view if exists datamap1")
    sql("create materialized view datamap1  as select l_returnflag, l_linestatus,l_shipdate, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,count(*) as count_order from lineitem group by l_returnflag, l_linestatus,l_shipdate")
    val df = sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,count(*) as count_order from lineitem where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap1"))
//    checkAnswer(df, sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,count(*) as count_order from lineitem1 where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"))
    sql(s"drop materialized view datamap1")
  }

  test("test create materialized view with tpch1 with order") {
    sql(s"drop materialized view if exists datamap2")
    sql("create materialized view datamap2  as select l_returnflag, l_linestatus,l_shipdate, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge from lineitem group by l_returnflag, l_linestatus,l_shipdate order by l_returnflag, l_linestatus")
    val df = sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge from lineitem where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap2"))
//    checkAnswer(df, sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge from lineitem1 where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"))
    sql(s"drop materialized view datamap2")
  }

  test("test create materialized view with tpch1 with sub group by") {
    sql(s"drop materialized view if exists datamap3")
    sql("create materialized view datamap3  as select l_returnflag, l_linestatus,l_shipdate, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge from lineitem group by l_returnflag, l_linestatus,l_shipdate")
    val df = sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price from lineitem where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap3"))
//    checkAnswer(df, sql("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price from lineitem1 where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"))
    sql(s"drop materialized view datamap3")
  }

  ignore("test create materialized view with tpch3") {
    sql(s"drop materialized view if exists datamap4")
    sql("create materialized view datamap4  as select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority")
    val df = sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap4"))
//    checkAnswer(df, sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer1, orders1, lineitem1 where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"))
    sql(s"drop materialized view datamap4")
  }

  test("test create materialized view with tpch3 with no filters on mv") {
    sql(s"drop materialized view if exists datamap5")
    sql("create materialized view datamap5  as select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority,c_mktsegment,l_shipdate, c_custkey as c1, o_custkey as c2,o_orderkey as o1  from customer, orders, lineitem where c_custkey = o_custkey and l_orderkey = o_orderkey group by l_orderkey, o_orderdate, o_shippriority,c_mktsegment,l_shipdate,c_custkey,o_custkey, o_orderkey ")
    val df = sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap5"))
//    checkAnswer(df, sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer1, orders1, lineitem1 where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"))
    sql(s"drop materialized view datamap5")
  }

  ignore("test create materialized view with tpch3 with filters on mv and all filter columns on projection") {
    sql(s"drop materialized view if exists datamap5")
    sql("create materialized view datamap5  as select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority,c_mktsegment,l_shipdate from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority,c_mktsegment,l_shipdate")
    val df = sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap5"))
//    checkAnswer(df, sql("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer1, orders1, lineitem1 where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date('1995-03-15') and l_shipdate > date('1995-03-15') group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"))
    sql(s"drop materialized view datamap5")
  }

  ignore("test create materialized view with tpch4 (core issue)") {
    sql(s"drop materialized view if exists datamap6")
    sql("create materialized view datamap6  as select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date('1993-07-01') and o_orderdate < date('1993-10-01') and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority")
    val df = sql("select o_orderpriority, count(*) as order_count from orders where o_orderdate >= date('1993-07-01') and o_orderdate < date('1993-10-01') and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap6"))
//    checkAnswer(df, sql("select o_orderpriority, count(*) as order_count from orders1 where o_orderdate >= date('1993-07-01') and o_orderdate < date('1993-10-01') and exists ( select * from lineitem1 where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority"))
    sql(s"drop materialized view datamap6")
  }

  ignore("test create materialized view with tpch5") {
    sql(s"drop materialized view if exists datamap7")
    sql("create materialized view datamap7  as select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name")
    val df = sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name order by revenue desc")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap7"))
//    checkAnswer(df, sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer1, orders1, lineitem1, supplier1, nation1, region1 where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name order by revenue desc"))
    sql(s"drop materialized view datamap7")
  }

  test("test create materialized view with tpch5 with no filters on mv") {
    sql(s"drop materialized view if exists datamap8")
    sql("create materialized view datamap8  as select n_name,o_orderdate,r_name, sum(l_extendedprice * (1 - l_discount)) as revenue, sum(c_custkey), sum(o_custkey), sum(l_orderkey),sum(o_orderkey), sum(l_suppkey), sum(s_suppkey), sum(c_nationkey), sum(s_nationkey), sum(n_nationkey), sum(n_regionkey), sum(r_regionkey)  from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey  group by n_name,o_orderdate,r_name")
    val df = sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name order by revenue desc")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap8"))
//    checkAnswer(df, sql("select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer1, orders1, lineitem1, supplier1, nation1, region1 where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA' and o_orderdate >= date('1994-01-01') and o_orderdate < date('1995-01-01') group by n_name order by revenue desc"))
    sql(s"drop materialized view datamap8")
  }

  test("test create materialized view with tpch6") {
    sql(s"drop materialized view if exists datamap9")
    sql("create materialized view datamap9  as select sum(l_extendedprice * l_discount) as revenue, count(l_shipdate), sum(l_discount),sum(l_quantity)  from lineitem where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24")
    val df = sql("select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap9"))
    assert(verifyAgg(df.queryExecution.optimizedPlan))
//    checkAnswer(df, sql("select sum(l_extendedprice * l_discount) as revenue from lineitem1 where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24"))
    sql(s"drop materialized view datamap9")
  }

  test("test create materialized view with tpch6 with no filters on mv") {
    sql(s"drop materialized view if exists datamap10")
    sql("create materialized view datamap10  as select sum(l_extendedprice * l_discount) as revenue,l_shipdate,l_discount,l_quantity from lineitem group by l_shipdate,l_discount,l_quantity")
    val df = sql("select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap10"))
    assert(verifyAgg(df.queryExecution.optimizedPlan))
//    checkAnswer(df, sql("select sum(l_extendedprice * l_discount) as revenue from lineitem1 where l_shipdate >= date('1994-01-01') and l_shipdate < date('1995-01-01') and l_discount between 0.05 and 0.07 and l_quantity < 24"))
    sql(s"drop materialized view datamap10")
  }

  test("test create materialized view with tpch7 part of query1") {
    sql(s"drop materialized view if exists datamap11")
    sql("create materialized view datamap11  as select l_shipdate,n_name , l_extendedprice , l_discount, s_suppkey,l_suppkey, o_orderkey,l_orderkey, c_custkey, o_custkey, s_nationkey,  n1.n_nationkey, c_nationkey from supplier,lineitem,orders,customer,nation n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey")
    val df = sql("select year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem,orders,customer,nation n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey and ( (n1.n_name = 'FRANCE') or (n1.n_name = 'GERMANY') ) and l_shipdate between date('1995-01-01') and date('1996-12-31')")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap11"))
//    checkAnswer(df, sql("select year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier1,lineitem1,orders1,customer1,nation1 n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey and ( (n1.n_name = 'FRANCE') or (n1.n_name = 'GERMANY') ) and l_shipdate between date('1995-01-01') and date('1996-12-31')"))
    sql(s"drop materialized view datamap11")
  }

  test("test create materialized view with tpch7 part of query2 (core issue)") {
    sql(s"drop materialized view if exists datamap12")
    sql("create materialized view datamap12  as select n1.n_name, l_shipdate, l_extendedprice ,l_discount,s_suppkey, l_suppkey,o_orderkey,l_orderkey, c_custkey,o_custkey,s_nationkey,  n1.n_nationkey,c_nationkey from supplier,lineitem,orders,customer,nation n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey")
    val df = sql("select supp_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem,orders,customer,nation n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey and ( (n1.n_name = 'FRANCE' ) or (n1.n_name = 'GERMANY') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, l_year order by supp_nation, l_year")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap12"))
//    checkAnswer(df, sql("select supp_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier1,lineitem1,orders1,customer1,nation1 n1 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n1.n_nationkey and ( (n1.n_name = 'FRANCE' ) or (n1.n_name = 'GERMANY') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, l_year order by supp_nation, l_year"))
    sql(s"drop materialized view datamap12")
  }

  ignore("test create materialized view with tpch7 part of query3 (self join issue)") {
    sql(s"drop materialized view if exists datamap13")
    sql("create materialized view datamap13  as select n1.n_name as supp_nation, n2.n_name as cust_nation, l_shipdate, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem,orders,customer,nation n1,nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey")
    val df = sql("select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem,orders,customer,nation n1,nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year")
    assert(TestUtil.verifyMVDataMap(df.queryExecution.optimizedPlan, "datamap13"))
//    checkAnswer(df, sql("select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, year(l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier,lineitem1,orders1,customer1,nation1 n1,nation1 n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') ) and l_shipdate between date('1995-01-01') and date('1996-12-31') ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year"))
    sql(s"drop materialized view datamap13")
  }

  def verifyAgg(logicalPlan: LogicalPlan): Boolean = {
    var aggExpExists = false
    logicalPlan transform {
      case a:Aggregate =>
        aggExpExists = true
        a
    }
    aggExpExists
  }


  def drop(): Unit = {
    sql("drop table IF EXISTS LINEITEM")
    sql("drop table IF EXISTS ORDERS")
    sql("drop table IF EXISTS CUSTOMER")
    sql("drop table IF EXISTS REGION")
    sql("drop table IF EXISTS NATION")
    sql("drop table IF EXISTS SUPPLIER")
    sql("drop table IF EXISTS LINEITEM1")
    sql("drop table IF EXISTS ORDERS1")
    sql("drop table IF EXISTS CUSTOMER1")
    sql("drop table IF EXISTS REGION1")
    sql("drop table IF EXISTS NATION1")
    sql("drop table IF EXISTS SUPPLIER1")
  }

  override def afterAll {
//    drop()
  }
}

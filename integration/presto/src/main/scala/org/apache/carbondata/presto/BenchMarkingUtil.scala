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

package org.apache.carbondata.presto

import scala.io.Source

object BenchMarkingUtil {

  val queries: Array[Query] = Array(

    //Query("select * from LINEITEM limit 10","","")
/*
    Query("select\n        l_returnflag,\n        l_linestatus,\n        sum(l_quantity) as " +
           "sum_qty,\n        sum(l_extendedprice) as sum_base_price,\n        sum(l_extendedprice" +
           " * (1 - l_discount)) as sum_disc_price,\n        sum(l_extendedprice * (1 - " +
           "l_discount) * (1 + l_tax)) as sum_charge,\n        avg(l_quantity) as avg_qty,\n      " +
           "  avg(l_extendedprice) as avg_price,\n        avg(l_discount) as avg_disc,\n        " +
           "count(*) as count_order\nfrom\n        lineitem\nwhere\n        l_shipdate <= " +
           "cast('1998-09-16' as timestamp)\ngroup by\n        l_returnflag,\n        l_linestatus\norder by\n       " +
           " l_returnflag,\n        l_linestatus","",""),

     Query("select\n        l_orderkey,\n        sum(l_extendedprice * (1 - l_discount)) as " +
           "revenue,\n        o_orderdate,\n        o_shippriority\nfrom\n        customer,\n     " +
           "   orders,\n        lineitem\nwhere\n        c_mktsegment = 'BUILDING'\n        and " +
           "c_custkey = o_custkey\n        and l_orderkey = o_orderkey\n        and o_orderdate < " +
           " cast('1995-03-22' as timestamp)\n        and l_shipdate > cast('1995-03-22' as timestamp)\ngroup by\n        l_orderkey,\n  " +
           "      o_orderdate,\n        o_shippriority\norder by\n        revenue desc,\n        " +
           "o_orderdate\nlimit 10","",""),

     Query("select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n        " +
           "orders as o\nwhere\n        o_orderdate >= cast('1996-05-01' as timestamp)\n        and o_orderdate < " +
           "cast('1996-08-01' as timestamp)\n        and exists (\n                select\n                        " +
           "*\n                from\n                        lineitem\n                where\n    " +
           "                    l_orderkey = o.o_orderkey\n                        and " +
           "l_commitdate < l_receiptdate\n        )\ngroup by\n        o_orderpriority\norder by\n" +
           "        o_orderpriority","",""),

     Query("select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
           "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
           "supplier,\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n    " +
           "    and l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and " +
           "c_nationkey = s_nationkey\n        and s_nationkey = n_nationkey\n        and " +
           "n_regionkey = r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= " +
           "cast('1993-01-01' as timestamp)\n        and o_orderdate < cast('1994-01-01' as timestamp)" +
           "\ngroup by\n        n_name\norder " +
           "by\n        revenue desc","",""),

     Query("select\n        o_orderpriority,\n        count(*) as order_count\nfrom\n        " +
           "orders as o\nwhere\n        o_orderdate >= cast('1996-05-01' as timestamp)\n        and o_orderdate < " +
           "cast('1996-08-01' as timestamp)\n        and exists (\n                select\n                        " +
           "*\n                from\n                        lineitem\n                where\n    " +
           "                    l_orderkey = o.o_orderkey\n                        and " +
           "l_commitdate < l_receiptdate\n        )\ngroup by\n        o_orderpriority\norder by\n" +
           "        o_orderpriority","",""),

     Query("select\n        n_name,\n        sum(l_extendedprice * (1 - l_discount)) as " +
           "revenue\nfrom\n        customer,\n        orders,\n        lineitem,\n        " +
           "supplier,\n        nation,\n        region\nwhere\n        c_custkey = o_custkey\n    " +
           "    and l_orderkey = o_orderkey\n        and l_suppkey = s_suppkey\n        and " +
           "c_nationkey = s_nationkey\n        and s_nationkey = n_nationkey\n        and " +
           "n_regionkey = r_regionkey\n        and r_name = 'AFRICA'\n        and o_orderdate >= " +
           "cast('1993-01-01' as timestamp)\n        and o_orderdate < cast('1994-01-01' as timestamp) \ngroup by\n        n_name\norder " +
           "by\n        revenue desc","",""),

     Query("select\n        sum(l_extendedprice * l_discount) as revenue\nfrom\n        " +
           "lineitem\nwhere\n        l_shipdate >= cast('1993-01-01' as timestamp)\n        and l_shipdate < " +
           "cast('1994-01-01' as timestamp) \n        and l_discount between 0.06 - 0.01 and 0.06 + 0.01\n        and " +
           "l_quantity < 25","",""),

     Query("select\n        supp_nation,\n        cust_nation,\n        l_year,\n        sum" +
           "(volume) as revenue\nfrom\n        (\n                select\n                        " +
           "n1.n_name as supp_nation,\n                        n2.n_name as cust_nation,\n        " +
           "                year(l_shipdate) as l_year,\n                        l_extendedprice *" +
           " (1 - l_discount) as volume\n                from\n                        supplier,\n" +
           "                        lineitem,\n                        orders,\n                  " +
           "      customer,\n                        nation n1,\n                        nation " +
           "n2\n                where\n                        s_suppkey = l_suppkey\n            " +
           "            and o_orderkey = l_orderkey\n                        and c_custkey = " +
           "o_custkey\n                        and s_nationkey = n1.n_nationkey\n                 " +
           "       and c_nationkey = n2.n_nationkey\n                        and (\n              " +
           "                  (n1.n_name = 'KENYA' and n2.n_name = 'PERU')\n                      " +
           "          or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')\n                        )\n" +
           "                        and l_shipdate between cast('1995-01-01' as timestamp) and cast('1996-12-31' as timestamp)\n        " +
           ") as shipping\ngroup by\n        supp_nation,\n        cust_nation,\n        " +
           "l_year\norder by\n        supp_nation,\n        cust_nation,\n        l_year","",""),

     Query("select\n        o_year,\n        sum(case\n                when nation = 'PERU' then " +
           "volume\n                else 0\n        end) / sum(volume) as mkt_share\nfrom\n       " +
           " (\n                select\n                        year(o_orderdate) as o_year,\n    " +
           "                    l_extendedprice * (1 - l_discount) as volume,\n                   " +
           "     n2.n_name as nation\n                from\n                        part,\n       " +
           "                 supplier,\n                        lineitem,\n                       " +
           " orders,\n                        customer,\n                        nation n1,\n     " +
           "                   nation n2,\n                        region\n                where\n" +
           "                        p_partkey = l_partkey\n                        and s_suppkey =" +
           " l_suppkey\n                        and l_orderkey = o_orderkey\n                     " +
           "   and o_custkey = c_custkey\n                        and c_nationkey = n1" +
           ".n_nationkey\n                        and n1.n_regionkey = r_regionkey\n              " +
           "          and r_name = 'AMERICA'\n                        and s_nationkey = n2" +
           ".n_nationkey\n                        and o_orderdate between cast('1995-01-01' as timestamp) and " +
           " cast('1996-12-31' as timestamp)\n                        and p_type = 'ECONOMY BURNISHED NICKEL'\n       " +
           " ) as all_nations\ngroup by\n        o_year\norder by\n        o_year","",""),


     Query("select\n        c_custkey,\n        c_name,\n        sum(l_extendedprice * (1 - " +
           "l_discount)) as revenue,\n        c_acctbal,\n        n_name,\n        c_address,\n   " +
           "     c_phone,\n        c_comment\nfrom\n        customer,\n        orders,\n        " +
           "lineitem,\n        nation\nwhere\n        c_custkey = o_custkey\n        and " +
           "l_orderkey = o_orderkey\n        and o_orderdate >= cast('1993-07-01' as timestamp)\n        and " +
           "o_orderdate < cast('1993-10-01' as timestamp)\n        and l_returnflag = 'R'\n        and c_nationkey = " +
           "n_nationkey\ngroup by\n        c_custkey,\n        c_name,\n        c_acctbal,\n      " +
           "  c_phone,\n        n_name,\n        c_address,\n        c_comment\norder by\n        " +
           "revenue desc\nlimit 20","",""),

     Query("select\n        l_shipmode,\n        sum(case\n                when o_orderpriority = " +
           "'1-URGENT'\n                        or o_orderpriority = '2-HIGH'\n                   " +
           "     then 1\n                else 0\n        end) as high_line_count,\n        sum" +
           "(case\n                when o_orderpriority <> '1-URGENT'\n                        and" +
           " o_orderpriority <> '2-HIGH'\n                        then 1\n                else 0\n" +
           "        end) as low_line_count\nfrom\n        orders,\n        lineitem\nwhere\n      " +
           "  o_orderkey = l_orderkey\n        and l_shipmode in ('REG AIR', 'MAIL')\n        and " +
           "l_commitdate < l_receiptdate\n        and l_shipdate < l_commitdate\n        and " +
           "l_receiptdate >= cast('1995-01-01' as timestamp) \n        and l_receiptdate < cast('1996-01-01' as timestamp) \ngroup by\n   " +
           "     l_shipmode\norder by\n        l_shipmode","",""),

     Query("select\n        c_count,\n        count(*) as custdist\nfrom\n        (\n             " +
           "   select\n                        c_custkey,\n                        count" +
           "(o_orderkey) as c_count\n                from\n                        customer left " +
           "outer join orders on\n                                c_custkey = o_custkey\n         " +
           "                       and o_comment not like '%unusual%accounts%'\n                " +
           "group by\n                        c_custkey\n        ) c_orders\ngroup by\n        " +
           "c_count\norder by\n        custdist desc,\n        c_count desc","",""),

     Query("select\n        100.00 * sum(case\n                when p_type like 'PROMO%'\n        " +
           "                then l_extendedprice * (1 - l_discount)\n                else 0\n     " +
           "   end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue\nfrom\n        " +
           "lineitem,\n        part\nwhere\n        l_partkey = p_partkey\n        and l_shipdate " +
           ">= cast('1995-08-01' as timestamp) \n        and l_shipdate < cast('1995-09-01' as timestamp)","",""),*/

     Query("select\n        p_brand,\n        p_type,\n        p_size,\n        count(distinct " +
           "ps_suppkey) as supplier_cnt\nfrom\n        partsupp,\n        part\nwhere\n        " +
           "p_partkey = ps_partkey\n        and p_brand <> 'Brand#34'\n        and p_type not like" +
           " 'ECONOMY BRUSHED%'\n        and p_size in (22, 14, 27, 49, 21, 33, 35, 28)\n        " +
           "and partsupp.ps_suppkey not in (\n                select\n                        " +
           "s_suppkey\n                from\n                        supplier\n                " +
           "where\n                        s_comment like '%Customer%Complaints%'\n        )" +
           "\ngroup by\n        p_brand,\n        p_type,\n        p_size\norder by\n        " +
           "supplier_cnt desc,\n        p_brand,\n        p_type,\n        p_size","",""),

     Query("with q17_part as (\n  select p_partkey from part where  \n  p_brand = 'Brand#23'\n  " +
           "and p_container = 'MED BOX'\n),\nq17_avg as (\n  select l_partkey as t_partkey, 0.2 * " +
           "avg(l_quantity) as t_avg_quantity\n  from lineitem \n  where l_partkey IN (select " +
           "p_partkey from q17_part)\n  group by l_partkey\n),\nq17_price as (\n  select\n  " +
           "l_quantity,\n  l_partkey,\n  l_extendedprice\n  from\n  lineitem\n  where\n  l_partkey" +
           " IN (select p_partkey from q17_part)\n)\nselect cast(sum(l_extendedprice) / 7.0 as " +
           "decimal(32,2)) as avg_yearly\nfrom q17_avg, q17_price\nwhere \nt_partkey = l_partkey " +
           "and l_quantity < t_avg_quantity","",""),

     Query("-- explain formatted \nwith tmp1 as (\n    select p_partkey from part where p_name " +
           "like 'forest%'\n),\ntmp2 as (\n    select s_name, s_address, s_suppkey\n    from " +
           "supplier, nation\n    where s_nationkey = n_nationkey\n    and n_name = 'CANADA'\n)," +
           "\ntmp3 as (\n    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey\n " +
           "   from lineitem, tmp2\n    where l_shipdate >= cast('1994-01-01' as timestamp) and l_shipdate <= " +
           "cast('1995-01-01' as timestamp)\n    and l_suppkey = s_suppkey \n    group by l_partkey, l_suppkey\n)," +
           "\ntmp4 as (\n    select ps_partkey, ps_suppkey, ps_availqty\n    from partsupp \n    " +
           "where ps_partkey IN (select p_partkey from tmp1)\n),\ntmp5 as (\nselect\n    " +
           "ps_suppkey\nfrom\n    tmp4, tmp3\nwhere\n    ps_partkey = l_partkey\n    and " +
           "ps_suppkey = l_suppkey\n    and ps_availqty > sum_quantity\n)\nselect\n    s_name,\n  " +
           "  s_address\nfrom\n    supplier\nwhere\n    s_suppkey IN (select ps_suppkey from tmp5)" +
           "\norder by s_name","","")
  )


  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    (System.currentTimeMillis() - start).toDouble / 1000
  }

  def writeResults(content: String, file: String): Unit = {
    scala.tools.nsc.io.File(file).appendAll(content)
  }

  def readFromFile(file: String): List[String] = {
    Source.fromFile(file).getLines.toList
  }
}
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
  // performance test queries, they are designed to test various data access type
  val queries: Array[Query] = Array(
    // ===========================================================================
    // ==                     FULL SCAN AGGREGATION                             ==
    // ===========================================================================
    Query(
      "select sum(m1) from $table",
      "full scan",
      "full scan query, 1 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2) from $table",
      "full scan",
      "full scan query, 2 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3) from $table",
      "full scan",
      "full scan query, 3 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3), sum(m4) from $table",
      "full scan",
      "full scan query, 4 aggregate"
    ),
    Query(
      "select sum(m1), sum(m2), sum(m3), sum(m4), avg(m5) from $table",
      "full scan",
      "full scan query, 5 aggregate"
    ),
    Query(
      "select count(distinct id) from $table",
      "full scan",
      "full scan and count distinct of high card column"
    ),
    Query(
      "select count(distinct country) from $table",
      "full scan",
      "full scan and count distinct of medium card column"
    ),
    Query(
      "select count(distinct city) from $table",
      "full scan",
      "full scan and count distinct of low card column"
    ),
    // ===========================================================================
    // ==                      FULL SCAN GROUP BY AGGREGATE                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) from $table group by country",
      "aggregate",
      "group by on big data, on medium card column, medium result set,"
    ),
    Query(
      "select city, sum(m1) from $table group by city",
      "aggregate",
      "group by on big data, on low card column, small result set,"
    ),
    Query(
      "select id, sum(m1) as metric from $table group by id order by metric desc limit 100",
      "topN",
      "top N on high card column"
    ),
    Query(
      "select country,sum(m1) as metric from $table group by country order by metric desc limit 10",
      "topN",
      "top N on medium card column"
    ),
    Query(
      "select city,sum(m1) as metric from $table group by city order by metric desc limit 10",
      "topN",
      "top N on low card column"
    ),
    // ===========================================================================
    // ==                  FILTER SCAN GROUP BY AGGREGATION                     ==
    // ===========================================================================
    Query(
      "select country, sum(m1) from $table where city='city8' group by country ",
      "filter scan and aggregate",
      "group by on large data, small result set"
    ),
    Query(
      "select id, sum(m1) from $table where planet='planet10' group by id",
      "filter scan and aggregate",
      "group by on medium data, large result set"
    ),
    Query(
      "select city, sum(m1) from $table where country='country12' group by city ",
      "filter scan and aggregate",
      "group by on medium data, small result set"
    ),
    // ===========================================================================
    // ==                             FILTER SCAN                               ==
    // ===========================================================================
    Query(
      "select * from $table where city = 'city3' limit 10000",
      "filter scan",
      "filter on low card dimension, limit, medium result set, fetch all columns"
    ),
    Query(
      "select * from $table where country = 'country9' ",
      "filter scan",
      "filter on low card dimension, medium result set, fetch all columns"
    ),
    Query(
      "select * from $table where planet = 'planet101' ",
      "filter scan",
      "filter on medium card dimension, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where id = '408938' ",
      "filter scan",
      "filter on high card dimension"
    ),
    Query(
      "select * from $table where country='country10000'  ",
      "filter scan",
      "filter on low card dimension, not exist"
    ),
    Query(
      "select * from $table where country='country2' and city ='city8' ",
      "filter scan",
      "filter on 2 dimensions, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where city='city1' and country='country2' and planet ='planet3' ",
      "filter scan",
      "filter on 3 dimensions, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where m1 < 3",
      "filter scan",
      "filter on measure, small result set, fetch all columns"
    ),
    Query(
      "select * from $table where id like '1%' ",
      "fuzzy filter scan",
      "like filter, big result set"
    ),
    Query(
      "select * from $table where id like '%111'",
      "fuzzy filter scan",
      "like filter, medium result set"
    ),
    Query(
      "select * from $table where id like 'xyz%' ",
      "fuzzy filter scan",
      "like filter, full scan but not exist"
    )
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

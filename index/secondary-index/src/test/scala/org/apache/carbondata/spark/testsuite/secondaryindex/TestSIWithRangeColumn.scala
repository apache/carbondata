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

class TestSIWithRangeColumn extends QueryTest {
  test("test SI on range column with and without global sort") {
    sql("drop table if exists carbon_range_column")
    sql(
      """
        | CREATE TABLE carbon_range_column(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES(
        | 'SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='city')
      """.stripMargin)
    sql("CREATE INDEX range_si on carbon_range_column(city) as 'carbondata'")
    sql("INSERT into carbon_range_column values(1,'nko','blr',25)")
    checkAnswer(sql("SELECT count(*) FROM range_si"), Seq(Row(1)))
    checkAnswer(sql("SELECT name FROM carbon_range_column where city='blr'"), Seq(Row("nko")))
    sql("drop index if exists range_si on carbon_range_column")
    sql("CREATE INDEX range_si on carbon_range_column(city) as 'carbondata'" +
      " PROPERTIES('sort_scope'='global_sort', 'Global_sort_partitions'='1')")
    checkAnswer(sql("SELECT count(*) FROM range_si"), Seq(Row(1)))
    sql("drop table if exists carbon_range_column")
  }

  test("test SI creation with range column") {
    sql("drop table if exists carbon_range_column")
    sql(
      """
        | CREATE TABLE carbon_range_column(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES(
        | 'SORT_SCOPE'='LOCAL_SORT', 'SORT_COLUMNS'='name, city', 'range_column'='city')
      """.stripMargin)
    val ex = intercept[Exception] {
      sql("CREATE INDEX range_si on carbon_range_column(city) as 'carbondata' " +
      "PROPERTIES('range_column'='city')")
    }
    assert(ex.getMessage.contains("Unsupported Table property in index creation: range_column"))
  }

  test("test compaction on range column with SI") {
    sql("drop table if exists table1")
    sql("create table table1(c1 int,c2 string,c3 string) stored as carbondata" +
      " TBLPROPERTIES('range_column'='c3')")
    sql("create index idx1 on table table1(c3) as 'carbondata'")
    for (i <- 0 until 5) {
      sql(s"insert into table1 values(${i + 1},'a$i','b$i')")
    }
    sql("ALTER TABLE table1 COMPACT 'MINOR'")

    val segments = sql("SHOW SEGMENTS FOR TABLE idx1")
    val segInfos = segments.collect().map { each =>
      (each.toSeq.head.toString, (each.toSeq) (1).toString)
    }
    assert(segInfos.length == 6)
    assert(segInfos.contains(("0", "Compacted")))
    assert(segInfos.contains(("1", "Compacted")))
    assert(segInfos.contains(("2", "Compacted")))
    assert(segInfos.contains(("3", "Compacted")))
    assert(segInfos.contains(("0.1", "Success")))
    assert(segInfos.contains(("4", "Success")))
    checkAnswer(sql("select * from table1 where c3='b2'"), Seq(Row(3, "a2", "b2")))
    sql("drop table if exists table1")
  }
}

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
package org.apache.carbondata.spark.testsuite.iud

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class DeleteCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {

    sql("use default")
    sql("drop database  if exists iud_db cascade")
    sql("create database  iud_db")

    sql("""create table iud_db.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED BY 'org.apache.carbondata.format'""")
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/source2.csv' INTO table iud_db.source2""")
    sql("use iud_db")
  }
  test("delete data from carbon table with alias [where clause ]") {

  }
  /*test("delete data from carbon table with alias [where clause ]") {
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest d where d.c1 = 'a'""")
    // sql("""select c2 from iud_db.dest""").show()
    checkAnswer(
      sql("""select c2 from iud_db.dest"""),
      Seq(Row(2), Row(3),Row(4), Row(5))
    )
  }
  test("delete data from  carbon table[where clause ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from dest where c2 = 2""")
    // sql("""select c1 from dest""").show()
    checkAnswer(
      sql("""select c1 from dest"""),
      Seq(Row("a"), Row("c"), Row("d"), Row("e"))
    )
  }
  test("delete data from  carbon table[where IN  ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from dest where c1 IN ('d', 'e')""")
    // sql("""select c1 from dest""").show()
    checkAnswer(
      sql("""select c1 from dest"""),
      Seq(Row("a"), Row("b"),Row("c"))
    )
  }

  test("delete data from  carbon table[with alias No where clause]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from iud_db.dest a""")
    // sql("""select c1 from iud_db.dest""").show()
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq()
    )
  }
  test("delete data from  carbon table[No alias No where clause]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from dest""").show()
    // sql("""select c1 from dest""").show()
    checkAnswer(
      sql("""select c1 from dest"""),
      Seq()
    )
  }
  test("delete data from  carbon table[where IN (sub query) ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    //sql("""delete from  iud_db.dest where c1 IN (select c11 from source2)""").show()
    sql("""delete from  iud_db.dest where c1 IN (select c11 from source2)""").show(truncate = false)
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq(Row("c"), Row("d"), Row("e"))
    )
  }
  test("delete data from  carbon table[where IN (sub query with where clause) ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from  iud_db.dest where c1 IN (select c11 from source2 where c11 = 'b')""").show()
    checkAnswer(
      sql("""select c1 from iud_db.dest"""),
      Seq(Row("a"), Row("c"), Row("d"), Row("e"))
    )
  }
  test("delete data from  carbon table[where numeric condition  ]") {
    sql("""drop table if exists iud_db.dest""")
    sql("""create table iud_db.dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""").show()
    sql("""LOAD DATA LOCAL INPATH './src/test/resources/IUD/dest.csv' INTO table iud_db.dest""")
    sql("""delete from  iud_db.dest where c2 >= 4""").show()
    checkAnswer(
      sql("""select count(*) from iud_db.dest"""),
      Seq(Row(3))
    )
  }*/
  override def afterAll {
    sql("use default")
    sql("drop database  if exists iud_db cascade")
  }
}
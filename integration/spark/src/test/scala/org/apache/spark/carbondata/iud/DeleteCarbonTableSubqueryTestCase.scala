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
package org.apache.spark.carbondata.iud

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class DeleteCarbonTableSubqueryTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("use default")
    sql("drop database  if exists iud_db_sub cascade")
    sql("create database  iud_db_sub")

    sql("""create table iud_db_sub.source2 (c11 string,c22 int,c33 string,c55 string, c66 int) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/source2.csv' INTO table iud_db_sub.source2""")
    sql("use iud_db_sub")
  }

  test("delete data from  carbon table[where IN (sub query) ]") {
    sql("""drop table if exists iud_db_sub.dest""")
    sql("""create table iud_db_sub.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db_sub.dest""")
    sql("""delete from  iud_db_sub.dest where c1 IN (select c11 from source2)""").show(truncate = false)
    checkAnswer(
      sql("""select c1 from iud_db_sub.dest"""),
      Seq(Row("c"), Row("d"), Row("e"))
    )
    sql("drop table if exists iud_db_sub.dest")
  }

  test("delete data from  carbon table[where IN (sub query with where clause) ]") {
    sql("""drop table if exists iud_db_sub.dest""")
    sql("""create table iud_db_sub.dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""").show()
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table iud_db_sub.dest""")
    sql("""delete from  iud_db_sub.dest where c1 IN (select c11 from source2 where c11 = 'b')""").show()
    checkAnswer(
      sql("""select c1 from iud_db_sub.dest"""),
      Seq(Row("a"), Row("c"), Row("d"), Row("e"))
    )
    sql("drop table if exists iud_db_sub.dest")
  }

  override def afterAll {
    sql("use default")
    sql("drop table if exists iud_db_sub.source2")
    sql("drop database  if exists iud_db_sub cascade")
  }
}
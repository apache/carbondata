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
package org.apache.carbondata.spark.testsuite.measurenullvalue

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class NullMeasureValueTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("drop table IF EXISTS measure_null_tab1")
    sql("drop table IF EXISTS measure_null_tab1_multiple_load")
    sql("drop table IF EXISTS measure_null_tab2_1Byte_Column")
    sql(
      "CREATE TABLE IF NOT EXISTS measure_null_tab1 (c1 int, c2 string, c3 smallint, c4 bigint, " +
      "c5 short, c6 boolean) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "CREATE TABLE IF NOT EXISTS measure_null_tab1_multiple_load (c1 int, c2 string, c3 smallint," +
      " c4 bigint, " +
      "c5 short, c6 boolean) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "CREATE TABLE IF NOT EXISTS measure_null_tab2_1Byte_Column (c1 int, c2 string, c3 smallint," +
      " c4 bigint, " +
      "c5 short, c6 boolean, c7 int, c8 string, c9 smallint, c10 bigint, c11 short, " +
      "c12 boolean, c13 int, c14 string, c15 smallint, c16 bigint, c17 short, c18 boolean) STORED" +
      " BY 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurepruning.csv' into table " +
        s"measure_null_tab1");
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurepruning.csv' into table " +
        s"measure_null_tab1_multiple_load");
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurepruning.csv' into table " +
        s"measure_null_tab1_multiple_load");
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurepruninglargecol.csv' into table " +
        s"measure_null_tab2_1Byte_Column");
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/nullmeasurepruninglargecol.csv' into table " +
        s"measure_null_tab2_1Byte_Column");
  }

  test("Block Prunning Boolean Measure") {
    sqlContext.sparkSession.sparkContext.setLogLevel("INFO")
    sql("drop table if exists measure_multiblock")
    sql("CREATE TABLE IF NOT EXISTS measure_multiblock (c1 int, c2 string, c3 smallint, c4 bigint, " +
        "c5 short, c6 boolean) STORED BY 'org.apache.carbondata.format'").show(200,false)
    sql(
      s"""
         | insert into measure_multiblock select 1,'1111111',111,null,1111,true
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 2,'2222222',null,null,2222,false
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 3,'3333333',333,3333333333,3333,null
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 4,'4444444',444,4444444444,null,null
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 5,'5555555',555,5555555555,null,null
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 6,'6666666',666,null,6666,true
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 7,'7777777',777,null,7777,true
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 8,'88888888',null,null,8888,null
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 9,'99999999',null,null,null,true
       """.stripMargin).show()

    sql(
      s"""
         | insert into measure_multiblock select 10,'10000000',1000,null,100000,true
       """.stripMargin).show()


    checkAnswer(
      sql("select c1 from measure_multiblock where c6 is not null"),
      Seq(Row(7), Row(6), Row(2), Row(10), Row(1), Row(9)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c6 is null"),
      Seq(Row(3), Row(4), Row(5), Row(8)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c5 is not null"),
      Seq(Row(7), Row(6), Row(2), Row(3), Row(8), Row(1)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c5 is null"),
      Seq(Row(4), Row(5), Row(9), Row(10)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c4 is not null"),
      Seq(Row(3), Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c4 is null"),
      Seq(Row(1), Row(2), Row(6), Row(7), Row(8), Row(9), Row(10)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c3 is not null"),
      Seq(Row(7), Row(6), Row(3),Row(10), Row(1), Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c3 is null"),
      Seq(Row(2), Row(8), Row(9)))


    checkAnswer(
      sql("select c1 from measure_multiblock where c3 is not null and c5 is null"),
      Seq(Row(10), Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_multiblock where c3 is null and c5 is not null"),
      Seq(Row(8), Row(2)))

    sql("drop table if exists measure_multiblock")
  }
  test("Null Select from Measure boolean") {

    checkAnswer(
      sql("select c1 from measure_null_tab1 where c6 is null"),
      Seq(Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9)))

      checkAnswer(
        sql("select c1 from measure_null_tab1 where c6 is not null"),
        Seq(Row(1), Row(6), Row(10)))
  }

  test("Null Select from Measure Short") {
    checkAnswer(
      sql("select c1 from measure_null_tab1 where c5 is null"),
      Seq(Row(10), Row(6), Row(7), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab1 where c5 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(8)))
  }

  test("Null Select from Measure bigint") {
    checkAnswer(
      sql("select c1 from measure_null_tab1 where c4 is null"),
      Seq(Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab1 where c4 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(7), Row(6), Row(10)))
  }

  test("Null Select from Measure smallint") {
    checkAnswer(
      sql("select c1 from measure_null_tab1 where c3 is null"),
      Seq(Row(10), Row(1), Row(2), Row(3), Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_null_tab1 where c3 is not null"),
      Seq(Row(6), Row(7), Row(8), Row(9)))
  }

  test("Null Select from Measure boolean multiple load") {
    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c6 is null"),
      Seq(Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9),
        Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c6 is not null"),
      Seq(Row(1), Row(6), Row(10), Row(1), Row(6), Row(10)))
  }

  test("Null Select from Measure Short Multiple load") {
    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c5 is null"),
      Seq(Row(10), Row(6), Row(7), Row(9), Row(10), Row(6), Row(7), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c5 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(8), Row(1), Row(2), Row(3), Row(4), Row(5),
        Row(8)))
  }

  test("Null Select from Measure bigint multiple load") {
    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c4 is null"),
      Seq(Row(8), Row(9), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c4 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(7), Row(6), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5), Row(7), Row(6), Row(10)))
  }

  test("Null Select from Measure smallint multiple load") {
    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c3 is null"),
      Seq(Row(10), Row(1), Row(2), Row(3), Row(4), Row(5), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_null_tab1_multiple_load where c3 is not null"),
      Seq(Row(6), Row(7), Row(8), Row(9), Row(6), Row(7), Row(8), Row(9)))
  }

  test("Null Select from Measure boolean large col") {
    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c6 is null"),
      Seq(Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9),
        Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c12 is null"),
      Seq(Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9),
        Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c18 is null"),
      Seq(Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9),
        Row(2), Row(3), Row(4), Row(5), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c6 is not null"),
      Seq(Row(1), Row(6), Row(10), Row(1), Row(6), Row(10)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c12 is not null"),
      Seq(Row(1), Row(6), Row(10), Row(1), Row(6), Row(10)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c18 is not null"),
      Seq(Row(1), Row(6), Row(10), Row(1), Row(6), Row(10)))
  }

  test("Null Select from Measure Short large col") {
    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c5 is null"),
      Seq(Row(10), Row(6), Row(7), Row(9), Row(10), Row(6), Row(7), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c11 is null"),
      Seq(Row(10), Row(6), Row(7), Row(9), Row(10), Row(6), Row(7), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c17 is null"),
      Seq(Row(10), Row(6), Row(7), Row(9), Row(10), Row(6), Row(7), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c5 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(8), Row(1), Row(2), Row(3), Row(4), Row(5),
        Row(8)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c11 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(8), Row(1), Row(2), Row(3), Row(4), Row(5),
        Row(8)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c17 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(8), Row(1), Row(2), Row(3), Row(4), Row(5),
        Row(8)))
  }

  test("Null Select from Measure bigint large col") {
    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c4 is null"),
      Seq(Row(8), Row(9), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c10 is null"),
      Seq(Row(8), Row(9), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c16 is null"),
      Seq(Row(8), Row(9), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c4 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(7), Row(6), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5), Row(7), Row(6), Row(10)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c10 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(7), Row(6), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5), Row(7), Row(6), Row(10)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c16 is not null"),
      Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(7), Row(6), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5), Row(7), Row(6), Row(10)))
  }

  test("Null Select from Measure smallint large col") {
    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c3 is null"),
      Seq(Row(10), Row(1), Row(2), Row(3), Row(4), Row(5), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c9 is null"),
      Seq(Row(10), Row(1), Row(2), Row(3), Row(4), Row(5), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c15 is null"),
      Seq(Row(10), Row(1), Row(2), Row(3), Row(4), Row(5), Row(10), Row(1), Row(2), Row(3),
        Row(4), Row(5)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c3 is not null"),
      Seq(Row(6), Row(7), Row(8), Row(9), Row(6), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c9 is not null"),
      Seq(Row(6), Row(7), Row(8), Row(9), Row(6), Row(7), Row(8), Row(9)))

    checkAnswer(
      sql("select c1 from measure_null_tab2_1Byte_Column where c15 is not null"),
      Seq(Row(6), Row(7), Row(8), Row(9), Row(6), Row(7), Row(8), Row(9)))
  }



  override def afterAll {
    sql("drop table measure_null_tab1")
    sql("drop table IF EXISTS measure_null_tab1_multiple_load")
    sql("drop table IF EXISTS measure_null_tab2_1Byte_Column")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}

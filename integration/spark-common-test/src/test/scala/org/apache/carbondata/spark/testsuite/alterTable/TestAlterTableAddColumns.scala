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

package org.apache.carbondata.spark.testsuite.alterTable

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestAlterTableAddColumns extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
  }

  private def testAddColumnForComplexTable(): Unit = {
    val tableName = "test_add_column_for_complex_table"
    sql(s"""DROP TABLE IF EXISTS ${ tableName }""")
    sql(
      s"""
        | CREATE TABLE IF NOT EXISTS ${ tableName }(id INT, name STRING, file array<array<float>>,
        | city STRING, salary FLOAT, ls STRING, map_column map<short,int>, struct_column struct<s:short>)
        | STORED AS carbondata
        | TBLPROPERTIES('sort_columns'='name', 'SORT_SCOPE'='LOCAL_SORT', 'LONG_STRING_COLUMNS'='ls',
        | 'LOCAL_DICTIONARY_ENABLE'='true', 'LOCAL_DICTIONARY_INCLUDE'='city')
      """.stripMargin)
    sql(
      s"""
        | insert into table ${tableName} values
        | (1, 'name1', array(array(1.1, 2.1), array(1.1, 2.1)), 'city1', 40000.0, '${ ("123" * 12000) }', map(1,1), named_struct('s',1)),
        | (2, 'name2', array(array(1.2, 2.2), array(1.2, 2.2)), 'city2', 50000.0, '${ ("456" * 12000) }', map(2,2), named_struct('s',2)),
        | (3, 'name3', array(array(1.3, 2.3), array(1.3, 2.3)), 'city3', 60000.0, '${ ("789" * 12000) }', map(3,3), named_struct('s',3))
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(3)))
    checkAnswer(sql(s"select name, city from ${ tableName } where id = 3"), Seq(Row("name3", "city3")))

    sql(s"""desc formatted ${tableName}""").show(100, false)
    sql(s"""alter table ${tableName} add columns (add_column string) TBLPROPERTIES('LOCAL_DICTIONARY_INCLUDE'='add_column')""")
    sql(s"""ALTER TABLE ${tableName} SET TBLPROPERTIES('SORT_COLUMNS'='id, add_column, city')""")
    sql(s"""desc formatted ${tableName}""").show(100, false)

    sql(
      s"""
        | insert into table ${tableName} values
        | (4, 'name4', array(array(1.4, 2.4), array(1.4, 2.4)), 'city4', 70000.0, '${ ("123" * 12000) }', map(4,4), named_struct('s',4), 'add4'),
        | (5, 'name5', array(array(1.5, 2.5), array(1.5, 2.5)), 'city5', 80000.0, '${ ("456" * 12000) }', map(5,5), named_struct('s',5), 'add5'),
        | (6, 'name6', array(array(1.6, 2.6), array(1.6, 2.6)), 'city6', 90000.0, '${ ("789" * 12000) }', map(6,6), named_struct('s',6), 'add6')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(6)))
    checkAnswer(sql(s"""select add_column, id, city, name from ${ tableName } where id = 6"""),
        Seq(Row("add6", 6, "city6", "name6")))

    sql(s"""desc formatted ${tableName}""").show(100, false)
    sql(s"""ALTER TABLE ${ tableName } DROP COLUMNS (city)""")
    sql(s"""desc formatted ${tableName}""").show(100, false)

    sql(
      s"""
        | insert into table ${tableName} values
        | (7, 'name7', array(array(1.4, 2.4), array(1.4, 2.4)), 70000.0, '${ ("123" * 12000) }', map(7,7), named_struct('s',7), 'add7'),
        | (8, 'name8', array(array(1.5, 2.5), array(1.5, 2.5)), 80000.0, '${ ("456" * 12000) }', map(8,8), named_struct('s',8), 'add8'),
        | (9, 'name9', array(array(1.6, 2.6), array(1.6, 2.6)), 90000.0, '${ ("789" * 12000) }', map(9,9), named_struct('s',9), 'add9')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(9)))
    checkAnswer(sql(s"""select id, add_column, name from ${ tableName } where id = 9"""), Seq(Row(9, "add9", "name9")))

    sql(s"""desc formatted ${tableName}""").show(100, false)
    sql(s"""alter table ${tableName} add columns (add_column_ls string) TBLPROPERTIES('LONG_STRING_COLUMNS'='add_column_ls')""")
    sql(s"""desc formatted ${tableName}""").show(100, false)

    sql(
      s"""
        | insert into table ${tableName} values
        | (10, 'name10', array(array(1.4, 2.4), array(1.4, 2.4)), 100000.0, '${ ("123" * 12000) }', map(4,4), named_struct('s',4), 'add4', '${ ("999" * 12000) }'),
        | (11, 'name11', array(array(1.5, 2.5), array(1.5, 2.5)), 110000.0, '${ ("456" * 12000) }', map(5,5), named_struct('s',5), 'add5', '${ ("888" * 12000) }'),
        | (12, 'name12', array(array(1.6, 2.6), array(1.6, 2.6)), 120000.0, '${ ("789" * 12000) }', map(6,6), named_struct('s',6), 'add6', '${ ("777" * 12000) }')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(12)))
    checkAnswer(sql(s"""select id, name, file, add_column_ls, map_column, struct_column from ${ tableName } where id = 10"""),
        Seq(Row(10, "name10",
            mutable.WrappedArray.make(Array(mutable.WrappedArray.make(Array(1.4, 2.4)), mutable.WrappedArray.make(Array(1.4, 2.4)))),
            ("999" * 12000), Map(4 -> 4), Row(4))))

    sql(s"""DROP TABLE IF EXISTS ${ tableName }""")
  }

  test("[CARBONDATA-3596] Fix exception when execute load data command or select sql on a table which includes complex columns after execute 'add column' command") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // test for not vector reader
    testAddColumnForComplexTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // test for vector reader
    testAddColumnForComplexTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
  }
}

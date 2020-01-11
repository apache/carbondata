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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * test functionality for alter table with table comment
  */
class TestAlterTableWithTableComment extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop table if exists alterTableWithTableComment")
    sql("drop table if exists alterTableWithoutTableComment")
    sql("drop table if exists alterTableUnsetTableComment")
  }

  test("test add table comment using alter table set ") {
    sql(
      s"""
         | create table alterTableWithTableComment(
         | id int,
         | name string
         | )
         | STORED AS carbondata
       """.stripMargin
    )

    val create_result = sql("describe formatted alterTableWithTableComment")

    checkExistence(create_result, true, "Comment")
    checkExistence(create_result, false, "This is table comment")

    sql(
      s"""
         | alter table alterTableWithTableComment
         | SET TBLPROPERTIES (
         | 'comment'='This table comment is added by alter table'
         | )
       """.stripMargin
    )

    val alter_result = sql("describe formatted alterTableWithTableComment")

    checkExistence(alter_result, true, "Comment")
    checkExistence(alter_result, true, "This table comment is added by alter table")
  }

  test("test modifiy table comment using alter table set ") {
    sql(
      s"""
         | create table alterTableWithoutTableComment(
         | id int,
         | name string
         | comment "This is table comment"
         | )
         | STORED AS carbondata
       """.stripMargin
    )

    sql(
      s"""
         | alter table alterTableWithoutTableComment
         | SET TBLPROPERTIES (
         | 'comment'='This table comment is modified by alter table set'
         | )
       """.stripMargin
    )

    val alter_result = sql("describe formatted alterTableWithoutTableComment")

    checkExistence(alter_result, true, "Comment")
    checkExistence(alter_result, true, "This table comment is modified by alter table set")
  }

  test("test remove table comment using alter table unset ") {
    sql(
      s"""
         | create table alterTableUnsetTableComment(
         | id int,
         | name string
         | )
         | comment "This is table comment"
         | STORED AS carbondata
       """.stripMargin
    )

    val create_result = sql("describe formatted alterTableUnsetTableComment")

    checkExistence(create_result, true, "Comment")
    checkExistence(create_result, true, "This is table comment")

    sql(
      s"""
         | alter table alterTableUnsetTableComment
         | UNSET TBLPROPERTIES ('comment')
       """.stripMargin
    )

    val alter_result = sql("describe formatted alterTableUnsetTableComment")

    checkExistence(alter_result, true, "Comment")
    checkExistence(alter_result, false, "This is table comment")
  }

  override def afterAll: Unit = {
    sql("use default")
    sql("drop table if exists alterTableWithTableComment")
    sql("drop table if exists alterTableWithoutTableComment")
    sql("drop table if exists alterTableUnsetTableComment")
  }

}

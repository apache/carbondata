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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for AlterTableTestCase to verify all scenerios
 */

class TableCommentAlterTableTestCase extends QueryTest with BeforeAndAfterAll {

  // scalastyle:off lineLength
  // Check create table with table comment
  test("TableCommentAlterTable_001_01", Include) {
    sql(s"""drop table if exists table_comment""").collect
    sql(s"""create table table_comment(id int, name string) comment "This is table comment" STORED AS carbondata""").collect
    val result = sql("describe formatted table_comment")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "This is table comment")
  }

  // Check create table with comment as numeric/alphanumeric/special symbols
  test("TableCommentAlterTable_001_02", Include) {
    sql(s"""drop table if exists table_comment_special""").collect
    sql(s"""create table table_comment_special(id int, name string) comment "3432432 @This $$$$ is table comment #$$%^&*@# 34234" STORED AS carbondata""").collect
    val result = sql("describe formatted table_comment_special")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "3432432 @This $$ is table comment #$%^&*@# 34234")
  }

  // Check create table with comment as blank
  test("TableCommentAlterTable_001_03", Include) {
    sql(s"""drop table if exists table_comment_alphabets""").collect
    sql(s"""create table table_comment_alphabets(id int, name string) comment "" STORED AS carbondata""").collect
    val result = sql("describe formatted table_comment")
    checkExistence(result, true, "Comment")
  }

  // Check create table with comment as high values
  test("TableCommentAlterTable_001_04", Include) {
    sql(s"""drop table if exists table_comment_high""").collect
    sql(s"""create table table_comment_high(id int, name string) comment "This is table comment jgfhdsjgfhsjdgjfsdgfbcfgr763  23x4bt23n8z30bt20t49ct4cb3g93t53945ctbugebgec4c3trbcu4grehrvsgyabrgyuwarcnrwbrcwe rwergrburygwae rwaeuigrwawewrcargw7aer wr766tznzYQEqoie weqeqewqeqweqweqweewqeqwe" STORED AS carbondata""").collect
    val result = sql("describe formatted table_comment_high")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "This is table comment jgfhdsjgfhsjdgjfsdgfbcfgr763  23x4bt23n8z30bt20t49ct4cb3g93t53945ctbugebgec4c3trbcu4grehrvsgyabrgyuwarcnrwbrcwe rwergrburygwae rwaeuigrwawewrcargw7aer wr766tznzYQEqoie weqeqewqeqweqweqweewqeqwe")
  }

  // Check create table with comment in the table properties
  test("TableCommentAlterTable_001_05", Include) {
    sql(s"""drop table if exists table_comment_asprop""").collect
    sql(s"""create table table_comment_asprop (imei string,num bigint,game double,date timestamp,Profit decimal(3,2)) STORED AS carbondata TBLPROPERTIES('comment'='This is table comment..!!')""").collect
    val result = sql("describe formatted table_comment")
    checkExistence(result, true, "Comment")
    checkExistence(result, false, "This is table comment..!!")
  }

  // Check create table with comment after stored as clause
  // This behavior is okay in Spark-2.3 but may fail with earlier spark versions.
  test("TableCommentAlterTable_001_06", Include) {
    sql("create table table_comment_afterstoredby (id int, name string) STORED AS carbondata comment 'This is table comment'")
  }

  // Check the comment by "describe formatted"
  test("TableCommentAlterTable_001_07", Include) {
    sql(s"""drop table if exists table_comment""").collect
    sql(s"""create table table_comment(id int, name string) comment "This is table comment" STORED AS carbondata""").collect
    val result = sql("describe formatted table_comment")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "This is table comment")
  }

  // Check SET Command by using alter command
  test("TableCommentAlterTable_001_08", Include) {
    sql(s"""drop table if exists table_comment_set""").collect
    sql(s"""create table table_comment_set (id int, name string) STORED AS carbondata""").collect
    sql(s"""alter table table_comment_set SET TBLPROPERTIES ('comment'='This table comment is SET by alter table set')""").collect
    val result = sql("describe formatted table_comment_set")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "This table comment is SET by alter table set")
  }

  // Check UNSET Command by using alter command
  test("TableCommentAlterTable_001_09", Include) {
    sql(s"""drop table if exists table_comment_unset""").collect
    sql(s"""create table table_comment_unset(id int, name string) comment "This is table comment" STORED AS carbondata""").collect
    val result1 = sql("describe formatted table_comment_unset")
    checkExistence(result1, true, "Comment")
    checkExistence(result1, true, "This is table comment")

    sql(s"""alter table table_comment_unset UNSET TBLPROPERTIES IF EXISTS ('comment')""").collect
    val result2 = sql("describe formatted table_comment_unset")
    checkExistence(result2, true, "Comment")
    checkExistence(result2, false, "This is table comment")
  }

  // Check RENAME by using alter command
  test("TableCommentAlterTable_001_10", Include) {
    sql(s"""drop table if exists table_comment_rename1""").collect
    sql(s"""create table table_comment_rename1 (id int, name string) STORED AS carbondata""").collect
    sql(s"""alter table table_comment_rename1 rename to table_comment_rename2""").collect
    val result = sql("describe formatted table_comment_rename2")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "table_comment")
  }

  // Check ADD Columns by using alter command
  test("TableCommentAlterTable_001_11", Include) {
    sql(s"""drop table if exists table_add_column """).collect
    sql(s"""create table table_add_column (name string, country string, upd_time timestamp) STORED AS carbondata""").collect
    sql(s"""alter table table_add_column add columns (id bigint)""").collect
    val result = sql("describe table_add_column ")
    checkExistence(result, true, "id")
    checkExistence(result, true, "bigint")
  }

  // Check CHANGE DATA TYPE by using alter command
  test("TableCommentAlterTable_001_12", Include) {
    sql(s"""drop table if exists table_change_datatype""").collect
    sql(s"""create table table_change_datatype (name string, id decimal(3,2),country string) STORED AS carbondata""").collect
    sql(s"""alter table table_change_datatype change id id decimal(10,4)""").collect
    val result = sql("describe formatted table_change_datatype")
    checkExistence(result, true, "Comment")
    checkExistence(result, true, "decimal(10,4)")
  }
  // scalastyle:on lineLength
}

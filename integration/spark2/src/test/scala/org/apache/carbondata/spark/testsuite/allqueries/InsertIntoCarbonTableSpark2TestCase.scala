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
package org.apache.carbondata.spark.testsuite.allqueries

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class InsertIntoCarbonTableSpark2TestCase extends Spark2QueryTest with BeforeAndAfterAll {
  override def beforeAll: Unit = {
    sql("drop table if exists OneRowTable")
    sql("drop table if exists tempparquet1")
    sql("drop table if exists tempparquet2")
    sql("DROP database if exists inserttemptableDB cascade")
  }

  test("insert select one row") {
    sql("create table OneRowTable(col1 string, col2 string, col3 int, col4 double) stored by 'carbondata'")
    sql("insert into OneRowTable select '0.1', 'a.b', 1, 1.2")
    checkAnswer(sql("select * from OneRowTable"), Seq(Row("0.1", "a.b", 1, 1.2)))
  }

  test("update into parquet temp table") {
    val tempLocation = Files.createTempDirectory("tempparquet1")
    sql("create database if not exists inserttemptableDB")
    sql("use inserttemptableDB")
    sql("drop table if exists tempparquet1")
    try {
      sql(
        "create temporary table tempparquet1(imei string,deviceInformationId int) using PARQUET " +
        s"options(path='$tempLocation')")
      sql("create table tempparquet1(imei string,deviceInformationId int) stored by 'carbondata'")
      sql("insert into tempparquet1 values('qqqq',257)")
      sql("insert into tempparquet1 values('qqqq',368)")
      sql("insert into inserttemptableDB.tempparquet1 select 'aaaa',12")
      sql("insert into inserttemptableDB.tempparquet1 select 'aaaa',13")
      sql("update inserttemptableDB.tempparquet1 set(imei)=('bbbb') where deviceInformationId = 12")
        .show()
      sql("select * from inserttemptableDB.tempparquet1").show(200,false)
      checkAnswer(sql("select * from inserttemptableDB.tempparquet1"),
        Seq(Row("bbbb", 12), Row("aaaa", 13)))
    } catch {
      case e: Exception =>
        System.out.println("message: "+e.getMessage)
        assert(false)
    } finally {
      sql("USE default")
      FileUtils.deleteQuietly(tempLocation.toFile)

    }
  }

  test("delete in parquet temp table") {
    val tempLocation = Files.createTempDirectory("tempparquet2")
    sql("create database if not exists inserttemptableDB")
    sql("use inserttemptableDB")
    sql("drop table if exists tempparquet2")
    try {
      sql(
        "create temporary table tempparquet2(imei string,deviceInformationId int) using PARQUET " +
        s"options(path='$tempLocation')")
      sql("create table tempparquet2(imei string,deviceInformationId int) stored by 'carbondata'")
      sql("insert into tempparquet2 values('qqqq',257)")
      sql("insert into tempparquet2 values('qqqq',368)")
      sql("insert into inserttemptableDB.tempparquet2 select 'aaaa',12")
      sql("insert into inserttemptableDB.tempparquet2 select 'aaaa',13")
      sql("delete from inserttemptableDB.tempparquet2 t1 where t1.deviceInformationId=12")
        .show()
      sql("select * from inserttemptableDB.tempparquet2").show(200,false)
      checkAnswer(sql("select * from inserttemptableDB.tempparquet2"),
        Seq(Row("aaaa", 13)))
    } catch {
      case e: Exception =>
        System.out.println("message: "+e.getMessage)
        assert(false)
    } finally {
      sql("USE default")
      FileUtils.deleteQuietly(tempLocation.toFile)
    }
  }

  override def afterAll {
    sql("drop table if exists OneRowTable")
    sql("drop table if exists tempparquet1")
    sql("drop table if exists tempparquet2")
    sql("DROP database if exists inserttemptableDB cascade")
  }
}

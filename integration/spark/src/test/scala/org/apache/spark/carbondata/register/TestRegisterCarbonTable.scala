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
package org.apache.spark.carbondata.register

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 *
 */
class TestRegisterCarbonTable extends QueryTest with BeforeAndAfterEach {

  override def beforeEach {
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
  }

  def restoreData(dblocation: String, tableName: String) = {
    val destination = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val source = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
      FileUtils.deleteDirectory(new File(source))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data restore failed.")
    } finally {

    }
  }
  def backUpData(dblocation: String, database: Option[String], tableName: String) = {
    val source = CarbonEnv.getTablePath(database, tableName)(sqlContext.sparkSession)
    val destination = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data backup failed.")
    }
  }

  test("register tables test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      checkAnswer(sql("select count(*) from carbontable"), Row(1))
      checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    }
  }

  test("register table test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      checkAnswer(sql("select count(*) from carbontable"), Row(1))
      checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    }
  }

  test("Update operation on carbon table should pass after registration or refresh") {
    sql(s"create database carbon1 location '$dblocation'")
    sql("use carbon1")
    sql("drop table if exists carbontable")
    sql("""create table carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dblocation, Some("carbon1"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      // update operation
      sql("""update carbon1.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
      sql("""update carbon1.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'b'""").show()
      checkAnswer(
        sql("""select c1,c2,c3,c5 from carbon1.carbontable"""),
        Seq(Row("a", 2, "aa", "aaa"), Row("b", 2, "bb", "bbb"))
      )
    }
  }

  test("Update operation on carbon table") {
    sql(s"create database carbon1 location '$dblocation'")
    sql("use carbon1")
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED AS carbondata
      """)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    backUpData(dblocation, Some("carbon1"), "automerge")
    sql("drop table automerge")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "automerge")
      sql("refresh table automerge")
      // update operation
      sql("""update carbon1.automerge d  set (d.id) = (d.id + 1) where d.id > 2""").show()
      checkAnswer(
        sql("select count(*) from automerge"),
        Seq(Row(6))
      )
    }
  }

  test("Delete operation on carbon table") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      // delete operation
      sql("""delete from carbontable where c3 = 'aa'""").show
      checkAnswer(
        sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
        Seq(Row("b", 1, "bb", "bbb"))
      )
      sql("drop table carbontable")
    }
  }

  test("Alter table add column test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      sql("Alter table carbontable add columns(c4 string) " +
          "TBLPROPERTIES('DEFAULT.VALUE.c4'='def')")
      checkAnswer(
        sql("""select c1,c2,c3,c5,c4 from carbon.carbontable"""),
        Seq(Row("a", 1, "aa", "aaa", "def"), Row("b", 1, "bb", "bbb", "def"))
      )
      sql("drop table carbontable")
    }
  }

  test("Alter table change column datatype test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      sql("Alter table carbontable change c2 c2 long")
      checkAnswer(
        sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
        Seq(Row("a", 1, "aa", "aaa"), Row("b", 1, "bb", "bbb"))
      )
      sql("drop table carbontable")
    }
  }

  test("Alter table drop column test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dblocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dblocation, "carbontable")
      sql("refresh table carbontable")
      sql("Alter table carbontable drop columns(c2)")
      checkAnswer(
        sql("""select * from carbon.carbontable"""),
        Seq(Row("a", "aa", "aaa"), Row("b", "bb", "bbb"))
      )
      sql("drop table carbontable")
    }
  }

  override def afterEach {
    sql("use default")
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
  }
}

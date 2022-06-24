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
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.view.rewrite.TestUtil

/**
 *
 */
class TestRegisterCarbonTable extends QueryTest with BeforeAndAfterEach {

  override def beforeEach {
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
    sql("set carbon.enable.mv = true")
  }

  test("register tables test") {
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      sql("refresh table carbontable")
      checkAnswer(sql("select count(*) from carbontable"), Row(1))
      checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    }
  }

  test("register table test") {
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      sql("refresh table carbontable")
      checkAnswer(sql("select count(*) from carbontable"), Row(1))
      checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    }
  }

  test("Update operation on carbon table should pass after registration or refresh") {
    sql(s"create database carbon1 location '$dbLocation'")
    sql("use carbon1")
    sql("drop table if exists carbontable")
    sql("""create table carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dbLocation, Some("carbon1"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      sql("refresh table carbontable")
      // update operation
      sql("""update carbon1.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").collect()
      sql("""update carbon1.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'b'""").collect()
      checkAnswer(
        sql("""select c1,c2,c3,c5 from carbon1.carbontable"""),
        Seq(Row("a", 2, "aa", "aaa"), Row("b", 2, "bb", "bbb"))
      )
    }
  }

  test("Update operation on carbon table") {
    sql(s"create database carbon1 location '$dbLocation'")
    sql("use carbon1")
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED AS carbondata
      """)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    backUpData(dbLocation, Some("carbon1"), "automerge")
    sql("drop table automerge")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "automerge")
      sql("refresh table automerge")
      // update operation
      sql("""update carbon1.automerge d  set (d.id) = (d.id + 1) where d.id > 2""").collect()
      checkAnswer(
        sql("select count(*) from automerge"),
        Seq(Row(6))
      )
    }
  }

  test("Delete operation on carbon table") {
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      sql("refresh table carbontable")
      // delete operation
      sql("""delete from carbontable where c3 = 'aa'""").collect()
      checkAnswer(
        sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
        Seq(Row("b", 1, "bb", "bbb"))
      )
      sql("drop table carbontable")
    }
  }

  test("Alter table add column test") {
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
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
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
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
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      sql("refresh table carbontable")
      sql("Alter table carbontable drop columns(c2)")
      checkAnswer(
        sql("""select * from carbon.carbontable"""),
        Seq(Row("a", "aa", "aaa"), Row("b", "bb", "bbb"))
      )
      sql("drop table carbontable")
    }
  }

  test("test register table with mv") {
    sql(s"create database carbon location '$dbLocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (
        |c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("create materialized view mv1 as select avg(c2),c3 from carbontable group by c3")
    backUpData(dbLocation, Some("carbon"), "carbontable")
    backUpData(dbLocation, Some("carbon"), "mv1")
    val source = dbLocation + CarbonCommonConstants.FILE_SEPARATOR + "_system" +
                 CarbonCommonConstants.FILE_SEPARATOR + "mv_schema.mv1"
    val destination = dbLocation + "_back" + CarbonCommonConstants.FILE_SEPARATOR + "mv_schema.mv1"
    backUpMvSchema(source, destination)
    sql("drop table carbontable")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      restoreData(dbLocation, "carbontable")
      restoreData(dbLocation, "mv1")
      backUpMvSchema(destination, source)
      sql("refresh table carbontable")
      sql("refresh table mv1")
      sql("refresh materialized view mv1")
      checkAnswer(sql("select count(*) from carbontable"), Row(1))
      checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
      val df = sql("select avg(c2),c3 from carbontable group by c3")
      assert(TestUtil.verifyMVHit(df.queryExecution.optimizedPlan, "mv1"))
    }
  }

  private def backUpMvSchema(source: String, destination: String) = {
    try {
      FileUtils.copyFile(new File(source), new File(destination))
    } catch {
      case e: Exception =>
        throw new IOException("Mv schema file backup/restore failed.")
    }
  }

  override def afterEach {
    sql("set carbon.enable.mv = false")
    sql("use default")
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
  }
}

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
package org.apache.carbondata.cluster.sdv.register

import java.io.IOException

import org.scalatest.BeforeAndAfterAll
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.spark.exception.ProcessMetaDataException

/**
 *
 */
class TestRegisterCarbonTable extends QueryTest with BeforeAndAfterAll {
  var dbLocationCustom = TestQueryExecutor.warehouse +
                         CarbonCommonConstants.FILE_SEPARATOR + "dbName"
  override def beforeAll {
    sql("drop database if exists carbon cascade")
  }

  def restoreData(dbLocationCustom: String, tableName: String) = {
    val destination = dbLocationCustom + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val source = dbLocationCustom+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      val fs = new Path(source).getFileSystem(FileFactory.getConfiguration)
      val sourceFileStatus = fs.getFileStatus(new Path(source))
      FileUtil.copy(fs,
        sourceFileStatus,
        fs,
        new Path(destination),
        true,
        true,
        FileFactory.getConfiguration)
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data restore failed.")
    } finally {

    }
  }
  def backUpData(dbLocationCustom: String, tableName: String) = {
    val source = dbLocationCustom + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val destination = dbLocationCustom+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      val fs = new Path(source).getFileSystem(FileFactory.getConfiguration)
      val sourceFileStatus = fs.getFileStatus(new Path(source))
      FileUtil.copy(fs,
        sourceFileStatus,
        fs,
        new Path(destination),
        false,
        true,
        FileFactory.getConfiguration)
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data backup failed.")
    }
  }

  test("register tables test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
  }

  test("register table test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
  }

  test("Update operation on carbon table should pass after registration or refresh") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    // update operation
    sql("""update carbon.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    sql("""update carbon.carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'b'""").show()
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a", 2, "aa", "aaa"), Row("b", 2, "bb", "bbb"))
    )
  }

  test("Delete operation on carbon table") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    // delete operation
    sql("""delete from carbontable where c3 = 'aa'""").show
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("b", 1, "bb", "bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table add column test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    sql("Alter table carbontable add columns(c4 string) " +
        "TBLPROPERTIES('DEFAULT.VALUE.c4'='def')")
    checkAnswer(
      sql("""select c1,c2,c3,c5,c4 from carbon.carbontable"""),
      Seq(Row("a", 1, "aa", "aaa", "def"), Row("b", 1, "bb", "bbb", "def"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change column datatype test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    sql("Alter table carbontable change c2 c2 long")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a", 1, "aa", "aaa"), Row("b", 1, "bb", "bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table drop column test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dbLocationCustom'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    if (!CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore.isReadFromHiveMetaStore) {
      backUpData(dbLocationCustom, "carbontable")
      sql("drop table carbontable")
      restoreData(dbLocationCustom, "carbontable")
      sql("refresh table carbontable")
    }
    sql("Alter table carbontable drop columns(c2)")
    checkAnswer(
      sql("""select * from carbon.carbontable"""),
      Seq(Row("a", "aa", "aaa"), Row("b", "bb", "bbb"))
    )
    sql("drop table carbontable")
  }

  override def afterAll {
    sql("use default")
    sql("drop database if exists carbon cascade")
  }
}

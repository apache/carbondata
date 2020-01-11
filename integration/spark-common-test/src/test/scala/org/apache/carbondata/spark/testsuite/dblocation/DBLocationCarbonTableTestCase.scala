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
package org.apache.carbondata.spark.testsuite.dblocation

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterEach

/**
 *
 */
class DBLocationCarbonTableTestCase extends QueryTest with BeforeAndAfterEach {

  def getMdtFileAndType() = {
    // if mdt file path is configured then take configured path else take default path
    val configuredMdtPath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT).trim
    var timestampFile = configuredMdtPath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    timestampFile = CarbonUtil.checkAndAppendFileSystemURIScheme(timestampFile)
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)

  }

  override def beforeEach {
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
  }

  //TODO fix this test case
  test("Update operation on carbon table with insert into") {
    sql(s"create database carbon2 location '$dblocation'")
    sql("use carbon2")
    sql("""create table carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    // update operation
    sql("""update carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'a'""").show()
    sql("""update carbontable d  set (d.c2) = (d.c2 + 1) where d.c1 = 'b'""").show()
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbontable"""),
      Seq(Row("a",2,"aa","aaa"),Row("b",2,"bb","bbb"))
    )
    sql("drop database if exists carbon2 cascade")
  }

  test("create and drop database test") {
    sql(s"create database carbon location '$dblocation'")
    sql("drop database if exists carbon cascade")
  }

  test("create two databases at same table") {
    sql(s"create database carbon location '$dblocation'")
    try {
      sql(s"create database carbon1 location '$dblocation'")
    } catch {
      case e: AnalysisException =>
        assert(true)
    }
  }

  test("create table and load data") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/dblocation/test.csv' INTO table carbon.carbontable""")
    checkAnswer(sql("select count(*) from carbontable"), Row(5))
  }

  test("create table and insert data") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
  }

  test("create table and 2 times data load") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'aa','aaa'")
    checkAnswer(sql("select count(*) from carbontable"), Row(2))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a"), Row("b")))
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
    // update operation
    sql("""update carbon1.automerge d  set (d.id) = (d.id + 1) where d.id > 2""").show()
    checkAnswer(
      sql("select count(*) from automerge"),
      Seq(Row(6))
    )
    //    sql("drop table carbontable")
  }

  test("Delete operation on carbon table") {
    sql(s"create database carbon1 location '$dblocation'")
    sql("use carbon1")
    sql("""create table carbon1.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    // delete operation
    sql("""delete from carbontable where c3 = 'aa'""").show
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon1.carbontable"""),
      Seq(Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table add column test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns(c4 string) " +
        "TBLPROPERTIES('DEFAULT.VALUE.c4'='def')")
    checkAnswer(
      sql("""select c1,c2,c3,c5,c4 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa","def"), Row("b",1,"bb","bbb","def"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change column datatype test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable change c2 c2 long")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa"), Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change dataType with sort column after adding measure column test"){
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS' = 'c2')
        |""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns (c6 int)")
    sql("Alter table carbontable change c2 c2 bigint")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa"), Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change dataType with sort column after adding date datatype with default value test"){
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS' = 'c2')
        |""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns (dateData date) TBLPROPERTIES('DEFAULT.VALUE.dateData' = '1999-01-01')")
    sql("Alter table carbontable change c2 c2 bigint")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa"), Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change dataType with sort column after adding dimension column with default value test"){
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS' = 'c2')
        |""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns (name String) TBLPROPERTIES('DEFAULT.VALUE.name' = 'hello')")
    sql("Alter table carbontable change c2 c2 bigint")
    checkAnswer(
      sql("""select c1,c2,c3,c5,name from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa","hello"), Row("b",1,"bb","bbb","hello"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change dataType with sort column after rename test"){
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql(
      """create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string)
        |STORED AS carbondata
        |TBLPROPERTIES('SORT_COLUMNS' = 'c2')
        |""".stripMargin)
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns (name String) TBLPROPERTIES('DEFAULT.VALUE.name' = 'hello')")
    sql("Alter table carbontable rename to carbontable1")
    sql("Alter table carbontable1 change c2 c2 bigint")
    checkAnswer(
      sql("""select c1,c2,c3,c5,name from carbon.carbontable1"""),
      Seq(Row("a",1,"aa","aaa","hello"), Row("b",1,"bb","bbb","hello"))
    )
    sql("drop table if exists carbontable")
    sql("drop table if exists carbontable1")
  }

  test("Alter table drop column test") {
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable drop columns(c2)")
    checkAnswer(
      sql("""select * from carbon.carbontable"""),
      Seq(Row("a","aa","aaa"), Row("b","bb","bbb"))
    )
    sql("drop table carbontable")
  }

  override def afterEach {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT)
    sql("use default")
    sql("drop database if exists carbon cascade")
    sql("drop database if exists carbon1 cascade")
    sql("drop database if exists carbon2 cascade")
  }
}

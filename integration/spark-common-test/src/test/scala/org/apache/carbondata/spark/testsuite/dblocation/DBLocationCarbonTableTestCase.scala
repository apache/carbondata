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
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 *
 */
class DBLocationCarbonTableTestCase extends QueryTest with BeforeAndAfterAll {

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

  override def beforeAll {
    sql("drop database if exists carbon cascade")
  }

  //TODO fix this test case
  test("Update operation on carbon table with insert into") {
    sql("drop database if exists carbon2 cascade")
    sql(s"create database carbon2 location '$dblocation'")
    sql("use carbon2")
    sql("""create table carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
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
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/dblocation/test.csv' INTO table carbon.carbontable""")
    checkAnswer(sql("select count(*) from carbontable"), Row(5))
  }

  test("create table and insert data") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
  }

  test("create table and 2 times data load") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'aa','aaa'")
    checkAnswer(sql("select count(*) from carbontable"), Row(2))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a"), Row("b")))
  }


  test("Update operation on carbon table") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql(
      """
         CREATE TABLE automerge(id int, name string, city string, age int)
         STORED BY 'org.apache.carbondata.format'
      """)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table automerge")
    // update operation
    sql("""update carbon.automerge d  set (d.id) = (d.id + 1) where d.id > 2""").show()
    checkAnswer(
      sql("select count(*) from automerge"),
      Seq(Row(6))
    )
    //    sql("drop table carbontable")
  }

  test("Delete operation on carbon table") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    // delete operation
    sql("""delete from carbontable where c3 = 'aa'""").show
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table add column test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable add columns(c4 string) " +
        "TBLPROPERTIES('DICTIONARY_EXCLUDE'='c4', 'DEFAULT.VALUE.c4'='def')")
    checkAnswer(
      sql("""select c1,c2,c3,c5,c4 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa","def"), Row("b",1,"bb","bbb","def"))
    )
    sql("drop table carbontable")
  }

  test("Alter table change column datatype test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable change c2 c2 long")
    checkAnswer(
      sql("""select c1,c2,c3,c5 from carbon.carbontable"""),
      Seq(Row("a",1,"aa","aaa"), Row("b",1,"bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("Alter table drop column test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("insert into carbontable select 'b',1,'bb','bbb'")
    sql("Alter table carbontable drop columns(c2)")
    checkAnswer(
      sql("""select * from carbon.carbontable"""),
      Seq(Row("a","aa","aaa"), Row("b","bb","bbb"))
    )
    sql("drop table carbontable")
  }

  test("test mdt file path with configured paths") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '$dblocation'")
    sql("use carbon")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER, "/tmp/carbondata1/carbondata2/")
    val (timestampFile, timestampFileType) = getMdtFileAndType()
    FileFactory.deleteFile(timestampFile, timestampFileType)
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("drop table carbontable")
    // perform file check
    assert(FileFactory.isFileExist(timestampFile, timestampFileType, true))

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT)
    val (timestampFile2, timestampFileType2) = getMdtFileAndType()
    FileFactory.deleteFile(timestampFile2, timestampFileType2)
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata.format'""")
    sql("drop table carbontable")
    // perform file check
    assert(FileFactory.isFileExist(timestampFile, timestampFileType, true))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT)
    sql("use default")
    sql("drop database if exists carbon cascade")
  }
}

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

package org.apache.spark.carbondata.restructure.vectorreader

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class AlterTableColumnRenameTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable()
    createTableAndLoad()
  }

  test("test only column rename operation") {
    sql("alter table rename change empname empAddress string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("empAddress"))
    assert(null == carbonTable.getColumnByName("empname"))
  }

  test("test only column rename operation with datatype change also") {
    dropTable()
    createTable()
    intercept[ProcessMetaDataException] {
      sql("alter table rename change empname empAddress Bigint")
    }
    sql("alter table rename change deptno classNo Bigint")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("classNo"))
    assert(null == carbonTable.getColumnByName("deptno"))
  }

  test("test trying to rename column which does not exists") {
    dropTable()
    createTable()
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename change carbon empAddress Bigint")
    }
    assert(ex.getMessage.contains("Column does not exist: carbon"))
  }

  test("test rename when new column name already in schema") {
    dropTable()
    createTable()
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename change empname workgroupcategoryname string")
    }
    assert(ex.getMessage.contains("New column name workgroupcategoryname already exists in table rename"))
  }

  test("column rename for different datatype"){
    dropTable()
    createTable()
    sql("alter table rename change projectenddate newDate Timestamp")
    sql("alter table rename change workgroupcategory newCategory int")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("newDate"))
    assert(null == carbonTable.getColumnByName("projectenddate"))
    assert(null != carbonTable.getColumnByName("newCategory"))
    assert(null == carbonTable.getColumnByName("workgroupcategory"))
  }

  test("query count after column rename and filter results"){
    dropTable()
    createTableAndLoad()
    val df1 = sql("select empname from rename").collect()
    val df3 = sql("select workgroupcategory from rename where empname = 'bill' or empname = 'sibi'").collect()
    sql("alter table rename change empname empAddress string")
    val df2 = sql("select empAddress from rename").collect()
    val df4 = sql("select workgroupcategory from rename where empAddress = 'bill' or empAddress = 'sibi'").collect()
    intercept[Exception] {
      sql("select empname from rename")
    }
    assert(df1.length == df2.length)
    assert(df3.length == df4.length)
  }

  test("compaction after column rename and count"){
    dropTable()
    createTableAndLoad()
    for(i <- 0 to 2) {
      loadToTable()
    }
    val df1 = sql("select empname,deptno from rename")
    sql("alter table rename change empname empAddress string")
    sql("alter table rename change deptno classNo Bigint")
    sql("alter table rename compact 'minor'")
    val df2 = sql("select empAddress,classNo from rename")
    assert(df1.count() == df2.count())
  }

  test("test rename after adding column and drop column") {
    dropTable()
    createTableAndLoad()
    sql("alter table rename add columns(newAdded string)")
    var carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("newAdded"))
    sql("alter table rename change newAdded addedRename string")
    carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("addedRename"))
    assert(null == carbonTable.getColumnByName("newAdded"))
    sql("alter table rename drop columns(addedRename)")
    carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null == carbonTable.getColumnByName("addedRename"))
    intercept[ProcessMetaDataException] {
      sql("alter table rename change addedRename test string")
    }
  }

  test("test column rename and update and insert and delete") {
    dropTable()
    createTableAndLoad()
    sql("alter table rename change empname name string")
    sql("update rename set (name) = ('joey') where workgroupcategory = 'developer'").show()
    sql("insert into rename select 20,'bill','PM','01-12-2015',3,'manager',14,'Learning',928479,'01-01-2016','30-11-2016',75,94,13547")
    val df1Count = sql("select * from rename where name = 'joey'").count
    sql("alter table rename change name empname string")
    val df2 = sql("select * from rename where empname = 'joey'")
    assert(df1Count == df2.count())
    sql("delete from rename where empname = 'joey'")
    val df3 = sql("select empname from rename")
    sql("alter table rename change empname newname string")
    intercept[Exception] {
      sql("delete from rename where empname = 'joey'")
    }
    val df4 = sql("select newname from rename")
    assert(df3.count() == df4.count())
  }

  test("test sort columns, local dictionary and other column properties in DESC formatted, check case sensitive also") {
    dropTable()
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata tblproperties(" +
      "'local_dictionary_include'='workgroupcategoryname','local_dictionary_exclude'='deptname','COLUMN_META_CACHE'='projectcode,attendance'," +
      "'SORT_COLUMNS'='workgroupcategory,utilization,salary')")
    sql("alter table rename change eMPName name string")
    sql("alter table rename change workgroupcategoryname workgroup string")
    sql("alter table rename change DEPtNaMe depTADDress string")
    sql("alter table rename change attEnDance bUNk int")
    sql("alter table rename change uTiLIZation utILIty int")

    val descLoc = sql("describe formatted rename").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("workgroup"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,designation,deptaddress"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Sort Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("workgroupcategory, utility, salary"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Cached Min/Max Index Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("projectcode, bunk"))
      case None => assert(false)
    }
  }

  test("test rename on partition column") {
    sql("drop table if exists partitiontwo")
    sql(
      """
        | CREATE TABLE partitiontwo (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp, empname String)
        | STORED AS carbondata
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table partitiontwo change empname name string")
    }
    ex.getMessage.contains("Renaming the partition column name is not allowed")
  }

  test("test rename column with lucene") {
    sql("DROP TABLE IF EXISTS datamap_test")
    sql(
      """
        | CREATE TABLE datamap_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm ON TABLE datamap_test
         | USING 'lucene'
         | DMProperties('INDEX_COLUMNS'='Name , cIty')
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table datamap_test change Name myName string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index datamap")
    sql("DROP TABLE IF EXISTS datamap_test")
  }

  test("test rename column with bloom datamap") {
    sql("DROP TABLE IF EXISTS bloomtable")
    sql(
      s"""
         | CREATE TABLE bloomtable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP dm3 ON TABLE bloomtable
         | USING 'bloomfilter'
         | DMProperties('INDEX_COLUMNS'='city,id', 'BLOOM_SIZE'='640000')
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table bloomtable change city nation string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index datamap")
    sql("drop table if exists bloomtable")
  }

  test("test rename on complex column") {
    sql("drop table if exists complex")
    sql(
      "create table complex (id int, name string, structField struct<intval:int, stringval:string>) STORED AS carbondata")
    val ex = intercept[AnalysisException] {
      sql("alter table complex change structField complexTest struct")
    }
    assert(ex.getMessage.contains("DataType struct is not supported"))
  }

  test("test SET command with column rename") {
    dropTable()
     createTable()
    sql("alter table rename change workgroupcategoryname testset string")
    val ex = intercept[Exception] {
      sql("alter table rename set tblproperties('column_meta_cache'='workgroupcategoryname')")
    }
    assert(ex.getMessage.contains("Column workgroupcategoryname does not exists in the table rename"))
    sql("alter table rename set tblproperties('column_meta_cache'='testset')")
    val descLoc = sql("describe formatted rename").collect
    descLoc.find(_.get(0).toString.contains("Cached Min/Max Index Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("testset"))
      case None => assert(false)
    }
  }

  test("test column rename with change datatype for decimal datatype") {
    sql("drop table if exists deciTable")
    sql("create table decitable(name string, age int, avg decimal(30,10)) STORED AS carbondata")
    sql("alter table decitable change avg newAvg decimal(32,11)")
    val descLoc = sql("describe formatted decitable").collect
    descLoc.find(_.get(0).toString.contains("newavg")) match {
      case Some(row) => assert(row.get(1).toString.contains("decimal(32,11)"))
      case None => assert(false)
    }
    sql("drop table if exists decitable")
  }

  test("test column rename of bigint column") {
    sql("drop table if exists biginttable")
    sql("create table biginttable(name string, age int, bigintfield bigint) STORED AS carbondata")
    sql("alter table biginttable change bigintfield testfield bigint")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "biginttable")
    assert(null != carbonTable.getColumnByName("testfield"))
    assert(null == carbonTable.getColumnByName("bigintfield"))
    sql("drop table if exists biginttable")
  }

  test("test column comment after column rename") {
    dropTable()
    createTable()
    checkExistence(sql("describe formatted rename"), true, "This column has comment ")
    sql("alter table rename change deptno classno bigint")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      checkExistence(sql("describe formatted rename"), false, "This column has comment ")
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      checkExistence(sql("describe formatted rename"), true, "This column has comment ")
    }
  }

  test("test compaction after table rename and alter set tblproerties") {
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
    sql(
      "CREATE TABLE test_rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_rename rename to test_rename_compact")
    sql("alter table test_rename_compact set tblproperties('sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_rename_compact OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    val res1 = sql("select * from test_rename_compact")
    sql("alter table test_rename_compact compact 'major'")
    val res2 = sql("select * from test_rename_compact")
    assert(res1.collectAsList().containsAll(res2.collectAsList()))
    checkExistence(sql("show segments for table test_rename_compact"), true, "Compacted")
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
  }

  test("test compaction after alter set tblproerties- add and drop") {
    sql("DROP TABLE IF EXISTS test_alter")
    sql(
      "CREATE TABLE test_alter (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_alter set tblproperties('sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
    sql("alter table test_alter drop columns(deptno)")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_alter add columns(deptno int)")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    val res1 = sql("select * from test_alter")
    sql("alter table test_alter compact 'major'")
    val res2 = sql("select * from test_alter")
    assert(res1.collectAsList().containsAll(res2.collectAsList()))
    sql("DROP TABLE IF EXISTS test_alter")
  }

  override def afterAll(): Unit = {
    dropTable()
  }

  def dropTable(): Unit = {
    sql("DROP TABLE IF EXISTS RENAME")
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
    sql("DROP TABLE IF EXISTS test_alter")
  }

  def createTableAndLoad(): Unit = {
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def loadToTable():Unit = {
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def createTable(): Unit = {
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int comment \"This column " +
      "has comment\", deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
  }
}

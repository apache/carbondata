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
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class AlterTableColumnRenameTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable()
    createNonPartitionTableAndLoad()
  }

  test("test only column rename operation") {
    sql("alter table rename change empname empAddress string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("empAddress"))
    assert(null == carbonTable.getColumnByName("empname"))
  }

  test("CARBONDATA-4053 test rename column, column name in table properties changed correctly") {
    sql("create table simple_table(a string, aa1 string) stored as carbondata" +
        " tblproperties(\"sort_columns\"=\"a,aa1\")")
    sql("alter table simple_table change a a1 string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "simple_table")
    val sort_columns = carbonTable.getTableInfo.getFactTable.getTableProperties.get("sort_columns")
    assert(sort_columns.equals("a1,aa1"))
    sql("drop table simple_table")
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
    assert(ex.getMessage.contains(
      "New column name workgroupcategoryname already exists in table rename"))
  }

  test("test change column command with comment") {
    dropTable()
    createNonPartitionTableAndLoad()
    createPartitionTableAndLoad()

    // Non-Partition Column with Non-Complex Datatype
    testChangeColumnWithComment("rename")
    testChangeColumnWithComment("rename_partition")

    // Non-Partition Column with Complex Datatype
    sql("DROP TABLE IF EXISTS rename_complextype")
    sql(s"create table rename_complextype(mapcol map<string,string>," +
      s" arraycol array<string>) stored as carbondata")
    testChangeColumnWithComment("rename_complextype", "mapcol",
      "mapcol", "map<string,string>", "map<string,string>", "map comment", false)
    testChangeColumnWithComment("rename_complextype", "arraycol",
      "arraycol", "array<string>", "array<string>", "array comment", false)

    // Partition Column
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename_partition " +
          "change projectcode projectcode int comment 'partitoncolumn comment'")
    }
    ex.getMessage.contains(s"Alter on partition column projectcode is not supported")
    checkExistence(sql(s"describe formatted rename_partition"), false, "partitoncolumn comment")

    // Bucket Column
    sql("DROP TABLE IF EXISTS rename_bucket")
    sql("CREATE TABLE rename_bucket (ID Int, date Timestamp, country String, name String)" +
      " STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    testChangeColumnWithComment("rename_bucket", "name",
      "name", "string", "string", "bucket comment", false)
  }

  test("column rename for different datatype") {
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

  test("query count after column rename and filter results") {
    dropTable()
    createNonPartitionTableAndLoad()
    val df1 = sql("select empname from rename").collect()
    val df3 =
      sql("select workgroupcategory from rename where empname = 'bill' or empname = 'sibi'")
        .collect()
    sql("alter table rename change empname empAddress string")
    val df2 = sql("select empAddress from rename").collect()
    val df4 =
      sql("select workgroupcategory from rename where empAddress = 'bill' or empAddress = 'sibi'")
        .collect()
    intercept[Exception] {
      sql("select empname from rename")
    }
    assert(df1.length == df2.length)
    assert(df3.length == df4.length)
  }

  test("compaction after column rename and count") {
    dropTable()
    createNonPartitionTableAndLoad()
    for (i <- 0 to 2) {
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
    createNonPartitionTableAndLoad()
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
    createNonPartitionTableAndLoad()
    sql("alter table rename change empname name string")
    sql("update rename set (name) = ('joey') where workgroupcategory = 'developer'").collect()
    sql("insert into rename select 20,'bill','PM','01-12-2015',3,'manager',14,'Learning'," +
        "928479,'01-01-2016','30-11-2016',75,94,13547")
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

  test("test sort columns, local dictionary and other column properties in DESC formatted, " +
       "check case sensitive also") {
    dropTable()
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata tblproperties(" +
      "'local_dictionary_include'='workgroupcategoryname','local_dictionary_exclude'='deptname'," +
      "'COLUMN_META_CACHE'='projectcode,attendance'," +
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
    sql("DROP TABLE IF EXISTS index_test")
    sql(
      """
        | CREATE TABLE index_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX dm
         | ON TABLE index_test (name, city)
         | AS 'lucene'
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table index_test change Name myName string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index indexSchema")
    sql("DROP TABLE IF EXISTS index_test")
  }

  test("test rename column with bloom indexSchema") {
    sql("DROP TABLE IF EXISTS bloomtable")
    sql(
      s"""
         | CREATE TABLE bloomtable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX dm3
         | ON TABLE bloomtable (city, id)
         | AS 'bloomfilter'
         | Properties('BLOOM_SIZE'='640000')
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table bloomtable change city nation string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index")
    sql("drop table if exists bloomtable")
  }

  test("test rename on complex column") {
    sql("drop table if exists complex")
    sql("create table complex (id int, name string, " +
      "structField struct<intval:int, stringval:string>) STORED AS carbondata")
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table complex change structField complexTest struct")
    }
    assert(ex.getMessage.contains("Rename column is unsupported for complex datatype"))
  }

  test("test SET command with column rename") {
    dropTable()
    createTable()
    sql("alter table rename change workgroupcategoryname testset string")
    val ex = intercept[Exception] {
      sql("alter table rename set tblproperties('column_meta_cache'='workgroupcategoryname')")
    }
    assert(ex.getMessage.contains(
      "Column workgroupcategoryname does not exists in the table rename"))
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
    checkExistence(sql("describe formatted rename"), true, "This column has comment ")
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
    sql("alter table test_rename_compact set tblproperties(" +
        "'sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
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
    sql("alter table test_alter set tblproperties(" +
        "'sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
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
    sql("DROP TABLE IF EXISTS rename")
    sql("DROP TABLE IF EXISTS rename_partition")
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
    sql("DROP TABLE IF EXISTS test_alter")
    sql("DROP TABLE IF EXISTS simple_table")
  }

  def testChangeColumnWithComment(tableName: String): Unit = {
    // testcase1: columnrename: no; datatypechange: no;
    testChangeColumnWithComment(tableName, "testcase1_1_col",
      "testcase1_1_col", "string", "string", "testcase1_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase1_2_col",
      "testcase1_2_col", "int", "int", "testcase1_2 comment", true)
    testChangeColumnWithComment(tableName, "testcase1_3_col",
      "testcase1_3_col", "decimal(30,10)", "decimal(30,10)", "testcase1_3 comment", true)

    // testcase2: columnrename: yes; datatypechange: no;
    testChangeColumnWithComment(tableName, "testcase2_col",
      "testcase2_col_renamed", "string", "string", "testcase2 comment", true)

    // testcase3: columnrename: no; datatypechange: yes
    testChangeColumnWithComment(tableName, "testcase3_1_col",
      "testcase3_1_col", "int", "bigint", "testcase3_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase3_2_col",
      "testcase3_2_col", "decimal(30,10)", "decimal(32,11)", "testcase3_2 comment", true)

    // testcase4: columnrename: yes; datatypechange: yes,
    testChangeColumnWithComment(tableName, "testcase4_1_col",
      "testcase4_1_col_renamed", "int", "bigint", "testcase4_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase4_2_col",
      "testcase4_2_col_renmaed", "decimal(30,10)", "decimal(32,11)", "testcase4_2 comment", true)

    // testcase5: special characters in comments
    // scalastyle:off
    testChangeColumnWithComment(tableName, "testcase5_1_col",
      "testcase5_1_col_renamed", "string", "string", "测试comment", true)
    // scalastyle:on
    testChangeColumnWithComment(tableName, "testcase5_2_col",
      "testcase5_2_col_renmaed", "decimal(30,10)", "decimal(32,11)", "\001\002comment", true)
  }

  def testChangeColumnWithComment(tableName: String, oldColumnName: String,
      newColumnName: String, oldDataType: String, newDataType: String, comment: String,
      needCreateOldColumn: Boolean): Unit = {
    checkExistence(sql(s"describe formatted $tableName"), false, comment)
    if (needCreateOldColumn) {
      sql(s"alter table $tableName add columns ($oldColumnName $oldDataType)")
    }
    sql(s"alter table $tableName " +
        s"change $oldColumnName $newColumnName $newDataType comment '$comment'")
    checkExistence(sql(s"describe formatted $tableName"), true, comment)
    if (!newDataType.equalsIgnoreCase(oldDataType)) {
      sql(s"describe formatted $tableName")
        .collect.find(_.get(0).toString.contains(newColumnName)) match {
        case Some(row) => assert(row.get(1).toString.contains(newDataType))
        case None => assert(false)
      }
    }
  }

  def createNonPartitionTableAndLoad(): Unit = {
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def createPartitionTableAndLoad(): Unit = {
    sql(
      "CREATE TABLE rename_partition (empno int, empname String, designation String," +
        " doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int," +
        " deptname String," +
        " projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        " utilization int,salary int) PARTITIONED BY (projectcode int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def loadToTable(): Unit = {
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

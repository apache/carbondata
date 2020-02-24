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
package org.apache.carbondata.spark.testsuite.flatfolder

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class FlatFolderTableLoadingTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql(
      """
        | CREATE TABLE originTable (empno int, empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE originTable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

  }

  def validateDataFiles(tableUniqueName: String, segmentId: String): Unit = {
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
    val files = FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
    assert(files.exists(_.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT)))
  }

  test("data loading for flat folder with global sort") {
    sql(
      """
        | CREATE TABLE flatfolder_gs (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,empno int)
        | STORED AS carbondata tblproperties('sort_scope'='global_sort', 'flat_folder'='true')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_gs OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_flatfolder_gs", "0")

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from flatfolder_gs order by empno"),
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for flat folder") {
    sql(
      """
        | CREATE TABLE flatfolder (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,empno int)
        | STORED AS carbondata tblproperties('flat_folder'='true')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_flatfolder", "0")

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from flatfolder order by empno"),
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("merge index flat folder issue") {
    sql("drop table if exists t1")
    sql("create table t1(c1 int,c2 string,c3 float,c4 date) STORED AS carbondata TBLPROPERTIES('flat_folder'='true')")
    sql("insert into t1 select 1,'a',1001,'1999-01-02'")
    sql("insert into t1 select 2,'b',20.01,'1998-01-02'")
    sql("insert into t1 select 3,'c',30.01,'1997-01-02'")
    sql("insert into t1 select 4,'d',40.01,'1996-01-02'")
    sql("insert into t1 select 5,'d',40.01,'1996-01-02'")
    sql("delete from table t1 where segment.id in(1)")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "t1")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 5)
    sql("clean files for table t1")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 4)
    sql("Alter table t1 compact 'minor'")
    sql("show segments for table t1").show()
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 5)
    sql("clean files for table t1")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 1)
    sql("drop table if exists t1")
  }

  test("merge index flat folder and delete delta issue") {
    sql("drop table if exists flatfolder_delete")
    sql(
      """
        | CREATE TABLE flatfolder_delete (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,empno int)
        | STORED AS carbondata tblproperties('flat_folder'='true')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "flatfolder_delete")
    sql(s"""delete from flatfolder_delete where empname='anandh'""")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 4)
    sql("Alter table flatfolder_delete compact 'minor'")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 4)
    sql("clean files for table flatfolder_delete")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 1)
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles().filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 0)
    sql("drop table if exists flatfolder_delete")
  }

  test("merge index flat folder and delete delta issue with GLOBAL SORT") {
    sql("drop table if exists flatfolder_delete")
    sql(
      """
        | CREATE TABLE flatfolder_delete (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,empno int)
        | STORED AS carbondata tblproperties('flat_folder'='true', 'SORT_SCOPE'='GLOBAL_SORT' )
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='4')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='4')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='4')""")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_delete OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"', 'GLOBAL_SORT_PARTITIONS'='4')""")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "flatfolder_delete")
    sql(s"""delete from flatfolder_delete where empname='anandh'""")
    sql(s"""delete from flatfolder_delete where empname='arvind'""")
    sql(s"""select * from flatfolder_delete""").show()
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
             .filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 8)
    sql("Alter table flatfolder_delete compact 'minor'")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
             .filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 8)
    sql("clean files for table flatfolder_delete")
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
             .filter(_.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)).length == 1)
    assert(FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
             .filter(_.getName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)).length == 0)
    sql("drop table if exists flatfolder_delete")
  }

  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
    dropTable
  }

  def dropTable = {
    sql("drop table if exists originTable")
    sql("drop table if exists flatfolder")
    sql("drop table if exists flatfolder_gs")
    sql("drop table if exists flatfolder_preagg")
  }

}

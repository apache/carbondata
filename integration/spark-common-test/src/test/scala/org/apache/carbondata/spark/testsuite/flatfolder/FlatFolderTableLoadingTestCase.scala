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
        | STORED BY 'org.apache.carbondata.format'
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
        | STORED BY 'org.apache.carbondata.format' tblproperties('sort_scope'='global_sort', 'flat_folder'='true')
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
        | STORED BY 'org.apache.carbondata.format' tblproperties('flat_folder'='true')
      """.stripMargin)
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_flatfolder", "0")

    checkAnswer(sql("select empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from flatfolder order by empno"),
      sql("select  empno, empname, designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, salary from originTable order by empno"))

  }

  test("data loading for flat folder pre-agg") {
    sql(
      """
        | CREATE TABLE flatfolder_preagg (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int,empno int)
        | STORED BY 'org.apache.carbondata.format' tblproperties('flat_folder'='true')
      """.stripMargin)
    sql("create datamap p2 on table flatfolder_preagg using 'preaggregate' as select empname, designation, min(salary) from flatfolder_preagg group by empname, designation ")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE flatfolder_preagg OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")

    validateDataFiles("default_flatfolder_preagg", "0")
    validateDataFiles("default_flatfolder_preagg_p2", "0")

    checkAnswer(sql("select empname, designation, min(salary) from flatfolder_preagg group by empname, designation"),
      sql("select empname, designation, min(salary) from originTable group by empname, designation"))

  }

  override def afterAll = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION ,
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

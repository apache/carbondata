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

package org.apache.carbondata.spark.testsuite.externalformat


import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

class CsvBasedCarbonTableSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {

  val carbonTable = "fact_carbon_table"
  val csvCarbonTable = "fact_carbon_csv_table"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"

  // prepare normal carbon table for comparison
  override protected def beforeAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
    sql(
      s"""
         | CREATE TABLE $carbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
       """.stripMargin
    )
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvFile' INTO TABLE $carbonTable
         | OPTIONS('DELIMITER'=',',
         | 'QUOTECHAR'='\"',
         | 'FILEHEADER'='EMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY')
        """.stripMargin)
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
  }

  override protected def beforeEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")

  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  private def checkQuery() {
    // query all the columns
    checkAnswer(sql(s"SELECT * FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT * FROM $carbonTable WHERE empno = 15"))
    // query part of the columns
    checkAnswer(sql(s"SELECT empno,empname FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empno,empname FROM $carbonTable WHERE empno = 15"))
    // sequence of projection column are not same with that in DDL
    checkAnswer(sql(s"SELECT empname, empno FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empname, empno FROM $carbonTable WHERE empno = 15"))
    // query with greater
    checkAnswer(sql(s"SELECT empname, empno FROM $csvCarbonTable WHERE empno > 15"),
      sql(s"SELECT empname, empno FROM $carbonTable WHERE empno > 15"))
    // query with filter on dimension
    checkAnswer(sql(s"SELECT empname, empno FROM $csvCarbonTable WHERE empname = 'ayushi'"),
      sql(s"SELECT empname, empno FROM $carbonTable WHERE empname = 'ayushi'"))
    // aggreate query
    checkAnswer(sql(s"SELECT designation, sum(empno), avg(empno) FROM $csvCarbonTable GROUP BY designation"),
      sql(s"SELECT designation, sum(empno), avg(empno) FROM $carbonTable GROUP BY designation"))
  }

  test("test csv based carbon table") {
    // create csv based carbon table
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.delimiter'=',',
         | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )
    // check that the external format info is stored in tableinfo
    val tblInfo =
      CarbonEnv.getCarbonTable(Option("default"), csvCarbonTable)(Spark2TestQueryExecutor.spark)
    assertResult("csv")(tblInfo.getTableInfo.getFormat)
    assertResult(2)(tblInfo.getTableInfo.getFormatProperties.size())
    assertResult(
      "eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY".toLowerCase)(
      tblInfo.getTableInfo.getFormatProperties.get("csv.header"))
    assertResult(",")(tblInfo.getTableInfo.getFormatProperties.get("csv.delimiter"))

    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")

    // check that the fact files has been stored in tablestatus
    val metadataPath = CarbonTablePath.getMetadataPath(tblInfo.getTablePath)
    val details = SegmentStatusManager.readLoadMetadata(metadataPath)
    assertResult(1)(details.length)
    assertResult(csvFile)(details(0).getFactFilePath)

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // check query on csv based carbontable
    LOGGER.info("Query with vector reader on")
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    LOGGER.info("Query with vector reader off")
    // checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }
}
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

import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath

class CsvBasedCarbonTableSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {

  val csvCarbonTable = "fact_carbon_csv_table"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"

  override protected def beforeEach(): Unit = {
    // sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
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
         | 'csv.header'='MPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )
    // check that the external format info is stored in tableinfo
    val tblInfo =
      CarbonEnv.getCarbonTable(Option("default"), csvCarbonTable)(Spark2TestQueryExecutor.spark)
    assertResult("csv")(tblInfo.getTableInfo.getFormat)
    assertResult(2)(tblInfo.getTableInfo.getFormatProperties.size())
    assertResult(
      "MPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY".toLowerCase)(
      tblInfo.getTableInfo.getFormatProperties.get("csv.header"))
    assertResult(",")(tblInfo.getTableInfo.getFormatProperties.get("csv.delimiter"))

    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")

    // check that the fact files has been stored in tablestatus
    val metadataPath = CarbonTablePath.getMetadataPath(tblInfo.getTablePath)
    val details = SegmentStatusManager.readLoadMetadata(metadataPath)
    assertResult(1)(details.length)
    assertResult(csvFile)(details(0).getFactFilePath)

    // query on csv based carbontable
    // sql(s"SELECT * FROM $csvCarbonTable WHERE empno = 15").show(false)

  }
}
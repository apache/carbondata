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

package org.apache.carbondata.spark.testsuite.externalds


import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CsvBasedCarbonTableSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {

  val csvCarbonTable = "fact_carbon_csv_table"
  val carbonTable = "fact_carbon_table"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"
  val datamapName = "fact_dm"

  override protected def beforeEach(): Unit = {
    //    sql(s"DROP TABLE IF EXISTS $textCarbonTable")
    //    sql(s"DROP TABLE IF EXISTS $carbonTable")
  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
    //    sql(s"DROP TABLE IF EXISTS $carbonTable")
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
    val tblInfo =
      CarbonEnv.getCarbonTable(Option("default"), csvCarbonTable)(Spark2TestQueryExecutor.spark)
    assertResult("csv")(tblInfo.getTableInfo.getFormat)
    assert(tblInfo.getTableInfo.getFormatProperties.size() == 2)
    assert(tblInfo.getTableInfo.getFormatProperties.get("csv.header")
      .equals("MPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY".toLowerCase))
    assert(tblInfo.getTableInfo.getFormatProperties.get("csv.delimiter").equals(","))

//    sql(s"")

    // create datamap on textfile based carbontable
//    sql(
//      s"""
//         | CREATE DATAMAP $datamapName ON TABLE $csvCarbonTable
//         | USING 'bloomfilter'
//         | WITH DEFERRED REBUILD
//         | DMProperties('INDEX_COLUMNS'='empname')
//       """.stripMargin)

//    sql(s"SHOW DATAMAP ON TABLE $csvCarbonTable").show(false)
//    checkExistence(sql(s"SHOW DATAMAP ON TABLE $csvCarbonTable"), true, datamapName)

  }
}
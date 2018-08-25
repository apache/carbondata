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


import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class CsvExternalFormatWithIndexDataMapSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {
  val carbonTable = "fact_carbon_table"
  val csvCarbonTable = "fact_carbon_csv_table"
  val indexOnCsvCarbonTablePrefix = "index_on_fact_csv_"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  override protected def beforeEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  override protected def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $carbonTable")
    sql(s"DROP TABLE IF EXISTS $csvCarbonTable")
  }

  private def checkQuery() {
    // query all the columns
    checkAnswer(sql(s"SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY FROM $carbonTable WHERE empno = 15"))
    // query part of the columns
    checkAnswer(sql(s"SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empno,empname, deptname, doj FROM $carbonTable WHERE empno = 15"))
    // sequence of projection column are not same with that in DDL
    checkAnswer(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno = 15"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno = 15"))
    // query with greater
    checkAnswer(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empno > 15"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empno > 15"))
    // query with filter on dimension
    checkAnswer(sql(s"SELECT empname, empno, deptname, doj FROM $csvCarbonTable WHERE empname = 'ayushi'"),
      sql(s"SELECT empname, empno, deptname, doj FROM $carbonTable WHERE empname = 'ayushi'"))
    // aggreate query
    checkAnswer(sql(s"SELECT designation, sum(empno), avg(empno) FROM $csvCarbonTable GROUP BY designation"),
      sql(s"SELECT designation, sum(empno), avg(empno) FROM $carbonTable GROUP BY designation"))
  }

  private def createNormalTableForComparison(): Unit = {
    sql(
      s"""
         | CREATE TABLE $carbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
       """.stripMargin
    )
  }

  private def loadNormalTableForComparison(): Unit = {
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$csvFile' INTO TABLE $carbonTable
         | OPTIONS('DELIMITER'=',',
         | 'QUOTECHAR'='\"',
         | 'FILEHEADER'='EMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY')
        """.stripMargin)
  }

  private def createCsvExternalFormatTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )
  }

  private def loadCsvExternalFormatTable(): Unit = {
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")
  }
  /**
   * This test case test the:
   * 1. direct generate bloom datamap index
   * 2. build bloom datamap on existing data
   * 3. multiple loads
   * 4. multiple datamaps
   * 5. query with multiple datamaps
   */
  test("test building and rebuilding bloomfilter multiple datamaps on CSV external format table and querying on it") {
    createNormalTableForComparison()
    createCsvExternalFormatTable()

    loadNormalTableForComparison()
    loadNormalTableForComparison()
    loadCsvExternalFormatTable()
    loadCsvExternalFormatTable()
    // create index datamap on external format table, this will trigger rebuiding datamap on existing data
    sql(
      s"""
         | CREATE DATAMAP ${indexOnCsvCarbonTablePrefix}1
         | ON TABLE $csvCarbonTable
         | USING 'bloomfilter'
         | DMPROPERTIES(
         | 'INDEX_COLUMNS'='empname,empno'
         | )
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ${indexOnCsvCarbonTablePrefix}2
         | ON TABLE $csvCarbonTable
         | USING 'bloomfilter'
         | DMPROPERTIES(
         | 'INDEX_COLUMNS'='deptno, designation, doj, workgroupcategory, workgroupcategoryname'
         | )
       """.stripMargin)
    sql(
      s"""
         | CREATE DATAMAP ${indexOnCsvCarbonTablePrefix}3
         | ON TABLE $csvCarbonTable
         | USING 'bloomfilter'
         | DMPROPERTIES(
         | 'INDEX_COLUMNS'='projectcode'
         | )
       """.stripMargin)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $csvCarbonTable"), true,
      s"${indexOnCsvCarbonTablePrefix}1", s"${indexOnCsvCarbonTablePrefix}2", s"${indexOnCsvCarbonTablePrefix}3")

    loadNormalTableForComparison()
    loadNormalTableForComparison()
    // add segment for csv based carbontable, this will trigger direct building datamap
    loadCsvExternalFormatTable()
    loadCsvExternalFormatTable()

    assert(sql(s"SHOW SEGMENTS FOR TABLE $csvCarbonTable").collect().length ==
           sql(s"SHOW SEGMENTS FOR TABLE $csvCarbonTable").collect().length)

    // this will skip 0 blocklets
    sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15").show(false)
    checkExistence(sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15"),
      true, "bloomfilter", s"${indexOnCsvCarbonTablePrefix}1", "skipped blocklets: 0")
    sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15 AND empname='ayushi' AND designation='SSA' AND deptno=12 AND projectcode=928375").show(false)
    checkExistence(sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15 AND empname='ayushi' AND designation='SSA' AND deptno=12 AND projectcode=928375"),
      true, "bloomfilter", s"${indexOnCsvCarbonTablePrefix}1", s"${indexOnCsvCarbonTablePrefix}2", s"${indexOnCsvCarbonTablePrefix}3", "skipped blocklets: 0")

    // this will skip all the blocklets
    sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 155").show(false)
    checkExistence(sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 155"),
      true, "bloomfilter", s"${indexOnCsvCarbonTablePrefix}1", "skipped blocklets: 4")
    sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15 AND empname='fake_ayushi' AND designation='SSA' AND deptno=12 AND projectcode=928375").show(false)
    checkExistence(sql(s"explain SELECT empno,empname, deptname, doj FROM $csvCarbonTable WHERE empno = 15 AND empname='fake_ayushi' AND designation='SSA' AND deptno=12 AND projectcode=928375"),
      true, "bloomfilter", s"${indexOnCsvCarbonTablePrefix}1", s"${indexOnCsvCarbonTablePrefix}2", s"${indexOnCsvCarbonTablePrefix}3", "skipped blocklets: 4")

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }
}
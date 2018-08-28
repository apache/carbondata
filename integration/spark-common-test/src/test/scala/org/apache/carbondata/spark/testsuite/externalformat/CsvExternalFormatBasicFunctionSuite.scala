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

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class CsvExternalFormatBasicFunctionSuite extends QueryTest
  with BeforeAndAfterEach with BeforeAndAfterAll {

  val carbonTable = "fact_carbon_table"
  val csvCarbonTable = "fact_carbon_csv_table"
  val csvFile = s"$resourcesPath/datawithoutheader.csv"
  val csvFile_delimiter_separator = s"$resourcesPath/datawithoutheader_delimiter_separator.csv"

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
         | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )
    // check that the external format info is stored in tableinfo
    val tblInfo =
      CarbonEnv.getCarbonTable(Option("default"), csvCarbonTable)(Spark2TestQueryExecutor.spark)
    assertResult("csv")(tblInfo.getTableInfo.getFormat)
    assertResult(1)(tblInfo.getTableInfo.getFormatProperties.size())
    assertResult(
      "eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY".toLowerCase)(
      tblInfo.getTableInfo.getFormatProperties.get("csv.header"))

    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")

    // check that the fact files has been stored in tablestatus
    val metadataPath = CarbonTablePath.getMetadataPath(tblInfo.getTablePath)
    val details = SegmentStatusManager.readLoadMetadata(metadataPath)
    assertResult(1)(details.length)
    assertResult(csvFile)(details(0).getFactFilePath)

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // check query on csv based carbontable
    // query with vector reader on
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // query with vector reader off
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("test csv based carbon table: only support csv now") {
    val expectedException = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE $csvCarbonTable(empname String, empno smallint, designation string,
           | deptname String, projectcode int, projectjoindate String,projectenddate String,
           | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
           | attendance String, utilization String,salary String)
           | STORED BY 'carbondata'
           | TBLPROPERTIES(
           | 'format'='parquet',
           | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
           | )
       """.stripMargin
      )
    }

    assert(expectedException.getMessage.contains("Unsupported external format parquet"))
  }

  test("test csv based carbon table: the sequence of header does not match schema") {
    // create csv based carbon table, the sequence in schema is not the same in csv.header
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empname String, empno smallint, designation string,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.header'='eMPno, empname,designation, doj, workgroupcategory, workgroupcategoryname, deptno, deptname, projectcode, projectjoindate, projectenddate, attendance, utilization, SALARY'
         | )
       """.stripMargin
    )
    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // check query on csv based carbontable
    // query with vector reader on
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // query with vector reader off
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("test csv based carbon table: not specify the header") {
    // create csv based carbon table, the sequence in schema is not the same in csv.header
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv'
         | )
       """.stripMargin
    )

    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile'")

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // check query on csv based carbontable
    // query with vector reader on
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // query with vector reader off
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("test csv based carbon table: user specified delimiter") {
    // create csv based carbon table, the sequence in schema is not the same in csv.header
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empno smallint, empname String, designation string,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.delimiter'='|'
         | )
       """.stripMargin
    )

    // add segment for csv based carbontable
    sql(s"ALTER TABLE $csvCarbonTable ADD SEGMENT LOCATION '$csvFile_delimiter_separator'")

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // check query on csv based carbontable
    // query with vector reader on
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // query with vector reader off
    checkQuery()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
      CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }

  test("block update data on external format table") {
    createCsvCaronTable
    // block update data on external table.
    var exception = intercept[MalformedCarbonCommandException] {
      sql(s"update $csvCarbonTable set (empname) = ('b') where designation = 'a' ").collect
    }
    assert(exception.getMessage.contains("Unsupported operation on external format table"))
  }

  test("block delete data on external format table") {
    createCsvCaronTable
    val exception = intercept[MalformedCarbonCommandException] {
      sql(s"delete from $csvCarbonTable where designation = 'a' ")
    }
    assert(exception.getMessage.contains("Unsupported operation on external format table"))
  }

  test("block add columns on external format table") {
    createCsvCaronTable
    val exception = intercept[MalformedCarbonCommandException] {
      sql(s"alter table $csvCarbonTable add columns(addcolumn int)")
    }
    assert(exception.getMessage.contains("Unsupported operation on external format table"))
  }

  test("block drop columns on external format table") {
    createCsvCaronTable
    val exception = intercept[MalformedCarbonCommandException] {
      sql(s"alter table $csvCarbonTable drop columns(empname,empno)")
    }
    assert(exception.getMessage.contains("Unsupported operation on external format table"))
  }

  test("block change datatype on external format table") {
    createCsvCaronTable
    val exception = intercept[MalformedCarbonCommandException] {
      sql(s"alter table $csvCarbonTable change empno empno bigint")
    }
    assert(exception.getMessage.contains("Unsupported operation on external format table"))
  }

  test("block create external format table if columns not in format.header") {
    val exception = intercept[ProcessMetaDataException] {
      sql(
        s"""
           | CREATE TABLE $csvCarbonTable(empname String, empno smallint, designation string,
           | deptname String, projectcode int, projectjoindate String,projectenddate String,
           | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
           | attendance String, utilization String,salary String)
           | STORED BY 'carbondata'
           | TBLPROPERTIES(
           | 'format'='csv',
           | 'csv.header'='empname, designation, doj,
           | workgroupcategory, workgroupcategoryname, deptno,
           | deptname, projectcode, projectjoindate, projectenddate,
           | attendance, utilization, SALARY',
           | 'csv.delimiter'='|'
           | )
       """.stripMargin
      )
    }
    assert(exception.getMessage.contains("column 'empno' not in tblproperty 'csv.header'"))
  }

  test("test desc formatted for external format table") {
    createCsvCaronTable
    val result = sql(s"desc formatted $csvCarbonTable").collect()
    assert(result.exists(_.mkString("|").replaceAll("\\s","")
      .contains("format|csv")))
    assert(result.exists(_.mkString("|").replaceAll("\\s","")
      .contains("csv.delimiter||")))
    assert(result.exists(_.mkString("|").replaceAll("\\s","")
      .contains("csv.header|empno,empname,designation,doj,workgroupcategory," +
                "workgroupcategoryname,deptno,deptname,projectcode,projectjoindate," +
                "projectenddate,attendance,utilization,salary")))
  }

  def createCsvCaronTable(): Unit = {
    sql(
      s"""
         | CREATE TABLE $csvCarbonTable(empname String, empno smallint, designation string,
         | deptname String, projectcode int, projectjoindate String,projectenddate String,
         | doj String, workgroupcategory int, workgroupcategoryname String,deptno int,
         | attendance String, utilization String,salary String)
         | STORED BY 'carbondata'
         | TBLPROPERTIES(
         | 'format'='csv',
         | 'csv.header'='empno, empname, designation, doj,
         | workgroupcategory, workgroupcategoryname, deptno,
         | deptname, projectcode, projectjoindate, projectenddate,
         | attendance, utilization, SALARY',
         | 'csv.delimiter'='|'
         | )
       """.stripMargin
    )
  }
}
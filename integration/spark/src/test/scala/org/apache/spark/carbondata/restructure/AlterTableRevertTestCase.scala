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

package org.apache.spark.carbondata.restructure

import java.io.File

import mockit.{Mock, MockUp}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.hive.MockClassForAlterRevertTests
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class AlterTableRevertTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll() {
    mock()
    sql("drop table if exists reverttest")
    sql(
      "CREATE TABLE reverttest(intField int,stringField string,timestampField timestamp," +
      "decimalField decimal(6,2)) STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE reverttest " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
  }

  test("test to revert new added columns on failure") {
    intercept[ProcessMetaDataException] {
      sql(
        "Alter table reverttest add columns(newField string) TBLPROPERTIES" +
        "('DEFAULT.VALUE.newField'='def')")
      intercept[AnalysisException] {
        sql("select newField from reverttest")
      }
    }
  }

  test("test to revert table name on failure") {
    val exception = intercept[ProcessMetaDataException] {
      new File(TestQueryExecutor.warehouse + "/reverttest_fail").mkdir()
      sql("alter table reverttest rename to reverttest_fail")
      new File(TestQueryExecutor.warehouse + "/reverttest_fail").delete()
    }
    val result = sql("select * from reverttest").count()
    assert(result.equals(1L))
    sql("drop table if exists reverttest_fail")
  }

  test("test to revert drop columns on failure") {
    intercept[ProcessMetaDataException] {
      sql("Alter table reverttest drop columns(decimalField)")
    }
    assert(sql("select decimalField from reverttest").count().equals(1L))
  }

  test("test to revert changed datatype on failure") {
    intercept[ProcessMetaDataException] {
      sql("Alter table reverttest change intField intfield bigint")
    }
    assert(
      sql("select intfield from reverttest").schema.fields.apply(0).dataType.simpleString == "int")
  }

  test("test to check if dictionary files are deleted for new column if query fails") {
    intercept[ProcessMetaDataException] {
      sql(
        "Alter table reverttest add columns(newField string) TBLPROPERTIES" +
        "('DEFAULT.VALUE.newField'='def')")
      intercept[AnalysisException] {
        sql("select newField from reverttest")
      }
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable("default", "reverttest")

      assert(new File(carbonTable.getMetadataPath).listFiles().length < 6)
    }
  }

  test("alter add operations revert testcase") {
    unMock()
    try {
      sql("drop table if exists reverttest")
      sql(
        "CREATE TABLE reverttest(intField int,stringField string) using carbondata")
      sql("insert into reverttest select 1, 'abc'")
      sql(
        "Alter table reverttest add columns(newField1 string) TBLPROPERTIES" +
        "('DEFAULT.VALUE.newField1'='def')")
      new MockUp[MockClassForAlterRevertTests]() {
        @Mock
        @throws[ProcessMetaDataException]
        def mockForAlterAddColRevertTest(): Unit = {
          throw new ProcessMetaDataException("default", "reverttest", "thrown in mock")
        }
      }
      // check revert schema with alter add column
      intercept[ProcessMetaDataException] {
        sql(
          "Alter table reverttest add columns(newField2 string) TBLPROPERTIES" +
          "('DEFAULT.VALUE.newField2'='def')")
      }
      checkAnswerAfterAlter()
      // check revert schema with alter drop column
      intercept[ProcessMetaDataException] {
        sql("Alter table reverttest drop columns(newField1)")
      }
      checkAnswerAfterAlter()
      // check revert schema with alter change column
      intercept[ProcessMetaDataException] {
        sql("alter table reverttest change newField1 newField2 string")
      }
      checkAnswerAfterAlter()

      def checkAnswerAfterAlter(): Unit = {
        val desCols = sql("desc reverttest").collect()
        assert(desCols.length == 3)
        assert(desCols.mkString("Array(", ", ", ")").contains("newfield1"))
        checkAnswer(sql("select intField,stringField,newField1 from reverttest"),
          Seq(Row(1, "abc", "def")))
      }
    } finally {
      mock()
    }
  }

  override def afterAll() {
    unMock()
    sql("drop table if exists reverttest")
    sql("drop table if exists reverttest_fail")
  }

  private def mock(): Unit = {
    new MockUp[MockClassForAlterRevertTests]() {
      @Mock
      @throws[ProcessMetaDataException]
      def mockForAlterRevertTest(): Unit = {
        throw new ProcessMetaDataException("default", "reverttest", "thrown in mock")
      }
    }
  }

  private def unMock(): Unit = {
    new MockUp[MockClassForAlterRevertTests]() {
      @Mock
      def mockForAlterRevertTest(): Unit = {
      }

      @Mock
      def mockForAlterAddColRevertTest(): Unit = {
      }
    }
  }
}

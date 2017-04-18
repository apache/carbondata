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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.apache.spark.util.AlterTableUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata

class AlterTableRevertTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll() {
    sql("drop table if exists reverttest")
    sql(
      "CREATE TABLE reverttest(intField int,stringField string,timestampField timestamp," +
      "decimalField decimal(6,2)) STORED BY 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE reverttest " +
        s"options('FILEHEADER'='intField,stringField,timestampField,decimalField')")
  }

  test("test to revert new added columns on failure") {
    intercept[RuntimeException] {
      hiveClient.runSqlHive("set hive.security.authorization.enabled=true")
      sql(
        "Alter table reverttest add columns(newField string) TBLPROPERTIES" +
        "('DEFAULT.VALUE.newField'='def')")
      hiveClient.runSqlHive("set hive.security.authorization.enabled=false")
      intercept[AnalysisException] {
        sql("select newField from reverttest")
      }
    }
  }

  test("test to revert table name on failure") {
    intercept[RuntimeException] {
      new File(TestQueryExecutor.warehouse + "/reverttest_fail").mkdir()
      sql("alter table reverttest rename to reverttest_fail")
      new File(TestQueryExecutor.warehouse + "/reverttest_fail").delete()
    }
    val result = sql("select * from reverttest").count()
    assert(result.equals(1L))
  }

  test("test to revert drop columns on failure") {
    intercept[Exception] {
      hiveClient.runSqlHive("set hive.security.authorization.enabled=true")
      sql("Alter table reverttest drop columns(decimalField)")
      hiveClient.runSqlHive("set hive.security.authorization.enabled=false")
    }
    assert(sql("select decimalField from reverttest").count().equals(1L))
  }

  test("test to revert changed datatype on failure") {
    intercept[Exception] {
      hiveClient.runSqlHive("set hive.security.authorization.enabled=true")
      sql("Alter table reverttest change intField intfield bigint")
      hiveClient.runSqlHive("set hive.security.authorization.enabled=false")
    }
    assert(
      sql("select intfield from reverttest").schema.fields.apply(0).dataType.simpleString == "int")
  }

  test("test to check if dictionary files are deleted for new column if query fails") {
    intercept[RuntimeException] {
      hiveClient.runSqlHive("set hive.security.authorization.enabled=true")
      sql(
        "Alter table reverttest add columns(newField string) TBLPROPERTIES" +
        "('DEFAULT.VALUE.newField'='def')")
      hiveClient.runSqlHive("set hive.security.authorization.enabled=false")
      intercept[AnalysisException] {
        sql("select newField from reverttest")
      }
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable("default_reverttest")

      assert(new File(carbonTable.getMetaDataFilepath).listFiles().length < 6)
    }
  }

  test("test to check if exception during rename table does not throws table not found exception") {
    val locks = AlterTableUtil
      .validateTableAndAcquireLock("default", "reverttest", List("meta.lock"))(sqlContext
        .sparkSession)
    val exception = intercept[RuntimeException] {
      sql("alter table reverttest rename to revert")
    }
    AlterTableUtil.releaseLocks(locks)
    assert(exception.getMessage == "Alter table rename table operation failed: Table is locked for updation. Please try after some time")
  }

  override def afterAll() {
    hiveClient.runSqlHive("set hive.security.authorization.enabled=false")
    sql("drop table if exists reverttest")
  }

}

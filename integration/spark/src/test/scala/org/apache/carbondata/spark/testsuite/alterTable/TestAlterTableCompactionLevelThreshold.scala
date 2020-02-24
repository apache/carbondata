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

package org.apache.carbondata.spark.testsuite.alterTable

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test class for validating alter table set properties with alter_compaction_level properties
 */
class TestAlterTableCompactionLevelThreshold extends QueryTest with BeforeAndAfterAll {

  private def isExpectedValueValid(dbName: String,
                                   tableName: String,
                                   key: String,
                                   expectedValue: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    val value = carbonTable.getTableInfo.getFactTable.getTableProperties.get(key)
    expectedValue.equals(value)
  }

  private def dropTable = {
    sql("drop table if exists alter_compaction_level_threshold")
  }

  override def beforeAll {
    // drop table
    dropTable
    // create table
    sql("create table alter_compaction_level_threshold(c1 String) stored as carbondata")
  }

  test("validate alter compaction level_threshold") {
    sql("ALTER TABLE alter_compaction_level_threshold SET TBLPROPERTIES('COMPACTION_LEVEL_THRESHOLD'='500,0')")
    assert(isExpectedValueValid("default", "alter_compaction_level_threshold", "compaction_level_threshold", "500,0"))
  }

  test("validate alter compaction level_threshold with wrong threshold") {
    var exception = intercept[Exception] {
      sql("ALTER TABLE alter_compaction_level_threshold SET TBLPROPERTIES('COMPACTION_LEVEL_THRESHOLD'='20000,0')")
    }
    assert(exception.getMessage.contains("Alter table newProperties operation failed"))

    exception = intercept[Exception] {
      sql("ALTER TABLE alter_compaction_level_threshold SET TBLPROPERTIES('COMPACTION_LEVEL_THRESHOLD'='?,?')")
    }
    assert(exception.getMessage.contains("Alter table newProperties operation failed"))
  }

  override def afterAll: Unit = {
    // drop table
    dropTable
  }
}

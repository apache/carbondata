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

package org.apache.spark.util

import java.io.File
import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

class CarbonCommandSuite extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable("csv_table")
    dropTable("carbon_table")
    createAndLoadInputTable("csv_table", s"$resourcesPath/data_alltypes.csv")
    createAndLoadTestTable("carbon_table", "csv_table")
  }

  override def afterAll(): Unit = {
    dropTable("csv_table")
    dropTable("carbon_table")
  }

  private lazy val location =
    CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION)
  test("show segment") {
    ShowSegments.main(Array(s"${location}", "carbon_table"))
  }

  test("delete segment by id") {
    DeleteSegmentById.main(Array(s"${location}", "carbon_table", "0"))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table",location,  "0"))
  }

  test("delete segment by date") {
    createAndLoadTestTable("carbon_table2", "csv_table")
    val time = new Timestamp(new Date().getTime)
    DeleteSegmentByDate.main(Array(s"${location}", "carbon_table2", time.toString))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table2", location, "0"))
    dropTable("carbon_table2")
  }

  test("clean files") {
    val table = "carbon_table3"
    createAndLoadTestTable(table, "csv_table")
    ShowSegments.main(Array(s"${location}", table))
    DeleteSegmentById.main(Array(s"${location}", table, "0"))
    ShowSegments.main(Array(s"${location}", table))
    CleanFiles.main(Array(s"${location}", table))
    ShowSegments.main(Array(s"${location}", table))
    val tablePath = s"${location}${File.separator}default${File.separator}$table"
    val f = new File(s"$tablePath/Fact/Part0")
    assert(f.isDirectory)

    // all segment folders should be deleted after CleanFiles command
    assert(f.list().length == 0)
    dropTable(table)
  }

  test("clean files with force clean option") {
    val table = "carbon_table4"
    dropTable(table)
    createAndLoadTestTable(table, "csv_table")
    CleanFiles.main(Array(s"${location}", table, "true"))
    val tablePath = s"${location}${File.separator}default${File.separator}$table"
    val f = new File(tablePath)
    assert(!f.exists())

    dropTable(table)
  }
}

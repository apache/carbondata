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

import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.core.util.CarbonUtil

class CarbonCommandSuite extends Spark2QueryTest with BeforeAndAfterAll {

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

  test("show segment") {
    ShowSegments.main(Array(s"${CarbonUtil.getCarbonStorePath}", "carbon_table"))
  }

  test("delete segment by id") {
    DeleteSegmentById.main(Array(s"${CarbonUtil.getCarbonStorePath}", "carbon_table", "0"))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table", "0"))
  }

  test("delete segment by date") {
    createAndLoadTestTable("carbon_table2", "csv_table")
    val time = new Timestamp(new Date().getTime)
    DeleteSegmentByDate.main(Array(s"${CarbonUtil.getCarbonStorePath}", "carbon_table2", time.toString))
    assert(!CarbonStore.isSegmentValid("default", "carbon_table2", "0"))
    dropTable("carbon_table2")
  }

  test("clean files") {
    val table = "carbon_table3"
    createAndLoadTestTable(table, "csv_table")
    ShowSegments.main(Array(s"${CarbonUtil.getCarbonStorePath}", table))
    DeleteSegmentById.main(Array(s"${CarbonUtil.getCarbonStorePath}", table, "0"))
    ShowSegments.main(Array(s"${CarbonUtil.getCarbonStorePath}", table))
    CleanFiles.main(Array(s"${CarbonUtil.getCarbonStorePath}", table))
    ShowSegments.main(Array(s"${CarbonUtil.getCarbonStorePath}", table))
    val tablePath = s"${CarbonUtil.getCarbonStorePath}${File.separator}default${File.separator}$table"
    val f = new File(s"$tablePath/Fact/Part0")
    assert(f.isDirectory)

    // all segment folders should be deleted after CleanFiles command
    assert(f.list().length == 0)
    dropTable(table)
  }
}

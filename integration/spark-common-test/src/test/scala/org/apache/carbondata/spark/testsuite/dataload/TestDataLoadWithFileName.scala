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

package org.apache.carbondata.spark.testsuite.dataload

import java.io.{File, FilenameFilter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.reader.CarbonIndexFileReader
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata

class TestDataLoadWithFileName extends QueryTest with BeforeAndAfterAll {
  var originVersion = ""

  override def beforeAll() {
    originVersion =
      CarbonProperties.getInstance.getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION)
  }

  test("Check the file_name in carbonindex with v3 format") {
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "3")
    sql("DROP TABLE IF EXISTS test_table_v3")
    sql(
      """
        | CREATE TABLE test_table_v3(id int, name string, city string, age int)
        | STORED BY 'org.apache.carbondata.format'
      """.stripMargin)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table test_table_v3")
    val indexReader = new CarbonIndexFileReader()
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "test_table_v3")
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val segmentDir = carbonTablePath.getCarbonDataDirectoryPath("0")
    val carbonIndexPaths = new File(segmentDir)
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.endsWith(CarbonTablePath.getCarbonIndexExtension)
        }
      })
    for (carbonIndexPath <- carbonIndexPaths) {
      indexReader.openThriftReader(carbonIndexPath.getCanonicalPath)
      assert(indexReader.readIndexHeader().getVersion === 3)
      while (indexReader.hasNext) {
        val readBlockIndexInfo = indexReader.readBlockIndexInfo()
        assert(readBlockIndexInfo.getFile_name.startsWith(CarbonTablePath.getCarbonDataPrefix))
        assert(readBlockIndexInfo.getFile_name.endsWith(CarbonTablePath.getCarbonDataExtension))
      }
    }
  }

  override protected def afterAll() {
    sql("DROP TABLE IF EXISTS test_table_v1")
    sql("DROP TABLE IF EXISTS test_table_v2")
    sql("DROP TABLE IF EXISTS test_table_v3")
    CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      originVersion)
  }
}

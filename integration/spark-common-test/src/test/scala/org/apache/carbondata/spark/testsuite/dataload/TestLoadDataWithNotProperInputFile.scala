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

import org.apache.spark.util.FileUtils
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test class of loading data for carbon table with not proper input file
 *
 */
class TestLoadDataWithNotProperInputFile extends QueryTest {

  test("test loading data with input path exists but has nothing") {
    val e = intercept[Throwable] {
      val dataPath = s"$resourcesPath/nullSample.csv"
      FileUtils.getPaths(dataPath)
    }
    assert(e.getMessage.contains("Please check your input path and make sure " +
      "that files end with '.csv' and content is not empty"))
  }

  test("test loading data with input file not ends with '.csv'") {
    try {
      val dataPath = s"$resourcesPath/noneCsvFormat.cs"
      FileUtils.getPaths(dataPath)
      assert(true)
    } catch {
      case e: Throwable =>
        assert(false)
    }
  }

  test("test loading data with input file does not exist") {
    val e = intercept[Throwable] {
      val dataPath = s"$resourcesPath/input_file_does_not_exist.csv"
      FileUtils.getPaths(dataPath)
    }
    assert(e.getMessage.contains("The input file does not exist"))
  }
}

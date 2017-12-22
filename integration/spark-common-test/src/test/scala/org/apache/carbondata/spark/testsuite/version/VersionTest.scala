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

package org.apache.carbondata.spark.testsuite.version

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Created by root on 12/22/17.
 */
class VersionTest extends QueryTest with BeforeAndAfterAll {
  test("CarbonData version test: null") {
    val version = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBONDATA_VERSION)
    assert(null == version)
  }

  test("CarbonData version test: add and then get") {
    CarbonProperties.getInstance
      .addProperty(CarbonCommonConstants.CARBONDATA_VERSION,
        CarbonCommonConstants.CARBONDATA_DEFAULT_VERSION)
    val version = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBONDATA_VERSION)
    assert(CarbonCommonConstants.CARBONDATA_DEFAULT_VERSION.equals(version))
  }

  test("data file version test") {
    val version = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION)
    assert("V3".equals(version))
  }

  test("new carbonsession test") {
    try {
      VersionTest.main(null)
      assert(true)
    } catch {
      case _: Exception => assert(false)
    }
  }
}

object VersionTest {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.CarbonSession._
    val carbonSession = SparkSession
      .builder()
      .master("local")
      .enableHiveSupport()
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreateCarbonSession()
    carbonSession.sparkContext.setLogLevel("warn")
    val version = carbonSession.asInstanceOf[CarbonSession].carbonVersion
    assert(version.equals(CarbonCommonConstants.CARBONDATA_DEFAULT_VERSION))
    carbonSession.close()
  }
}
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

package org.apache.carbondata.integration.spark.testsuite.complexType

import java.io.File

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test class of Adaptive Encoding UnSafe Column Page with Complex Data type
 *
 */

class TestAdaptiveEncodingUnsafeHeapColumnPageForComplexDataType
  extends QueryTest with BeforeAndAfterAll with TestAdaptiveComplexType {

  override def beforeAll(): Unit = {

    new File(CarbonProperties.getInstance().getSystemFolderLocation).delete()
    sql("DROP TABLE IF EXISTS adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
        "false")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS adaptive")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        "true")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
        "true")
  }


}

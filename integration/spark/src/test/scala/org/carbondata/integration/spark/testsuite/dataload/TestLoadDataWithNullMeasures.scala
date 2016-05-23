/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading when there are null measures in data
 *
 */
class TestLoadDataWithNullMeasures extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("CREATE CUBE carboncube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
  }
  test("test carbon table data loading when there are null measures in data") {
    try {
      sql("LOAD DATA FACT FROM './src/test/resources/datawithnullmsrs.csv' INTO CUBE carboncube OPTIONS(DELIMITER ',')");
    } catch {
      case e : Throwable => e.printStackTrace()
    }
  }

  override def afterAll {
    sql("drop cube carboncube")
  }
}

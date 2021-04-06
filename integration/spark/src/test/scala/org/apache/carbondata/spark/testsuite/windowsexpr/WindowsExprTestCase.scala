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

package org.apache.carbondata.spark.testsuite.windowsexpr

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for all query on multiple datatypes
 */
class WindowsExprTestCase extends QueryTest with BeforeAndAfterAll {
  // scalastyle:off lineLength
  override def beforeAll {
    sql("drop table if exists windowstable")
    sql("drop table if exists hivewindowstable")
    sql("CREATE TABLE IF NOT EXISTS windowstable (ID double, date Timestamp, country String,name String, phonetype String, serialname String, salary double) STORED AS carbondata")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("CREATE TABLE IF NOT EXISTS hivewindowstable (ID double, date Timestamp, country String,name String, phonetype String, serialname String, salary double) row format delimited fields terminated by ','")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/source_without_header.csv' INTO table hivewindowstable""")
    sql("insert into windowstable select * from hivewindowstable")
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table windowstable")
    sql("drop table hivewindowstable")
  }

  test("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM windowstable) tmp WHERE rank <= 2 order by country") {
      sql("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM windowstable) tmp WHERE rank <= 2 order by country").collect()
      sql("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM hivewindowstable) tmp WHERE rank <= 2 order by country").collect()
  }

  test("SELECT ID, country, SUM(salary) OVER (PARTITION BY country ) AS TopBorcT FROM windowstable") {
    checkAnswer(
      sql("SELECT ID, country, SUM(salary) OVER (PARTITION BY country ) AS TopBorcT FROM windowstable"),
      sql("SELECT ID, country, SUM(salary) OVER (PARTITION BY country ) AS TopBorcT FROM hivewindowstable"))
  }

  test("SELECT country,name,salary,ROW_NUMBER() OVER (PARTITION BY country ORDER BY salary DESC) as rownum FROM windowstable") {
    checkAnswer(
      sql("SELECT country,name,salary,ROW_NUMBER() OVER (PARTITION BY country ORDER BY salary DESC) as rownum FROM windowstable"),
      sql("SELECT country,name,salary,ROW_NUMBER() OVER (PARTITION BY country ORDER BY salary DESC) as rownum FROM hivewindowstable"))
  }
  // scalastyle:on lineLength
}

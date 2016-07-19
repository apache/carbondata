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

package org.carbondata.spark.testsuite.windowsexpr

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

/**
  * Test Class for all query on multiple datatypes
  *
  */
class WindowsExprTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    sql("CREATE TABLE IF NOT EXISTS windowstable (ID double, date Timestamp, country String,name String, phonetype String, serialname String, salary double) STORED BY 'carbondata'");
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,"dd-MM-yyyy")
    sql("LOAD DATA LOCAL INPATH '"+currentDirectory+"/src/test/resources/windows.csv' INTO table windowstable options('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'ID,date,country,name,phonetype,serialname,salary')");
    sql("CREATE TABLE IF NOT EXISTS hivewindowstable (ID double, date Timestamp, country String,name String, phonetype String, serialname String, salary double) row format delimited fields terminated by ','");
    sql("LOAD DATA LOCAL INPATH '"+currentDirectory+"/src/test/resources/windows.csv' INTO table hivewindowstable ");

  }

  override def afterAll {
    sql("drop table windowstable")
    sql("drop table hivewindowstable")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

  test("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM windowstable) tmp WHERE rank <= 2 order by country") {
    checkAnswer(
      sql("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM windowstable) tmp WHERE rank <= 2 order by country"),
      sql("SELECT country,name,salary FROM (SELECT country,name,salary,dense_rank() OVER (PARTITION BY country ORDER BY salary DESC) as rank FROM hivewindowstable) tmp WHERE rank <= 2 order by country"))
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
  
}
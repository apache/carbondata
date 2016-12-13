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

package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.constants.CarbonCommonConstants

/*
 * Test Class for query without data load
 *
 */
class TestQueryWithOldCarbonDataFile extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
	  CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "V1");
    sql("drop table if exists OldFormatTable")
    sql("drop table if exists OldFormatTableHIVE")
     sql("""
           CREATE TABLE IF NOT EXISTS OldFormatTable
           (country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)
      sql("""
           CREATE TABLE IF NOT EXISTS OldFormatTableHIVE
           (country String,
           name String, phonetype String, serialname String, salary Int)
          row format delimited fields terminated by ','
           """)      
    sql("LOAD DATA local inpath './src/test/resources/OLDFORMATTABLE.csv' INTO table OldFormatTable")
   sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/OLDFORMATTABLEHIVE.csv' into table OldFormatTableHIVE
           """)

  }

  CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "V2")
  test("Test select * query") {
    checkAnswer(
      sql("select * from OldFormatTable"), sql("select * from OldFormatTableHIVE")
    )
  }

  override def afterAll {
     CarbonProperties.getInstance.addProperty(CarbonCommonConstants.CARBON_DATA_FILE_VERSION, "V1")
    sql("drop table if exists OldFormatTable")
    sql("drop table if exists OldFormatTableHIVE")
  }

}

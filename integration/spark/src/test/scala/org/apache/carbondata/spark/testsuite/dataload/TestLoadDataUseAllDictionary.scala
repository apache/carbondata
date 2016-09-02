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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestLoadDataUseAllDictionary extends QueryTest with BeforeAndAfterAll{
  override def beforeAll {
    sql("DROP TABLE IF EXISTS t3")
    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)
  }

  test("test load data use all dictionary, and given wrong format dictionary values") {
    try {
      sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/windows.csv' into table t3
           options('FILEHEADER'='id,date,country,name,phonetype,serialname,salary',
           'All_DICTIONARY_PATH'='./src/test/resources/dict.txt')
           """)
      assert(false)
    } catch {
      case e: DataLoadingException =>
        assert(e.getMessage.equals("Data Loading failure, dictionary values are " +
          "not in correct format!"))
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS t3")
  }
}

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

package org.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for detailed query on Int and BigInt of No Dictionary col
  *
  */
class NoDictionaryColumnTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE carbonTable (imei String, age Int, num BigInt) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='age,num')")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/datawithNegtiveNumber.csv' INTO TABLE carbonTable")
    sql("CREATE TABLE hiveTable (imei String, age Int, num BigInt) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/datawithNegeativewithoutHeader.csv' INTO TABLE hiveTable")
  }

  test("SELECT IntType FROM carbonTable") {
    checkAnswer(
      sql("SELECT imei,age FROM carbonTable ORDER BY imei"),
      sql("SELECT imei,age FROM hiveTable ORDER BY imei")
    )
  }

  test("SELECT BigIntType FROM carbonTable") {
    checkAnswer(
      sql("SELECT imei,num FROM carbonTable ORDER BY imei"),
      sql("SELECT imei,num FROM hiveTable ORDER BY imei")
    )
  }

  override def afterAll {
    sql("DROP TABLE carbonTable")
    sql("DROP TABLE hiveTable")
  }
}
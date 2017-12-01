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

package org.apache.carbondata.spark.testsuite.createTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateTableWithInvalidProperty extends QueryTest with BeforeAndAfterAll  {

  override def beforeAll: Unit = {
    sql("use default")
    sql("DROP TABLE IF EXISTS invalidtblproperty")
  }

  test("Create table with invalid table properties") {

    val exception_invalid_prop: Exception = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE invalidtblproperty(
           | intField INT,
           | stringField STRING
           | )
           | STORED BY 'carbondata'
           | TBLPROPERTIES('Invalid_property'='stringField')
       """.stripMargin)
    }
    assert(exception_invalid_prop.getMessage.contains("Error: Invalid option(s): invalid_property"))
  }

  override def afterAll {
    sql("drop table if exists invalidtblproperty")
  }

}

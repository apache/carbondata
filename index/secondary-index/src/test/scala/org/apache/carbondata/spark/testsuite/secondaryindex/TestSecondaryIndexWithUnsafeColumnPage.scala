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
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestSecondaryIndexWithUnsafeColumnPage extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("drop table if exists testSecondryIndex")
    sql("create table testSecondryIndex( a string,b string,c string) STORED AS carbondata")
    sql("insert into testSecondryIndex select 'babu','a','6'")
    sql("create index testSecondryIndex_IndexTable on table testSecondryIndex(b) AS 'carbondata'")
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from testSecondryIndex_IndexTable")
    ,Seq(Row(1)))
  }

  override def afterAll {
    sql("drop table if exists testIndexTable")
  }

}

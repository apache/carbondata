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

package org.apache.spark.carbondata.scalarsubquery

import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.command.LoadTable
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class ScalarSubqueryTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    sql("DROP TABLE IF EXISTS scalarsubquery")
    // clean data folder

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
    """
           CREATE TABLE scalarsubquery
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
      """)
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataDiff.csv' INTO table scalarsubquery")
  }

  test("test scalar subquery with equal") {
    sql(
      """select sum(salary) from scalarsubquery t1
        |where ID = (select sum(ID) from scalarsubquery t2 where t1.name = t2.name)""".stripMargin)
      .count()
  }

  test("test scalar subquery with lessthan") {
    sql(
      """select sum(salary) from scalarsubquery t1
        |where ID < (select sum(ID) from scalarsubquery t2 where t1.name = t2.name)""".stripMargin)
      .count()
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS scalarsubquery")
  }
}

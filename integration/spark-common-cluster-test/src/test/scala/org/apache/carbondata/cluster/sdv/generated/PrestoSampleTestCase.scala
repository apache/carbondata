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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

class PrestoSampleTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS sample_table")
    if (System.getProperty("spark.master.url") != null) {
    QueryTest.PrestoQueryTest.initJdbcConnection("default")
    }
  }

  test("test read spark store from presto ") {
    sql("show tables").show(false)

    sql("DROP TABLE IF EXISTS sample_table")
    sql("CREATE TABLE sample_table (name string) STORED AS carbondata")
    sql("insert into sample_table select 'ajantha'")
    sql("select * from sample_table ").show(200, false)
    sql("describe formatted sample_table ").show(200, false)
    if (System.getProperty("spark.master.url") != null) {
      // supports only running through cluster
      val actualResult: List[Map[String, Any]] = QueryTest.PrestoQueryTest
              .executeQuery("select * from sample_table")
     println("ans---------" + actualResult(0).toString())
      val expectedResult: List[Map[String, Any]] = List(Map(
        "name" -> "ajantha"))
      assert(actualResult.toString() equals expectedResult.toString())
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS sample_table")
    QueryTest.PrestoQueryTest.closeJdbcConnection()
  }

}
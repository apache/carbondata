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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException

class TestPreAggregateDrop extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists maintable")
    sql("create table maintable (a string, b string, c string) stored by 'carbondata'")
  }

  test("create and drop preaggregate table") {
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql("drop datamap if exists preagg1 on table maintable")
    checkExistence(sql("show tables"), false, "maintable_preagg1")
  }

  test("dropping 1 aggregate table should not drop others") {
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql(
      "create datamap preagg2 on table maintable using 'preaggregate' as select" +
      " a,sum(c) from maintable group by a")
    sql("drop datamap if exists preagg2 on table maintable")
    val showTables = sql("show tables")
    checkExistence(showTables, false, "maintable_preagg2")
    checkExistence(showTables, true, "maintable_preagg1")
  }

  test("drop datamap which is not existed") {
    intercept[NoSuchDataMapException] {
      sql("drop datamap newpreagg on table maintable")
    }
  }

  test("drop datamap with same name on different tables") {
    sql("drop table if exists maintable1")
    sql("create datamap preagg_same on table maintable using 'preaggregate' as select" +
    " a,sum(c) from maintable group by a")
    sql("create table maintable1 (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap preagg_same on table maintable1 using 'preaggregate' as select" +
    " a,sum(c) from maintable1 group by a")

    sql("drop datamap preagg_same on table maintable")
    var showTables = sql("show tables")
    checkExistence(showTables, false, "maintable_preagg_same")
    checkExistence(showTables, true, "maintable1_preagg_same")
    sql("drop datamap preagg_same on table maintable1")
    showTables = sql("show tables")
    checkExistence(showTables, false, "maintable1_preagg_same")
    sql("drop table if exists maintable1")
  }

  test("drop datamap and create again with same name") {
    sql("create datamap preagg_same1 on table maintable using 'preaggregate' as select" +
    " a,sum(c) from maintable group by a")

    sql("drop datamap preagg_same1 on table maintable")
    var showTables = sql("show tables")
    checkExistence(showTables, false, "maintable_preagg_same1")
    sql("create datamap preagg_same1 on table maintable using 'preaggregate' as select" +
        " a,sum(c) from maintable group by a")
    showTables = sql("show tables")
    checkExistence(showTables, true, "maintable_preagg_same1")
    sql("drop datamap preagg_same1 on table maintable")
  }



  test("drop main table and check if preaggreagte is deleted") {
    sql(
      "create datamap preagg2 on table maintable using 'preaggregate' as select" +
      " a,sum(c) from maintable group by a")
    sql("drop table if exists maintable")
    checkExistence(sql("show tables"), false, "maintable_preagg1", "maintable", "maintable_preagg2")
  }

  override def afterAll() {
    sql("drop table if exists maintable")
    sql("drop table if exists maintable1")
  }
  
}

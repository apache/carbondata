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

package org.apache.spark.carbondata.query

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.sources.{And, EqualTo, Filter, Or}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestFilterReordering extends QueryTest with BeforeAndAfterAll{

  override protected def beforeAll(): Unit = {
    sql("drop table if exists filter_reorder")
    sql("create table filter_reorder(one string, two string, three string, four int, " +
        "five int) stored as carbondata")
  }

  test("Test filter reorder with various conditions") {
    val filter1 = Or(And(EqualTo("four", 11), EqualTo("two", 11)), EqualTo("one", 11))
    val table = CarbonEnv.getCarbonTable(None, "filter_reorder")(sqlContext.sparkSession)
    var d: (Filter, Int) = CarbonFilters.reorderFilter(filter1, table)
    assert(d._1.references.sameElements(Array("one", "two", "four")))

    val filter2 = Or(Or(EqualTo("four", 11), EqualTo("two", 11)),
      Or(EqualTo("one", 11), Or(EqualTo("five", 11), EqualTo("three", 11))))
    d = CarbonFilters.reorderFilter(filter2, table)
    assert(d._1.references.sameElements(Array("one", "two", "three", "four", "five")))

    val filter3 = Or(Or(EqualTo("four", 11), EqualTo("two", 11)),
      Or(EqualTo("one", 11), Or(EqualTo("five", 11),
        And(EqualTo("three", 11), EqualTo("three", 11)))))
    d = CarbonFilters.reorderFilter(filter3, table)
    assert(d._1.references.sameElements(Array("one", "three", "three", "five", "two", "four")))
  }

  override protected def afterAll(): Unit = {
    sql("drop table if exists filter_reorder")
  }
}

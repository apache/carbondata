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
package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

class MVInvalidTestCase  extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    drop
    sql("create table main_table (name string,age int,height int) stored by 'carbondata'")
  }

  def drop {
    sql("drop datamap if exists main_table_mv")
    sql("drop table if exists main_table")
  }

  test("test mv different filter") {
    val querySQL = "select age,name from main_table where name = 'lily' order by name limit 10"
    sql("insert into main_table select 'tom',20,170")
    sql("insert into main_table select 'lily',30,160")
    sql("create datamap main_table_mv on table main_table using 'mv' as select age,name,height from main_table where name = 'tom'")
    sql("rebuild datamap main_table_mv")

    assert(!TestUtil.verifyMVDataMap(sql(querySQL).queryExecution.analyzed, "main_table_mv"))
  }

  override def afterAll(): Unit = {
    drop
  }
}

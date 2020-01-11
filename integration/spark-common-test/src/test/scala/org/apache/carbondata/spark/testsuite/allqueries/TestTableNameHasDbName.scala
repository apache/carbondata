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

package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/*
 * Test Class for query when part of tableName has dbName
 *
 */
class TestTableNameHasDbName extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS tabledefault")
    sql("CREATE TABLE tabledefault (empno int, workgroupcategory string, " +
      "deptno int, projectcode int,attendance int)" +
      " STORED AS carbondata")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE tabledefault")
  }

  test("test query when part of tableName has dbName") {
    try {
      sql("SELECT * FROM tabledefault").collect()
    } catch {
      case ex: Exception =>
        assert(false)
    }
  }

  override def afterAll {
    sql("DROP TABLE tabledefault")
  }

}


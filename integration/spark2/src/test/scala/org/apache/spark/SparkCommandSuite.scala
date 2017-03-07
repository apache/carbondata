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

package org.apache.spark

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class SparkCommandSuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS src_pqt")
    sql("DROP TABLE IF EXISTS src_orc")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS src_pqt")
    sql("DROP TABLE IF EXISTS src_orc")
  }

  test("CARBONDATA-734: Support the syntax of 'STORED BY PARQUET/ORC'") {
    sql("CREATE TABLE src_pqt(key INT, value STRING) STORED AS PARQUET")
    sql("CREATE TABLE src_orc(key INT, value STRING) STORED AS ORC")
  }
}

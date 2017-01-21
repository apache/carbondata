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

package org.apache.carbondata.spark.testsuite.aggquery


import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for aggregate query on Integer datatypes
 *
 */
class IntegerDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE integertypetableAgg (empno int, workgroupcategory string, deptno int, projectcode int, attendance int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE integertypetableAgg OPTIONS ('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'='')""")
  }

  test("select empno from integertypetableAgg") {
    checkAnswer(
      sql("select empno from integertypetableAgg"),
      Seq(Row(11), Row(12), Row(13), Row(14), Row(15), Row(16), Row(17), Row(18), Row(19), Row(20)))
  }

  override def afterAll {
    sql("drop table integertypetableAgg")
  }
}
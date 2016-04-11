/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for detailed query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE CUBE alldatatypescube DIMENSIONS (empno Integer, empname String, designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE alldatatypescube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      Seq(Row(11, "arvind", 96.2, 1, 11), Row(15, "ayushi", 91.5, 1, 15)))
  }

  override def afterAll {
    sql("drop cube alldatatypescube")
  }
}
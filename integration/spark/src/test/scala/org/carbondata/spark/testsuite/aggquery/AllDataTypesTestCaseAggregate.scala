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

package org.carbondata.spark.testsuite.aggquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

/**
 * Test Class for aggregate query on multiple datatypes
 *
 */
class AllDataTypesTestCaseAggregate extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("CREATE CUBE alldatatypescubeAGG DIMENSIONS (empno Integer, empname String, designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE alldatatypescubeAGG PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      Seq(Row(11, "arvind", 96.2, 1, 11), Row(15, "ayushi", 91.5, 1, 15)))
  }

  test("select empname,trim(designation),avg(salary),avg(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empname,trim(designation)") {
    checkAnswer(
      sql("select empname,trim(designation),avg(salary),avg(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empname,trim(designation)"),
      Seq(Row("arvind", "SE", 5040.56, 11.0), Row("ayushi", "SSA", 13245.48, 15.0)))
  }

  test("select empname,length(designation),max(empno),min(empno), avg(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empname,length(designation) order by empname") {
    checkAnswer(
      sql("select empname,length(designation),max(empno),min(empno), avg(empno) from alldatatypescubeAGG where empname in ('arvind','ayushi') group by empname,length(designation) order by empname"),
      Seq(Row("arvind", 2, 11, 11, 11.0), Row("ayushi", 3, 15, 15, 15.0)))
  }

  override def afterAll {
    sql("drop cube alldatatypescubeAGG")
  }
}
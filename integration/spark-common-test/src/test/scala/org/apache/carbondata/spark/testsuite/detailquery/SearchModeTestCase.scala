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

package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for detailed query on multiple datatypes
 */

class SearchModeTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypestable (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE alldatatypestable OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')""")

    sql("CREATE TABLE alldatatypestable_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO TABLE alldatatypestable_hive""")

  }

  test("SearchMode Query: row result") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE, "true")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
        checkAnswer(
      sql("select empno,empname,utilization from alldatatypestable where empname = 'ayushi'"),
      sql("select empno,empname,utilization from alldatatypestable_hive where empname = 'ayushi'"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE,
      CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE_DEFAULT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  }
  test("SearchMode Query: vector result") {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE, "true")
    checkAnswer(
      sql("select empno,empname,utilization from alldatatypestable where empname = 'ayushi'"),
      sql("select empno,empname,utilization from alldatatypestable_hive where empname = 'ayushi'"))
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE,
      CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE_DEFAULT)
  }

  override def afterAll {
    sql("drop table alldatatypestable")
    sql("drop table alldatatypestable_hive")
  }
}
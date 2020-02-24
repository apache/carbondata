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

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
 * Test Class for Range Filters.
 */
class CastColumnTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    //For the Hive table creation and data loading
    sql("drop table if exists DICTIONARY_CARBON_1")
    sql("drop table if exists DICTIONARY_HIVE_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_2")
    sql("drop table if exists NO_DICTIONARY_HIVE_2")

    //For Carbon cube creation.
    sql("CREATE TABLE DICTIONARY_CARBON_1 (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE DICTIONARY_CARBON_1 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table DICTIONARY_HIVE_1(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )

    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "DICTIONARY_HIVE_1"
    )


    sql("CREATE TABLE NO_DICTIONARY_CARBON_2 (empno string, " +
        "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
        "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
        "projectenddate Timestamp, designation String,attendance Int,utilization " +
        "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE NO_DICTIONARY_CARBON_2 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql(
      "create table NO_DICTIONARY_HIVE_2(empno string,empname string,designation string,doj " +
      "Timestamp,workgroupcategory int, " +
      "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
      "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
      + "utilization int,salary int) row format delimited fields terminated by ',' " +
      "tblproperties(\"skip.header.line.count\"=\"1\") " +
      ""
    )

    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
      "NO_DICTIONARY_HIVE_2"
    )
  }
  test("Dictionary String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno = 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno = 11")
    )
  }

  test("Dictionary String OR") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno = '11' or empno = '15'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno = '11' or empno = '15'")
    )
  }

  test("Dictionary String OR Implicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno = 11 or empno = 15"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno = 11 or empno = 15")
    )
  }

  test("Dictionary String OR explicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) = 11 or cast(empno as int) = 15"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(empno as int) = 11 or cast (empno as int) = 15")
    )
  }

  test("Dictionary INT OR to implicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory = '1' or  workgroupcategory = '2'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory = '1' or  workgroupcategory = '2'")
    )
  }

  test("Dictionary INT OR to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as string)= '1' or cast(workgroupcategory as string) = '2'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as string) = '1' or  cast(workgroupcategory as string) = '2'")
    )
  }

  test("Dictionary INT OR to explicit double") {
    sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as double)= 1.0 or cast(workgroupcategory as double) = 2.0").show(2000, false)
    sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) = 1.0 or  cast(workgroupcategory as double) = 2.0 ").show(2000, false)
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as double)= 1.0 or cast(workgroupcategory as double) = 2.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) = 1.0 or  cast(workgroupcategory as double) = 2.0 ")
    )
  }

  test("Dictionary String Not") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno != '11'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno != '11'")
    )
  }

  test("Dictionary String Not Implicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno != 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno != 11")
    )
  }

  test("Dictionary String Not explicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) != 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(empno as int) != 11")
    )
  }

  test("Dictionary INT Not to implicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory != '1'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory != '1'")
    )
  }

  test("Dictionary INT Not to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as string) != '1'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as string) != '1'")
    )
  }

  test("Dictionary INT Not to explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as double) != 1.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) != 1.0")
    )
  }

  test("Dictionary String In") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno in ('11', '15')"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno in ('11', '15')")
    )
  }

  test("Dictionary String In Implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno in (11, 15)"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno in (11, 15)")
    )
  }

  test("Dictionary String In Explicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (empno as int) in (11, 15)"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) in (11, 15)")
    )
  }

  test("Dictionary String In Explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (empno as double) in (11.0, 15.0)"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as double) in (11.0, 15.0)")
    )
  }

  test("Dictionary String Not in Explicit double 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (empno as double)  not in (11.0, 15.0)"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as double) not in (11.0, 15.0)")
    )
  }

  test("Dictionary INT In to implicit Int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory in ('1', '2')"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory in ('1', '2')")
    )
  }

  test("Dictionary INT In to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as string) in ('1', '2')"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as string) in ('1', '2')")
    )
  }

  test("Dictionary INT In to explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(workgroupcategory as double) in (1.0, 2.0)"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) in (1.0, 2.0)")
    )
  }

  test("Dictionary String Greater Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno > '11'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno > '11'")
    )
  }

  test("Dictionary String Greater Than Implicit") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno > 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno > 11")
    )
  }

  test("Dictionary String Greater Than explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) > '11'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) > '11'")
    )
  }


  test("Dictionary String Greater Than explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) > 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) > 11")
    )
  }

  test("Dictionary INT Greater Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory > 1"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory > 1")
    )
  }


  test("Dictionary INT Greater Than implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory > '1'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory > '1'")
    )
  }


  test("Dictionary INT Greater Than double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) > 1.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) > 1.0")
    )
  }


  test("Dictionary INT Greater Than String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) > '1.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) > '1.0'")
    )
  }


  test("Dictionary String Greater Than equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno >= '11'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno >= '11'")
    )
  }

  test("Dictionary String Greater Than Implicit equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno >= 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno >= 11")
    )
  }

  test("Dictionary String Greater Than equal explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) >= '11'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) >= '11'")
    )
  }


  test("Dictionary String Greater Than equal explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) >= 11"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) >= 11")
    )
  }

  test("Dictionary INT Greater Than equal ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory >= 1"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory >= 1")
    )
  }


  test("Dictionary INT Greater Than equal implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory >= '1'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory >= '1'")
    )
  }


  test("Dictionary INT Greater Than equal 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) >= 1.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) >= 1.0")
    )
  }


  test("Dictionary INT Greater equal Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) >= '1.0'")
    )
  }

  test("Dictionary String less Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno < '20'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno < '20'")
    )
  }

  test("Dictionary String Less Than Implicit") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno < 20"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno < 20")
    )
  }

  test("Dictionary String Less Than explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) < '20'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) < '20'")
    )
  }


  test("Dictionary String Less Than explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) < 20"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) < 20")
    )
  }

  test("Dictionary INT Less Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory < 2"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory < 2")
    )
  }


  test("Dictionary INT Less Than implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory < '2'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory < '2'")
    )
  }


  test("Dictionary INT Less Than double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) < 2.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) < 2.0")
    )
  }


  test("Dictionary INT Less Than String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) < '2.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) < '2.0'")
    )
  }


  test("Dictionary String Less Than equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno <= '20'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno <= '20'")
    )
  }

  test("Dictionary String Less Than Implicit equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where empno <= 20"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where empno <= 20")
    )
  }

  test("Dictionary String Less Than equal explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) <= '20'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) <= '20'")
    )
  }


  test("Dictionary String Less Than equal explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast(empno as int) <= 20"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast (empno as int) <= 20")
    )
  }

  test("Dictionary INT Less Than equal ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory <= 2"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory <= 2")
    )
  }


  test("Dictionary INT Less Than equal implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory <= '2'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory <= '2'")
    )
  }


  test("Dictionary INT Less Than equal 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) <= 2.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) <= 2.0")
    )
  }


  test("Dictionary INT Less equal Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) <= '2.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) <= '2.0'")
    )
  }


  test("Dictionary INT greater less Than explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as double) <= '2.0' and cast (workgroupcategory as double) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as double) <= '2.0' and cast (workgroupcategory as double) >= '1.0'")
    )
  }


  test("Dictionary INT greater less Than explicit string") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where cast (workgroupcategory as string) <= '2.0' and cast (workgroupcategory as string) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where cast(workgroupcategory as string) <= '2.0' and cast (workgroupcategory as string) >= '1.0'")
    )
  }


  test("Dictionary INT greater less Than implicit  string") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory <= '2.0' and workgroupcategory >= '1.0'"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory  <= '2.0' and workgroupcategory >= '1.0'")
    )
  }


  test("Dictionary INT greater less Than implicit int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory <= 2 and workgroupcategory >= 1"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory  <= 2 and workgroupcategory >= 1")
    )
  }


  test("Dictionary INT greater less Than implicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from DICTIONARY_CARBON_1 where workgroupcategory <= 2.0 and workgroupcategory >= 1.0"),
      sql("select empno,empname,workgroupcategory from DICTIONARY_HIVE_1 where workgroupcategory  <= 2.0 and workgroupcategory >= 1.0")
    )
  }


  // No dictionary cases
  test("NO Dictionary String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno = 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno = 11")
    )
  }

  test("NO Dictionary String OR") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno = '11' or empno = '15'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno = '11' or empno = '15'")
    )
  }

  test("NO Dictionary String OR Implicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno = 11 or empno = 15"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno = 11 or empno = 15")
    )
  }

  test("NO Dictionary String OR explicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) = 11 or cast(empno as int) = 15"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(empno as int) = 11 or cast (empno as int) = 15")
    )
  }

  test("NO Dictionary INT OR to implicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory = '1' or  workgroupcategory = '2'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory = '1' or  workgroupcategory = '2'")
    )
  }

  test("NO Dictionary INT OR to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as string)= '1' or cast(workgroupcategory as string) = '2'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as string) = '1' or  cast(workgroupcategory as string) = '2'")
    )
  }

  test("NO Dictionary INT OR to explicit double") {
    sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as double)= 1.0 or cast(workgroupcategory as double) = 2.0").show(2000, false)
    sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) = 1.0 or  cast(workgroupcategory as double) = 2.0 ").show(2000, false)
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as double)= 1.0 or cast(workgroupcategory as double) = 2.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) = 1.0 or  cast(workgroupcategory as double) = 2.0 ")
    )
  }

  test("NO Dictionary String Not") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno != '11'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno != '11'")
    )
  }

  test("NO Dictionary String Not Implicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno != 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno != 11")
    )
  }

  test("NO Dictionary String Not explicit Cast to int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) != 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(empno as int) != 11")
    )
  }

  test("NO Dictionary INT Not to implicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory != '1'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory != '1'")
    )
  }

  test("NO Dictionary INT Not to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as string) != '1'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as string) != '1'")
    )
  }

  test("NO Dictionary INT Not to explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as double) != 1.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) != 1.0")
    )
  }

  test("NO Dictionary String In") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno in ('11', '15')"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno in ('11', '15')")
    )
  }

  test("NO Dictionary String In Implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno in (11, 15)"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno in (11, 15)")
    )
  }

  test("NO Dictionary String In Explicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (empno as int) in (11, 15)"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) in (11, 15)")
    )
  }

  test("NO Dictionary String In Explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (empno as double) in (11.0, 15.0)"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as double) in (11.0, 15.0)")
    )
  }

  test("NO Dictionary String Not in Explicit double 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (empno as double)  not in (11.0, 15.0)"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as double) not in (11.0, 15.0)")
    )
  }

  test("NO Dictionary INT In to implicit Int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory in ('1', '2')"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory in ('1', '2')")
    )
  }

  test("NO Dictionary INT In to excplicit String") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as string) in ('1', '2')"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as string) in ('1', '2')")
    )
  }

  test("NO Dictionary INT In to explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(workgroupcategory as double) in (1.0, 2.0)"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) in (1.0, 2.0)")
    )
  }

  test("NO Dictionary String Greater Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno > '11'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno > '11'")
    )
  }

  test("NO Dictionary String Greater Than Implicit") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno > 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno > 11")
    )
  }

  test("NO Dictionary String Greater Than explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) > '11'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) > '11'")
    )
  }


  test("NO Dictionary String Greater Than explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) > 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) > 11")
    )
  }

  test("NO Dictionary INT Greater Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory > 1"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory > 1")
    )
  }


  test("NO Dictionary INT Greater Than implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory > '1'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory > '1'")
    )
  }


  test("NO Dictionary INT Greater Than double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) > 1.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) > 1.0")
    )
  }


  test("NO Dictionary INT Greater Than String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) > '1.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) > '1.0'")
    )
  }


  test("NO Dictionary String Greater Than equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno >= '11'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno >= '11'")
    )
  }

  test("NO Dictionary String Greater Than Implicit equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno >= 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno >= 11")
    )
  }

  test("NO Dictionary String Greater Than equal explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) >= '11'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) >= '11'")
    )
  }


  test("NO Dictionary String Greater Than equal explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) >= 11"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) >= 11")
    )
  }

  test("NO Dictionary INT Greater Than equal ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory >= 1"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory >= 1")
    )
  }


  test("NO Dictionary INT Greater Than equal implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory >= '1'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory >= '1'")
    )
  }


  test("NO Dictionary INT Greater Than equal 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) >= 1.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) >= 1.0")
    )
  }


  test("NO Dictionary INT Greater equal Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) >= '1.0'")
    )
  }

  test("NO Dictionary String less Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno < '20'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno < '20'")
    )
  }

  test("NO Dictionary String Less Than Implicit") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno < 20"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno < 20")
    )
  }

  test("NO Dictionary String Less Than explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) < '20'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) < '20'")
    )
  }


  test("NO Dictionary String Less Than explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) < 20"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) < 20")
    )
  }

  test("NO Dictionary INT Less Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory < 2"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory < 2")
    )
  }


  test("NO Dictionary INT Less Than implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory < '2'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory < '2'")
    )
  }


  test("NO Dictionary INT Less Than double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) < 2.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) < 2.0")
    )
  }


  test("NO Dictionary INT Less Than String ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) < '2.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) < '2.0'")
    )
  }


  test("NO Dictionary String Less Than equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno <= '20'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno <= '20'")
    )
  }

  test("NO Dictionary String Less Than Implicit equal") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where empno <= 20"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where empno <= 20")
    )
  }

  test("NO Dictionary String Less Than equal explicit 1") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) <= '20'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) <= '20'")
    )
  }


  test("NO Dictionary String Less Than equal explicit 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast(empno as int) <= 20"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast (empno as int) <= 20")
    )
  }

  test("NO Dictionary INT Less Than equal ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory <= 2"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory <= 2")
    )
  }


  test("NO Dictionary INT Less Than equal implicit ") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory <= '2'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory <= '2'")
    )
  }


  test("NO Dictionary INT Less Than equal 2") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) <= 2.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) <= 2.0")
    )
  }


  test("NO Dictionary INT Less equal Than") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) <= '2.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) <= '2.0'")
    )
  }


  test("NO Dictionary INT greater less Than explicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as double) <= '2.0' and cast (workgroupcategory as double) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as double) <= '2.0' and cast (workgroupcategory as double) >= '1.0'")
    )
  }


  test("NO Dictionary INT greater less Than explicit string") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where cast (workgroupcategory as string) <= '2.0' and cast (workgroupcategory as string) >= '1.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where cast(workgroupcategory as string) <= '2.0' and cast (workgroupcategory as string) >= '1.0'")
    )
  }


  test("NO Dictionary INT greater less Than implicit  string") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory <= '2.0' and workgroupcategory >= '1.0'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory  <= '2.0' and workgroupcategory >= '1.0'")
    )
  }


  test("NO Dictionary INT greater less Than implicit int") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory <= 2 and workgroupcategory >= 1"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory  <= 2 and workgroupcategory >= 1")
    )
  }


  test("NO Dictionary INT greater less Than implicit double") {
    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_2 where workgroupcategory <= 2.0 and workgroupcategory >= 1.0"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_2 where workgroupcategory  <= 2.0 and workgroupcategory >= 1.0")
    )
  }

  // Direct Dictionary and Timestamp Column.




  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists DICTIONARY_CARBON_1")
    sql("drop table if exists DICTIONARY_HIVE_1")
    sql("drop table if exists NO_DICTIONARY_CARBON_2")
    sql("drop table if exists NO_DICTIONARY_HIVE_2")
  }
}
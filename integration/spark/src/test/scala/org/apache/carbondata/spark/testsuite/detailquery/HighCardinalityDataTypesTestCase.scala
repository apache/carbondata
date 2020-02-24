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
  * Test Class for verifying NO_DICTIONARY_COLUMN feature.
  */
class NO_DICTIONARY_COL_TestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    //For the Hive table creation and data loading
    sql("drop table if exists filtertestTable")
    sql("drop table if exists NO_DICTIONARY_HIVE_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_7")
    
    sql(
      "create table NO_DICTIONARY_HIVE_6(empno string,empname string,designation string,doj " +
        "Timestamp,workgroupcategory int, " +
        "workgroupcategoryname string,deptno int, deptname string, projectcode int, " +
        "projectjoindate Timestamp,projectenddate Timestamp,attendance int, "
        + "utilization int,salary int) row format delimited fields terminated by ',' " +
        "tblproperties(\"skip.header.line.count\"=\"1\") " +
        ""
    )
    sql(
      s"load data local inpath '$resourcesPath/datawithoutheader.csv' into table " +
        "NO_DICTIONARY_HIVE_6"
    );
    //For Carbon cube creation.
    sql("CREATE TABLE NO_DICTIONARY_CARBON_6 (empno string, " +
      "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
      "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
      "projectenddate Timestamp, designation String,attendance Int,utilization " +
      "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE NO_DICTIONARY_CARBON_6 " +
        "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )

    sql("CREATE TABLE NO_DICTIONARY_CARBON_7 (empno string, " +
      "doj Timestamp, workgroupcategory Int, empname String,workgroupcategoryname String, " +
      "deptno Int, deptname String, projectcode Int, projectjoindate Timestamp, " +
      "projectenddate Timestamp, designation String,attendance Int,utilization " +
      "Int,salary Int) STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE NO_DICTIONARY_CARBON_7 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql("CREATE TABLE filtertestTable (ID string,date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary Int) " +
        "STORED AS carbondata "
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data2.csv' INTO TABLE filtertestTable OPTIONS"+
        s"('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )

  }

  test("Count (*) with filter") {
    sql("select * from NO_DICTIONARY_CARBON_6 where empno > '11' and empno <= '30'")
    checkAnswer(
      sql("select count(*) from NO_DICTIONARY_CARBON_6 where empno='11'"),
      Seq(Row(1))
    )
  }


  test("Detail Query with NO_DICTIONARY_COLUMN Compare With HIVE RESULT") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_6"),
      Seq(Row("11"), Row("12"), Row("13"), Row("14"), Row("15"), Row("16"), Row("17"), Row("18"), Row("19"), Row("20"))
    )


  }

  test("Detail Query with NO_DICTIONARY_COLUMN with Like range filter") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_7 where empno like '12%'"),
      Seq(Row("12"))
    )
  }

  test("Detail Query with NO_DICTIONARY_COLUMN with greater than range filter") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_7 where empno>'19'"),
      Seq(Row("20"))
    )
  }

  test("Detail Query with NO_DICTIONARY_COLUMN with  in filter Compare With HIVE RESULT") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_6 where empno in('11','12','13')"),
      Seq(Row("11"), Row("12"), Row("13"))
    )
  }
  test("Detail Query with NO_DICTIONARY_COLUMN with not in filter Compare With HIVE RESULT") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_6 where empno not in('11','12','13','14','15','16','17')"),
      Seq(Row("18"), Row("19"), Row("20"))
    )
  }

  test("Detail Query with NO_DICTIONARY_COLUMN with equals filter Compare With HIVE RESULT") {


    checkAnswer(
      sql("select empno from NO_DICTIONARY_CARBON_6 where empno='17'"),
      Seq(Row("17"))
    )
  }
  test("Detail Query with NO_DICTIONARY_COLUMN with IS NOT NULL filter") {


    checkAnswer(
      sql("select id  from filtertestTable where id is not null"),
      Seq(Row("4"),Row("6"),Row("abc"))
    )
  }
test("filter with arithmetic expression") {
    checkAnswer(
      sql("select id from filtertestTable " + "where id+2 = 6"),
      Seq(Row("4"))
    )
  }
  test("Detail Query with NO_DICTIONARY_COLUMN with equals multiple filter Compare With HIVE " +
    "RESULT"
  ) {


    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 where empno='17'"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 where empno='17'")
    )
  }


  test("ORDER Query with NO_DICTIONARY_COLUMN Compare With HIVE RESULT") {

    checkAnswer(
      sql("select empno from NO_DICTIONARY_HIVE_6 order by empno"),
      sql("select empno from NO_DICTIONARY_CARBON_6 order by empno")
    )
  }
  //TODO need to add filter test cases for no dictionary columns
  //
  //    test("Filter Query with NO_DICTIONARY_COLUMN and DICTIONARY_COLUMN Compare With HIVE
  // RESULT") {
  //
  //     checkAnswer(
  //      sql("select empno from NO_DICTIONARY_HIVE_6 where empno=15 and deptno=12"),
  //      sql("select empno from NO_DICTIONARY_CARBON_6 where empno=15 and deptno=12"))
  //   }

  test("Distinct Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {

    checkAnswer(
      sql("select count(distinct empno) from NO_DICTIONARY_HIVE_6"),
      sql("select count(distinct empno) from NO_DICTIONARY_CARBON_6")
    )
  }
  test("Sum Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {

    checkAnswer(
      sql("select sum(empno) from NO_DICTIONARY_HIVE_6"),
      sql("select sum(empno) from NO_DICTIONARY_CARBON_6")
    )
  }

  test("average Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {

    checkAnswer(
      sql("select avg(empno) from NO_DICTIONARY_HIVE_6"),
      sql("select avg(empno) from NO_DICTIONARY_CARBON_6")
    )
  }


  test("Multiple column  group Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {

    checkAnswer(
      sql(
        "select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6 group by empno,empname," +
          "workgroupcategory"
      ),
      sql(
        "select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 group by empno," +
          "empname,workgroupcategory"
      )
    )
  }

  test("Multiple column  Detail Query with NO_DICTIONARY_COLUMN  Compare With HIVE RESULT") {

    checkAnswer(
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_HIVE_6"),
      sql("select empno,empname,workgroupcategory from NO_DICTIONARY_CARBON_6 ")
    )
  }
        


  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists filtertestTable")
    sql("drop table if exists NO_DICTIONARY_HIVE_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_6")
    sql("drop table if exists NO_DICTIONARY_CARBON_7")
    //sql("drop cube NO_DICTIONARY_CARBON_1")
  }
}
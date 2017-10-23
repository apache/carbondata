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

package org.apache.spark.carbondata.restructure

import java.io.File
import java.math.{BigDecimal, RoundingMode}
import java.sql.Timestamp

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class AlterTableValidationTestCase extends Spark2QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
        new File("./target/test/badRecords").getCanonicalPath)

    sql("drop table if exists restructure")
    sql("drop table if exists table1")
    sql("drop table if exists restructure_test")
    sql("drop table if exists restructure_new")
    sql("drop table if exists restructure_bad")
    sql("drop table if exists restructure_badnew")
    // clean data folder
    CarbonProperties.getInstance()
    sql(
      "CREATE TABLE restructure (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql(
      "CREATE TABLE restructure_test (empno int, empname String, designation String, doj " +
      "Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure_test OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)

    sql(
      """CREATE TABLE IF NOT EXISTS restructure_bad(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED BY 'carbondata'""")

    sql(
    s"""LOAD DATA LOCAL INPATH '$resourcesPath/badrecords/datasample.csv' INTO TABLE
         |restructure_bad OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'bad_records_logger_enable'='true',
         |'bad_records_action'='force')"""
      .stripMargin)

  }

  test("test add dictionary column") {
    sql(
      "alter table restructure add columns(dict int) TBLPROPERTIES ('DICTIONARY_INCLUDE'='dict', " +
      "'DEFAULT.VALUE.dict'= '9999')")
    checkAnswer(sql("select distinct(dict) from restructure"), Row(9999))
  }
  test("test add no dictionary column") {
    sql(
      "alter table restructure add columns(nodict string) TBLPROPERTIES " +
      "('DICTIONARY_EXCLUDE'='nodict', 'DEFAULT.VALUE.NoDict'= 'abcd')")
    checkAnswer(sql("select distinct(nodict) from restructure"), Row("abcd"))
  }
  test("test add timestamp no dictionary column") {
    sql(
      "alter table restructure add columns(tmpstmp timestamp) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".tmpstmp'= '17-01-2007')")
    checkAnswer(sql("select distinct(tmpstmp) from restructure"),
      Row(new java.sql.Timestamp(107, 0, 17, 0, 0, 0, 0)))
    checkExistence(sql("desc restructure"), true, "tmpstmptimestamp")
  }

  test("test add timestamp direct dictionary column") {
    sql(
      "alter table restructure add columns(tmpstmp1 timestamp) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".tmpstmp1'= '17-01-3007','DICTIONARY_INCLUDE'='tmpstmp1')")
    checkAnswer(sql("select distinct(tmpstmp1) from restructure"),
      Row(null))
    checkExistence(sql("desc restructure"), true, "tmpstmptimestamp")
  }

  test("test add timestamp column and load as dictionary") {
    sql("create table table1(name string) stored by 'carbondata'")
    sql("insert into table1 select 'abc'")
    sql("alter table table1 add columns(tmpstmp timestamp) TBLPROPERTIES " +
        "('DEFAULT.VALUE.tmpstmp'='17-01-3007','DICTIONARY_INCLUDE'= 'tmpstmp')")
    sql("insert into table1 select 'name','17-01-2007'")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc",null),
        Row("name",Timestamp.valueOf("2007-01-17 00:00:00.0"))))
  }

  test("test add msr column") {
    sql(
      "alter table restructure add columns(msrField decimal(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    checkExistence(sql("desc restructure"), true, "msrfielddecimal(5,2)")
    val output = sql("select msrField from restructure").collect
    checkAnswer(sql("select distinct(msrField) from restructure"),
      Row(new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP)))
  }

  test("test add all datatype supported dictionary column") {
    sql(
      "alter table restructure add columns(strfld string, datefld date, tptfld timestamp, " +
      "shortFld smallInt, " +
      "intFld int, longFld bigint, dblFld double,dcml decimal(5,4))TBLPROPERTIES" +
      "('DICTIONARY_INCLUDE'='datefld,shortFld,intFld,longFld,dblFld,dcml', 'DEFAULT.VALUE" +
      ".dblFld'= '12345')")
    checkAnswer(sql("select distinct(dblFld) from restructure"),
      Row(java.lang.Double.parseDouble("12345")))
    checkExistence(sql("desc restructure"), true, "strfldstring")
    checkExistence(sql("desc restructure"), true, "dateflddate")
    checkExistence(sql("desc restructure"), true, "tptfldtimestamp")
    checkExistence(sql("desc restructure"), true, "shortfldsmallint")
    checkExistence(sql("desc restructure"), true, "intfldint")
    checkExistence(sql("desc restructure"), true, "longfldbigint")
    checkExistence(sql("desc restructure"), true, "dblflddouble")
    checkExistence(sql("desc restructure"), true, "dcmldecimal(5,4)")
  }

  test(
    "test add decimal without scale and precision, default precision and scale (10,0) should be " +
    "used")
  {
    sql("alter table restructure add columns(dcmldefault decimal)")
    checkExistence(sql("desc restructure"), true, "dcmldefaultdecimal(10,0)")
  }

  test("test adding existing measure as dimension") {
    sql("alter table restructure add columns(dcmlfld decimal(5,4))")
    try {
      sql("alter table restructure add columns(dcmlfld string)")
      sys.error("Exception should be thrown as dcmlfld is already exist as measure")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test adding existing dimension as measure") {
    sql("alter table restructure add columns(dimfld string)")
    try {
      sql("alter table restructure add columns(dimfld decimal(5,4))")
      sys.error("Exception should be thrown as dimfld is already exist as dimension")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test adding existing column again") {
    sql("alter table restructure add columns(dimfld1 string, msrCol double)")
    try {
      sql(
        "alter table restructure add columns(dimfld1 int)TBLPROPERTIES" +
        "('DICTIONARY_INCLUDE'='dimfld1')")
      sys.error("Exception should be thrown as dimfld1 is already exist")
    } catch {
      case e: Exception =>
        println(e.getMessage)
        try {
          sql("alter table restructure add columns(msrCol decimal(5,3))")
          sys.error("Exception should be thrown as msrCol is already exist")
        } catch {
          case e: Exception =>
            println(e.getMessage)
        }
    }
  }

  test("test adding no dictionary column with numeric type") {
    try {
      sql(
        "alter table restructure add columns(dimfld2 double) TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='dimfld2')")
      sys.error("Exception should be thrown as msrCol is already exist")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test adding complex datatype column") {
    try {
      sql("alter table restructure add columns(arr array<string>)")
      sys.error("Exception should be thrown for complex column add")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test drop and add same column with different datatype and default value") {
    sql("alter table restructure drop columns(empname)")
    sql(
      "alter table restructure add columns(empname int) TBLPROPERTIES" +
      "('DICTIONARY_INCLUDE'='empname', 'DEFAULT.VALUE.empname'='12345')")
    checkAnswer(sql("select distinct(empname) from restructure"), Row(12345))
    checkAnswer(sql("select count(empname) from restructure"), Row(10))
  }

  test("test drop column and select query on dropped column should fail") {
    sql("alter table restructure drop columns(empname)")
    try {
      sql("select distinct(empname) from restructure")
      sys.error("Exception should be thrown as selecting dropped column")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
    sql(
      "alter table restructure add columns(empname string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='empname', 'DEFAULT.VALUE.empname'='testuser')")
    checkAnswer(sql("select distinct(empname) from restructure"), Row("testuser"))
    checkAnswer(sql("select count(empname) from restructure"), Row(10))
  }

  test("test add duplicate column names") {
    try {
      sql("alter table restructure add columns(newField string, newField int)")
      sys.error("Exception should be thrown for duplicate column add")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test drop duplicate column names") {
    try {
      sql("alter table restructure drop columns(empname, empname)")
      sys.error("Exception should be thrown for duplicate column drop")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test dropping non-existing column") {
    try {
      sql("alter table restructure drop columns(abcd)")
      sys.error("Exception should be thrown for non-existing column drop")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test drop dimension, measure column") {
    sql("alter table default.restructure drop columns(empno, designation, doj)")
    checkExistence(sql("desc restructure"), false, "empnoint")
    checkExistence(sql("desc restructure"), false, "designationstring")
    checkExistence(sql("desc restructure"), false, "dojtimestamp")
    assert(sql("select * from restructure").schema
             .filter(p => p.name.equalsIgnoreCase("empno") ||
                          p.name.equalsIgnoreCase("designation") || p.name.equalsIgnoreCase("doj"))
             .size == 0)
    sql("alter table restructure add columns(empno int, designation string, doj timestamp)")
  }

  test("test drop & add same column multiple times as dict, nodict, timestamp and msr") {
    // drop and add dict column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table default.restructure add columns(designation int) TBLPROPERTIES" +
      "('DICTIONARY_INCLUDE'='designation', 'DEFAULT.VALUE.designation'='12345')")
    checkAnswer(sql("select distinct(designation) from restructure"), Row(12345))
    // drop and add nodict column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table restructure add columns(designation string) TBLPROPERTIES" +
      "('DICTIONARY_EXCLUDE'='designation', 'DEFAULT.VALUE.designation'='abcd')")
    checkAnswer(sql("select distinct(designation) from restructure"), Row("abcd"))
    // drop and add directdict column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table restructure add columns(designation timestamp) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".designation'= '17-01-2007')")
    checkAnswer(sql("select distinct(designation) from restructure"),
      Row(new java.sql.Timestamp(107, 0, 17, 0, 0, 0, 0)))
    // drop and add msr column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table default.restructure add columns(designation int) TBLPROPERTIES" +
      "('DEFAULT.VALUE.designation'='67890')")
    checkAnswer(sql("select distinct(designation) from restructure"), Row(67890))
  }

  test("test change datatype of int and decimal column") {
    sql("alter table restructure add columns(intfield int, decimalfield decimal(10,2))")
    sql("alter table default.restructure change intfield intField bigint")
    checkExistence(sql("desc restructure"), true, "intfieldbigint")
    sql("alter table default.restructure change decimalfield deciMalfield Decimal(11,3)")
    sql("alter table default.restructure change decimalfield deciMalfield Decimal(12,3)")
    intercept[RuntimeException] {
      sql("alter table default.restructure change decimalfield deciMalfield Decimal(12,3)")
    }
    intercept[RuntimeException] {
      sql("alter table default.restructure change decimalfield deciMalfield Decimal(13,1)")
    }
    intercept[RuntimeException] {
      sql("alter table default.restructure change decimalfield deciMalfield Decimal(13,5)")
    }
    sql("alter table default.restructure change decimalfield deciMalfield Decimal(13,4)")
  }

  test("test change datatype of string to int column") {
    try {
      sql("alter table restructure change empname empname bigint")
      sys.error("Exception should be thrown as empname string type change to bigint")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test change datatype of int to string column") {
    try {
      sql("alter table restructure change empno Empno string")
      sys.error("Exception should be thrown as empno int type change to string")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test change datatype of non-existing column") {
    try {
      sql("alter table restructure change abcd abcd string")
      sys.error("Exception should be thrown for datatype change on non-existing column")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test change datatype of decimal column from higher to lower precision/scale") {
    sql("alter table restructure add columns(decField decimal(10,2))")
    try {
      sql("alter table restructure change decField decField decimal(10,1)")
      sys.error("Exception should be thrown for downgrade of scale in decimal type")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
    try {
      sql("alter table restructure change decField decField decimal(5,3)")
      sys.error("Exception should be thrown for downgrade of precision in decimal type")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test to rename table") {
    sql("alter table restructure_test rename to restructure_new")
    val result = sql("select * from restructure_new")
    assert(result.count().equals(10L))
  }

  test("test to check if bad record folder name is changed") {
    sql("alter table restructure_bad rename to restructure_badnew")
    val oldLocation = new File("./target/test/badRecords/default/restructure_bad")
    val newLocation = new File("./target/test/badRecords/default/restructure_badnew")
    assert(!oldLocation.exists())
    assert(newLocation.exists())
  }

  test("test to rename table with invalid table name") {
    try {
      sql("alter table restructure_invalid rename to restructure_new")
      sys.error("Invalid table name error should be thrown")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test to rename table with table already exists") {
    try {
      sql("alter table restructure rename to restructure")
      sys.error("same table name exception should be thrown")
    } catch {
      case e: Exception =>
        println(e.getMessage)
        assert(true)
    }
  }

  test("test to load data after rename") {
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure_new OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    val result = sql("select * from restructure_new")
    assert(result.count().equals(20L))
  }

  test("test table rename without use db, let current db be default") {
    sql("drop database if exists testdb cascade")
    sql("create database testdb")
    sql("create table testdb.test1(name string, id int) stored by 'carbondata'")
    sql("insert into testdb.test1 select 'xx',1")
    sql("insert into testdb.test1 select 'xx',11")
    sql("alter table testdb.test1 rename to testdb.test2")
    checkAnswer(sql("select * from testdb.test2"), Seq(Row("xx", 1), Row("xx", 11)))
    sql("drop table testdb.test2")
    sql("drop database testdb")
  }

  test("test to check if the lock file is successfully deleted") {
      sql("create table lock_check(id int, name string) stored by 'carbondata'")
    sql("alter table lock_check rename to lock_rename")
    assert(!new File(s"${ CarbonCommonConstants.STORE_LOCATION } + /default/lock_rename/meta.lock")
      .exists())
  }

  test("table rename with dbname in Camel Case") {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
    sql("""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String) STORED BY 'org.apache.carbondata.format'""")
    sql("""insert into table uniqdata values(1,"hello")""")
    sql("alter table Default.uniqdata rename to uniqdata1")
    checkAnswer(sql("select * from Default.uniqdata1"), Row(1,"hello"))
  }

  // test query before&after renaming, see CARBONDATA-1690
  test("RenameTable_query_before_and_after_renaming") {
    try {
      sql(s"""create table test1 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""create table test2 (name string, id int) stored by 'carbondata'""").collect
      sql(s"""insert into test1 select 'xx1',1""").collect
      sql(s"""insert into test2 select 'xx2',2""").collect
      // query before rename
      checkAnswer(sql(s"""select * from test1"""), Seq(Row("xx1", 1)))
      sql(s"""alter table test1 RENAME TO test3""").collect
      sql(s"""alter table test2 RENAME TO test1""").collect
      // query after rename
      checkAnswer(sql(s"""select * from test1"""), Seq(Row("xx2", 2)))
    } catch {
      case e: Exception =>
        assert(false)
    } finally {
      sql(s"""drop table if exists test1""").collect
      sql(s"""drop table if exists test3""").collect
      sql(s"""drop table if exists test2""").collect
    }
  }

  test("describe formatted for default sort_columns pre and post alter") {
    sql("CREATE TABLE defaultSortColumnsWithAlter (empno int, empname String, designation String,role String, doj Timestamp) STORED BY 'org.apache.carbondata.format' " +
        "tblproperties('DICTIONARY_INCLUDE'='empno','DICTIONARY_EXCLUDE'='role')")
    sql("alter table defaultSortColumnsWithAlter drop columns (designation)")
    sql("alter table defaultSortColumnsWithAlter add columns (designation12 String)")
    checkExistence(sql("describe formatted defaultSortColumnsWithAlter"),true,"SORT_COLUMNS")
    checkExistence(sql("describe formatted defaultSortColumnsWithAlter"),true,"empno,empname,role,doj")
  }
  test("describe formatted for specified sort_columns pre and post alter") {
    sql("CREATE TABLE specifiedSortColumnsWithAlter (empno int, empname String, designation String,role String, doj Timestamp) STORED BY 'org.apache.carbondata.format' " +
        "tblproperties('sort_columns'='empno,empname,designation,role,doj','DICTIONARY_INCLUDE'='empno','DICTIONARY_EXCLUDE'='role')")
    sql("alter table specifiedSortColumnsWithAlter drop columns (designation)")
    sql("alter table specifiedSortColumnsWithAlter add columns (designation12 String)")
    checkExistence(sql("describe formatted specifiedSortColumnsWithAlter"),true,"SORT_COLUMNS")
    checkExistence(sql("describe formatted specifiedSortColumnsWithAlter"),true,"empno,empname,role,doj")
  }

  test("test to check if new parent table name is reflected in pre-aggregate tables") {
    sql("drop table if exists preaggMain")
    sql("drop table if exists preaggmain_new")
    sql("drop table if exists preagg1")
    sql("create table preaggMain (a string, b string, c string) stored by 'carbondata'")
    sql(
      "create table preagg1 stored BY 'carbondata' tblproperties('parent'='PreAggMain') as select" +
      " a,sum(b) from PreAggMain group by a")
    intercept[RuntimeException] {
      sql("alter table preagg1 rename to preagg2")
    }.getMessage.contains("Rename operation for pre-aggregate table is not supported.")
    intercept[RuntimeException] {
      sql("alter table preaggmain rename to preaggmain_new")
    }.getMessage.contains("Rename operation is not supported for table with pre-aggregate tables")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS restructure")
    sql("drop table if exists table1")
    sql("DROP TABLE IF EXISTS restructure_new")
    sql("DROP TABLE IF EXISTS restructure_test")
    sql("DROP TABLE IF EXISTS restructure_bad")
    sql("DROP TABLE IF EXISTS restructure_badnew")
    sql("DROP TABLE IF EXISTS lock_rename")
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
    sql("drop table if exists defaultSortColumnsWithAlter")
    sql("drop table if exists specifiedSortColumnsWithAlter")
  }
}

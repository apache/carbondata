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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class AlterTableValidationTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    sql("drop table if exists restructure")
    sql("drop table if exists table1")
    sql("drop table if exists restructure_test")
    sql("drop table if exists restructure_new")
    sql("drop table if exists restructure_bad")
    sql("drop table if exists restructure_badnew")
    sql("drop table if exists allKeyCol")
    sql("drop table if exists testalterwithboolean")
    sql("drop table if exists testalterwithbooleanwithoutdefaultvalue")
    sql("drop table if exists test")
    sql("drop table if exists retructure_iud")
    sql("drop table if exists restructure_random_select")
    sql("drop table if exists alterTable")
    sql("drop table if exists alterPartitionTable")

    // clean data folder
    CarbonProperties.getInstance()
    sql(
      "CREATE TABLE restructure (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql(
      "CREATE TABLE restructure_test (empno int, empname String, designation String, doj " +
      "Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname " +
      "String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance " +
      "int,utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure_test OPTIONS
          |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)

    sql(
      """CREATE TABLE IF NOT EXISTS restructure_bad(ID BigInt, date Timestamp, country String,
          actual_price Double, Quantity int, sold_price Decimal(19,2)) STORED AS carbondata""")

    sql(
    s"""LOAD DATA LOCAL INPATH '$resourcesPath/badrecords/datasample.csv' INTO TABLE
         |restructure_bad OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'bad_records_logger_enable'='true',
         |'bad_records_action'='force')"""
      .stripMargin)

  }

  test("test add dictionary column") {
    sql(
      "alter table restructure add columns(dict int) TBLPROPERTIES ( " +
      "'DEFAULT.VALUE.dict'= '9999')")
    checkAnswer(sql("select distinct(dict) from restructure"), Row(9999))
  }
  test("test add no dictionary column") {
    sql(
      "alter table restructure add columns(nodict string) TBLPROPERTIES " +
      "('DEFAULT.VALUE.NoDict'= 'abcd')")
    checkAnswer(sql("select distinct(nodict) from restructure"), Row("abcd"))
  }

  ignore ("test add timestamp no dictionary column") {
    sql(
      "alter table restructure add columns(tmpstmp timestamp) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".tmpstmp'= '2007-01-17')")
    sql("select tmpstmp from restructure").show(200,false)
    sql("select distinct(tmpstmp) from restructure").show(200,false)
    checkAnswer(sql("select distinct(tmpstmp) from restructure"),
      Row(new java.sql.Timestamp(107, 0, 17, 0, 0, 0, 0)))
    checkExistence(sql("desc restructure"), true, "tmpstmp", "timestamp")
  }

  test ("test add timestamp direct dictionary column") {
    sql(
      "alter table restructure add columns(tmpstmp1 timestamp) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".tmpstmp1'= '3007-01-17')")
    checkAnswer(sql("select distinct(tmpstmp1) from restructure"),
      Row(null))
    checkExistence(sql("desc restructure"), true, "tmpstmp", "timestamp")
  }

  ignore ("test add timestamp column and load as dictionary") {
    sql("create table table1(name string) STORED AS carbondata")
    sql("insert into table1 select 'abc'")
    sql("alter table table1 add columns(tmpstmp timestamp) TBLPROPERTIES " +
        "('DEFAULT.VALUE.tmpstmp'='17-01-3007')")
    sql("insert into table1 select 'name','17-01-2007'")
    checkAnswer(sql("select * from table1"),
      Seq(Row("abc",null),
        Row("name",Timestamp.valueOf("2007-01-17 00:00:00.0"))))
  }

  test("test add msr column") {
    sql(
      "alter table restructure add columns(msrField decimal(5,2))TBLPROPERTIES ('DEFAULT.VALUE" +
      ".msrfield'= '123.45')")
    sql("desc restructure").show(2000,false)
    checkExistence(sql("desc restructure"), true, "msrfield", "decimal(5,2)")
    val output = sql("select msrField from restructure").collect
    sql("select distinct(msrField) from restructure").show(2000,false)
    checkAnswer(sql("select distinct(msrField) from restructure"),
      Row(new BigDecimal("123.45").setScale(2, RoundingMode.HALF_UP)))
  }

  // test alter add LONG datatype before load, see CARBONDATA-2131
  test("test add long column before load") {
    sql("drop table if exists alterLong")
    sql("create table alterLong (name string) STORED AS carbondata")
    sql("alter table alterLong add columns(newCol long)")
    sql("insert into alterLong select 'a',60000")
    checkAnswer(sql("select * from alterLong"), Row("a", 60000))
    sql("drop table if exists alterLong")
  }

  // test alter add LONG datatype after load, see CARBONDATA-2131
  test("test add long column after load") {
    sql("drop table if exists alterLong1")
    sql("create table alterLong1 (name string) STORED AS carbondata")
    sql("insert into alterLong1 select 'a'")
    sql("alter table alterLong1 add columns(newCol long)")
    checkAnswer(sql("select * from alterLong1"), Row("a", null))
    sql("insert into alterLong1 select 'b',70")
    checkAnswer(sql("select * from alterLong1"), Seq(Row("a", null),Row("b", 70)))
    sql("drop table if exists alterLong1")
  }

  test("test add all datatype supported dictionary column") {
    sql(
      "alter table restructure add columns(strfld string, datefld date, tptfld timestamp, " +
      "shortFld smallInt, " +
      "intFld int, longFld bigint, dblFld double,dcml decimal(5,4))TBLPROPERTIES" +
      "('DEFAULT.VALUE.dblFld'= '12345')")
    checkAnswer(sql("select distinct(dblFld) from restructure"),
      Row(java.lang.Double.parseDouble("12345")))
    checkExistence(sql("desc restructure"), true, "strfld", "string")
    checkExistence(sql("desc restructure"), true, "datefld", "date")
    checkExistence(sql("desc restructure"), true, "tptfld", "timestamp")
    checkExistence(sql("desc restructure"), true, "shortfld", "smallint")
    checkExistence(sql("desc restructure"), true, "intfld", "int")
    checkExistence(sql("desc restructure"), true, "longfld", "bigint")
    checkExistence(sql("desc restructure"), true, "dblfld", "double")
    checkExistence(sql("desc restructure"), true, "dcml", "decimal(5,4)")
  }

  test("test drop all keycolumns in a table") {
    sql(
      "create table allKeyCol (name string, age int, address string) STORED AS carbondata")
    try {
      sql("alter table allKeyCol drop columns(name,address)")
      assert(true)
    } catch {
      case _: Exception =>
        assert(false)
    }
  }

  ignore(
    "test add decimal without scale and precision, default precision and scale (10,0) should be " +
    "used")
  {
    sql("alter table restructure add columns(dcmldefault decimal)")
    checkExistence(sql("desc restructure"), true, "dcmldefault", "decimal(10,0)")
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
        "alter table restructure add columns(dimfld1 int)")
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
      sql("alter table restructure add columns(dimfld2 double)")
      sys.error("Exception should be thrown as msrCol is already exist")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test adding complex datatype column") {
    try {
      sql("alter table restructure add columns(arr array<string>)")
      assert(false, "Exception should be thrown for complex column add")
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  test("test drop and add same column with different datatype and default value") {
    sql("alter table restructure drop columns(empname)")
    sql(
      "alter table restructure add columns(empname int) TBLPROPERTIES" +
      "('DEFAULT.VALUE.empname'='12345')")
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
      "('DEFAULT.VALUE.empname'='testuser')")
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
    checkExistence(sql("desc restructure"), false, "empno")
    checkExistence(sql("desc restructure"), false, "designation")
    checkExistence(sql("desc restructure"), false, "doj")
    assert(sql("select * from restructure").schema
             .filter(p => p.name.equalsIgnoreCase("empno") ||
                          p.name.equalsIgnoreCase("designation") || p.name.equalsIgnoreCase("doj"))
             .size == 0)
    sql("alter table restructure add columns(empno int, designation string, doj timestamp)")
  }

  test ("test drop & add same column multiple times as dict, nodict, timestamp and msr") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    // drop and add dict column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table default.restructure add columns(designation int) TBLPROPERTIES" +
      "('DEFAULT.VALUE.designation'='12345')")
    checkAnswer(sql("select distinct(designation) from restructure"), Row(12345))
    // drop and add nodict column
    sql("alter table restructure drop columns(designation)")
    sql(
      "alter table restructure add columns(designation string) TBLPROPERTIES" +
      "('DEFAULT.VALUE.designation'='abcd')")
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  }

  test("test change datatype of int and decimal column") {
    sql("alter table restructure add columns(intfield int, decimalfield decimal(10,2))")
    sql("alter table default.restructure change intfield intField bigint")
    checkExistence(sql("desc restructure"), true, "intfield", "bigint")
    sql("alter table default.restructure change decimalfield deciMalfield Decimal(11,3)")
    sql("alter table default.restructure change decimalfield deciMalfield Decimal(12,3)")
    intercept[ProcessMetaDataException] {
      sql("alter table default.restructure change decimalfield deciMalfield Decimal(12,3)")
    }
    intercept[ProcessMetaDataException] {
      sql("alter table default.restructure change decimalfield deciMalfield Decimal(13,1)")
    }
    intercept[ProcessMetaDataException] {
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

  ignore("test to check if bad record folder name is changed") {
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
    sql("create table testdb.test1(name string, id int) STORED AS carbondata")
    sql("insert into testdb.test1 select 'xx',1")
    sql("insert into testdb.test1 select 'xx',11")
    sql("alter table testdb.test1 rename to testdb.test2")
    checkAnswer(sql("select * from testdb.test2"), Seq(Row("xx", 1), Row("xx", 11)))
    sql("drop table testdb.test2")
    sql("drop database testdb")
  }

  test("test to check if the lock file is successfully deleted") {
      sql("create table lock_check(id int, name string) STORED AS carbondata")
    sql("alter table lock_check rename to lock_rename")
    assert(!new File(s"${ CarbonCommonConstants.STORE_LOCATION } + /lock_rename/meta.lock")
      .exists())
  }

  test("table rename with dbname in Camel Case") {
    sql("drop table if exists uniqdata")
    sql("drop table if exists uniqdata1")
    sql("""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String) STORED AS carbondata""")
    sql("""insert into table uniqdata values(1,"hello")""")
    sql("alter table Default.uniqdata rename to uniqdata1")
    checkAnswer(sql("select * from Default.uniqdata1"), Row(1,"hello"))
  }

  // test query before&after renaming, see CARBONDATA-1690
  test("RenameTable_query_before_and_after_renaming") {
    try {
      sql(s"""create table test1 (name string, id int) STORED AS carbondata""").collect
      sql(s"""create table test2 (name string, id int) STORED AS carbondata""").collect
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

  // after changing default sort_scope to no_sort, all dimensions are not selected for sorting.
  ignore("describe formatted for default sort_columns pre and post alter") {
    sql("CREATE TABLE defaultSortColumnsWithAlter (empno int, empname String, designation String,role String, doj Timestamp) STORED AS carbondata ")
    sql("alter table defaultSortColumnsWithAlter drop columns (designation)")
    sql("alter table defaultSortColumnsWithAlter add columns (designation12 String)")
    checkExistence(sql("describe formatted defaultSortColumnsWithAlter"),true,"Sort Columns empno, empname, role, doj")
  }
  test("describe formatted for specified sort_columns pre and post alter") {
    sql("CREATE TABLE specifiedSortColumnsWithAlter (empno int, empname String, designation String,role String, doj Timestamp) STORED AS carbondata " +
        "tblproperties('sort_columns'='empno,empname,designation,role,doj')")
    sql("alter table specifiedSortColumnsWithAlter drop columns (designation)")
    sql("alter table specifiedSortColumnsWithAlter add columns (designation12 String)")
    checkExistence(sql("describe formatted specifiedSortColumnsWithAlter"),true,"Sort Columns empno, empname, role, doj")
  }

  test("test to check select columns after alter commands with null values"){
    sql("drop table if exists restructure")
    sql("drop table if exists restructure1")
    sql(
      "CREATE TABLE restructure (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE restructure OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("ALTER TABLE restructure rename to restructure1")
    sql("ALTER TABLE restructure1 ADD COLUMNS (projId int)")
    sql("ALTER TABLE restructure1 DROP COLUMNS (projId)")
    sql("ALTER TABLE restructure1 CHANGE empno empno BIGINT")
    sql("ALTER TABLE restructure1 ADD COLUMNS (a1 INT, b1 STRING)")
    checkAnswer(sql("select a1,b1,empname from restructure1 where a1 is null and b1 is null and empname='arvind'"),Row(null,null,"arvind"))
    sql("drop table if exists restructure1")
    sql("drop table if exists restructure")
  }
test("test alter command for boolean data type with correct default measure value") {
  sql("create table testalterwithboolean(id int,name string) STORED AS carbondata ")
  sql("insert into testalterwithboolean values(1,'anubhav')  ")
  sql(
    "alter table testalterwithboolean add columns(booleanfield boolean) tblproperties('default.value.booleanfield'='true')")
  checkAnswer(sql("select * from testalterwithboolean"),Seq(Row(1,"anubhav",true)))
}
  test("test alter command for boolean data type with out default measure value") {
    sql("create table testalterwithbooleanwithoutdefaultvalue(id int,name string) STORED AS carbondata ")
    sql("insert into testalterwithbooleanwithoutdefaultvalue values(1,'anubhav')  ")
    sql(
      "alter table testalterwithbooleanwithoutdefaultvalue add columns(booleanfield boolean)")
    checkAnswer(sql("select * from testalterwithbooleanwithoutdefaultvalue"),Seq(Row(1,"anubhav",null)))
  }
  test("test alter command for filter on default values on date datatype") {
    sql("drop table if exists test")
    sql(
      "create table test(id int,vin string,phonenumber long,area string,salary int,country " +
      "string,longdate date) STORED AS carbondata")
    sql("insert into test select 1,'String1',12345,'area',20,'country','2017-02-12'")
    sql("alter table test add columns (c3 date) TBLPROPERTIES('DEFAULT.VALUE.c3' = '1993-01-01')")
    sql("alter table test add columns (c4 date)")
    sql("insert into test select 2,'String1',12345,'area',20,'country','2017-02-12','1994-01-01','1994-01-01'")
    sql("insert into test select 3,'String1',12345,'area',20,'country','2017-02-12','1995-01-01','1995-01-01'")
    sql("insert into test select 4,'String1',12345,'area',20,'country','2017-02-12','1996-01-01','1996-01-01'")
    checkAnswer(sql("select id from test where c3='1993-01-01'"), Seq(Row(1)))
    checkAnswer(sql("select id from test where c3!='1993-01-01'"), Seq(Row(2),Row(3),Row(4)))
    checkAnswer(sql("select id from test where c3<'1995-01-01'"), Seq(Row(1), Row(2)))
    checkAnswer(sql("select id from test where c3>'1994-01-01'"), Seq(Row(3), Row(4)))
    checkAnswer(sql("select id from test where c3>='1995-01-01'"), Seq(Row(3), Row(4)))
    checkAnswer(sql("select id from test where c3<='1994-01-01'"), Seq(Row(1), Row(2)))
    checkAnswer(sql("select id from test where c4 IS NULL"), Seq(Row(1)))
    checkAnswer(sql("select id from test where c4 IS NOT NULL"), Seq(Row(2),Row(3),Row(4)))
  }

  test("test alter command for filter on default values on timestamp datatype") {
    def testFilterWithDefaultValue(flag: Boolean) = {
      try {
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            "yyyy/MM/dd HH:mm:ss")
        sql("drop table if exists test")
        sql(
          "create table test(id int,vin string,phonenumber long,area string,salary int,country " +
          "string,longdate date) STORED AS carbondata")
        sql("insert into test select 1,'String1',12345,'area',20,'country','2017-02-12'")
        if (flag) {
          sql(
            "alter table test add columns (c3 timestamp) TBLPROPERTIES('DEFAULT.VALUE.c3' = " +
            "'1996/01/01 11:11:11')")
        } else {
          sql(
            "alter table test add columns (c3 timestamp) TBLPROPERTIES('DEFAULT.VALUE.c3' = " +
            "'1996/01/01 11:11:11')")
        }
        sql("alter table test add columns (c4 timestamp)")
        sql(
          "insert into test select 2,'String1',12345,'area',20,'country','2017-02-12','1994-01-01 10:10:10','1994-01-01 10:10:10'")
        sql(
          "insert into test select 3,'String1',12345,'area',20,'country','2017-02-12','1995-01-01 11:11:11','1995-01-01 11:11:11'")
        sql(
          "insert into test select 4,'String1',12345,'area',20,'country','2017-02-12','1996-01-01 10:10:10','1996-01-01 10:10:10'")
        checkAnswer(sql("select id from test where c3='1996-01-01 11:11:11'"), Seq(Row(1)))
        checkAnswer(sql("select id from test where c3!='1996-01-01 11:11:11'"), Seq(Row(2),Row(3),Row(4)))
        checkAnswer(sql("select id from test where c3<'1995-01-01 11:11:11'"), Seq(Row(2)))
        checkAnswer(sql("select id from test where c3>'1994-01-02 11:11:11'"),
          Seq(Row(3), Row(4), Row(1)))
        checkAnswer(sql("select id from test where c3>='1995-01-01 11:11:11'"),
          Seq(Row(3), Row(4), Row(1)))
        checkAnswer(sql("select id from test where c3<='1995-01-02 11:11:11'"), Seq(Row(2), Row(3)))
        checkAnswer(sql("select id from test where c4 IS NULL"), Seq(Row(1)))
        checkAnswer(sql("select id from test where c4 IS NOT NULL"), Seq(Row(2),Row(3),Row(4)))
      }
      finally {
        CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      }
    }

    testFilterWithDefaultValue(false)
    testFilterWithDefaultValue(true)

  }

  test("test alter command for filter on default values on int") {
    def testFilterWithDefaultValue(flag: Boolean) = {
      sql("drop table if exists test")
      sql(
        "create table test(id int,vin string,phonenumber long,area string,salary int,country " +
        "string,longdate date) STORED AS carbondata")
      sql("insert into test select 1,'String1',12345,'area',5000,'country','2017/02/12'")
      if (flag) {
        sql(s"alter table test add columns (c3 int) TBLPROPERTIES('DEFAULT.VALUE.c3' = '23')")
      } else {
        sql(s"alter table test add columns (c3 int) TBLPROPERTIES('DEFAULT.VALUE.c3' = '23')")
      }
      sql(s"alter table test add columns (c4 int)")
      sql("insert into test select 2,'String1',12345,'area',5000,'country','2017/02/12',25,25")
      sql("insert into test select 3,'String1',12345,'area',5000,'country','2017/02/12',35,35")
      sql("insert into test select 4,'String1',12345,'area',5000,'country','2017/02/12',45,45")
      checkAnswer(sql("select id from test where c3=23"), Seq(Row(1)))
      checkAnswer(sql("select id from test where c3!=23"), Seq(Row(2), Row(3), Row(4)))
      checkAnswer(sql("select id from test where c3<34"), Seq(Row(1), Row(2)))
      checkAnswer(sql("select id from test where c3>24"), Seq(Row(2), Row(3), Row(4)))
      checkAnswer(sql("select id from test where c3>=35"), Seq(Row(3), Row(4)))
      checkAnswer(sql("select id from test where c3<=35"), Seq(Row(1), Row(2), Row(3)))
      checkAnswer(sql("select id from test where c4 IS NULL"), Seq(Row(1)))
      checkAnswer(sql("select id from test where c4 IS NOT NULL"), Seq(Row(2),Row(3),Row(4)))
    }

    testFilterWithDefaultValue(true)
    testFilterWithDefaultValue(false)
  }
  test("Filter query on Restructure and updated table") {
    sql(
      """
         CREATE TABLE retructure_iud(id int, name string, city string, age int)
         STORED AS carbondata
      """)
    val testData = s"$resourcesPath/sample.csv"
    sql(s"LOAD DATA LOCAL INPATH '$testData' into table retructure_iud")
    sql("ALTER TABLE retructure_iud ADD COLUMNS (newField STRING) " +
        "TBLPROPERTIES ('DEFAULT.VALUE.newField'='def')").show()
    sql("ALTER TABLE retructure_iud ADD COLUMNS (newField1 STRING) " +
        "TBLPROPERTIES ('DEFAULT.VALUE.newField1'='def')").show()
    // update operation
    sql("""update retructure_iud d  set (d.id) = (d.id + 1) where d.id > 2""").show()
    checkAnswer(
      sql("select count(*) from retructure_iud where id = 2 and newfield1='def'"),
      Seq(Row(1))
    )
  }

  test("Alter table selection in random order"){
    def test(): Unit ={
      sql("drop table if exists restructure_random_select")
      sql("create table restructure_random_select (imei string,channelsId string,gamePointId double,deviceInformationId double," +
          " deliverycharge double) STORED AS carbondata TBLPROPERTIES('table_blocksize'='2000','sort_columns'='imei')")
      sql("insert into restructure_random_select values('abc','def',50.5,30.2,40.6) ")
      sql("Alter table restructure_random_select add columns (age int,name String)")
      checkAnswer(
        sql("select gamePointId,deviceInformationId,age,name from restructure_random_select where name is NULL or channelsId=4"),
        Seq(Row(50.5,30.2,null,null)))
      checkAnswer(
        sql("select age,name,gamePointId,deviceInformationId from restructure_random_select where name is NULL or channelsId=4"),
        Seq(Row(null,null,50.5,30.2)))
    }
    try {
      test()
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
      test()
    }
    finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    }
  }

  test("load table after alter drop column scenario") {
    sql("drop table if exists alterTable")
    sql(
      "create table alterTable(empno string, salary string) STORED AS carbondata tblproperties" +
      "('sort_columns'='')")
    sql("alter table alterTable drop columns(empno)")
    sql("alter table alterTable add columns(empno string)")
    sql(s"load data local inpath '$resourcesPath/double.csv' into table alterTable options" +
        s"('header'='true')")
    checkAnswer(sql("select salary from alterTable limit 1"), Row(" 775678765456789098765432.789"))
  }

  test("load partition table after alter drop column scenario") {
    val timestampFormat = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("drop table if exists alterPartitionTable")
    sql(
      """
        | CREATE TABLE alterPartitionTable (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (empno int)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='empname,deptno,projectcode,projectjoindate,
        | projectenddate,attendance')
      """.stripMargin)
    sql("alter table alterPartitionTable drop columns(projectenddate)")
    sql("alter table alterPartitionTable add columns(projectenddate timestamp)")
    sql(s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE alterPartitionTable OPTIONS('DELIMITER'= ',', " +
              "'QUOTECHAR'= '\"')")
    sql("select * from alterPartitionTable where empname='bill'").show(false)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, timestampFormat)
  }


  test("Alter Table Change Sort Scope 1") {
    sql("DROP TABLE IF EXISTS t1")
    sql(s"CREATE TABLE t1(age int, name string) STORED AS carbondata TBLPROPERTIES" +
        s"('sort_columns'='age', 'sort_scope'='local_sort')")
    sql("ALTER TABLE t1 SET TBLPROPERTIES('sort_scope'='global_sort')")
    assert(sortScopeInDescFormatted("t1").equalsIgnoreCase("global_sort"))
    sql("DROP TABLE t1")
  }

  test("Alter Table Change Sort Scope 2") {
    sql("DROP TABLE IF EXISTS t1")
    sql(s"CREATE TABLE t1(age int, name string) STORED AS carbondata TBLPROPERTIES" +
        s"('sort_columns'='age', 'sort_scope'='local_sort')")
    sql("ALTER TABLE t1 SET TBLPROPERTIES('sort_scope'='no_sort')")
    assert(sortScopeInDescFormatted("t1").equalsIgnoreCase("NO_SORT"))
    sql("DROP TABLE t1")
  }

  test("Alter Table Change Sort Scope 3") {
    sql("DROP TABLE IF EXISTS t1")
    sql(s"CREATE TABLE t1(age int, name string) STORED AS carbondata TBLPROPERTIES" +
        s"('sort_columns'='')")

    // This throws exception as SORT_COLUMNS is empty
    intercept[RuntimeException] {
      sql("ALTER TABLE t1 SET TBLPROPERTIES('sort_scope'='local_sort')")
    }

    // Even if we change the SORT_SCOPE to LOCAL_SORT
    // the SORT_SCOPE should remain to NO_SORT
    // because SORT_COLUMNS does not contain anything.
    assert(sortScopeInDescFormatted("t1").equalsIgnoreCase("NO_SORT"))
    sql("DROP TABLE t1")
  }

  test("Alter Table Change Sort Scope 4") {
    sql("DROP TABLE IF EXISTS t1")
    sql(s"CREATE TABLE t1(age int, name string) STORED AS carbondata TBLPROPERTIES" +
        s"('sort_columns'='age', 'sort_scope'='local_sort')")
    sql("ALTER TABLE t1 UNSET TBLPROPERTIES('sort_scope')")

    // Unsetting the SORT_SCOPE should change the SORT_SCOPE to
    // CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT
    assert(sortScopeInDescFormatted("t1")
      .equalsIgnoreCase(CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    sql("DROP TABLE t1")
  }

  test("Alter Table Change Sort Scope 5") {
    sql("DROP TABLE IF EXISTS t1")
    sql(s"CREATE TABLE t1(age int, name string) STORED AS carbondata TBLPROPERTIES" +
        s"('sort_scope'='local_sort', 'sort_columns'='age')")
    intercept[RuntimeException] {
      sql("ALTER TABLE t1 SET TBLPROPERTIES('sort_scope'='fake_sort')")
    }

    // SORT_SCOPE should remain unchanged
    assert(sortScopeInDescFormatted("t1").equalsIgnoreCase("LOCAL_SORT"))
    sql("DROP TABLE t1")
  }

  def sortScopeInDescFormatted(tableName: String): String = {
    sql(s"DESCRIBE FORMATTED $tableName").filter(
      (x: Row) => x.getString(0).equalsIgnoreCase("sort scope")
    ).collectAsList().get(0).get(1).toString
  }


  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
        CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
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
    sql("drop table if exists allKeyCol")
    sql("drop table if exists testalterwithboolean")
    sql("drop table if exists testalterwithbooleanwithoutdefaultvalue")
    sql("drop table if exists test")
    sql("drop table if exists retructure_iud")
    sql("drop table if exists restructure_random_select")
    sql("drop table if exists alterTable")
    sql("drop table if exists alterPartitionTable")
  }
}

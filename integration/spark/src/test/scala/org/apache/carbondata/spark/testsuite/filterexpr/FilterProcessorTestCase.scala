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

package org.apache.carbondata.spark.testsuite.filterexpr

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.spark.testsuite.datacompaction.CompactionSupportGlobalSortBigFileTest

/**
  * Test Class for filter expression query on String datatypes
  *
  */
class FilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  val file1 = resourcesPath + "/filter/file1.csv"

  override def beforeAll {
    sql("drop table if exists filtertestTables")
    sql("drop table if exists filtertestTablesWithDecimal")
    sql("drop table if exists filtertestTablesWithNull")
    sql("drop table if exists filterTimestampDataType")
    sql("drop table if exists noloadtable")
    sql("drop table if exists like_filter")

    CompactionSupportGlobalSortBigFileTest.createFile(file1, 500000, 0)

    sql("CREATE TABLE filtertestTables (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
        "STORED AS carbondata"
    )
    sql("CREATE TABLE noloadtable (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
      "STORED AS carbondata"
    )
     CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss")

     sql("CREATE TABLE filterTimestampDataType (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
        "STORED AS carbondata"
    )
       CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data2_DiffTimeFormat.csv' INTO TABLE " +
        s"filterTimestampDataType " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/source.csv' INTO TABLE filtertestTables " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    sql(
      "CREATE TABLE filtertestTablesWithDecimal (ID decimal, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String, salary int) " +
      "STORED AS carbondata"
    )
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/source.csv' INTO TABLE " +
        s"filtertestTablesWithDecimal " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    sql("DROP TABLE IF EXISTS filtertestTablesWithNull")
    sql(
      "CREATE TABLE filtertestTablesWithNull (ID int, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String,salary int) " +
      "STORED AS carbondata"
    )
    sql("DROP TABLE IF EXISTS filtertestTablesWithNullJoin")
    sql(
      "CREATE TABLE filtertestTablesWithNullJoin (ID int, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String,salary int) " +
      "STORED AS carbondata"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data2.csv' INTO TABLE " +
        s"filtertestTablesWithNull " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
        sql(
      s"LOAD DATA LOCAL INPATH '$resourcesPath/data2.csv' INTO TABLE " +
        s"filtertestTablesWithNullJoin " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )

    sql("DROP TABLE IF EXISTS big_int_basicc")
    sql("DROP TABLE IF EXISTS big_int_basicc_1")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive_1")
    sql("CREATE TABLE big_int_basicc (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql("CREATE TABLE big_int_basicc_1 (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))STORED AS carbondata")
    sql("CREATE TABLE big_int_basicc_Hive (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate date,enddate date,PointId double,score decimal(10,3))row format delimited fields terminated by ',' " +
        "tblproperties(\"skip.header.line.count\"=\"1\") ")
    sql("CREATE TABLE big_int_basicc_Hive_1 (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate date,enddate date,PointId double,score decimal(10,3))row format delimited fields terminated by ',' " +
        "tblproperties(\"skip.header.line.count\"=\"1\") ")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql(s"""LOAD DATA INPATH '$resourcesPath/big_int_Decimal.csv'  INTO TABLE big_int_basicc options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$$','COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')""")
    sql(s"""LOAD DATA INPATH '$resourcesPath/big_int_Decimal.csv'  INTO TABLE big_int_basicc_1 options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$$','COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')""")
    sql(s"load data local inpath '$resourcesPath/big_int_Decimal.csv' into table big_int_basicc_Hive")
    sql(s"load data local inpath '$resourcesPath/big_int_Decimal.csv' into table big_int_basicc_Hive_1")

    sql("create table if not exists date_test(name String, age int, dob date,doj timestamp) STORED AS carbondata ")
    sql("insert into date_test select 'name1',12,'2014-01-01','2014-01-01 00:00:00' ")
    sql("insert into date_test select 'name2',13,'2015-01-01','2015-01-01 00:00:00' ")
    sql("insert into date_test select 'name3',14,'2016-01-01','2016-01-01 00:00:00' ")
  }

  test("Is not null filter") {
    checkAnswer(
      sql("select id from filtertestTablesWithNull " + "where id is not null"),
      Seq(Row(4), Row(6))
    )
  }
  
    test("join filter") {
    checkAnswer(
      sql("select b.name from filtertestTablesWithNull a join filtertestTablesWithNullJoin b  " + "on a.name=b.name"),
      Seq(Row("aaa4"), Row("aaa5"),Row("aaa6"))
    )
  }
  
    test("Between  filter") {
    checkAnswer(
      sql("select date from filtertestTablesWithNull " + " where date between '2014-01-20 00:00:00' and '2014-01-28 00:00:00'"),
      Seq(Row(Timestamp.valueOf("2014-01-21 00:00:00")), Row(Timestamp.valueOf("2014-01-22 00:00:00")))
    )
  }
    test("Multi column with invalid member filter") {
    checkAnswer(
      sql("select id from filtertestTablesWithNull " + "where id = salary"),
      Seq()
    )
  }

  test("Greater Than Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id > 99"),
      Seq(Row(100))
    )
  }
  test("Greater Than Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTablesWithDecimal " + "where id > 99"),
      Seq(Row(100))
    )
  }

  test("Greater Than equal to Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >= 99"),
      Seq(Row(99), Row(100))
    )
  }
  
    test("Greater Than equal to Filter with limit") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >= 99 order by id desc limit 1"),
      Seq(Row(100))
    )
  }

  test("Greater Than equal to Filter with aggregation limit") {
    sql("select * from filtertestTables").show(100)
//    checkAnswer(
//      sql("select count(id),country from filtertestTables " + "where id >= 99 group by country limit 1"),
//      Seq(Row(2,"china"))
//    )
  }
  test("Greater Than equal to Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >= 99"),
      Seq(Row(99), Row(100))
    )
  }
  test("Include Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id = 99"),
      Seq(Row(99))
    )
  }
  test("In Filter") {
    checkAnswer(
      sql(
        "select Country from filtertestTables where Country in ('china','france') group by Country"
      ),
      Seq(Row("china"), Row("france"))
    )
  }

  test("Logical condition") {
    checkAnswer(
      sql("select id,country from filtertestTables " + "where country='china' and name='aaa1'"),
      Seq(Row(1, "china"))
    )
  }
  
  test("filter query over table having no data") {
    checkAnswer(
      sql("select * from noloadtable " + "where country='china' and name='aaa1'"),
      Seq()
    )
  }


    
     test("Time stamp filter with diff time format for load greater") {
    checkAnswer(
      sql("select date  from filterTimestampDataType where date > '2014-07-10 00:00:00'"),
      Seq(Row(Timestamp.valueOf("2014-07-20 00:00:00.0")),
        Row(Timestamp.valueOf("2014-07-25 00:00:00.0"))
      )
    )
  }
    test("Time stamp filter with diff time format for load less") {
    checkAnswer(
      sql("select date  from filterTimestampDataType where date < '2014-07-20 00:00:00'"),
      Seq(Row(Timestamp.valueOf("2014-07-10 00:00:00.0"))
      )
    )
  }
   test("Time stamp filter with diff time format for load less than equal") {
    checkAnswer(
      sql("select date  from filterTimestampDataType where date <= '2014-07-20 00:00:00'"),
      Seq(Row(Timestamp.valueOf("2014-07-10 00:00:00.0")),Row(Timestamp.valueOf("2014-07-20 00:00:00.0"))
      )
    )
  }
      test("Time stamp filter with diff time format for load greater than equal") {
    checkAnswer(
      sql("select date  from filterTimestampDataType where date >= '2014-07-20 00:00:00'"),
      Seq(Row(Timestamp.valueOf("2014-07-20 00:00:00.0")),Row(Timestamp.valueOf("2014-07-25 00:00:00.0"))
      )
    )
  }
    test("join query with bigdecimal filter") {

    checkAnswer(
      sql("select b.level from big_int_basicc_Hive a join big_int_basicc_Hive_1 b on a.level=b.level order by level"),
      sql("select b.level from big_int_basicc a join big_int_basicc_1 b on a.level=b.level order by level")
    )
  }
    
        test("join query with bigint filter") {

    checkAnswer(
      sql("select b.task from big_int_basicc_Hive a join big_int_basicc_Hive_1 b on a.task=b.task"),
      sql("select b.task from big_int_basicc a join big_int_basicc_1 b on a.task=b.task")
    )
  }


  test("test FilterUnsupportedException when using big numbers") {
    sql("drop table if exists outofrange")
    sql("CREATE table outofrange (column1 STRING, column2 STRING,column3 INT, column4 INT,column5 INT, column6 INT) STORED AS carbondata")
    sql(s"""LOAD DATA INPATH '$resourcesPath/outofrange.csv' INTO TABLE outofrange OPTIONS('DELIMITER'=',','QUOTECHAR'='\"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""")

    sql("select * from outofrange where column4=-9223372036854775808").show()
    sql("drop table if exists outofrange")
  }

  test("check invalid  date value") {
    val df=sql("select * from date_test where dob=''")
    assert(df.count()==0,"Wrong data are displayed on invalid date ")
  }

  test("check invalid  date with and filter value ") {
    val df=sql("select * from date_test where dob='' and age=13")
    assert(df.count()==0,"Wrong data are displayed on invalid date ")
  }

  test("check invalid  date with or filter value ") {
    val df=sql("select * from date_test where dob='' or age=13")
    checkAnswer(df,Seq(Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0"))))
  }

  test("check invalid  date Geaterthan filter value ") {
    val df=sql("select * from date_test where doj > '0' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0")),
      Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0")),
      Row("name3",14,Date.valueOf("2016-01-01"),Timestamp.valueOf("2016-01-01 00:00:00.0"))))
  }
  test("check invalid  date Geaterthan and lessthan filter value ") {
    val df=sql("select * from date_test where doj > '0' and doj < '2015-01-01' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0"))))
  }
  test("check invalid  date Geaterthan or lessthan filter value ") {
    val df=sql("select * from date_test where doj > '0' or doj < '2015-01-01' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0")),
      Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0")),
      Row("name3",14,Date.valueOf("2016-01-01"),Timestamp.valueOf("2016-01-01 00:00:00.0"))))
  }

  test("check invalid  timestamp value") {
    val df=sql("select * from date_test where dob=''")
    assert(df.count()==0,"Wrong data are displayed on invalid timestamp ")
  }

  test("check invalid  timestamp with and filter value ") {
    val df=sql("select * from date_test where doj='' and age=13")
    assert(df.count()==0,"Wrong data are displayed on invalid timestamp ")
  }

  test("check invalid  timestamp with or filter value ") {
    val df=sql("select * from date_test where doj='' or age=13")
    checkAnswer(df,Seq(Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0"))))
  }

  test("check invalid  timestamp Geaterthan filter value ") {
    val df=sql("select * from date_test where doj > '0' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0")),
      Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0")),
    Row("name3",14,Date.valueOf("2016-01-01"),Timestamp.valueOf("2016-01-01 00:00:00.0"))))
  }
  test("check invalid  timestamp Geaterthan and lessthan filter value ") {
    val df=sql("select * from date_test where doj > '0' and doj < '2015-01-01 00:00:00' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0"))))
  }
  test("check invalid  timestamp Geaterthan or lessthan filter value ") {
    val df=sql("select * from date_test where doj > '0' or doj < '2015-01-01 00:00:00' ")
    checkAnswer(df,Seq(Row("name1",12,Date.valueOf("2014-01-01"),Timestamp.valueOf("2014-01-01 00:00:00.0")),
      Row("name2",13,Date.valueOf("2015-01-01"),Timestamp.valueOf("2015-01-01 00:00:00.0")),
      Row("name3",14,Date.valueOf("2016-01-01"),Timestamp.valueOf("2016-01-01 00:00:00.0"))))
  }

  test("like% test case with restructure") {
    sql("drop table if exists like_filter")
    sql(
      """
        | CREATE TABLE like_filter(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file1' INTO TABLE like_filter OPTIONS('header'='false')")
    sql(
      "ALTER TABLE like_filter ADD COLUMNS(filter STRING) TBLPROPERTIES ('DEFAULT.VALUE" +
      ".FILTER'='altered column')")
    checkAnswer(sql("select count(*) from like_filter where filter like '%column'"), Row(500000))
  }




  override def afterAll {
    sql("drop table if exists filtertestTables")
    sql("drop table if exists filtertestTablesWithDecimal")
    sql("drop table if exists filtertestTablesWithNull")
    sql("drop table if exists filterTimestampDataType")
    sql("drop table if exists noloadtable")
    sql("DROP TABLE IF EXISTS big_int_basicc")
    sql("DROP TABLE IF EXISTS big_int_basicc_1")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive_1")
    sql("DROP TABLE IF EXISTS filtertestTablesWithNull")
    sql("DROP TABLE IF EXISTS filtertestTablesWithNullJoin")
    sql("drop table if exists like_filter")
    CompactionSupportGlobalSortBigFileTest.deleteFile(file1)
    sql("drop table if exists date_test")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }

  test("test if query is giving empty results for table with no segments") {
    sql("drop table if exists q1")
    sql("create table q1(a string) STORED AS carbondata ")
    assert(sql("select * from q1 where a > 10").count() == 0)
    sql("drop table if exists q1")
  }
}
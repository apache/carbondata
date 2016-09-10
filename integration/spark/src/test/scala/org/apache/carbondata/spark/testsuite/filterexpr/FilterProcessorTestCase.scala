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

package org.apache.carbondata.spark.testsuite.filterexpr

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for filter expression query on String datatypes
  *
  */
class FilterProcessorTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists filtertestTables")
    sql("drop table if exists filtertestTablesWithDecimal")
    sql("drop table if exists filtertestTablesWithNull")
    sql("drop table if exists filterTimestampDataType")
    sql("drop table if exists noloadtable")

    sql("CREATE TABLE filtertestTables (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
        "STORED BY 'org.apache.carbondata.format'"
    )
    sql("CREATE TABLE noloadtable (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )
     CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss")
        
     sql("CREATE TABLE filterTimestampDataType (ID int, date Timestamp, country String, " +
      "name String, phonetype String, serialname String, salary int) " +
        "STORED BY 'org.apache.carbondata.format'"
    )
       CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss")
    sql(
      s"LOAD DATA LOCAL INPATH './src/test/resources/data2_DiffTimeFormat.csv' INTO TABLE " +
        s"filterTimestampDataType " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      s"LOAD DATA local inpath './src/test/resources/dataDiff.csv' INTO TABLE filtertestTables " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    sql(
      "CREATE TABLE filtertestTablesWithDecimal (ID decimal, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String, salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      s"LOAD DATA LOCAL INPATH './src/test/resources/dataDiff.csv' INTO TABLE " +
        s"filtertestTablesWithDecimal " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
    sql("DROP TABLE IF EXISTS filtertestTablesWithNull")
    sql(
      "CREATE TABLE filtertestTablesWithNull (ID int, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String,salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )
    sql("DROP TABLE IF EXISTS filtertestTablesWithNullJoin")
    sql(
      "CREATE TABLE filtertestTablesWithNullJoin (ID int, date Timestamp, country " +
        "String, " +
        "name String, phonetype String, serialname String,salary int) " +
      "STORED BY 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql(
      s"LOAD DATA LOCAL INPATH './src/test/resources/data2.csv' INTO TABLE " +
        s"filtertestTablesWithNull " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )
        sql(
      s"LOAD DATA LOCAL INPATH './src/test/resources/data2.csv' INTO TABLE " +
        s"filtertestTablesWithNullJoin " +
        s"OPTIONS('DELIMITER'= ',', " +
        s"'FILEHEADER'= '')"
    )

    sql("DROP TABLE IF EXISTS big_int_basicc")
    sql("DROP TABLE IF EXISTS big_int_basicc_1")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive")
    sql("DROP TABLE IF EXISTS big_int_basicc_Hive_1")
    sql("CREATE TABLE big_int_basicc (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE big_int_basicc_1 (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE big_int_basicc_Hive (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate date,enddate date,PointId double,score decimal(10,3))row format delimited fields terminated by ',' " +
        "tblproperties(\"skip.header.line.count\"=\"1\") ")
    sql("CREATE TABLE big_int_basicc_Hive_1 (imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate date,enddate date,PointId double,score decimal(10,3))row format delimited fields terminated by ',' " +
        "tblproperties(\"skip.header.line.count\"=\"1\") ")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
    sql("LOAD DATA INPATH './src/test/resources/big_int_Decimal.csv'  INTO TABLE big_int_basicc options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$','COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')")
    sql("LOAD DATA INPATH './src/test/resources/big_int_Decimal.csv'  INTO TABLE big_int_basicc_1 options ('DELIMITER'=',', 'QUOTECHAR'='\"', 'COMPLEX_DELIMITER_LEVEL_1'='$','COMPLEX_DELIMITER_LEVEL_2'=':', 'FILEHEADER'= '')")
    sql("load data local inpath './src/test/resources/big_int_Decimal.csv' into table big_int_basicc_Hive")
    sql("load data local inpath './src/test/resources/big_int_Decimal.csv' into table big_int_basicc_Hive_1")
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
      sql("select id from filtertestTables " + "where id >999"),
      Seq(Row(1000))
    )
  }
  test("Greater Than Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTablesWithDecimal " + "where id >999"),
      Seq(Row(1000))
    )
  }

  test("Greater Than equal to Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >=999"),
      Seq(Row(999), Row(1000))
    )
  }
  
    test("Greater Than equal to Filter with limit") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >=999 limit 1"),
      Seq(Row(1000))
    )
  }

      test("Greater Than equal to Filter with aggregation limit") {
    checkAnswer(
      sql("select count(id),country from filtertestTables " + "where id >=999 group by country limit 1"),
      Seq(Row(2,"china"))
    )
  }
  test("Greater Than equal to Filter with decimal") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id >=999"),
      Seq(Row(999), Row(1000))
    )
  }
  test("Include Filter") {
    checkAnswer(
      sql("select id from filtertestTables " + "where id =999"),
      Seq(Row(999))
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
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
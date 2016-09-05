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

package org.apache.carbondata.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for data loading with hive syntax and old syntax
  *
  */
class TestLoadDataWithHiveSyntax extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql(
      "CREATE table carbontable (empno int, empname String, designation String, doj String, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
        "projectcode int, projectjoindate String, projectenddate String, attendance int," +
        "utilization int,salary int) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "create table hivetable(empno int, empname String, designation string, doj String, " +
        "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance String," +
        "utilization String,salary String)row format delimited fields terminated by ','"
    )

    sql("drop table if exists carbontable1")
    sql("drop table if exists hivetable1")
  }

  test("test data loading and validate query output") {
    //Create test cube and hive table
    sql(
      "CREATE table testtable (empno int, empname String, designation String, doj String, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
        "projectcode int, projectjoindate String, projectenddate String,attendance double," +
        "utilization double,salary double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='empno,empname,designation,doj,workgroupcategory," +
        "workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate')"
    )
    sql(
      "create table testhivetable(empno int, empname String, designation string, doj String, " +
        "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance double," +
        "utilization double,salary double)row format delimited fields terminated by ','"
    )
    //load data into test cube and hive table and validate query result
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table testtable")
    sql(
      "LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table " +
        "testhivetable"
    )
    checkAnswer(sql("select * from testtable"), sql("select * from testhivetable"))
    //load data incrementally and validate query result
    sql(
      "LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE testtable OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testhivetable"
    )
    checkAnswer(sql("select * from testtable"), sql("select * from testhivetable"))
    //drop test cube and table
    sql("drop table testtable")
    sql("drop table testhivetable")
  }

  /**
    * TODO: temporarily changing cube names to different names,
    * however deletion and creation of cube with same name
    */
  test("test data loading with different case file header and validate query output") {
    //Create test cube and hive table
    sql(
      "CREATE table testtable1 (empno int, empname String, designation String, doj String, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
        "projectcode int, projectjoindate String, projectenddate String,attendance double," +
        "utilization double,salary double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='empno,empname,designation,doj,workgroupcategory," +
        "workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate')"
    )
    sql(
      "create table testhivetable1(empno int, empname String, designation string, doj String, " +
        "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance double," +
        "utilization double,salary double)row format delimited fields terminated by ','"
    )
    //load data into test cube and hive table and validate query result
    sql(
      "LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testtable1 " +
        "options('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='EMPno, empname,designation,doj," +
        "workgroupcategory,workgroupcategoryname,   deptno,deptname,projectcode,projectjoindate," +
        "projectenddate,  attendance,   utilization,SALARY')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table " +
        "testhivetable1"
    )
    checkAnswer(sql("select * from testtable1"), sql("select * from testhivetable1"))
    //drop test cube and table
    sql("drop table testtable1")
    sql("drop table testhivetable1")
  }

  test("test hive table data loading") {
    sql(
      "LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table " +
        "hivetable"
    )
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table hivetable")
  }

  test("test carbon table data loading using old syntax") {
    sql(
      "LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE carbontable OPTIONS" +
        "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
  }

  test("test carbon table data loading using new syntax compatible with hive") {
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table carbontable")
    sql(
      "LOAD DATA local inpath './src/test/resources/data.csv' INTO table carbontable options" +
        "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
  }

  test("test carbon table data loading using new syntax with overwrite option compatible with hive")
  {
    try {
      sql("LOAD DATA local inpath './src/test/resources/data.csv' overwrite INTO table carbontable")
    } catch {
      case e: Throwable => {
        assert(e.getMessage
          .equals("Overwrite is not supported for carbon table with default.carbontable")
        )
      }
    }
  }

  test("complex types data loading") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
      "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
      "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double) " +
      "STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/complexdata.csv' INTO table " +
        "complexcarbontable " +
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
        "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("drop table if exists complexcarbontable")
  }

  test(
    "complex types data loading with more unused columns and different order of complex columns " +
      "in csv and create table"
  ) {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
      "mobile struct<imei:string, imsi:string>, ROMSize string, purchasedate string," +
      "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double) " +
      "STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId','DICTIONARY_EXCLUDE'='channelsId')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/complextypediffentcolheaderorder.csv' INTO " +
        "table complexcarbontable " +
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
        "ROMSize,purchasedate,MAC,abc,mobile,locationinfo,proddate,gamePointId,contractNumber'," +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("select count(*) from complexcarbontable")
    sql("drop table complexcarbontable")
  }

  test("test carbon table data loading with csv file Header in caps") {
    sql("drop table if exists header_test")
    sql(
      "create table header_test(empno int, empname String, designation string, doj String, " +
        "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance String," +
        "utilization String,salary String) STORED BY 'org.apache.carbondata.format'"
    )
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath = currentDirectory + "/src/test/resources/data_withCAPSHeader.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table header_test OPTIONS " +
      "('DELIMITER'=',', 'QUOTECHAR'='\"')");
    checkAnswer(sql("select empno from header_test"),
      Seq(Row(11), Row(12))
    )
  }

  test("test duplicate column validation") {
    try {
      sql("create table duplicateColTest(col1 string, Col1 string)")
    }
    catch {
      case e: Exception => {
        assert(e.getMessage.contains("Duplicate column name"))
      }
    }
  }

  test(
    "test carbon table data loading with csv file Header in Mixed Case and create table columns " +
      "in mixed case"
  ) {
    sql("drop table if exists mixed_header_test")
    sql(
      "create table mixed_header_test(empno int, empname String, Designation string, doj String, " +
        "Workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance String," +
        "utilization String,salary String) STORED BY 'org.apache.carbondata.format'"
    )
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath = currentDirectory + "/src/test/resources/data_withMixedHeader.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table mixed_header_test OPTIONS " +
      "('DELIMITER'=',', 'QUOTECHAR'='\"')");
    checkAnswer(sql("select empno from mixed_header_test"),
      Seq(Row(11), Row(12))
    )
  }


  test("complex types data loading with hive column having more than required column values") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
      "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
      "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double) " +
      "STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/complexdatastructextra.csv' INTO table " +
        "complexcarbontable " +
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
        "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("drop table if exists complexcarbontable")
  }

  test("complex types & no dictionary columns data loading") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
      "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
      "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
      "double,contractNumber double) " +
      "STORED BY 'org.apache.carbondata.format' " +
      "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId', 'DICTIONARY_EXCLUDE'='ROMSize," +
      "purchasedate')"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/complexdata.csv' INTO table " +
        "complexcarbontable " +
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
        "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    );
    sql("drop table if exists complexcarbontable")
  }

  test("array<string> and string datatype for same column is not working properly") {
    sql("create table complexcarbontable(deviceInformationId int, MAC array<string>, channelsId string, "+
        "ROMSize string, purchasedate string, gamePointId double,contractNumber double) STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')")
    sql("LOAD DATA local inpath './src/test/resources/complexdatareordered.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,MAC,channelsId,ROMSize,purchasedate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists complexcarbontable")
    sql("create table primitivecarbontable(deviceInformationId int, MAC string, channelsId string, "+
        "ROMSize string, purchasedate string, gamePointId double,contractNumber double) STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')")
    sql("LOAD DATA local inpath './src/test/resources/complexdatareordered.csv' INTO table primitivecarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,MAC,channelsId,ROMSize,purchasedate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists primitivecarbontable")
  }

  test(
    "test carbon table data loading when table name is in different case with create table, for " +
      "UpperCase"
  ) {
    sql("create table UPPERCASEcube(empno Int, empname String, designation String, " +
      "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
      "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
      "attendance Int,utilization Double,salary Double) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/data.csv' INTO table uppercasecube OPTIONS" +
        "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
    sql("drop table UpperCaseCube")
  }

  test(
    "test carbon table data loading when table name is in different case with create table ,for " +
      "LowerCase"
  ) {
    sql("create table lowercaseCUBE(empno Int, empname String, designation String, " +
      "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
      "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
      "attendance Int,utilization Double,salary Double) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/data.csv' INTO table LOWERCASECUBE OPTIONS" +
        "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
    sql("drop table LowErcasEcube")
  }

  test("test carbon table data loading using escape char 1") {
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
      """
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    sql(
      s"""
           LOAD DATA LOCAL INPATH './src/test/resources/datawithbackslash.csv' into table t3
           OPTIONS('ESCAPECHAR'='@')
        """
    )
    checkAnswer(sql("select count(*) from t3"), Seq(Row(10)))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test carbon table data loading using escape char 2") {
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
         CREATE TABLE t3(imei string,specialchar string)
         STORED BY 'org.apache.carbondata.format'
      """
    )

    sql(
      """
       LOAD DATA LOCAL INPATH './src/test/resources/datawithescapecharacter.csv' into table t3
          options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='\')
      """
    )
    checkAnswer(sql("select count(*) from t3"), Seq(Row(21)))
    checkAnswer(sql("select specialchar from t3 where imei = '1AA44'"), Seq(Row("escapeesc")))
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test carbon table data loading using escape char 3") {
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
         CREATE TABLE t3(imei string,specialchar string)
         STORED BY 'org.apache.carbondata.format'
      """
    )

    sql(
      """
       LOAD DATA LOCAL INPATH './src/test/resources/datawithescapecharacter.csv' into table t3
          options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='@')
      """
    )
    checkAnswer(sql("select count(*) from t3"), Seq(Row(21)))
    checkAnswer(sql("select specialchar from t3 where imei in ('1232','12323')"), Seq(Row
    ("ayush@b.com"), Row("ayushb.com")
    )
    )
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test carbon table data loading with special character 1") {
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
         CREATE TABLE t3(imei string,specialchar string)
         STORED BY 'org.apache.carbondata.format'
      """
    )

    sql(
      """
       LOAD DATA LOCAL INPATH './src/test/resources/datawithspecialcharacter.csv' into table t3
          options ('DELIMITER'=',', 'QUOTECHAR'='"')
      """
    )
    checkAnswer(sql("select count(*) from t3"), Seq(Row(37)))
    checkAnswer(sql("select specialchar from t3 where imei='1AA36'"), Seq(Row("\"i\"")))
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test carbon table data loading with special character 2") {
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
        CREATE table t3(customer_id int, 124_string_level_province String, date_level String,
        Time_level String, lname String, fname String, mi String, address1 String, address2
        String, address3 String, address4 String, city String, country String, phone1 String,
        phone2 String, marital_status String, yearly_income String, gender String, education
        String, member_card String, occupation String, houseowner String, fullname String,
        numeric_level double, account_num double, customer_region_id int, total_children int,
        num_children_at_home int, num_cars_owned int)
        STORED BY 'org.apache.carbondata.format'
      """
    )

    sql(
      """
       LOAD DATA LOCAL INPATH './src/test/resources/datawithcomplexspecialchar.csv' into
       table t3 options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='"')
      """
    )
    checkAnswer(sql("select count(*) from t3"), Seq(Row(150)))
    checkAnswer(sql("select 124_string_level_province from t3 where customer_id=103"),
      Seq(Row("\"state province # 124\""))
    )
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test data which contain column less than schema"){
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
      """)

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH './src/test/resources/lessthandatacolumndata.csv' into table t3
        """)
    checkAnswer(sql("select count(*) from t3"),Seq(Row(10)))
  }

  test("test data which contain column with decimal data type in array."){
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID decimal(5,5), date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           array<decimal(4,2)>)
           STORED BY 'org.apache.carbondata.format'
      """
    )

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH './src/test/resources/complexTypeDecimal.csv' into table t3
        """)
    checkAnswer(sql("select count(*) from t3"),Seq(Row(8)))
    sql("DROP TABLE IF EXISTS t3")
  }

  test("test data which contain column with decimal data type in struct."){
    sql("DROP TABLE IF EXISTS t3")

    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID decimal(5,5), date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           struct<a:decimal(4,2)>)
           STORED BY 'org.apache.carbondata.format'
      """
    )

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH './src/test/resources/complexTypeDecimal.csv' into table t3
        """)
    checkAnswer(sql("select count(*) from t3"),Seq(Row(8)))
  }

  test("test data which contain column with decimal data type in array of struct."){
    sql("DROP TABLE IF EXISTS complex_t3")
    sql("DROP TABLE IF EXISTS complex_hive_t3")

    sql(
      """
           CREATE TABLE complex_t3
           (ID decimal, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           array<struct<a:decimal(4,2),str:string>>)
           STORED BY 'org.apache.carbondata.format'
      """
    )
    sql(
      """
           CREATE TABLE complex_hive_t3
           (ID decimal, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           array<struct<a:decimal(4,2),str:string>>)
           row format delimited fields terminated by ','
      """
    )

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH './src/test/resources/complexTypeDecimalNested.csv' into table complex_t3
        """)
    sql(s"""
         LOAD DATA LOCAL INPATH './src/test/resources/complexTypeDecimalNestedHive.csv' into table complex_hive_t3
        """)
    checkAnswer(sql("select count(*) from complex_t3"),sql("select count(*) from complex_hive_t3"))
    checkAnswer(sql("select id from complex_t3 where salary = 15000"),sql("select id from complex_hive_t3 where salary = 15000"))
  }

  test("test data loading when delimiter is '|' and data with header") {
    sql(
      "CREATE table carbontable1 (empno int, empname String, designation String, doj String, " +
        "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
        "projectcode int, projectjoindate String, projectenddate String,attendance double," +
        "utilization double,salary double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES" +
        "('DICTIONARY_EXCLUDE'='empno,empname,designation,doj,workgroupcategory," +
        "workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate')"
    )
    sql(
      "create table hivetable1 (empno int, empname String, designation string, doj String, " +
        "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
        "projectcode int, projectjoindate String,projectenddate String, attendance double," +
        "utilization double,salary double)row format delimited fields terminated by ','"
    )

    sql(
      "LOAD DATA local inpath './src/test/resources/datadelimiter.csv' INTO TABLE carbontable1 OPTIONS" +
        "('DELIMITER'= '|', 'QUOTECHAR'= '\"')"
    )

    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table hivetable1")

    checkAnswer(sql("select * from carbontable1"), sql("select * from hivetable1"))
  }

  test("test data loading with comment option") {
    sql("drop table if exists comment_test")
    sql(
      "create table comment_test(imei string, age int, task bigint, num double, level decimal(10," +
        "3), productdate timestamp, mark int, name string) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "LOAD DATA local inpath './src/test/resources/comment.csv' INTO TABLE comment_test " +
        "options('DELIMITER' = ',', 'QUOTECHAR' = '.', 'COMMENTCHAR' = '?','FILEHEADER'='imei,age,task,num,level,productdate,mark,name')"
    )
    checkAnswer(sql("select imei from comment_test"),Seq(Row("\".carbon"),Row("#?carbon"), Row(""),
      Row("~carbon,")))
  }


  override def afterAll {
    sql("drop table carbontable")
    sql("drop table hivetable")
    sql("drop table if exists header_test")
    sql("drop table if exists mixed_header_test")
    sql("drop table carbontable1")
    sql("drop table hivetable1")
    sql("drop table if exists comment_test")
  }
}
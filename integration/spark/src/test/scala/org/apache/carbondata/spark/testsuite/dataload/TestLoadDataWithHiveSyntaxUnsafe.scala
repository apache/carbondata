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

package org.apache.carbondata.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.QueryTest

/**
  * Test Class for data loading with Unsafe ColumnPage
  *
  */
class TestLoadDataWithHiveSyntaxUnsafe extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
      "true"
    )
    sql("drop table if exists escapechar1")
    sql("drop table if exists escapechar2")
    sql("drop table if exists escapechar3")
    sql("drop table if exists specialcharacter1")
    sql("drop table if exists specialcharacter2")
    sql("drop table if exists collessthanschema")
    sql("drop table if exists decimalarray")
    sql("drop table if exists decimalstruct")
    sql("drop table if exists carbontable")
    sql("drop table if exists hivetable")
    sql("drop table if exists testtable")
    sql("drop table if exists testhivetable")
    sql("drop table if exists testtable1")
    sql("drop table if exists testhivetable1")
    sql("drop table if exists complexcarbontable")
    sql("drop table if exists complex_t3")
    sql("drop table if exists complex_hive_t3")
    sql("drop table if exists header_test")
    sql("drop table if exists duplicateColTest")
    sql("drop table if exists mixed_header_test")
    sql("drop table if exists primitivecarbontable")
    sql("drop table if exists UPPERCASEcube")
    sql("drop table if exists lowercaseCUBE")
    sql("drop table if exists carbontable1")
    sql("drop table if exists hivetable1")
    sql("drop table if exists comment_test")
    sql("drop table if exists smallinttable")
    sql("drop table if exists smallinthivetable")
    sql("drop table if exists decimal_varlength")
    sql("drop table if exists decimal_varlength_hive")
    sql(
      "CREATE table carbontable (empno int, empname String, designation String, doj String, " +
          "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
          "projectcode int, projectjoindate String, projectenddate String, attendance int," +
          "utilization int,salary int) STORED AS carbondata"
    )
    sql(
      "create table hivetable(empno int, empname String, designation string, doj String, " +
          "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
          "projectcode int, projectjoindate String,projectenddate String, attendance String," +
          "utilization String,salary String)row format delimited fields terminated by ','"
    )
    sql(
      """
        | CREATE TABLE decimal_varlength(id string, value decimal(30,10))
        | STORED AS carbondata
      """.stripMargin
    )
    sql(
      """
        | CREATE TABLE decimal_varlength_hive(id string, value decimal(30,10))
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      """.stripMargin
    )
  }

  test("create table with smallint type and query smallint table") {
    sql("drop table if exists smallinttable")
    sql("drop table if exists smallinthivetable")
    sql(
      "create table smallinttable(empno smallint, empname String, designation string, " +
          "doj String, workgroupcategory int, workgroupcategoryname String,deptno int, " +
          "deptname String, projectcode int, projectjoindate String,projectenddate String, " +
          "attendance String, utilization String,salary String)" +
          "STORED AS carbondata"
    )

    sql(
      "create table smallinthivetable(empno smallint, empname String, designation string, " +
          "doj String, workgroupcategory int, workgroupcategoryname String,deptno int, " +
          "deptname String, projectcode int, projectjoindate String,projectenddate String, " +
          "attendance String, utilization String,salary String)" +
          "row format delimited fields terminated by ','"
    )

    sql(s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table smallinttable ")
    sql(s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' overwrite " +
        "INTO table smallinthivetable")

    checkAnswer(
      sql("select empno from smallinttable"),
      sql("select empno from smallinthivetable")
    )

    sql("drop table if exists smallinttable")
    sql("drop table if exists smallinthivetable")
  }

  test("test data loading and validate query output") {
    sql("drop table if exists testtable")
    sql("drop table if exists testhivetable")
    //Create test cube and hive table
    sql(
      "CREATE table testtable (empno string, empname String, designation String, doj String, " +
          "workgroupcategory string, workgroupcategoryname String, deptno string, deptname String, " +
          "projectcode string, projectjoindate String, projectenddate String,attendance double," +
          "utilization double,salary double) STORED AS carbondata "
    )
    sql(
      "create table testhivetable(empno string, empname String, designation string, doj String, " +
          "workgroupcategory string, workgroupcategoryname String,deptno string, deptname String, " +
          "projectcode string, projectjoindate String,projectenddate String, attendance double," +
          "utilization double,salary double)row format delimited fields terminated by ','"
    )
    //load data into test cube and hive table and validate query result
    sql(s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table testtable")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' overwrite INTO table " +
          "testhivetable"
    )
    checkAnswer(sql("select * from testtable"), sql("select * from testhivetable"))
    //load data incrementally and validate query result
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE testtable OPTIONS" +
          "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO table testhivetable"
    )
    checkAnswer(sql("select * from testtable"), sql("select * from testhivetable"))
    //drop test cube and table
    sql("drop table if exists testtable")
    sql("drop table if exists testhivetable")
  }

  /**
    * TODO: temporarily changing cube names to different names,
    * however deletion and creation of cube with same name
    */
  test("test data loading with different case file header and validate query output") {
    sql("drop table if exists testtable1")
    sql("drop table if exists testhivetable1")
    //Create test cube and hive table
    sql(
      "CREATE table testtable1 (empno string, empname String, designation String, doj String, " +
          "workgroupcategory string, workgroupcategoryname String, deptno string, deptname String, " +
          "projectcode string, projectjoindate String, projectenddate String,attendance double," +
          "utilization double,salary double) STORED AS carbondata "
    )
    sql(
      "create table testhivetable1(empno string, empname String, designation string, doj String, " +
          "workgroupcategory string, workgroupcategoryname String,deptno string, deptname String, " +
          "projectcode string, projectjoindate String,projectenddate String, attendance double," +
          "utilization double,salary double)row format delimited fields terminated by ','"
    )
    //load data into test cube and hive table and validate query result
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO table testtable1 " +
          "options('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='EMPno, empname,designation,doj," +
          "workgroupcategory,workgroupcategoryname,   deptno,deptname,projectcode,projectjoindate," +
          "projectenddate,  attendance,   utilization,SALARY')"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' overwrite INTO table " +
          "testhivetable1"
    )
    checkAnswer(sql("select * from testtable1"), sql("select * from testhivetable1"))
    //drop test cube and table
    sql("drop table if exists testtable1")
    sql("drop table if exists testhivetable1")
  }

  test("test hive table data loading") {
    sql(
      s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' overwrite INTO table " +
          "hivetable"
    )
    sql(s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO table hivetable")
  }

  test("test carbon table data loading using old syntax") {
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE carbontable OPTIONS" +
          "('DELIMITER'= ',', 'QUOTECHAR'= '\"')"
    )
  }

  test("test carbon table data loading using new syntax compatible with hive") {
    sql(s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table carbontable")
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table carbontable options" +
          "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
  }

  test("test carbon table data loading using new syntax with overwrite option compatible with hive")
  {
    try {
      sql(s"LOAD DATA local inpath '$resourcesPath/data.csv' overwrite INTO table carbontable")
    } catch {
      case e: Throwable => {
        assert(e.getMessage
            .equals("Overwrite is not supported for carbon table with default.carbontable")
        )
      }
    }
  }

  test("complex types data loading") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata "
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdata.csv' INTO table " +
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
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "mobile struct<imei:string, imsi:string>, ROMSize string, purchasedate string," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata "
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complextypediffentcolheaderorder.csv' INTO " +
          "table complexcarbontable " +
          "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
          "ROMSize,purchasedate,MAC,abc,mobile,locationinfo,proddate,gamePointId,contractNumber'," +
          "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("select count(*) from complexcarbontable")
    sql("drop table if exists complexcarbontable")
  }

  test("test carbon table data loading with csv file Header in caps") {
    sql("drop table if exists header_test")
    sql(
      "create table header_test(empno int, empname String, designation string, doj String, " +
          "workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, " +
          "projectcode int, projectjoindate String,projectenddate String, attendance String," +
          "utilization String,salary String) STORED AS carbondata"
    )
    val csvFilePath = s"$resourcesPath/data_withCAPSHeader.csv"
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
        assert(e.getMessage.contains("Duplicate column name") ||
            e.getMessage.contains("Found duplicate column"))
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
          "utilization String,salary String) STORED AS carbondata"
    )
    val csvFilePath = s"$resourcesPath/data_withMixedHeader.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO table mixed_header_test OPTIONS " +
        "('DELIMITER'=',', 'QUOTECHAR'='\"')");
    checkAnswer(sql("select empno from mixed_header_test"),
      Seq(Row(11), Row(12))
    )
  }


  test("complex types data loading with hive column having more than required column values") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata "
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdatastructextra.csv' INTO table " +
          "complexcarbontable " +
          "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
          "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
          "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    )
    sql("drop table if exists complexcarbontable")
  }

  test("complex types & no dictionary columns data loading") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, channelsId string," +
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>," +
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
        "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>," +
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId " +
        "double,contractNumber double) " +
        "STORED AS carbondata "
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/complexdata.csv' INTO table " +
          "complexcarbontable " +
          "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId," +
          "ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber'," +
          "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')"
    );
    sql("drop table if exists complexcarbontable")
  }

  test("array<string> and string datatype for same column is not working properly") {
    sql("drop table if exists complexcarbontable")
    sql("create table complexcarbontable(deviceInformationId int, MAC array<string>, channelsId string, "+
        "ROMSize string, purchasedate string, gamePointId double,contractNumber double) STORED AS carbondata ")
    sql(s"LOAD DATA local inpath '$resourcesPath/complexdatareordered.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,MAC,channelsId,ROMSize,purchasedate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists complexcarbontable")
    sql("create table primitivecarbontable(deviceInformationId int, MAC string, channelsId string, "+
        "ROMSize string, purchasedate string, gamePointId double,contractNumber double) STORED AS carbondata ")
    sql(s"LOAD DATA local inpath '$resourcesPath/complexdatareordered.csv' INTO table primitivecarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,MAC,channelsId,ROMSize,purchasedate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists primitivecarbontable")
  }

  test(
    "test carbon table data loading when table name is in different case with create table, for " +
        "UpperCase"
  ) {
    sql("drop table if exists UPPERCASEcube")
    sql("create table UPPERCASEcube(empno Int, empname String, designation String, " +
        "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
        "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
        "attendance Int,utilization Double,salary Double) STORED AS carbondata"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table uppercasecube OPTIONS" +
          "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
    sql("drop table if exists UpperCaseCube")
  }

  test(
    "test carbon table data loading when table name is in different case with create table ,for " +
        "LowerCase"
  ) {
    sql("drop table if exists lowercaseCUBE")
    sql("create table lowercaseCUBE(empno Int, empname String, designation String, " +
        "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
        "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
        "attendance Int,utilization Double,salary Double) STORED AS carbondata"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/data.csv' INTO table LOWERCASECUBE OPTIONS" +
          "('DELIMITER'=',', 'QUOTECHAR'='\"')"
    )
    sql("drop table if exists LowErcasEcube")
  }

  test("test carbon table data loading using escape char 1") {
    sql("DROP TABLE IF EXISTS escapechar1")

    sql(
      """
           CREATE TABLE IF NOT EXISTS escapechar1
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
      """
    )
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(
      s"""
           LOAD DATA LOCAL INPATH '$resourcesPath/datawithbackslash.csv' into table escapechar1
           OPTIONS('ESCAPECHAR'='@')
        """
    )
    checkAnswer(sql("select count(*) from escapechar1"), Seq(Row(10)))
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    sql("DROP TABLE IF EXISTS escapechar1")
  }

  test("test carbon table data loading using escape char 2") {
    sql("DROP TABLE IF EXISTS escapechar2")

    sql(
      """
         CREATE TABLE escapechar2(imei string,specialchar string)
         STORED AS carbondata
      """
    )

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/datawithescapecharacter.csv' into table escapechar2
          options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='\')
      """
    )
    checkAnswer(sql("select count(*) from escapechar2"), Seq(Row(21)))
    checkAnswer(sql("select specialchar from escapechar2 where imei = '1AA44'"), Seq(Row("escapeesc")))
    sql("DROP TABLE IF EXISTS escapechar2")
  }

  test("test carbon table data loading using escape char 3") {
    sql("DROP TABLE IF EXISTS escapechar3")

    sql(
      """
         CREATE TABLE escapechar3(imei string,specialchar string)
         STORED AS carbondata
      """
    )

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/datawithescapecharacter.csv' into table escapechar3
          options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='@')
      """
    )
    checkAnswer(sql("select count(*) from escapechar3"), Seq(Row(21)))
    checkAnswer(sql("select specialchar from escapechar3 where imei in ('1232','12323')"), Seq(Row
    ("ayush@b.com"), Row("ayushb.com")
    )
    )
    sql("DROP TABLE IF EXISTS escapechar3")
  }

  test("test carbon table data loading with special character 1") {
    sql("DROP TABLE IF EXISTS specialcharacter1")

    sql(
      """
         CREATE TABLE specialcharacter1(imei string,specialchar string)
         STORED AS carbondata
      """
    )

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/datawithspecialcharacter.csv' into table specialcharacter1
          options ('DELIMITER'=',', 'QUOTECHAR'='"')
      """
    )
    checkAnswer(sql("select count(*) from specialcharacter1"), Seq(Row(37)))
    checkAnswer(sql("select specialchar from specialcharacter1 where imei='1AA36'"), Seq(Row("\"i\"")))
    sql("DROP TABLE IF EXISTS specialcharacter1")
  }

  test("test carbon table data loading with special character 2") {
    sql("DROP TABLE IF EXISTS specialcharacter2")

    sql(
      """
        CREATE table specialcharacter2(customer_id int, 124_string_level_province String, date_level String,
        Time_level String, lname String, fname String, mi String, address1 String, address2
        String, address3 String, address4 String, city String, country String, phone1 String,
        phone2 String, marital_status String, yearly_income String, gender String, education
        String, member_card String, occupation String, houseowner String, fullname String,
        numeric_level double, account_num double, customer_region_id int, total_children int,
        num_children_at_home int, num_cars_owned int)
        STORED AS carbondata
      """
    )

    sql(
      s"""
       LOAD DATA LOCAL INPATH '$resourcesPath/datawithcomplexspecialchar.csv' into
       table specialcharacter2 options ('DELIMITER'=',', 'QUOTECHAR'='"','ESCAPECHAR'='"')
      """
    )
    checkAnswer(sql("select count(*) from specialcharacter2"), Seq(Row(150)))
    checkAnswer(sql("select 124_string_level_province from specialcharacter2 where customer_id=103"),
      Seq(Row("\"state province # 124\""))
    )
    sql("DROP TABLE IF EXISTS specialcharacter2")
  }

  test("test data which contain column less than schema"){
    sql("DROP TABLE IF EXISTS collessthanschema")

    sql(
      """
           CREATE TABLE IF NOT EXISTS collessthanschema
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED AS carbondata
      """)

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/lessthandatacolumndata.csv' into table collessthanschema
        """)
    checkAnswer(sql("select count(*) from collessthanschema"),Seq(Row(10)))
    sql("DROP TABLE IF EXISTS collessthanschema")
  }

  test("test data which contain column with decimal data type in array."){
    sql("DROP TABLE IF EXISTS decimalarray")

    sql(
      """
           CREATE TABLE IF NOT EXISTS decimalarray
           (ID decimal(5,5), date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           array<decimal(4,2)>)
           STORED AS carbondata
      """
    )

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/complexTypeDecimal.csv' into table decimalarray
        """)
    checkAnswer(sql("select count(*) from decimalarray"),Seq(Row(8)))
    sql("DROP TABLE IF EXISTS decimalarray")
  }

  test("test data which contain column with decimal data type in struct."){
    sql("DROP TABLE IF EXISTS decimalstruct")

    sql(
      """
           CREATE TABLE IF NOT EXISTS decimalstruct
           (ID decimal(5,5), date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int, complex
           struct<a:decimal(4,2)>)
           STORED AS carbondata
      """
    )

    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/complexTypeDecimal.csv' into table decimalstruct
        """)
    checkAnswer(sql("select count(*) from decimalstruct"),Seq(Row(8)))
    sql("DROP TABLE IF EXISTS decimalstruct")
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
           STORED AS carbondata
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
         LOAD DATA LOCAL INPATH '$resourcesPath/complexTypeDecimalNested.csv' into table complex_t3
        """)
    sql(s"""
         LOAD DATA LOCAL INPATH '$resourcesPath/complexTypeDecimalNestedHive.csv' into table complex_hive_t3
        """)
    checkAnswer(sql("select count(*) from complex_t3"),sql("select count(*) from complex_hive_t3"))
    checkAnswer(sql("select id from complex_t3 where salary = 15000"),sql("select id from complex_hive_t3 where salary = 15000"))
  }

  test("test data loading when delimiter is '|' and data with header") {
    sql(
      "CREATE table carbontable1 (empno string, empname String, designation String, doj String, " +
          "workgroupcategory string, workgroupcategoryname String, deptno string, deptname String, " +
          "projectcode string, projectjoindate String, projectenddate String,attendance double," +
          "utilization double,salary double) STORED AS carbondata "
    )
    sql(
      "create table hivetable1 (empno string, empname String, designation string, doj String, " +
          "workgroupcategory string, workgroupcategoryname String,deptno string, deptname String, " +
          "projectcode string, projectjoindate String,projectenddate String, attendance double," +
          "utilization double,salary double)row format delimited fields terminated by ','"
    )

    sql(
      s"LOAD DATA local inpath '$resourcesPath/datadelimiter.csv' INTO TABLE carbontable1 OPTIONS" +
          "('DELIMITER'= '|', 'QUOTECHAR'= '\"')"
    )

    sql(s"LOAD DATA local inpath '$resourcesPath/datawithoutheader.csv' INTO table hivetable1")

    checkAnswer(sql("select * from carbontable1"), sql("select * from hivetable1"))
  }

  test("test data loading with comment option") {
    sql("drop table if exists comment_test")
    sql(
      "create table comment_test(imei string, age int, task bigint, num double, level decimal(10," +
          "3), productdate timestamp, mark int, name string) STORED AS carbondata"
    )
    sql(
      s"LOAD DATA local inpath '$resourcesPath/comment.csv' INTO TABLE comment_test " +
          "options('DELIMITER' = ',', 'QUOTECHAR' = '.', 'COMMENTCHAR' = '?','FILEHEADER'='imei,age,task,num,level,productdate,mark,name', 'maxcolumns'='180')"
    )
    checkAnswer(sql("select imei from comment_test"),Seq(Row("\".carbon"),Row("#?carbon"), Row(""),
      Row("~carbon,")))
  }

  test("test decimal var lenght comlumn page") {
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/big_decimal_without_header.csv' INTO TABLE decimal_varlength" +
        s" OPTIONS('FILEHEADER'='id,value')")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/big_decimal_without_header.csv' INTO TABLE decimal_varlength_hive")
    checkAnswer(sql("select value from decimal_varlength"), sql("select value from decimal_varlength_hive"))
    checkAnswer(sql("select sum(value) from decimal_varlength"), sql("select sum(value) from decimal_varlength_hive"))
  }

  override def afterAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql("drop table if exists escapechar1")
    sql("drop table if exists escapechar2")
    sql("drop table if exists escapechar3")
    sql("drop table if exists specialcharacter1")
    sql("drop table if exists specialcharacter2")
    sql("drop table if exists collessthanschema")
    sql("drop table if exists decimalarray")
    sql("drop table if exists decimalstruct")
    sql("drop table if exists carbontable")
    sql("drop table if exists hivetable")
    sql("drop table if exists testtable")
    sql("drop table if exists testhivetable")
    sql("drop table if exists testtable1")
    sql("drop table if exists testhivetable1")
    sql("drop table if exists complexcarbontable")
    sql("drop table if exists complex_t3")
    sql("drop table if exists complex_hive_t3")
    sql("drop table if exists header_test")
    sql("drop table if exists duplicateColTest")
    sql("drop table if exists mixed_header_test")
    sql("drop table if exists primitivecarbontable")
    sql("drop table if exists UPPERCASEcube")
    sql("drop table if exists lowercaseCUBE")
    sql("drop table if exists carbontable1")
    sql("drop table if exists hivetable1")
    sql("drop table if exists comment_test")
    sql("drop table if exists decimal_varlength")
    sql("drop table if exists decimal_varlength_hive")
  }
}

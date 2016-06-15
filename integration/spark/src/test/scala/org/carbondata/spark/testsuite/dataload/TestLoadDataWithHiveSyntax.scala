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

package org.carbondata.spark.testsuite.dataload

import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading with hive syntax and old syntax
 *
 */
class TestLoadDataWithHiveSyntax extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE CUBE carboncube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table hivetable(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance String,utilization String,salary String)row format delimited fields terminated by ','")
  }

  test("test data loading and validate query output") {
    //Create test cube and hive table
    sql("CREATE CUBE testcube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table testhivetable(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance double,utilization double,salary double)row format delimited fields terminated by ','")
    //load data into test cube and hive table and validate query result
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table testcube")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table testhivetable")
    checkAnswer(sql("select * from testcube"), sql("select * from testhivetable"))
    //load data incrementally and validate query result
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE testcube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testhivetable")
    checkAnswer(sql("select * from testcube"), sql("select * from testhivetable"))
    //drop test cube and table
    sql("drop cube testcube")
    sql("drop table testhivetable")
  }

  /**
   * TODO: temporarily changing cube names to different names,
    * however deletion and creation of cube with same name
   */
  test("test data loading with different case file header and validate query output") {
    //Create test cube and hive table
    sql("CREATE CUBE testcube1 DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table testhivetable1(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance double,utilization double,salary double)row format delimited fields terminated by ','")
    //load data into test cube and hive table and validate query result
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testcube1 options('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='EMPno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,SALARY')")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table testhivetable1")
    checkAnswer(sql("select * from testcube1"), sql("select * from testhivetable1"))
    //drop test cube and table
    sql("drop cube testcube1")
    sql("drop table testhivetable1")
  }
  
  test("test hive table data loading") {
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table hivetable")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table hivetable")
  }

  test("test carbon table data loading using old syntax") {
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE carboncube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
  }
  
  test("test carbon table data loading using new syntax compatible with hive") {
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table carboncube")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table carboncube options('DELIMITER'=',', 'QUOTECHAR'='\"')")
  }
  
  test("test carbon table data loading using new syntax with overwrite option compatible with hive") {
    try {
      sql("LOAD DATA local inpath './src/test/resources/data.csv' overwrite INTO table carboncube")
    } catch {
      case e : Throwable => {
        assert(e.getMessage.equals("Overwrite is not supported for carbon table with default.carboncube"))
      }
    }
  }
  
  test("complex types data loading") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string,"+ 
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>,"+
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,"+
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double) "+
        "STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')")
    sql("LOAD DATA local inpath './src/test/resources/complexdata.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists complexcarbontable")
  }
  
  test("complex types data loading with more unused columns and different order of complex columns in csv and create table") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string,"+ 
        "mobile struct<imei:string, imsi:string>, ROMSize string, purchasedate string,"+
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,"+
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double) "+
        "STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId','DICTIONARY_EXCLUDE'='channelsId')")
    sql("LOAD DATA local inpath './src/test/resources/complextypediffentcolheaderorder.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,purchasedate,MAC,abc,mobile,locationinfo,proddate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("select count(*) from complexcarbontable")
    sql("drop table complexcarbontable")
  }

  test("test carbon table data loading with csv file Header in caps") {
    sql("drop table if exists header_test")
    sql("create table header_test(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance String,utilization String,salary String) STORED BY 'org.apache.carbondata.format'")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath = currentDirectory + "/src/test/resources/data_withCAPSHeader.csv"
    sql("LOAD DATA local inpath '"+ csvFilePath +"' INTO table header_test OPTIONS ('DELIMITER'=',', 'QUOTECHAR'='\"')");
    checkAnswer(sql("select empno from header_test"),
      Seq(Row(11), Row(12)))
  }

  test("test duplicate column validation"){
    try{
        sql("create table duplicateColTest(col1 string, Col1 string)")
    }
    catch {
      case e : Exception => {
        assert(e.getMessage.contains("Duplicate column name"))
      }
    }
  }

  test("test carbon table data loading with csv file Header in Mixed Case and create table columns in mixed case") {
    sql("drop table if exists mixed_header_test")
    sql("create table mixed_header_test(empno int, empname String, Designation string, doj String, Workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance String,utilization String,salary String) STORED BY 'org.apache.carbondata.format'")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath = currentDirectory + "/src/test/resources/data_withMixedHeader.csv"
    sql("LOAD DATA local inpath '"+ csvFilePath +"' INTO table mixed_header_test OPTIONS ('DELIMITER'=',', 'QUOTECHAR'='\"')");
    checkAnswer(sql("select empno from mixed_header_test"),
      Seq(Row(11), Row(12)))
  }


  test("complex types data loading with hive column having more than required column values") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string,"+ 
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>,"+
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,"+
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double) "+
        "STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId')")
    sql("LOAD DATA local inpath './src/test/resources/complexdatastructextra.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")
    sql("drop table if exists complexcarbontable")
  }
  
  test("complex types & no dictionary columns data loading") {
    sql("create table complexcarbontable(deviceInformationId int, channelsId string,"+ 
        "ROMSize string, purchasedate string, mobile struct<imei:string, imsi:string>,"+
        "MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>,"+
        "proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double) "+
        "STORED BY 'org.apache.carbondata.format' "+
        "TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId', 'DICTIONARY_EXCLUDE'='ROMSize,purchasedate')")
    sql("LOAD DATA local inpath './src/test/resources/complexdata.csv' INTO table complexcarbontable "+
        "OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber',"+
        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
    sql("drop table if exists complexcarbontable")
  }

  test("test carbon table data loading when table name is in different case with create table, for UpperCase") {
    sql("create table UPPERCASEcube(empno Int, empname String, designation String, " +
      "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
      "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
      "attendance Int,utilization Double,salary Double) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table uppercasecube OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"')")
    sql("drop table UpperCaseCube")
  }

  test("test carbon table data loading when table name is in different case with create table ,for LowerCase") {
    sql("create table lowercaseCUBE(empno Int, empname String, designation String, " +
      "doj String, workgroupcategory Int, workgroupcategoryname String, deptno Int, " +
      "deptname String, projectcode Int, projectjoindate String, projectenddate String, " +
      "attendance Int,utilization Double,salary Double) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table LOWERCASECUBE OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"')")
    sql("drop table LowErcasEcube")
  }
  
  test("test carbon table data loading using escape char") {
    sql("DROP TABLE IF EXISTS t3")

    sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           """)

    sql(s"""
           LOAD DATA LOCAL INPATH './src/test/resources/datawithbackslash.csv' into table t3
           OPTIONS('ESCAPECHAR'='@')
        """)
    checkAnswer(sql("select count(*) from t3"), Seq(Row(10)))
    sql("DROP TABLE IF EXISTS t3")
  }
  
  override def afterAll {
    sql("drop cube carboncube")
    sql("drop table hivetable")
    sql("drop table if exists header_test")
    sql("drop table if exists mixed_header_test")

  }
}
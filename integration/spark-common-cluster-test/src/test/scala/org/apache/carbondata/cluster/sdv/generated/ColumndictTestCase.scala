
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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for columndictTestCase to verify all scenerios
 */

class ColumndictTestCase extends QueryTest with BeforeAndAfterAll {
         

  //Load history data from CSV with/without header and specify/don't specify headers in command using external ALL_dictionary_PATH
  test("Columndict-TC001", Include) {
     sql(s"""drop table if exists t3""").collect
   sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load history data from CSV with/without header and specify/don't specify headers in command using external columndict
  test("Columndict-TC002", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external All_dictionary_path for CSV having incomplete/wrong data/no data/null data
  test("Columndict-TC003", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/inValidData.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external columndict for CSV having incomplete/wrong data/no data/null data
  test("Columndict-TC004", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/inValidData.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Load multiple CSV from folder into table , Multiple level of folders using external all_dictionary_path
  test("Columndict-TC005", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data1' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load multiple CSV from folder into table , Multiple level of folders using external columndict
  ignore("Columndict-TC006", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data1' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using CSV file with different extension (.dat, .xls, .doc,.txt) and without extension from external dictionary
  ignore("Columndict-TC007", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.dat' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using CSV file with different extension (.dat, .xls, .doc,.txt) and without extension from external dictionary
  ignore("Columndict-TC008", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.dat' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using MAXCOLUMNS during loading with external all_dictionary_path
  test("Columndict-TC009", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.dat' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary','maxcolumns'='8', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using MAXCOLUMNS during loading with external columndict
  ignore("Columndict-TC010", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.dat' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv','maxcolumns'='8', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Bad records logging after load using external all_dictionary_path
  ignore("Columndict-TC011", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Bad records logging after load using external columndict
  ignore("Columndict-TC012", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'=
  'country:$resourcesPath/Data/columndict/country.csv','BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Incremental Load using external dictionary
  test("Columndict-TC013", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select * from t3 where ID>=5""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //Incremental Load using external dictionary
  ignore("Columndict-TC014", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select * from t3 where ID>=5""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //Load using external dictionary for table without table properties
  ignore("Columndict-TC015", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external dictionary for table without table properties
  ignore("Columndict-TC016", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external all_dictionary_path for table with table properties(DICTIONARY_EXCLUDE, DICTIONARY_INCLUDE, BLOCKSIZE)
  ignore("Columndict-TC017", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES ('TABLE_BLOCKSIZE'= '256 MB','DICTIONARY_INCLUDE'='salary','DICTIONARY_EXCLUDE'='phonetype')""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external columndict for table with table properties(DICTIONARY_EXCLUDE, DICTIONARY_INCLUDE, BLOCKSIZE)
  test("Columndict-TC018", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES ('TABLE_BLOCKSIZE'= '256 MB','DICTIONARY_INCLUDE'='salary','DICTIONARY_EXCLUDE'='phonetype')""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external all_dictionary_path for measure and table properties(DICTIONARY_EXCLUDE, DICTIONARY_INCLUDE, BLOCKSIZE)
  ignore("Columndict-TC019", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES ('TABLE_BLOCKSIZE'= '256 MB','DICTIONARY_INCLUDE'='salary','DICTIONARY_EXCLUDE'='country')""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='salary:$resourcesPath/Data/columndict/salary.csv', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Load using external columndict for table with measure and tableproperties(DICTIONARY_EXCLUDE, DICTIONARY_INCLUDE, BLOCKSIZE)
  test("Columndict-TC020", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES ('TABLE_BLOCKSIZE'= '256 MB','DICTIONARY_EXCLUDE'='country')""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:'resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Columndict parameter name validation
  ignore("Columndict-TC021", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata' TBLPROPERTIES ('TABLE_BLOCKSIZE'= '256 MB','DICTIONARY_EXCLUDE'='country')""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='countries:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Columndict parameter value validation
  test("Columndict-TC022", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='salary:$resourcesPath/Data/columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for data validation in csv(empty/null/wrong data) for all_dictionary_path
  ignore("Columndict-TC023", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/inValidData.dictionary', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for data validation in csv(empty/null/wrong data) for columndict
  test("Columndict-TC024", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('COLUMNDICT'='country:'resourcesPath/Data/columndict/inValidData.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for validation of external all_dictionary_path folder with incorrect path
  test("Columndict-TC025", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('ALL_DICTIONARY_PATH'=''resourcesPath/Data/*.dictionary', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for validation of external all_dictionary_path folder with correct path
  test("Columndict-TC026", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/*.dictionary', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for validation of external columndict folder with correct path
  test("Columndict-TC027", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('COLUMNDICT'='country:'resourcesPath/Data/columndict/*.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for validation of external all_dictionary_path file( missing /wrong path / wrong name)
  test("Columndict-TC028", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('ALL_DICTIONARY_PATH'=''resourcesPath/Data/columndict/wrongName.dictionary', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for validation of external columndict file( missing /wrong path / wrong name)
  test("Columndict-TC029", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('COLUMNDICT'='country:'resourcesPath/Data/columndict/wrongName.csv', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //Check for different dictionary file extensions for all_dictionary_path
  test("Columndict-TC030", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.txt', 'SINGLE_PASS'='true')""").collect
     sql(s"""drop table if exists t3""").collect
  }


  //Check for different dictionary file extensions for columndict
  test("Columndict-TC031", Include) {
    intercept[Exception] {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
      sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/inValidData.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data/columndict/country.txt', 'SINGLE_PASS'='true')""").collect
    }
     sql(s"""drop table if exists t3""").collect
  }


  //To check limit for all_dictionary_path
  test("Columndict-TC032", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 limit 100""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check count for all_dictionary_path
  ignore("Columndict-TC033", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select count(*) from t3""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check sum for all_dictionary_path
  ignore("Columndict-TC034", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select sum(salary) from t3""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check >= for all_dictionary_path
  test("Columndict-TC035", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where ID >=5""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check != for all_dictionary_path
  ignore("Columndict-TC036", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where ID != 9""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check between for all_dictionary_path
  ignore("Columndict-TC037", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id between 2 and 9""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check like for all_dictionary_path
  ignore("Columndict-TC038", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id Like '9%'""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check group by for all_dictionary_path
  ignore("Columndict-TC039", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id > 3 group by id,name having id = 2""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check sort by for all_dictionary_path
  ignore("Columndict-TC040", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('ALL_DICTIONARY_PATH'='$resourcesPath/Data/columndict/data.dictionary', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id > 4 sort by name desc""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check limit for columndict
  ignore("Columndict-TC041", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 limit 100""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check count for columndict
  ignore("Columndict-TC042", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select count(*) from t3""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check sum for columndict
  ignore("Columndict-TC043", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select sum(salary) from t3""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check >= for columndict
  ignore("Columndict-TC044", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where ID >=5""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check != for columndict
  ignore("Columndict-TC045", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where ID != 9""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check between for columndict
  ignore("Columndict-TC046", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id between 2 and 9""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check like for columndict
  ignore("Columndict-TC047", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id Like '9%'""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check group by for columndict
  ignore("Columndict-TC048", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id > 3 group by id,name having id = 2""").collect

     sql(s"""drop table if exists t3""").collect
  }


  //To check sort by for columndict
  ignore("Columndict-TC049", Include) {
     sql(s"""CREATE TABLE IF NOT EXISTS t3 (ID Int, country String, name String, phonetype String, serialname String, salary Int,floatField float) STORED BY 'carbondata'""").collect
   sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/Data/columndict/data.csv' into table t3 options('COLUMNDICT'='country:$resourcesPath/Data//columndict/country.csv', 'SINGLE_PASS'='true')""").collect
    sql(s"""select ID,name from t3 where id > 4 sort by name desc""").collect

     sql(s"""drop table if exists t3""").collect
  }

}
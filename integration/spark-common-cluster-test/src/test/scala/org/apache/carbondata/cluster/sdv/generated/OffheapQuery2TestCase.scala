
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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for OffheapQuery2TestCase to verify all scenerios
 */

class OffheapQuery2TestCase extends QueryTest with BeforeAndAfterAll {
         

  //To check select query with limit
  test("OffHeapQuery-002-TC_120", Include) {
     sql(s"""CREATE TABLE uniqdataquery2 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect

    sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdataquery2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select * from uniqdataquery2 limit 100""").collect


  }


  //To check select query with limit as string
  test("OffHeapQuery-002-TC_121", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 limit """"").collect
    }
  }


  //To check select query with no input given at limit
  test("OffHeapQuery-002-TC_122", Include) {

    sql(s"""select * from uniqdataquery2 limit""").collect


  }


  //To check select count  query  with where and group by clause
  test("OffHeapQuery-002-TC_123", Include) {

    sql(s"""select count(*) from uniqdataquery2 where cust_name="CUST_NAME_00000" group by cust_name""").collect


  }


  //To check select count  query   and group by  cust_name using like operator
  test("OffHeapQuery-002-TC_124", Include) {

    sql(s"""select count(*) from uniqdataquery2 where cust_name like "cust_name_0%" group by cust_name""").collect


  }


  //To check select count  query   and group by  name using IN operator with empty values
  test("OffHeapQuery-002-TC_125", Include) {

    sql(s"""select count(*) from uniqdataquery2 where cust_name IN("","") group by cust_name""").collect


  }


  //To check select count  query   and group by  name using IN operator with specific  values
  test("OffHeapQuery-002-TC_126", Include) {

    sql(s"""select count(*) from uniqdataquery2 where cust_name IN(1,2,3) group by cust_name""").collect


  }


  //To check select distinct query
  test("OffHeapQuery-002-TC_127", Include) {

    sql(s"""select distinct cust_name from uniqdataquery2 group by cust_name""").collect


  }


  //To check where clause with OR and no operand
  test("OffHeapQuery-002-TC_128", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id > 1 OR """).collect
    }
  }


  //To check OR clause with LHS and RHS having no arguments
  test("OffHeapQuery-002-TC_129", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where OR """).collect
    }
  }


  //To check OR clause with LHS having no arguments
  test("OffHeapQuery-002-TC_130", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where OR cust_id > "1"""").collect
    }
  }


  //To check incorrect query
  test("OffHeapQuery-002-TC_132", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id > 0 OR name  """).collect
    }
  }


  //To check select query with rhs false
  test("OffHeapQuery-002-TC_133", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id > 9005 OR false""").collect


  }


  //To check count on multiple arguments
  test("OffHeapQuery-002-TC_134", Include) {

    sql(s"""select count(cust_id,cust_name) from uniqdataquery2 where cust_id > 10544""").collect


  }


  //To check count with no argument
  test("OffHeapQuery-002-TC_135", Include) {

    sql(s"""select count() from uniqdataquery2 where cust_id > 10544""").collect


  }


  //To check count with * as an argument
  test("OffHeapQuery-002-TC_136", Include) {

    sql(s"""select count(*) from uniqdataquery2 where cust_id>10544""").collect


  }


  //To check select count query execution with entire column
  test("OffHeapQuery-002-TC_137", Include) {

    sql(s"""select count(*) from uniqdataquery2""").collect


  }


  //To check select distinct query execution
  test("OffHeapQuery-002-TC_138", Include) {

    sql(s"""select distinct * from uniqdataquery2""").collect


  }


  //To check select multiple column query execution
  test("OffHeapQuery-002-TC_139", Include) {

    sql(s"""select cust_name,cust_id,count(cust_name) from uniqdataquery2 group by cust_name,cust_id""").collect


  }


  //To check select count and distinct query execution
  test("OffHeapQuery-002-TC_140", Include) {
    intercept[Exception] {
      sql(s"""select count(cust_id),distinct(cust_name) from uniqdataquery2""").collect
    }
  }


  //To check sum query execution
  test("OffHeapQuery-002-TC_141", Include) {

    sql(s"""select sum(cust_id) as sum,cust_name from uniqdataquery2 group by cust_name""").collect


  }


  //To check sum of names query execution
  test("OffHeapQuery-002-TC_142", Include) {

    sql(s"""select sum(cust_name) from uniqdataquery2""").collect


  }


  //To check select distinct and groupby query execution
  test("OffHeapQuery-002-TC_143", Include) {

    sql(s"""select distinct(cust_name,cust_id) from uniqdataquery2 group by cust_name,cust_id""").collect


  }


  //To check select with where clause on cust_name query execution
  test("OffHeapQuery-002-TC_144", Include) {

    sql(s"""select cust_id from uniqdataquery2 where cust_name="cust_name_00000"""").collect


  }


  //To check query execution with IN operator without paranthesis
  test("OffHeapQuery-002-TC_146", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id IN 9000,9005""").collect
    }
  }


  //To check query execution with IN operator with paranthesis
  test("OffHeapQuery-002-TC_147", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id IN (9000,9005)""").collect


  }


  //To check query execution with IN operator with out specifying any field.
  test("OffHeapQuery-002-TC_148", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where IN(1,2)""").collect
    }
  }


  //To check OR with correct syntax
  test("OffHeapQuery-002-TC_149", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id>9005 or cust_id=9005""").collect


  }


  //To check OR with boolean expression
  test("OffHeapQuery-002-TC_150", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id>9005 or false""").collect


  }


  //To check AND with correct syntax
  test("OffHeapQuery-002-TC_151", Include) {

    sql(s"""select * from uniqdataquery2 where true AND true""").collect


  }


  //To check AND with using booleans
  test("OffHeapQuery-002-TC_152", Include) {

    sql(s"""select * from uniqdataquery2 where true AND false""").collect


  }


  //To check AND with using booleans in invalid syntax
  test("OffHeapQuery-002-TC_153", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where AND true""").collect
    }
  }


  //To check AND Passing two conditions on same input
  test("OffHeapQuery-002-TC_154", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id=6 and cust_id>5""").collect


  }


  //To check AND changing case
  test("OffHeapQuery-002-TC_155", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id=6 aND cust_id>5""").collect


  }


  //To check AND using 0 and 1 treated as boolean values
  test("OffHeapQuery-002-TC_156", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where true aNd 0""").collect
    }
  }


  //To check AND on two columns
  test("OffHeapQuery-002-TC_157", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id=9000 and cust_name='cust_name_00000'""").collect


  }


  //To check '='operator with correct syntax
  test("OffHeapQuery-002-TC_158", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id=9000 and cust_name='cust_name_00000' and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect


  }


  //To check '='operator without Passing any value
  test("OffHeapQuery-002-TC_159", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id=""").collect
    }
  }


  //To check '='operator without Passing columnname and value.
  test("OffHeapQuery-002-TC_160", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where =""").collect
    }
  }


  //To check '!='operator with correct syntax
  test("OffHeapQuery-002-TC_161", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id!=9000""").collect


  }


  //To check '!='operator by keeping space between them
  test("OffHeapQuery-002-TC_162", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id !   = 9001""").collect
    }
  }


  //To check '!='operator by Passing boolean value whereas column expects an integer
  test("OffHeapQuery-002-TC_163", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id != true""").collect


  }


  //To check '!='operator without providing any value
  test("OffHeapQuery-002-TC_164", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id != """).collect
    }
  }


  //To check '!='operator without providing any column name
  test("OffHeapQuery-002-TC_165", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where  != false""").collect
    }
  }


  //To check 'NOT' with valid syntax
  test("OffHeapQuery-002-TC_166", Include) {

    sql(s"""select * from uniqdataquery2 where NOT(cust_id=9000)""").collect


  }


  //To check 'NOT' using boolean values
  test("OffHeapQuery-002-TC_167", Include) {

    sql(s"""select * from uniqdataquery2 where NOT(false)""").collect


  }


  //To check 'NOT' applying it on a value
  test("OffHeapQuery-002-TC_168", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id = 'NOT(false)'""").collect


  }


  //To check 'NOT' with between operator
  test("OffHeapQuery-002-TC_169", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id NOT BETWEEN 9000 and 9005""").collect


  }


  //To check 'NOT' operator in nested way
  test("OffHeapQuery-002-TC_170", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id NOT (NOT(true))""").collect
    }
  }


  //To check 'NOT' operator with parenthesis.
  test("OffHeapQuery-002-TC_171", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id NOT ()""").collect
    }
  }


  //To check 'NOT' operator without condition.
  test("OffHeapQuery-002-TC_172", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id NOT""").collect
    }
  }


  //To check 'NOT' operator checking case sensitivity.
  test("OffHeapQuery-002-TC_173", Include) {

    sql(s"""select * from uniqdataquery2 where nOt(false)""").collect


  }


  //To check '>' operator without specifying column
  test("OffHeapQuery-002-TC_174", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where > 20""").collect
    }
  }


  //To check '>' operator without specifying value
  test("OffHeapQuery-002-TC_175", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id > """).collect
    }
  }


  //To check '>' operator with correct syntax
  test("OffHeapQuery-002-TC_176", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id >9005""").collect


  }


  //To check '>' operator for Integer value
  test("OffHeapQuery-002-TC_177", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id > 9010""").collect


  }


  //To check '>' operator for String value
  test("OffHeapQuery-002-TC_178", Include) {

    sql(s"""select * from uniqdataquery2 where cust_name > 'cust_name_00000'""").collect


  }


  //To check '<' operator without specifying column
  test("OffHeapQuery-002-TC_179", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where < 5""").collect
    }
  }


  //To check '<' operator with correct syntax
  test("OffHeapQuery-002-TC_180", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id < 9005""").collect


  }


  //To check '<' operator for String value
  test("OffHeapQuery-002-TC_181", Include) {

    sql(s"""select * from uniqdataquery2 where cust_name < "cust_name_00001"""").collect


  }


  //To check '<=' operator without specifying column
  test("OffHeapQuery-002-TC_182", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where  <= 2""").collect
    }
  }


  //To check '<=' operator without providing value
  test("OffHeapQuery-002-TC_183", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where  cust_id <= """).collect
    }
  }


  //To check '<=' operator with correct syntax
  test("OffHeapQuery-002-TC_184", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id <=9002""").collect


  }


  //To check '<=' operator adding space between'<' and  '='
  test("OffHeapQuery-002-TC_185", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id < =  9002""").collect
    }

  }


  //To check 'BETWEEN' operator without providing range
  test("OffHeapQuery-002-TC_186", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where age between""").collect
    }
  }


  //To check  'BETWEEN' operator with correct syntax
  test("OffHeapQuery-002-TC_187", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id between 9002 and 9030""").collect


  }


  //To check  'BETWEEN' operator providing two same values
  test("OffHeapQuery-002-TC_188", Include) {

    sql(s"""select * from uniqdataquery2 where cust_name beTWeen 'CU%' and 'CU%'""").collect


  }


  //To check  'NOT BETWEEN' operator for integer
  test("OffHeapQuery-002-TC_189", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id NOT between 9024 and 9030""").collect


  }


  //To check  'NOT BETWEEN' operator for string
  test("OffHeapQuery-002-TC_190", Include) {

    sql(s"""select * from uniqdataquery2 where cust_name NOT beTWeen 'cust_name_00000' and 'cust_name_00001'""").collect


  }


  //To check  'IS NULL' for case sensitiveness.
  test("OffHeapQuery-002-TC_191", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id Is NulL""").collect


  }


  //To check  'IS NULL' for null field
  test("OffHeapQuery-002-TC_192", Include) {

    sql(s"""select * from uniqdataquery2 where cust_name Is NulL""").collect


  }


  //To check  'IS NULL' without providing column
  test("OffHeapQuery-002-TC_193", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where Is NulL""").collect
    }
  }


  //To check  'IS NOT NULL' without providing column
  test("OffHeapQuery-002-TC_194", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where IS NOT NULL""").collect
    }
  }


  //To check ''IS NOT NULL' operator with correct syntax
  test("OffHeapQuery-002-TC_195", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id IS NOT NULL""").collect


  }


  //To check  'Like' operator for integer
  test("OffHeapQuery-002-TC_196", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id Like '9%'""").collect


  }


  //To check Limit clause with where condition
  test("OffHeapQuery-002-TC_197", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id>10987 limit 15""").collect


  }


  //To check Limit clause with where condition and no argument
  test("OffHeapQuery-002-TC_198", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id=10987 limit""").collect
    }
  }


  //To check Limit clause with where condition and decimal argument
  test("OffHeapQuery-002-TC_199", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id=10987 limit 0.0""").collect
    }
  }


  //To check where clause with distinct and group by
  test("OffHeapQuery-002-TC_200", Include) {

    sql(s"""select distinct cust_name from uniqdataquery2 where cust_name IN("CUST_NAME_01999") group by cust_name""").collect


  }


  //To check subqueries
  test("OffHeapQuery-002-TC_201", Include) {

    sql(s"""select * from (select cust_id from uniqdataquery2 where cust_id IN (10987,10988)) uniqdataquery2 where cust_id IN (10987, 10988)""").collect


  }


  //To count with where clause
  test("OffHeapQuery-002-TC_202", Include) {

    sql(s"""select count(cust_id) from uniqdataquery2 where cust_id > 10874""").collect


  }


  //To check Join query
  test("OffHeapQuery-002-TC_203", Include) {
     sql(s"""CREATE TABLE uniqdataquery22 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdataquery22 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    sql(s"""select uniqdataquery2.CUST_ID from uniqdataquery2 join uniqdataquery22 where uniqdataquery2.CUST_ID > 10700 and uniqdataquery22.CUST_ID > 10500""").collect


  }


  //To check Left join with where clause
  test("OffHeapQuery-002-TC_204", Include) {

    sql(s"""select uniqdataquery2.CUST_ID from uniqdataquery2 LEFT join uniqdataquery22 where uniqdataquery2.CUST_ID > 10000""").collect


  }


  //To check Full join
  test("OffHeapQuery-002-TC_205", Include) {
    intercept[Exception] {
      sql(s"""select uniqdataquery2.CUST_ID from uniqdataquery2 FULL JOIN uniqdataquery22 where CUST_ID""").collect
    }
  }


  //To check Broadcast join
  test("OffHeapQuery-002-TC_206", Include) {

    sql(s"""select broadcast.cust_id from uniqdataquery2 broadcast join uniqdataquery22 where broadcast.cust_id > 10900""").collect

     sql(s"""drop table uniqdataquery22""").collect
  }


  //To avg function
  test("OffHeapQuery-002-TC_207", Include) {

    sql(s"""select avg(cust_name) from uniqdataquery2 where cust_id > 10544 group by cust_name""").collect


  }


  //To check subquery with aggrgate function avg
  test("OffHeapQuery-002-TC_208", Include) {

    sql(s"""select cust_id,avg(cust_id) from uniqdataquery2 where cust_id IN (select cust_id from uniqdataquery2 where cust_id > 0) group by cust_id""").collect


  }


  //To check HAVING on Measure
  test("OffHeapQuery-002-TC_209", Include) {

    sql(s"""select cust_id from uniqdataquery2 where cust_id > 10543 group by cust_id having cust_id = 10546""").collect


  }


  //To check HAVING on dimension
  test("OffHeapQuery-002-TC_210", Include) {

    sql(s"""select cust_name from uniqdataquery2 where cust_id > 10544 group by cust_name having cust_name like 'C%'""").collect


  }


  //To check HAVING on multiple columns
  test("OffHeapQuery-002-TC_211", Include) {

    sql(s"""select cust_id,cust_name from uniqdataquery2 where cust_id > 10544 group by cust_id,cust_name having cust_id = 10545 AND cust_name like 'C%'""").collect


  }


  //To check HAVING with empty condition
  test("OffHeapQuery-002-TC_213", Include) {

    sql(s"""select cust_name from uniqdataquery2 where cust_id > 10544 group by cust_name having """"").collect


  }


  //To check SORT on measure
  test("OffHeapQuery-002-TC_214", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id > 10544 sort by cust_id asc""").collect


  }


  //To check SORT on dimemsion
  test("OffHeapQuery-002-TC_215", Include) {

    sql(s"""select * from uniqdataquery2 where cust_id > 10544 sort by cust_name desc""").collect


  }


  //To check SORT using 'AND' on multiple column
  test("OffHeapQuery-002-TC_216", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 where cust_id > 10544 sort by cust_name desc and cust_id asc""").collect
    }
  }


  //To check Select average names and group by name query execution
  test("OffHeapQuery-002-TC_217", Include) {

    sql(s"""select avg(cust_name) from uniqdataquery2 group by cust_name""").collect


  }


  //To check Select average id and group by id query execution
  test("OffHeapQuery-002-TC_218", Include) {

    sql(s"""select avg(cust_id) from uniqdataquery2 group by cust_id""").collect


  }


  //To check average aggregate function with no arguments
  test("OffHeapQuery-002-TC_219", Include) {
    intercept[Exception] {
      sql(s"""select cust_id,avg() from uniqdataquery2 group by cust_id""").collect
    }
  }


  //To check average aggregate function with empty string
  test("OffHeapQuery-002-TC_220", Include) {

    sql(s"""select cust_id,avg("") from uniqdataquery2 group by cust_id""").collect


  }


  //To check nested  average aggregate function
  test("OffHeapQuery-002-TC_221", Include) {
    intercept[Exception] {
      sql(s"""select cust_id,avg(count(cust_id)) from uniqdataquery2 group by cust_id""").collect
    }
  }


  //To check Multilevel query
  test("OffHeapQuery-002-TC_222", Include) {

    sql(s"""select cust_id,avg(cust_id) from uniqdataquery2 where cust_id IN (select cust_id from uniqdataquery2) group by cust_id""").collect


  }


  //To check Using first() with group by clause
  test("OffHeapQuery-002-TC_223", Include) {

    sql(s"""select first(cust_id) from uniqdataquery2 group by cust_id""").collect


  }


  //To check max with groupby clause query execution
  test("OffHeapQuery-002-TC_224", Include) {

    sql(s"""select max(cust_name) from uniqdataquery2 group by(cust_name)""").collect


  }


  //To check max with groupby clause query with id execution
  test("OffHeapQuery-002-TC_225", Include) {

    sql(s"""select max(cust_name) from uniqdataquery2 group by(cust_name),cust_id""").collect


  }


  //To check  multiple aggregate functions
  test("OffHeapQuery-002-TC_226", Include) {

    sql(s"""select max(cust_name),sum(cust_name),count(cust_id) from uniqdataquery2 group by(cust_name),cust_id""").collect


  }


  //To check max with empty string as argument
  test("OffHeapQuery-002-TC_227", Include) {

    sql(s"""select max("") from uniqdataquery2 group by(cust_name)""").collect


  }


  //To check  select count of names with group by clause
  test("OffHeapQuery-002-TC_228", Include) {

    sql(s"""select count(cust_name) from uniqdataquery2 group by cust_name""").collect


  }


  //To check Order by ASC
  test("OffHeapQuery-002-TC_229", Include) {

    sql(s"""select * from uniqdataquery2 order by cust_id ASC""").collect


  }


  //To check Order by DESC
  test("OffHeapQuery-002-TC_230", Include) {

    sql(s"""select * from uniqdataquery2 order by cust_id DESC""").collect


  }


  //To check Order by without column name
  test("OffHeapQuery-002-TC_231", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery2 order by ASC""").collect
    }
  }


  //To check cast Int to String
  test("OffHeapQuery-002-TC_232", Include) {

    sql(s"""select cast(bigint_column1 as STRING) from uniqdataquery2""").collect


  }


  //To check cast string to int
  test("OffHeapQuery-002-TC_233", Include) {

    sql(s"""select cast(cust_name as INT) from uniqdataquery2""").collect


  }


  //To check cast int to decimal
  test("OffHeapQuery-002-TC_234", Include) {

    sql(s"""select cast(bigint_column1 as DECIMAL(10,4)) from uniqdataquery2""").collect


  }


  //To check Using window with order by
  test("OffHeapQuery-002-TC_235", Include) {

    sql(s"""select cust_name, sum(bigint_column1) OVER w from uniqdataquery2 WINDOW w AS (PARTITION BY bigint_column2 ORDER BY cust_id)""").collect


  }


  //To check Using window without partition
  test("OffHeapQuery-002-TC_236", Include) {
    intercept[Exception] {
      sql(s"""select cust_name, sum(bigint_column1) OVER w from uniqdataquery2 WINDOW w""").collect
    }
  }


  //To check Using ROLLUP with group by
  test("OffHeapQuery-002-TC_237", Include) {

    sql(s"""select cust_name from uniqdataquery2 group by cust_name with ROLLUP""").collect


  }


  //To check Using ROLLUP without group by clause
  test("OffHeapQuery-002-TC_238", Include) {
    intercept[Exception] {
      sql(s"""select cust_name from uniqdataquery2 with ROLLUP""").collect
    }
     sql(s"""drop table uniqdataquery2""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("enable.unsafe.in.query.processing", CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("enable.unsafe.in.query.processing", "false")
    prop.addProperty("use.offheap.in.query.processing", "true")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("enable.unsafe.in.query.processing", p1)
  }

}
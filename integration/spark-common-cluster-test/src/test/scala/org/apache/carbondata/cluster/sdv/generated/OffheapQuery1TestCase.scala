
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
 * Test Class for OffheapQuery1TestCase to verify all scenerios
 */

class OffheapQuery1TestCase extends QueryTest with BeforeAndAfterAll {
         

//To check select query with limit
test("OffHeapQuery-001-TC_001", Include) {
   sql(s"""CREATE TABLE uniqdataquery1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdataquery1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""select * from uniqdataquery1 limit 100""").collect
  
  
}
       

//To check select query with limit as string
test("OffHeapQuery-001-TC_002", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 limit """"").collect
  }
}
       

//To check select query with no input given at limit
test("OffHeapQuery-001-TC_003", Include) {
  
  sql(s"""select * from uniqdataquery1 limit""").collect
  
  
}
       

//To check select count  query  with where and group by clause
test("OffHeapQuery-001-TC_004", Include) {
  
  sql(s"""select count(*) from uniqdataquery1 where cust_name="CUST_NAME_00000" group by cust_name""").collect
  
  
}
       

//To check select count  query   and group by  cust_name using like operator
test("OffHeapQuery-001-TC_005", Include) {
  
  sql(s"""select count(*) from uniqdataquery1 where cust_name like "cust_name_0%" group by cust_name""").collect
  
  
}
       

//To check select count  query   and group by  name using IN operator with empty values
test("OffHeapQuery-001-TC_006", Include) {
  
  sql(s"""select count(*) from uniqdataquery1 where cust_name IN("","") group by cust_name""").collect
  
  
}
       

//To check select count  query   and group by  name using IN operator with specific  values
test("OffHeapQuery-001-TC_007", Include) {
  
  sql(s"""select count(*) from uniqdataquery1 where cust_name IN(1,2,3) group by cust_name""").collect
  
  
}
       

//To check select distinct query 
test("OffHeapQuery-001-TC_008", Include) {
  
  sql(s"""select distinct cust_name from uniqdataquery1 group by cust_name""").collect
  
  
}
       

//To check where clause with OR and no operand
test("OffHeapQuery-001-TC_009", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id > 1 OR """).collect
  }
}
       

//To check OR clause with LHS and RHS having no arguments
test("OffHeapQuery-001-TC_010", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where OR """).collect
  }
}
       

//To check OR clause with LHS having no arguments
test("OffHeapQuery-001-TC_011", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where OR cust_id > "1"""").collect
  }
}
       

//To check incorrect query 
test("OffHeapQuery-001-TC_013", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id > 0 OR name  """).collect
  }
}
       

//To check select query with rhs false
test("OffHeapQuery-001-TC_014", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id > 9005 OR false""").collect
  
  
}
       

//To check count on multiple arguments 
test("OffHeapQuery-001-TC_015", Include) {
  
  sql(s"""select count(cust_id,cust_name) from uniqdataquery1 where cust_id > 10544""").collect
  
  
}
       

//To check count with no argument 
test("OffHeapQuery-001-TC_016", Include) {
  
  sql(s"""select count() from uniqdataquery1 where cust_id > 10544""").collect
  
  
}
       

//To check count with * as an argument 
test("OffHeapQuery-001-TC_017", Include) {
  
  sql(s"""select count(*) from uniqdataquery1 where cust_id>10544""").collect
  
  
}
       

//To check select count query execution with entire column
test("OffHeapQuery-001-TC_018", Include) {
  
  sql(s"""select count(*) from uniqdataquery1""").collect
  
  
}
       

//To check select distinct query execution 
test("OffHeapQuery-001-TC_019", Include) {
  
  sql(s"""select distinct * from uniqdataquery1""").collect
  
  
}
       

//To check select multiple column query execution 
test("OffHeapQuery-001-TC_020", Include) {
  
  sql(s"""select cust_name,cust_id,count(cust_name) from uniqdataquery1 group by cust_name,cust_id""").collect
  
  
}
       

//To check select count and distinct query execution 
test("OffHeapQuery-001-TC_021", Include) {
  intercept[Exception] {
    sql(s"""select count(cust_id),distinct(cust_name) from uniqdataquery1""").collect
  }
}
       

//To check sum query execution 
test("OffHeapQuery-001-TC_022", Include) {
  
  sql(s"""select sum(cust_id) as sum,cust_name from uniqdataquery1 group by cust_name""").collect
  
  
}
       

//To check sum of names query execution 
test("OffHeapQuery-001-TC_023", Include) {
  
  sql(s"""select sum(cust_name) from uniqdataquery1""").collect
  
  
}
       

//To check select distinct and groupby query execution 
test("OffHeapQuery-001-TC_024", Include) {
  
  sql(s"""select distinct(cust_name,cust_id) from uniqdataquery1 group by cust_name,cust_id""").collect
  
  
}
       

//To check select with where clause on cust_name query execution 
test("OffHeapQuery-001-TC_025", Include) {
  
  sql(s"""select cust_id from uniqdataquery1 where cust_name="cust_name_00000"""").collect
  
  
}
       

//To check query execution with IN operator without paranthesis
test("OffHeapQuery-001-TC_027", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id IN 9000,9005""").collect
  }
}
       

//To check query execution with IN operator with paranthesis
test("OffHeapQuery-001-TC_028", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id IN (9000,9005)""").collect
  
  
}
       

//To check query execution with IN operator with out specifying any field.
test("OffHeapQuery-001-TC_029", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where IN(1,2)""").collect
  }
}
       

//To check OR with correct syntax
test("OffHeapQuery-001-TC_030", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id>9005 or cust_id=9005""").collect
  
  
}
       

//To check OR with boolean expression
test("OffHeapQuery-001-TC_031", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id>9005 or false""").collect
  
  
}
       

//To check AND with correct syntax
test("OffHeapQuery-001-TC_032", Include) {
  
  sql(s"""select * from uniqdataquery1 where true AND true""").collect
  
  
}
       

//To check AND with using booleans
test("OffHeapQuery-001-TC_033", Include) {
  
  sql(s"""select * from uniqdataquery1 where true AND false""").collect
  
  
}
       

//To check AND with using booleans in invalid syntax
test("OffHeapQuery-001-TC_034", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where AND true""").collect
  }
}
       

//To check AND Passing two conditions on same input
test("OffHeapQuery-001-TC_035", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id=6 and cust_id>5""").collect
  
  
}
       

//To check AND changing case
test("OffHeapQuery-001-TC_036", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id=6 aND cust_id>5""").collect
  
  
}
       

//To check AND using 0 and 1 treated as boolean values
test("OffHeapQuery-001-TC_037", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where true aNd 0""").collect
  }
}
       

//To check AND on two columns
test("OffHeapQuery-001-TC_038", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id=9000 and cust_name='cust_name_00000'""").collect
  
  
}
       

//To check '='operator with correct syntax
test("OffHeapQuery-001-TC_039", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id=9000 and cust_name='cust_name_00000' and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00000'""").collect
  
  
}
       

//To check '='operator without Passing any value
test("OffHeapQuery-001-TC_040", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id=""").collect
  }
}
       

//To check '='operator without Passing columnname and value.
test("OffHeapQuery-001-TC_041", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where =""").collect
  }
}
       

//To check '!='operator with correct syntax
test("OffHeapQuery-001-TC_042", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id!=9000""").collect
  
  
}
       

//To check '!='operator by keeping space between them
test("OffHeapQuery-001-TC_043", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id !   = 9001""").collect
  }
}
       

//To check '!='operator by Passing boolean value whereas column expects an integer 
test("OffHeapQuery-001-TC_044", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id != true""").collect
  
  
}
       

//To check '!='operator without providing any value
test("OffHeapQuery-001-TC_045", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id != """).collect
  }
}
       

//To check '!='operator without providing any column name
test("OffHeapQuery-001-TC_046", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where  != false""").collect
  }
}
       

//To check 'NOT' with valid syntax
test("OffHeapQuery-001-TC_047", Include) {
  
  sql(s"""select * from uniqdataquery1 where NOT(cust_id=9000)""").collect
  
  
}
       

//To check 'NOT' using boolean values
test("OffHeapQuery-001-TC_048", Include) {
  
  sql(s"""select * from uniqdataquery1 where NOT(false)""").collect
  
  
}
       

//To check 'NOT' applying it on a value
test("OffHeapQuery-001-TC_049", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id = 'NOT(false)'""").collect
  
  
}
       

//To check 'NOT' with between operator
test("OffHeapQuery-001-TC_050", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id NOT BETWEEN 9000 and 9005""").collect
  
  
}
       

//To check 'NOT' operator in nested way
test("OffHeapQuery-001-TC_051", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id NOT (NOT(true))""").collect
  }
}
       

//To check 'NOT' operator with parenthesis.
test("OffHeapQuery-001-TC_052", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id NOT ()""").collect
  }
}
       

//To check 'NOT' operator without condition.
test("OffHeapQuery-001-TC_053", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id NOT""").collect
  }
}
       

//To check 'NOT' operator checking case sensitivity.
test("OffHeapQuery-001-TC_054", Include) {
  
  sql(s"""select * from uniqdataquery1 where nOt(false)""").collect
  
  
}
       

//To check '>' operator without specifying column
test("OffHeapQuery-001-TC_055", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where > 20""").collect
  }
}
       

//To check '>' operator without specifying value
test("OffHeapQuery-001-TC_056", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id > """).collect
  }
}
       

//To check '>' operator with correct syntax
test("OffHeapQuery-001-TC_057", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id >9005""").collect
  
  
}
       

//To check '>' operator for Integer value
test("OffHeapQuery-001-TC_058", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id > 9010""").collect
  
  
}
       

//To check '>' operator for String value
test("OffHeapQuery-001-TC_059", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_name > 'cust_name_00000'""").collect
  
  
}
       

//To check '<' operator without specifying column
test("OffHeapQuery-001-TC_060", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where < 5""").collect
  }
}
       

//To check '<' operator with correct syntax
test("OffHeapQuery-001-TC_061", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id < 9005""").collect
  
  
}
       

//To check '<' operator for String value
test("OffHeapQuery-001-TC_062", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_name < "cust_name_00001"""").collect
  
  
}
       

//To check '<=' operator without specifying column
test("OffHeapQuery-001-TC_063", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where  <= 2""").collect
  }
}
       

//To check '<=' operator without providing value
test("OffHeapQuery-001-TC_064", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where  cust_id <= """).collect
  }
}
       

//To check '<=' operator with correct syntax
test("OffHeapQuery-001-TC_065", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id <=9002""").collect
  
  
}
       

//To check '<=' operator adding space between'<' and  '='
test("OffHeapQuery-001-TC_066", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id < =  9002""").collect
  }
}
       

//To check 'BETWEEN' operator without providing range
test("OffHeapQuery-001-TC_067", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where age between""").collect
  }
}
       

//To check  'BETWEEN' operator with correct syntax
test("OffHeapQuery-001-TC_068", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id between 9002 and 9030""").collect
  
  
}
       

//To check  'BETWEEN' operator providing two same values
test("OffHeapQuery-001-TC_069", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_name beTWeen 'CU%' and 'CU%'""").collect
  
  
}
       

//To check  'NOT BETWEEN' operator for integer
test("OffHeapQuery-001-TC_070", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id NOT between 9024 and 9030""").collect
  
  
}
       

//To check  'NOT BETWEEN' operator for string
test("OffHeapQuery-001-TC_071", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_name NOT beTWeen 'cust_name_00000' and 'cust_name_00001'""").collect
  
  
}
       

//To check  'IS NULL' for case sensitiveness.
test("OffHeapQuery-001-TC_072", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id Is NulL""").collect
  
  
}
       

//To check  'IS NULL' for null field
test("OffHeapQuery-001-TC_073", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_name Is NulL""").collect
  
  
}
       

//To check  'IS NULL' without providing column
test("OffHeapQuery-001-TC_074", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where Is NulL""").collect
  }
}
       

//To check  'IS NOT NULL' without providing column
test("OffHeapQuery-001-TC_075", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where IS NOT NULL""").collect
  }
}
       

//To check ''IS NOT NULL' operator with correct syntax
test("OffHeapQuery-001-TC_076", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id IS NOT NULL""").collect
  
  
}
       

//To check  'Like' operator for integer
test("OffHeapQuery-001-TC_077", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id Like '9%'""").collect
  
  
}
       

//To check Limit clause with where condition
test("OffHeapQuery-001-TC_078", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id>10987 limit 15""").collect
  
  
}
       

//To check Limit clause with where condition and no argument
test("OffHeapQuery-001-TC_079", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id=10987 limit""").collect
  }
}
       

//To check Limit clause with where condition and decimal argument
test("OffHeapQuery-001-TC_080", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id=10987 limit 0.0""").collect
  }
}
       

//To check where clause with distinct and group by
test("OffHeapQuery-001-TC_081", Include) {
  
  sql(s"""select distinct cust_name from uniqdataquery1 where cust_name IN("CUST_NAME_01999") group by cust_name""").collect
  
  
}
       

//To check subqueries
test("OffHeapQuery-001-TC_082", Include) {
  
  sql(s"""select * from (select cust_id from uniqdataquery1 where cust_id IN (10987,10988)) uniqdataquery1 where cust_id IN (10987, 10988)""").collect
  
  
}
       

//To count with where clause
test("OffHeapQuery-001-TC_083", Include) {
  
  sql(s"""select count(cust_id) from uniqdataquery1 where cust_id > 10874""").collect
  
  
}
       

//To check Join query
test("OffHeapQuery-001-TC_084", Include) {
  dropTable("uniqdataquery11")
   sql(s"""CREATE TABLE uniqdataquery11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdataquery11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 join uniqdataquery11 where uniqdataquery1.CUST_ID > 10700 and uniqdataquery11.CUST_ID > 10500""").collect
}
       

//To check Left join with where clause
test("OffHeapQuery-001-TC_085", Include) {
  
  sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 LEFT join uniqdataquery11 where uniqdataquery1.CUST_ID > 10000""").collect
  
  
}
       

//To check Full join 
test("OffHeapQuery-001-TC_086", Include) {
  intercept[Exception] {
    sql(s"""select uniqdataquery1.CUST_ID from uniqdataquery1 FULL JOIN uniqdataquery11 where CUST_ID""").collect
  }
}
       

//To check Broadcast join 
test("OffHeapQuery-001-TC_087", Include) {
  
  sql(s"""select broadcast.cust_id from uniqdataquery1 broadcast join uniqdataquery11 where broadcast.cust_id > 10900""").collect
  
   sql(s"""drop table uniqdataquery11""").collect
}
       

//To avg function
test("OffHeapQuery-001-TC_088", Include) {
  
  sql(s"""select avg(cust_name) from uniqdataquery1 where cust_id > 10544 group by cust_name""").collect
  
  
}
       

//To check subquery with aggrgate function avg 
test("OffHeapQuery-001-TC_089", Include) {
  
  sql(s"""select cust_id,avg(cust_id) from uniqdataquery1 where cust_id IN (select cust_id from uniqdataquery1 where cust_id > 0) group by cust_id""").collect
  
  
}
       

//To check HAVING on Measure 
test("OffHeapQuery-001-TC_090", Include) {
  
  sql(s"""select cust_id from uniqdataquery1 where cust_id > 10543 group by cust_id having cust_id = 10546""").collect
  
  
}
       

//To check HAVING on dimension
test("OffHeapQuery-001-TC_091", Include) {
  
  sql(s"""select cust_name from uniqdataquery1 where cust_id > 10544 group by cust_name having cust_name like 'C%'""").collect
  
  
}
       

//To check HAVING on multiple columns
test("OffHeapQuery-001-TC_092", Include) {
  
  sql(s"""select cust_id,cust_name from uniqdataquery1 where cust_id > 10544 group by cust_id,cust_name having cust_id = 10545 AND cust_name like 'C%'""").collect
  
  
}
       

//To check HAVING with empty condition  
test("OffHeapQuery-001-TC_094", Include) {
  
  sql(s"""select cust_name from uniqdataquery1 where cust_id > 10544 group by cust_name having """"").collect
  
  
}
       

//To check SORT on measure 
test("OffHeapQuery-001-TC_095", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_id asc""").collect
  
  
}
       

//To check SORT on dimemsion  
test("OffHeapQuery-001-TC_096", Include) {
  
  sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_name desc""").collect
  
  
}
       

//To check SORT using 'AND' on multiple column 
test("OffHeapQuery-001-TC_097", Include) {
  intercept[Exception] {
    sql(s"""select * from uniqdataquery1 where cust_id > 10544 sort by cust_name desc and cust_id asc""").collect
  }
}
       

//To check Select average names and group by name query execution
test("OffHeapQuery-001-TC_098", Include) {
  
  sql(s"""select avg(cust_name) from uniqdataquery1 group by cust_name""").collect
  
  
}
       

//To check Select average id and group by id query execution
test("OffHeapQuery-001-TC_099", Include) {
  
  sql(s"""select avg(cust_id) from uniqdataquery1 group by cust_id""").collect
  
  
}
       

//To check average aggregate function with no arguments
test("OffHeapQuery-001-TC_100", Include) {
  intercept[Exception] {
    sql(s"""select cust_id,avg() from uniqdataquery1 group by cust_id""").collect
  }
}
       

//To check average aggregate function with empty string
test("OffHeapQuery-001-TC_101", Include) {
  
  sql(s"""select cust_id,avg("") from uniqdataquery1 group by cust_id""").collect
  
  
}
       

//To check nested  average aggregate function
test("OffHeapQuery-001-TC_102", Include) {
  intercept[Exception] {
    sql(s"""select cust_id,avg(count(cust_id)) from uniqdataquery1 group by cust_id""").collect
  }
}
       

//To check Multilevel query
test("OffHeapQuery-001-TC_103", Include) {
  
  sql(s"""select cust_id,avg(cust_id) from uniqdataquery1 where cust_id IN (select cust_id from uniqdataquery1) group by cust_id""").collect
  
  
}
       

//To check Using first() with group by clause
test("OffHeapQuery-001-TC_104", Include) {
  
  sql(s"""select first(cust_id) from uniqdataquery1 group by cust_id""").collect
  
  
}
       

//To check max with groupby clause query execution
test("OffHeapQuery-001-TC_105", Include) {
  
  sql(s"""select max(cust_name) from uniqdataquery1 group by(cust_name)""").collect
  
  
}
       

//To check max with groupby clause query with id execution
test("OffHeapQuery-001-TC_106", Include) {
  
  sql(s"""select max(cust_name) from uniqdataquery1 group by(cust_name),cust_id""").collect
  
  
}
       

//To check  multiple aggregate functions 
test("OffHeapQuery-001-TC_107", Include) {
  
  sql(s"""select max(cust_name),sum(cust_name),count(cust_id) from uniqdataquery1 group by(cust_name),cust_id""").collect
  
  
}
       

//To check max with empty string as argument
test("OffHeapQuery-001-TC_108", Include) {
  
  sql(s"""select max("") from uniqdataquery1 group by(cust_name)""").collect


  }


  //To check  select count of names with group by clause
  test("OffHeapQuery-001-TC_109", Include) {

    sql(s"""select count(cust_name) from uniqdataquery1 group by cust_name""").collect


  }


  //To check Order by ASC
  test("OffHeapQuery-001-TC_110", Include) {

    sql(s"""select * from uniqdataquery1 order by cust_id ASC""").collect


  }


  //To check Order by DESC
  test("OffHeapQuery-001-TC_111", Include) {

    sql(s"""select * from uniqdataquery1 order by cust_id DESC""").collect


  }


  //To check Order by without column name
  test("OffHeapQuery-001-TC_112", Include) {
    intercept[Exception] {
      sql(s"""select * from uniqdataquery1 order by ASC""").collect
    }
  }


  //To check cast Int to String
  test("OffHeapQuery-001-TC_113", Include) {

    sql(s"""select cast(bigint_column1 as STRING) from uniqdataquery1""").collect


  }


  //To check cast string to int
  test("OffHeapQuery-001-TC_114", Include) {

    sql(s"""select cast(cust_name as INT) from uniqdataquery1""").collect


  }


  //To check cast int to decimal
  test("OffHeapQuery-001-TC_115", Include) {

    sql(s"""select cast(bigint_column1 as DECIMAL(10,4)) from uniqdataquery1""").collect


  }


  //To check Using window with order by
  test("OffHeapQuery-001-TC_116", Include) {

    sql(s"""select cust_name, sum(bigint_column1) OVER w from uniqdataquery1 WINDOW w AS (PARTITION BY bigint_column2 ORDER BY cust_id)""").collect


  }


  //To check Using window without partition
  test("OffHeapQuery-001-TC_117", Include) {
    intercept[Exception] {
      sql(s"""select cust_name, sum(bigint_column1) OVER w from uniqdataquery1 WINDOW w""").collect
    }
  }


  //To check Using ROLLUP with group by
  test("OffHeapQuery-001-TC_118", Include) {

    sql(s"""select cust_name from uniqdataquery1 group by cust_name with ROLLUP""").collect


  }


  //To check Using ROLLUP without group by clause
  test("OffHeapQuery-001-TC_119", Include) {
    intercept[Exception] {
      sql(s"""select cust_name from uniqdataquery1 with ROLLUP""").collect
    }
     sql(s"""drop table uniqdataquery1""").collect
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("enable.unsafe.in.query.processing", CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("enable.unsafe.in.query.processing", "true")
    prop.addProperty("use.offheap.in.query.processing", "true")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("enable.unsafe.in.query.processing", p1)
  }
       
}

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
 * Test Class for Vector1TestCase to verify all scenerios
 */

class Vector1TestCase extends QueryTest with BeforeAndAfterAll {
         

  //To check select all records with  vectorized carbon reader enabled
  test("Vector1-TC_001", Include) {
     sql(s"""CREATE TABLE uniqdatavector1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect
     sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdatavector1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select * from uniqdatavector1 """).collect



  }


  //To check  random measure select query with  vectorized carbon reader enabled
  test("Vector1-TC_002", Include) {

    sql(s"""select cust_name,DOB,DOJ from uniqdatavector1 where cust_id=10999""").collect


     sql(s"""drop table uniqdatavector1""").collect

  }


  //To check select random columns  and order with vectorized carbon reader enabled
  test("Vector1-TC_003", Include) {
     sql(s"""create table double(id double, name string) STORED AS carbondata """).collect
   sql(s"""load data  inpath '$resourcesPath/Data/InsertData/maxrange_double.csv' into table double""").collect

    sql(s"""select id from double order by id""").collect

  }


  //To check the logs of executor with vectorized carbon reader enabled
  test("Vector1-TC_004", Include) {

    sql(s"""select id from double order by id""").collect



  }


  //To check  for select random measures with group by and having clause with vectorized carbon reader enabled
  test("Vector1-TC_005", Include) {

    sql(s"""select id,count(*) from double group by id having count(*)=1""").collect
  }


  //To check for select count query with group by and having clause with vectorized carbon reader enabled
  test("Vector1-TC_006", Include) {

    sql(s"""select id,count(id) from double group by id having count(*)=1""").collect

     sql(s"""drop table double""").collect

  }


  //To applied cast method  with vectorized carbon reader enabled
  test("Vector1-TC_007", Include) {
     sql(s"""CREATE TABLE uniqdatavector11 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdatavector11 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

    sql(s"""select cast(Double_COLUMN1 as int) from uniqdatavector11""").collect



  }


  //To apply sum method on a column with select query with vectorized carbon reader enabled
  test("Vector1-TC_008", Include) {

    sql(s"""select sum(CUST_ID) from uniqdatavector11""").collect



  }


  //To apply the average method on a column with select query with vectorized carbon reader enabled
  test("Vector1-TC_009", Include) {

    sql(s"""select avg(CUST_ID) from uniqdatavector11""").collect



  }


  //To apply the percentile_approx method with vectorized carbon reader enabled
  test("Vector1-TC_010", Include) {

    sql(s"""select percentile_approx(1, 0.5 ,500)  from uniqdatavector11""").collect



  }


  //To apply the var_samp method with vectorized carbon reader enabled
  test("Vector1-TC_011", Include) {

    sql(s"""select var_samp(cust_id) from uniqdatavector11""").collect



  }


  //To apply the stddev_pop method with vectorized carbon reader enabled
  test("Vector1-TC_012", Include) {

    sql(s"""select stddev_pop(cust_id) from uniqdatavector11""").collect



  }


  //To apply the stddev_samp method with vectorized carbon reader enabled
  test("Vector1-TC_013", Include) {

    sql(s"""select stddev_samp(cust_id) from uniqdatavector11""").collect

  }


  //To apply percentile method with vectorized carbon reader enabled
  test("Vector1-TC_014", Include) {

    sql(s"""select percentile(0,1) from uniqdatavector11""").collect
  }


  //To apply min method with vectorized carbon reader enabled
  test("Vector1-TC_015", Include) {

    sql(s"""select min(CUST_ID) from uniqdatavector11""").collect
  }


  //To applied max method with vectorized carbon reader enabled
  test("Vector1-TC_016", Include) {

    sql(s"""select max(CUST_ID) from uniqdatavector11""").collect
  }


  //To apply sum method with plus operator with vectorized carbon reader enabled
  test("Vector1-TC_017", Include) {

    sql(s"""select sum(CUST_ID+1) from uniqdatavector11""").collect
  }


  //To apply sum method with minus operator with vectorized carbon reader enabled

  test("Vector1-TC_018", Include) {

    sql(s"""select sum(CUST_ID-1) from uniqdatavector11""").collect
  }


  //To apply count method  with distinct operator with vectorized carbon reader enabled
  test("Vector1-TC_019", Include) {

    sql(s"""select count(DISTINCT CUST_ID) from uniqdatavector11""").collect
  }


  //To check random measure select query with  AND operator and vectorized carbon reader enabled
  test("Vector1-TC_020", Include) {

    sql(s"""select cust_name,DOB,DOJ from uniqdatavector11 where cust_id=10999 and INTEGER_COLUMN1=2000 """).collect
  }


  //To check random measure select query with  OR operator and vectorized carbon reader enabled
  test("Vector1-TC_021", Include) {

    sql(s"""select cust_name,DOB,DOJ from uniqdatavector11 where cust_id=10999 or INTEGER_COLUMN1=2000 """).collect
  }


  //To apply count method with if operator with vectorized carbon reader enabled
  test("Vector1-TC_022", Include) {

    sql(s"""select count(if(CUST_ID<1999,NULL,CUST_NAME)) from uniqdatavector11""").collect
  }


  //To apply in operator with vectorized carbon reader enabled
  test("Vector1-TC_023", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID IN(1,22)""").collect
  }


  //To apply not in operator with vectorized carbon reader enabled
  test("Vector1-TC_024", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID NOT IN(1,22)""").collect
  }


  //To apply between operator with vectorized carbon reader enabled
  test("Vector1-TC_025", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID BETWEEN 1 AND 11000""").collect
  }


  //To apply not between operator with vectorized carbon reader enabled
  test("Vector1-TC_026", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID NOT BETWEEN 1 AND 11000""").collect
  }


  //To apply between in operator with order by clause with vectorized carbon reader enabled
  test("Vector1-TC_027", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID in (1,10999) order by 'CUST_ID'""").collect
  }


  //To apply between in operator with group by clause with vectorized carbon reader enabled
  test("Vector1-TC_028", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID in (1,10999) group by CUST_NAME""").collect



  }


  //To apply  null clause with vectorized carbon reader enabled
  test("Vector1-TC_029", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID is null""").collect



  }


  //To applied not null clause with vectorized carbon reader enabled
  test("Vector1-TC_030", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID is not null""").collect



  }


  //To apply > operator with vectorized carbon reader enabled
  test("Vector1-TC_031", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID>1""").collect



  }


  //To apply < operator with vectorized carbon reader enabled
  test("Vector1-TC_032", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID<1""").collect



  }


  //To apply != operator with vectorized carbon reader enabled
  test("Vector1-TC_033", Include) {

    sql(s"""select CUST_NAME from uniqdatavector11 where CUST_ID!=1""").collect



  }


  //To apply like clause with vectorized carbon reader enabled
  test("Vector1-TC_034", Include) {

    sql(s"""select CUST_ID from uniqdatavector11 where CUST_ID like 10999""").collect



  }


  //To apply like% clause with vectorized carbon reader enabled
  test("Vector1-TC_035", Include) {

    sql(s"""select CUST_ID from uniqdatavector11 where CUST_ID like '%10999%'""").collect



  }


  //To apply rlike clause with vectorized carbon reader enabled
  test("Vector1-TC_036", Include) {

    sql(s"""select CUST_ID from uniqdatavector11 where CUST_ID rlike 10999""").collect



  }


  //To apply rlike% clause with vectorized carbon reader enabled
  test("Vector1-TC_037", Include) {

    sql(s"""select CUST_ID from uniqdatavector11 where CUST_ID rlike '%10999'""").collect



  }


  //To apply alias clause with vectorized carbon reader enabled
  test("Vector1-TC_038", Include) {

    sql(s"""select count(cust_id)+10.364 as a from uniqdatavector11""").collect



  }


  //To apply aliase clause with group by clause with vectorized carbon reader enabled
  test("Vector1-TC_039", Include) {

    sql(s"""select count(cust_id)+10.364 as a from uniqdatavector11 group by CUST_ID""").collect



  }


  //To apply aliase clause with order by clause with vectorized carbon reader enabled
  test("Vector1-TC_040", Include) {

    sql(s"""select cust_id,count(cust_name) a from uniqdatavector11 group by cust_id order by cust_id""").collect



  }


  //To apply regexp_replace clause with vectorized carbon reader enabled
  test("Vector1-TC_041", Include) {

    sql(s"""select regexp_replace(cust_id, 'i', 'ment')  from uniqdatavector11""").collect



  }


  //To apply date_add method with vectorized carbon reader enabled
  test("Vector1-TC_048", Include) {

    sql(s"""SELECT date_add(DOB,1) FROM uniqdatavector11""").collect



  }


  //To apply date_sub method with vectorized carbon reader enabled
  test("Vector1-TC_049", Include) {

    sql(s"""SELECT date_sub(DOB,1) FROM uniqdatavector11""").collect



  }


  //To apply current_date method with vectorized carbon reader enabled
  test("Vector1-TC_050", Include) {

    sql(s"""SELECT current_date() FROM uniqdatavector11""").collect



  }


  //To apply add_month method with vectorized carbon reader enabled
  test("Vector1-TC_051", Include) {

    sql(s"""SELECT add_months(dob,1) FROM uniqdatavector11""").collect



  }


  //To apply last_day method with vectorized carbon reader enabled
  test("Vector1-TC_052", Include) {

    sql(s"""SELECT last_day(dob) FROM uniqdatavector11""").collect



  }


  //To apply next_day method with vectorized carbon reader enabled
  test("Vector1-TC_053", Include) {

    sql(s"""SELECT next_day(dob,'monday') FROM uniqdatavector11""").collect



  }


  //To apply months_between method on carbon table
  test("Vector1-TC_054", Include) {

    sql(s"""select months_between('2016-12-28', '2017-01-30') from uniqdatavector11""").collect



  }


  //Toapply date_diff method with vectorized carbon reader enabled
  test("Vector1-TC_055", Include) {

    sql(s"""select datediff('2009-03-01', '2009-02-27') from uniqdatavector11""").collect



  }


  //To apply concat method with vectorized carbon reader enabled
  test("Vector1-TC_056", Include) {

    sql(s"""SELECT concat('hi','hi') FROM uniqdatavector11""").collect



  }


  //To apply lower method with vectorized carbon reader enabled
  test("Vector1-TC_057", Include) {

    sql(s"""SELECT lower('H') FROM uniqdatavector11""").collect



  }


  //To apply substr method with vectorized carbon reader enabled
  test("Vector1-TC_058", Include) {

    sql(s"""select substr(cust_id,3) from uniqdatavector11""").collect



  }


  //To apply trim method with vectorized carbon reader enabled
  test("Vector1-TC_059", Include) {

    sql(s"""select trim(cust_id) from uniqdatavector11""").collect



  }


  //To apply split method with vectorized carbon reader enabled
  test("Vector1-TC_060", Include) {

    sql(s"""select split('knoldus','ol') from uniqdatavector11""").collect



  }


  //To apply split method  limit clause with vectorized carbon reader enabled
  test("Vector1-TC_061", Include) {

    sql(s"""select split('knoldus','ol') from uniqdatavector11 limit 1""").collect



  }


  //To apply reverse on carbon table with vectorized carbon reader enabled
  test("Vector1-TC_062", Include) {

    sql(s"""select reverse('knoldus') from uniqdatavector11""").collect



  }


  //To apply replace on carbon table with vectorized carbon reader enabled
  test("Vector1-TC_063", Include) {

    sql(s"""select regexp_replace('Tester', 'T', 't') from uniqdatavector11""").collect



  }


  //To apply replace with limit clause with vectorized carbon reader enabled
  test("Vector1-TC_064", Include) {

    sql(s"""select regexp_replace('Tester', 'T', 't') from uniqdatavector11 limit 1""").collect



  }


  //To apply FORMAT_STRING on carbon table with vectorized carbon reader enabled
  test("Vector1-TC_065", Include) {

    sql(s"""select format_string('data', cust_name) from uniqdatavector11""").collect



  }


  //To apply sentences method with vectorized carbon reader enabled
  test("Vector1-TC_066", Include) {

    sql(s"""select sentences(cust_name) from uniqdatavector11""").collect



  }


  //To apply space method on carbon table with vectorized carbon reader enabled
  test("Vector1-TC_067", Include) {

    sql(s"""select space(10) from uniqdatavector11""").collect



  }


  //To apply rtrim method with vectorized carbon reader enabled
  test("Vector1-TC_068", Include) {

    sql(s"""select rtrim("     testing           ") from uniqdatavector11""").collect



  }


  //To apply ascii method with vectorized carbon reader enabled
  test("Vector1-TC_069", Include) {

    sql(s"""select ascii('A') from uniqdatavector11""").collect



  }


  //To apply utc_timestamp method with vectorized carbon reader enabled
  test("Vector1-TC_070", Include) {

    sql(s"""select from_utc_timestamp('2016-12-12 08:00:00','PST') from uniqdatavector11""").collect


     sql(s"""drop table uniqdatavector11""").collect

  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.enable.vector.reader", "true")
  }

  override def afterAll: Unit = {
    //Reverting to old
    prop.addProperty("carbon.enable.vector.reader", p1)
  }
}
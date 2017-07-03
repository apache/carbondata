
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
 * Test Class for STRUCTOFARRAYcom to verify all scenerios
 */

class STRUCTOFARRAYCOMTestCase extends QueryTest with BeforeAndAfterAll {
         

//drop_STRUCT_OF_ARRAY_com
test("drop_STRUCT_OF_ARRAY_com", Include) {
  sql(s"""drop table if exists STRUCT_OF_ARRAY_com""").collect

  sql(s"""drop table if exists STRUCT_OF_ARRAY_com_hive""").collect

}
       

//Complex_StructUsingArray_TC_CreateTable
test("Complex_StructUsingArray_TC_CreateTable", Include) {
  sql(s"""create table STRUCT_OF_ARRAY_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table STRUCT_OF_ARRAY_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int,CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>,date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Complex_StructUsingArray_TC_DataLoad
test("Complex_StructUsingArray_TC_DataLoad", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/structofarray.csv' INTO table STRUCT_OF_ARRAY_com options ('DELIMITER'=',', 'QUOTECHAR'='"', 'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE,GENDER,EDUCATED,IS_MARRIED,STRUCT_OF_ARRAY,CARD_COUNT,DEBIT_COUNT,CREDIT_COUNT,DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$DOLLAR','COMPLEX_DELIMITER_LEVEL_2'='&')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/structofarray.csv' INTO table STRUCT_OF_ARRAY_com_hive """).collect

}
       

//Complex_StructUsingArray_TC_001
test("Complex_StructUsingArray_TC_001", Include) {
  sql(s"""select struct_of_array.ID, struct_of_array.ID+ 10 as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_002
test("Complex_StructUsingArray_TC_002", Include) {
  sql(s"""select struct_of_array.Sal1[1], struct_of_array.Sal1[1]+ 10 as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_003
test("Complex_StructUsingArray_TC_003", Include) {
  sql(s"""select struct_of_array.ID, struct_of_array.state[1]+ 10 as a  from STRUCT_OF_ARRAY_com """).collect
}
       

//Complex_StructUsingArray_TC_004
test("Complex_StructUsingArray_TC_004", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.SNo[1]+cust_id as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_005
test("Complex_StructUsingArray_TC_005", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.SNo[1]+age  as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_006
test("Complex_StructUsingArray_TC_006", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.SNo[1]+deposit  as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_007
test("Complex_StructUsingArray_TC_007", Include) {
  sql(s"""select concat(struct_of_array.state[1],'_',cust_id)  as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_008
test("Complex_StructUsingArray_TC_008", Include) {
  sql(s"""select concat(struct_of_array.state[1],'_',educated)  as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_009
test("Complex_StructUsingArray_TC_009", Include) {
  sql(s"""select concat(struct_of_array.state[1],'_',educated,'@',is_married)  as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_010
test("Complex_StructUsingArray_TC_010", Include) {
  sql(s"""select struct_of_array.state[1], sum(struct_of_array.ID+ 10) as a from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_011
test("Complex_StructUsingArray_TC_011", Include) {
  sql(s"""select struct_of_array.state[1], sum(struct_of_array.Sal1[1]+ 10) as a from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_012
test("Complex_StructUsingArray_TC_012", Include) {
  sql(s"""select struct_of_array.state[1], sum(struct_of_array.ID+ 10) as a from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_013
test("Complex_StructUsingArray_TC_013", Include) {
  sql(s"""select struct_of_array.state[1], sum(struct_of_array.Sal1[0]+ 10) as a from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_014
test("Complex_StructUsingArray_TC_014", Include) {
  sql(s"""select  struct_of_array.date1[1], sum(struct_of_array.ID+ 10) as a from STRUCT_OF_ARRAY_com  group by  struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_015
test("Complex_StructUsingArray_TC_015", Include) {
  sql(s"""select struct_of_array.date1[1], sum(struct_of_array.Sal1[1]+ 10) as a from STRUCT_OF_ARRAY_com  group by  struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_016
test("Complex_StructUsingArray_TC_016", Include) {
  sql(s"""select struct_of_array.SNo[1], sum(year+ 10) as a from STRUCT_OF_ARRAY_com  group by  struct_of_array.SNo[1] order by struct_of_array.SNo[1]""").collect
}
       

//Complex_StructUsingArray_TC_017
test("Complex_StructUsingArray_TC_017", Include) {
  sql(s"""select struct_of_array.SNo[1], avg(struct_of_array.SNo[1]+ 10) as a from STRUCT_OF_ARRAY_com group by  struct_of_array.SNo[1]""").collect
}
       

//Complex_StructUsingArray_TC_018
test("Complex_StructUsingArray_TC_018", Include) {
  sql(s"""select struct_of_array.state[1],avg(deposit+ 10) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_019
test("Complex_StructUsingArray_TC_019", Include) {
  sql(s"""select struct_of_array.date1[0], avg(struct_of_array.ID+deposit) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.date1[0] order by struct_of_array.date1[0]""").collect
}
       

//Complex_StructUsingArray_TC_021
test("Complex_StructUsingArray_TC_021", Include) {
  sql(s"""select struct_of_array.date1[0], count(struct_of_array.ID)+10 as a  from STRUCT_OF_ARRAY_com  group by  struct_of_array.date1[0]""").collect
}
       

//Complex_StructUsingArray_TC_022
test("Complex_StructUsingArray_TC_022", Include) {
  sql(s"""select struct_of_array.date1[0], count(struct_of_array.Sal1[0])+10 as a  from STRUCT_OF_ARRAY_com  group by struct_of_array.date1[0]""").collect
}
       

//Complex_StructUsingArray_TC_023
test("Complex_StructUsingArray_TC_023", Include) {
  sql(s"""select struct_of_array.state[1], count(struct_of_array.state[1]+ 10) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_024
test("Complex_StructUsingArray_TC_024", Include) {
  sql(s"""select struct_of_array.state[1], count(struct_of_array.state[1]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_025
test("Complex_StructUsingArray_TC_025", Include) {
  sql(s"""select struct_of_array.state[1], count(struct_of_array.state[1]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] having count(struct_of_array.state[1])>2""").collect
}
       

//Complex_StructUsingArray_TC_026
test("Complex_StructUsingArray_TC_026", Include) {
  sql(s"""select struct_of_array.state[1], count(struct_of_array.state[1])+10  Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_027
test("Complex_StructUsingArray_TC_027", Include) {
  sql(s"""select struct_of_array.date1[1], count(struct_of_array.date1[1])+10  Total from STRUCT_OF_ARRAY_com group by  struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_028
test("Complex_StructUsingArray_TC_028", Include) {
  sql(s"""select cust_id , count(struct_of_array.ID+struct_of_array.ID)  Total from STRUCT_OF_ARRAY_com group by  cust_id  order by cust_id""").collect
}
       

//Complex_StructUsingArray_TC_029
test("Complex_StructUsingArray_TC_029", Include) {
  sql(s"""select cust_id , count(struct_of_array.Sal1[0]+struct_of_array.Sal1[0])  Total from STRUCT_OF_ARRAY_com group by  cust_id  order by cust_id""").collect
}
       

//Complex_StructUsingArray_TC_030
test("Complex_StructUsingArray_TC_030", Include) {
  sql(s"""select struct_of_array.state[1], min(struct_of_array.state[1]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[1] order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_031
test("Complex_StructUsingArray_TC_031", Include) {
  sql(s"""select  min(struct_of_array.date1[1]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_032
test("Complex_StructUsingArray_TC_032", Include) {
  sql(s"""select cust_id , min(struct_of_array.ID+struct_of_array.ID)  Total from STRUCT_OF_ARRAY_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_StructUsingArray_TC_033
test("Complex_StructUsingArray_TC_033", Include) {
  sql(s"""select cust_id , min(struct_of_array.Sal1[0]+struct_of_array.Sal1[0])  Total from STRUCT_OF_ARRAY_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_StructUsingArray_TC_034
test("Complex_StructUsingArray_TC_034", Include) {
  sql(s"""select cust_id , max(struct_of_array.ID+struct_of_array.ID)  Total from STRUCT_OF_ARRAY_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_StructUsingArray_TC_035
test("Complex_StructUsingArray_TC_035", Include) {
  sql(s"""select cust_id , max(struct_of_array.Sal1[0]+struct_of_array.Sal1[0])  Total from STRUCT_OF_ARRAY_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_StructUsingArray_TC_036
test("Complex_StructUsingArray_TC_036", Include) {
  sql(s"""select max(struct_of_array.date1[1]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_037
test("Complex_StructUsingArray_TC_037", Include) {
  sql(s"""select max(struct_of_array.state[0]) Total from STRUCT_OF_ARRAY_com group by  struct_of_array.state[0] order by struct_of_array.state[0]""").collect
}
       

//Complex_StructUsingArray_TC_038
test("Complex_StructUsingArray_TC_038", Include) {
  sql(s"""select struct_of_array.ID ,sum(distinct struct_of_array.ID)+10 from STRUCT_OF_ARRAY_com group by struct_of_array.ID""").collect
}
       

//Complex_StructUsingArray_TC_039
test("Complex_StructUsingArray_TC_039", Include) {
  sql(s"""select struct_of_array.Sal1[0] ,sum(distinct struct_of_array.Sal1[0])+10 from STRUCT_OF_ARRAY_com group by struct_of_array.Sal1[0]""").collect
}
       

//Complex_StructUsingArray_TC_040
test("Complex_StructUsingArray_TC_040", Include) {
  sql(s"""select struct_of_array.state[0] ,sum(distinct struct_of_array.state[0])+10 from STRUCT_OF_ARRAY_com group by struct_of_array.state[0]""").collect
}
       

//Complex_StructUsingArray_TC_041
test("Complex_StructUsingArray_TC_041", Include) {
  sql(s"""select struct_of_array.Sal1[0] ,count(distinct struct_of_array.Sal1[0]) from STRUCT_OF_ARRAY_com group by struct_of_array.Sal1[0]""").collect
}
       

//Complex_StructUsingArray_TC_042
test("Complex_StructUsingArray_TC_042", Include) {
  sql(s"""select struct_of_array.ID ,count(distinct struct_of_array.ID) from STRUCT_OF_ARRAY_com group by struct_of_array.ID""").collect
}
       

//Complex_StructUsingArray_TC_043
test("Complex_StructUsingArray_TC_043", Include) {
  sql(s"""select struct_of_array.Sal1[0] ,count(distinct struct_of_array.Sal1[0]) from STRUCT_OF_ARRAY_com group by struct_of_array.Sal1[0]""").collect
}
       

//Complex_StructUsingArray_TC_044
test("Complex_StructUsingArray_TC_044", Include) {
  sql(s"""select count(distinct struct_of_array.state[0])+10 as a,struct_of_array.Sal1[0] from STRUCT_OF_ARRAY_com group by struct_of_array.Sal1[0]""").collect
}
       

//Complex_StructUsingArray_TC_045
test("Complex_StructUsingArray_TC_045", Include) {
  sql(s"""select count(distinct struct_of_array.state[0])+10 as a,struct_of_array.ID from STRUCT_OF_ARRAY_com group by struct_of_array.ID""").collect
}
       

//Complex_StructUsingArray_TC_046
test("Complex_StructUsingArray_TC_046", Include) {
  sql(s"""select count(*) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_047
test("Complex_StructUsingArray_TC_047", Include) {
  sql(s"""Select count(1) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_048
test("Complex_StructUsingArray_TC_048", Include) {
  sql(s"""select count(struct_of_array.date1[1]) as a   from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_049
test("Complex_StructUsingArray_TC_049", Include) {
  sql(s"""select count(struct_of_array.state[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_050
test("Complex_StructUsingArray_TC_050", Include) {
  sql(s"""select count(DISTINCT struct_of_array.date1[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_051
test("Complex_StructUsingArray_TC_051", Include) {
  sql(s"""select count(DISTINCT struct_of_array.state[0]) as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_052
test("Complex_StructUsingArray_TC_052", Include) {
  sql(s"""select count (if(struct_of_array.date1[1]>'100',NULL,struct_of_array.date1[1]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_053
test("Complex_StructUsingArray_TC_053", Include) {
  sql(s"""select count (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sno[0]))  a from  STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_054
test("Complex_StructUsingArray_TC_054", Include) {
  sql(s"""select count (if(struct_of_array.sal1[0]>100,NULL,struct_of_array.sal1[0]))  a from  STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_055
test("Complex_StructUsingArray_TC_055", Include) {
  sql(s"""select count (if(struct_of_array.state[0]>100,NULL,struct_of_array.state[0]))  a from  STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_056
test("Complex_StructUsingArray_TC_056", Include) {
  sql(s"""select sum(DISTINCT  struct_of_array.sno[0]) a  from  STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_057
test("Complex_StructUsingArray_TC_057", Include) {
  sql(s"""select sum(DISTINCT  struct_of_array.sal1[1]) a  from  STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_058
test("Complex_StructUsingArray_TC_058", Include) {
  sql(s"""select sum (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sno[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_059
test("Complex_StructUsingArray_TC_059", Include) {
  sql(s"""select sum (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sal1[1]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_060
test("Complex_StructUsingArray_TC_060", Include) {
  sql(s"""select sum (if(struct_of_array.state[0]>100,NULL,struct_of_array.state[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_061
test("Complex_StructUsingArray_TC_061", Include) {
  sql(s"""select sum (if(struct_of_array.date1[0]>'100',NULL,struct_of_array.state[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_062
test("Complex_StructUsingArray_TC_062", Include) {
  sql(s"""select avg(struct_of_array.sno[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_063
test("Complex_StructUsingArray_TC_063", Include) {
  sql(s"""select avg(struct_of_array.sal1[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_064
test("Complex_StructUsingArray_TC_064", Include) {
  sql(s"""select sum(struct_of_array.sno[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_065
test("Complex_StructUsingArray_TC_065", Include) {
  sql(s"""select sum(struct_of_array.sal1[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_066
test("Complex_StructUsingArray_TC_066", Include) {
  sql(s"""select max(struct_of_array.state[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_067
test("Complex_StructUsingArray_TC_067", Include) {
  sql(s"""select count(struct_of_array.state[0]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_068
test("Complex_StructUsingArray_TC_068", Include) {
  sql(s"""select min(struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_069
test("Complex_StructUsingArray_TC_069", Include) {
  sql(s"""select avg (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sno[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_070
test("Complex_StructUsingArray_TC_070", Include) {
  sql(s"""select avg (if(struct_of_array.sal1[1]>100,NULL,struct_of_array.sal1[1]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_071
test("Complex_StructUsingArray_TC_071", Include) {
  sql(s"""select avg (if(struct_of_array.state[0]>100,NULL,struct_of_array.state[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_072
test("Complex_StructUsingArray_TC_072", Include) {
  sql(s"""select min (if(struct_of_array.state[0]>100,NULL,struct_of_array.state[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_073
test("Complex_StructUsingArray_TC_073", Include) {
  sql(s"""select min (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sno[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_074
test("Complex_StructUsingArray_TC_074", Include) {
  sql(s"""select min (if(struct_of_array.sal1[1]>100,NULL,struct_of_array.sal1[1]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_075
test("Complex_StructUsingArray_TC_075", Include) {
  sql(s"""select max (if(struct_of_array.sno[0]>100,NULL,struct_of_array.sno[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_076
test("Complex_StructUsingArray_TC_076", Include) {
  sql(s"""select max (if(struct_of_array.sal1[1]>100,NULL,struct_of_array.sal1[1]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_077
test("Complex_StructUsingArray_TC_077", Include) {
  sql(s"""select max (if(struct_of_array.state[0]>100,NULL,struct_of_array.state[0]))  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_078
test("Complex_StructUsingArray_TC_078", Include) {
  sql(s"""select variance(struct_of_array.sno[1]) as a   from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_079
test("Complex_StructUsingArray_TC_079", Include) {
  sql(s"""select variance(struct_of_array.sal1[0]) as a   from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_080
test("Complex_StructUsingArray_TC_080", Include) {
  sql(s"""select var_pop(struct_of_array.sno[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_081
test("Complex_StructUsingArray_TC_081", Include) {
  sql(s"""select var_pop(struct_of_array.sal1[0])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_082
test("Complex_StructUsingArray_TC_082", Include) {
  sql(s"""select var_samp(struct_of_array.sno[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_083
test("Complex_StructUsingArray_TC_083", Include) {
  sql(s"""select var_samp(struct_of_array.sal1[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_084
test("Complex_StructUsingArray_TC_084", Include) {
  sql(s"""select stddev_pop(struct_of_array.sno[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_085
test("Complex_StructUsingArray_TC_085", Include) {
  sql(s"""select stddev_pop(struct_of_array.sal1[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_086
test("Complex_StructUsingArray_TC_086", Include) {
  sql(s"""select stddev_samp(struct_of_array.sno[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_087
test("Complex_StructUsingArray_TC_087", Include) {
  sql(s"""select stddev_samp(struct_of_array.sal1[0])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_088
test("Complex_StructUsingArray_TC_088", Include) {
  sql(s"""select covar_pop(struct_of_array.sno[1],struct_of_array.sno[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_089
test("Complex_StructUsingArray_TC_089", Include) {
  sql(s"""select covar_pop(struct_of_array.sal1[0],struct_of_array.sal1[0]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_090
test("Complex_StructUsingArray_TC_090", Include) {
  sql(s"""select covar_samp(struct_of_array.sno[1],struct_of_array.sno[1]) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_092
test("Complex_StructUsingArray_TC_092", Include) {
  sql(s"""select corr(struct_of_array.sno[1],struct_of_array.sno[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_093
test("Complex_StructUsingArray_TC_093", Include) {
  sql(s"""select corr(struct_of_array.sal1[1],struct_of_array.sal1[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_094
test("Complex_StructUsingArray_TC_094", Include) {
  sql(s"""select percentile(struct_of_array.sno[1],0.2) as  a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_096
test("Complex_StructUsingArray_TC_096", Include) {
  sql(s"""select percentile(struct_of_array.sno[1],array(0,0.2,0.3,1))  as  a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_098
test("Complex_StructUsingArray_TC_098", Include) {
  sql(s"""select percentile_approx(struct_of_array.sno[1],0.2) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_099
test("Complex_StructUsingArray_TC_099", Include) {
  sql(s"""select percentile_approx(struct_of_array.sal1[1],0.2) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_100
test("Complex_StructUsingArray_TC_100", Include) {
  sql(s"""select percentile_approx(struct_of_array.sno[1],0.2,5) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_101
test("Complex_StructUsingArray_TC_101", Include) {
  sql(s"""select percentile_approx(struct_of_array.sal1[1],0.2,5) as a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_102
test("Complex_StructUsingArray_TC_102", Include) {
  sql(s"""select percentile_approx(struct_of_array.sno[1],array(0.2,0.3,0.99))  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_103
test("Complex_StructUsingArray_TC_103", Include) {
  sql(s"""select percentile_approx(struct_of_array.sal1[1],array(0.2,0.3,0.99))  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_104
test("Complex_StructUsingArray_TC_104", Include) {
  sql(s"""select percentile_approx(struct_of_array.sno[1],array(0.2,0.3,0.99),5) as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_105
test("Complex_StructUsingArray_TC_105", Include) {
  sql(s"""select percentile_approx(struct_of_array.sal1[1],array(0.2,0.3,0.99),5) as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_106
test("Complex_StructUsingArray_TC_106", Include) {
  sql(s"""select * from (select collect_set(struct_of_array.state[0]) as myseries from STRUCT_OF_ARRAY_com) aa sort by myseries""").collect
}
       

//Complex_StructUsingArray_TC_107
test("Complex_StructUsingArray_TC_107", Include) {
  sql(s"""select corr(struct_of_array.sno[1],struct_of_array.sno[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_108
test("Complex_StructUsingArray_TC_108", Include) {
  sql(s"""select corr(struct_of_array.sno[1],struct_of_array.sal1[1])  as a from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_109
test("Complex_StructUsingArray_TC_109", Include) {
  sql(s"""select last(struct_of_array.state[1]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_110
test("Complex_StructUsingArray_TC_110", Include) {
  sql(s"""select first(struct_of_array.state[1]) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_111
test("Complex_StructUsingArray_TC_111", Include) {
  sql(s"""select struct_of_array.date1[1],count(struct_of_array.state[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_112
test("Complex_StructUsingArray_TC_112", Include) {
  sql(s"""select struct_of_array.state[0],count(struct_of_array.date1[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0] order by a""").collect
}
       

//Complex_StructUsingArray_TC_113
test("Complex_StructUsingArray_TC_113", Include) {
  sql(s"""select struct_of_array.date1[1],struct_of_array.state[1],count(struct_of_array.sno[1])  a from STRUCT_OF_ARRAY_com group by struct_of_array.state[1],struct_of_array.date1[1] order by struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_114
test("Complex_StructUsingArray_TC_114", Include) {
  sql(s"""select struct_of_array.date1[1],struct_of_array.state[1],count(struct_of_array.sal1[1])  a from STRUCT_OF_ARRAY_com group by struct_of_array.state[1],struct_of_array.date1[1] order by struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_115
test("Complex_StructUsingArray_TC_115", Include) {
  sql(s"""select count(distinct struct_of_array.state[0]) a,struct_of_array.sno[1] from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1]""").collect
}
       

//Complex_StructUsingArray_TC_116
test("Complex_StructUsingArray_TC_116", Include) {
  sql(s"""select count(distinct struct_of_array.state[0]) a,struct_of_array.sal1[1] from STRUCT_OF_ARRAY_com group by struct_of_array.sal1[1]""").collect
}
       

//Complex_StructUsingArray_TC_117
test("Complex_StructUsingArray_TC_117", Include) {
  sql(s"""select count(distinct struct_of_array.date1[1]) a,struct_of_array.sno[0],struct_of_array.state[1] from STRUCT_OF_ARRAY_com group by struct_of_array.sno[0],struct_of_array.state[1] order by struct_of_array.sno[0],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_118
test("Complex_StructUsingArray_TC_118", Include) {
  sql(s"""select count(distinct struct_of_array.date1[1]) a,struct_of_array.sal1[0],struct_of_array.state[1] from STRUCT_OF_ARRAY_com group by struct_of_array.sal1[0],struct_of_array.state[1] order by struct_of_array.sal1[0],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_119
test("Complex_StructUsingArray_TC_119", Include) {
  sql(s"""select struct_of_array.state[0],sum(struct_of_array.sno[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0] order by struct_of_array.state[0]""").collect
}
       

//Complex_StructUsingArray_TC_120
test("Complex_StructUsingArray_TC_120", Include) {
  sql(s"""select struct_of_array.state[0],sum(struct_of_array.sal1[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0] order by struct_of_array.state[0]""").collect
}
       

//Complex_StructUsingArray_TC_121
test("Complex_StructUsingArray_TC_121", Include) {
  sql(s"""select struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1],sum(struct_of_array.sno[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1] order by struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_122
test("Complex_StructUsingArray_TC_122", Include) {
  sql(s"""select struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1],avg(struct_of_array.sno[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1] order by struct_of_array.sno[0],struct_of_array.state[1],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_123
test("Complex_StructUsingArray_TC_123", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],min(struct_of_array.state[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_124
test("Complex_StructUsingArray_TC_124", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],min(struct_of_array.sno[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_125
test("Complex_StructUsingArray_TC_125", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],min(struct_of_array.date1[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_126
test("Complex_StructUsingArray_TC_126", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],max(struct_of_array.state[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_127
test("Complex_StructUsingArray_TC_127", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],max(struct_of_array.sno[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_128
test("Complex_StructUsingArray_TC_128", Include) {
  sql(s"""select struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1],max(struct_of_array.date1[0]) a from STRUCT_OF_ARRAY_com group by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1] order by struct_of_array.sno[1],struct_of_array.state[0],struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_130
test("Complex_StructUsingArray_TC_130", Include) {
  sql(s"""select struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1] IN ('England','Dublin')""").collect
}
       

//Complex_StructUsingArray_TC_131
test("Complex_StructUsingArray_TC_131", Include) {
  sql(s"""select struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1] not IN ('England','Dublin')""").collect
}
       

//Complex_StructUsingArray_TC_135
test("Complex_StructUsingArray_TC_135", Include) {
  sql(s"""select Upper(struct_of_array.state[1]) a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_136
test("Complex_StructUsingArray_TC_136", Include) {
  sql(s"""select Lower(struct_of_array.state[0]) a  from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_143
test("Complex_StructUsingArray_TC_143", Include) {
  sql(s"""select struct_of_array.state[0],sum(struct_of_array.id) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0] order by struct_of_array.state[0] desc""").collect
}
       

//Complex_StructUsingArray_TC_144
test("Complex_StructUsingArray_TC_144", Include) {
  sql(s"""select struct_of_array.date1[0],struct_of_array.state[0],sum(struct_of_array.SNo[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0],struct_of_array.date1[0] order by struct_of_array.state[0],struct_of_array.date1[0] desc ,a desc""").collect
}
       

//Complex_StructUsingArray_TC_145
test("Complex_StructUsingArray_TC_145", Include) {
  sql(s"""select struct_of_array.date1[0],struct_of_array.state[0],sum(struct_of_array.sal1[1]) a from STRUCT_OF_ARRAY_com group by struct_of_array.state[0],struct_of_array.date1[0] order by struct_of_array.state[0],struct_of_array.date1[0] desc ,a desc""").collect
}
       

//Complex_StructUsingArray_TC_146
test("Complex_StructUsingArray_TC_146", Include) {
  sql(s"""select struct_of_array.check_date,struct_of_array.SNo[1],struct_of_array.state[2] as a from STRUCT_OF_ARRAY_com  order by a asc limit 10""").collect
}
       

//Complex_StructUsingArray_TC_147
test("Complex_StructUsingArray_TC_147", Include) {
  sql(s"""select struct_of_array.check_date,struct_of_array.sal1[1],struct_of_array.state[2] as a from STRUCT_OF_ARRAY_com  order by a asc limit 10""").collect
}
       

//Complex_StructUsingArray_TC_148
test("Complex_StructUsingArray_TC_148", Include) {
  sql(s"""select struct_of_array.id from STRUCT_OF_ARRAY_com where  (struct_of_array.state[1] =='Aasgaardstrand') and (struct_of_array.check_date=='2017-08-30 00:00:00.0')""").collect
}
       

//Complex_StructUsingArray_TC_149
test("Complex_StructUsingArray_TC_149", Include) {
  sql(s"""select struct_of_array.id,struct_of_array.state[1],struct_of_array.check_date from STRUCT_OF_ARRAY_com where  (struct_of_array.state[1] == 'Cork') and (struct_of_array.id==123457777)""").collect
}
       

//Complex_StructUsingArray_TC_150
test("Complex_StructUsingArray_TC_150", Include) {
  sql(s"""select struct_of_array.id,struct_of_array.state[1],struct_of_array.check_date from STRUCT_OF_ARRAY_com where  (struct_of_array.state[1] == 'Abu Dhabi') or (struct_of_array.id==123457777)""").collect
}
       

//Complex_StructUsingArray_TC_151
test("Complex_StructUsingArray_TC_151", Include) {
  sql(s"""select struct_of_array.id,struct_of_array.state[1],struct_of_array.check_date  from STRUCT_OF_ARRAY_com where (struct_of_array.state[1] == 'Abu Dhabi' and struct_of_array.id==123457777) OR (struct_of_array.state[1] =='Aasgaardstrand' or struct_of_array.check_date=='2017-08-30 00:00:00.0')""").collect
}
       

//Complex_StructUsingArray_TC_152
test("Complex_StructUsingArray_TC_152", Include) {
  sql(s"""select struct_of_array.id,struct_of_array.state[1],struct_of_array.check_date  from STRUCT_OF_ARRAY_com where (struct_of_array.state[1] == 'Abu Dhabi') and (struct_of_array.id==123457777) OR (struct_of_array.state[1] =='Aasgaardstrand' )and (struct_of_array.check_date=='2017-08-30 00:00:00:0')""").collect
}
       

//Complex_StructUsingArray_TC_153
test("Complex_StructUsingArray_TC_153", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='Abu Dhabi' and  struct_of_array.check_date=='2017-08-30 00:00:00.0'""").collect
}
       

//Complex_StructUsingArray_TC_154
test("Complex_StructUsingArray_TC_154", Include) {
  sql(s"""select struct_of_array.sal1[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='Abu Dhabi' and  struct_of_array.check_date=='2017-08-30 00:00:00.0'""").collect
}
       

//Complex_StructUsingArray_TC_155
test("Complex_StructUsingArray_TC_155", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='England'""").collect
}
       

//Complex_StructUsingArray_TC_156
test("Complex_StructUsingArray_TC_156", Include) {
  sql(s"""select struct_of_array.Sal1[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='England'""").collect
}
       

//Complex_StructUsingArray_TC_157
test("Complex_StructUsingArray_TC_157", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='Abu Dhabi' or  struct_of_array.id!=5702""").collect
}
       

//Complex_StructUsingArray_TC_158
test("Complex_StructUsingArray_TC_158", Include) {
  sql(s"""select struct_of_array.Sal1[1],struct_of_array.state[1],struct_of_array.date1[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1]!='Abu Dhabi' or  struct_of_array.id!=5702""").collect
}
       

//Complex_StructUsingArray_TC_159
test("Complex_StructUsingArray_TC_159", Include) {
  sql(s"""select struct_of_array.SNo[1] as a from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]<=>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_160
test("Complex_StructUsingArray_TC_160", Include) {
  sql(s"""select struct_of_array.SNo[1] as a from STRUCT_OF_ARRAY_com where struct_of_array.sal1[1]<=>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_161
test("Complex_StructUsingArray_TC_161", Include) {
  sql(s"""select * from (select if( struct_of_array.SNo[1]=5702,NULL,struct_of_array.SNo[1]) as ab,NULL a from STRUCT_OF_ARRAY_com) qq where ab<=>a""").collect
}
       

//Complex_StructUsingArray_TC_162
test("Complex_StructUsingArray_TC_162", Include) {
  sql(s"""select * from (select if( struct_of_array.sal1[1]=5702,NULL,struct_of_array.SNo[1]) as ab,NULL a from STRUCT_OF_ARRAY_com) qq where ab<=>a""").collect
}
       

//Complex_StructUsingArray_TC_163
test("Complex_StructUsingArray_TC_163", Include) {
  sql(s"""select struct_of_array.SNo[1]  from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]<>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_164
test("Complex_StructUsingArray_TC_164", Include) {
  sql(s"""select struct_of_array.sal1[1]  from STRUCT_OF_ARRAY_com where struct_of_array.sal1[1]<>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_165
test("Complex_StructUsingArray_TC_165", Include) {
  sql(s"""select struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] != cust_id""").collect
}
       

//Complex_StructUsingArray_TC_166
test("Complex_StructUsingArray_TC_166", Include) {
  sql(s"""select struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] != cust_id""").collect
}
       

//Complex_StructUsingArray_TC_167
test("Complex_StructUsingArray_TC_167", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]<cust_id""").collect
}
       

//Complex_StructUsingArray_TC_168
test("Complex_StructUsingArray_TC_168", Include) {
  sql(s"""select struct_of_array.sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.sal1[1]<cust_id""").collect
}
       

//Complex_StructUsingArray_TC_169
test("Complex_StructUsingArray_TC_169", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]<=cust_id""").collect
}
       

//Complex_StructUsingArray_TC_170
test("Complex_StructUsingArray_TC_170", Include) {
  sql(s"""select struct_of_array.sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1]<=cust_id""").collect
}
       

//Complex_StructUsingArray_TC_171
test("Complex_StructUsingArray_TC_171", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_172
test("Complex_StructUsingArray_TC_172", Include) {
  sql(s"""select struct_of_array.Sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1]>cust_id""").collect
}
       

//Complex_StructUsingArray_TC_173
test("Complex_StructUsingArray_TC_173", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>=cust_id""").collect
}
       

//Complex_StructUsingArray_TC_174
test("Complex_StructUsingArray_TC_174", Include) {
  sql(s"""select struct_of_array.Sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1]>=cust_id""").collect
}
       

//Complex_StructUsingArray_TC_175
test("Complex_StructUsingArray_TC_175", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] NOT BETWEEN month AND  year""").collect
}
       

//Complex_StructUsingArray_TC_176
test("Complex_StructUsingArray_TC_176", Include) {
  sql(s"""select struct_of_array.Sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] NOT BETWEEN month AND  year""").collect
}
       

//Complex_StructUsingArray_TC_177
test("Complex_StructUsingArray_TC_177", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] BETWEEN month AND  year""").collect
}
       

//Complex_StructUsingArray_TC_178
test("Complex_StructUsingArray_TC_178", Include) {
  sql(s"""select struct_of_array.Sal1[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] BETWEEN month AND  year""").collect
}
       

//Complex_StructUsingArray_TC_179
test("Complex_StructUsingArray_TC_179", Include) {
  sql(s"""select struct_of_array.SNo[1], struct_of_array.state[1] from STRUCT_OF_ARRAY_com where struct_of_array.state[1] IS NULL""").collect
}
       

//Complex_StructUsingArray_TC_180
test("Complex_StructUsingArray_TC_180", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0] from STRUCT_OF_ARRAY_com where struct_of_array.state[1] IS NOT NULL""").collect
}
       

//Complex_StructUsingArray_TC_181
test("Complex_StructUsingArray_TC_181", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] IS NOT NULL""").collect
}
       

//Complex_StructUsingArray_TC_182
test("Complex_StructUsingArray_TC_182", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0],struct_of_array.date1[0] from STRUCT_OF_ARRAY_com where struct_of_array.date1[0] IS NOT NULL""").collect
}
       

//Complex_StructUsingArray_TC_183
test("Complex_StructUsingArray_TC_183", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0],struct_of_array.date1[0] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] NOT LIKE struct_of_array.state[1] AND struct_of_array.date1[0] NOT LIKE  cust_id""").collect
}
       

//Complex_StructUsingArray_TC_184
test("Complex_StructUsingArray_TC_184", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0],struct_of_array.date1[0] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]  LIKE struct_of_array.SNo[1] and struct_of_array.state[1] like struct_of_array.date1[0] """).collect
}
       

//Complex_StructUsingArray_TC_185
test("Complex_StructUsingArray_TC_185", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] >505""").collect
}
       

//Complex_StructUsingArray_TC_186
test("Complex_StructUsingArray_TC_186", Include) {
  sql(s"""select struct_of_array.Sal1[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] >505""").collect
}
       

//Complex_StructUsingArray_TC_187
test("Complex_StructUsingArray_TC_187", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] <505""").collect
}
       

//Complex_StructUsingArray_TC_188
test("Complex_StructUsingArray_TC_188", Include) {
  sql(s"""select struct_of_array.Sal1[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] <505""").collect
}
       

//Complex_StructUsingArray_TC_189
test("Complex_StructUsingArray_TC_189", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.date1[0] <'2011-07-01 00:00:00'""").collect
}
       

//Complex_StructUsingArray_TC_190
test("Complex_StructUsingArray_TC_190", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.date1[0] >'2011-07-01 00:00:00'""").collect
}
       

//Complex_StructUsingArray_TC_191
test("Complex_StructUsingArray_TC_191", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] >=505""").collect
}
       

//Complex_StructUsingArray_TC_192
test("Complex_StructUsingArray_TC_192", Include) {
  sql(s"""select struct_of_array.Sal1[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] >=505""").collect
}
       

//Complex_StructUsingArray_TC_193
test("Complex_StructUsingArray_TC_193", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] <=505""").collect
}
       

//Complex_StructUsingArray_TC_194
test("Complex_StructUsingArray_TC_194", Include) {
  sql(s"""select struct_of_array.Sal1[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] <=505""").collect
}
       

//Complex_StructUsingArray_TC_195
test("Complex_StructUsingArray_TC_195", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] <=2""").collect
}
       

//Complex_StructUsingArray_TC_196
test("Complex_StructUsingArray_TC_196", Include) {
  sql(s"""select struct_of_array.Sal1[1] from STRUCT_OF_ARRAY_com where struct_of_array.Sal1[1] <=2""").collect
}
       

//Complex_StructUsingArray_TC_197
test("Complex_StructUsingArray_TC_197", Include) {
  sql(s"""select struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] >=2""").collect
}
       

//Complex_StructUsingArray_TC_198
test("Complex_StructUsingArray_TC_198", Include) {
  sql(s"""select sum(struct_of_array.id) a from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] >=10 OR (struct_of_array.SNo[1] <=1 and struct_of_array.state[0]='England')""").collect
}
       

//Complex_StructUsingArray_TC_199
test("Complex_StructUsingArray_TC_199", Include) {
  sql(s"""select * from (select struct_of_array.state[0],if(struct_of_array.state[0]='England',NULL,struct_of_array.state[0]) a from STRUCT_OF_ARRAY_com) aa  where a IS not NULL""").collect
}
       

//Complex_StructUsingArray_TC_200
test("Complex_StructUsingArray_TC_200", Include) {
  sql(s"""select * from (select struct_of_array.state[0],if(struct_of_array.state[0]='England',NULL,struct_of_array.state[0]) a from STRUCT_OF_ARRAY_com) aa  where a IS  NULL""").collect
}
       

//Complex_StructUsingArray_TC_201
test("Complex_StructUsingArray_TC_201", Include) {
  sql(s"""select struct_of_array.id from STRUCT_OF_ARRAY_com where  (struct_of_array.SNo[1] == 5754) or (struct_of_array.date1[0]=='2016-09-06 00:00:00.0')""").collect
}
       

//Complex_StructUsingArray_TC_202
test("Complex_StructUsingArray_TC_202", Include) {
  sql(s"""select struct_of_array.id from STRUCT_OF_ARRAY_com where  (struct_of_array.SNo[1] == 5754) and (struct_of_array.date1[0]=='2016-09-06 00:00:00.0')""").collect
}
       

//Complex_StructUsingArray_TC_203
test("Complex_StructUsingArray_TC_203", Include) {
  sql(s"""select struct_of_array.state[1]  from STRUCT_OF_ARRAY_com where  (struct_of_array.state[1] like '%an%') OR ( struct_of_array.state[1] like '%t%') order by struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_204
test("Complex_StructUsingArray_TC_204", Include) {
  sql(s"""select cust_id,struct_of_array.check_date, struct_of_array.state[1],struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where  struct_of_array.SNo[1] BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_StructUsingArray_TC_205
test("Complex_StructUsingArray_TC_205", Include) {
  sql(s"""select cust_id,struct_of_array.check_date, struct_of_array.state[1],struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where  struct_of_array.id BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_StructUsingArray_TC_206
test("Complex_StructUsingArray_TC_206", Include) {
  sql(s"""select cust_id,struct_of_array.check_date, struct_of_array.state[1],struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where  struct_of_array.id not BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_StructUsingArray_TC_207
test("Complex_StructUsingArray_TC_207", Include) {
  sql(s"""select cust_id,struct_of_array.check_date, struct_of_array.state[1],struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where  struct_of_array.state[1] NOT LIKE '%r%' AND struct_of_array.SNo[1] NOT LIKE 5 ORDER BY struct_of_array.check_date limit 5""").collect
}
       

//Complex_StructUsingArray_TC_208
test("Complex_StructUsingArray_TC_208", Include) {
  sql(s"""select cust_id,struct_of_array.check_date, struct_of_array.state[1],struct_of_array.SNo[1] from STRUCT_OF_ARRAY_com where  struct_of_array.state[1] RLIKE '%t%' ORDER BY struct_of_array.SNo[1] limit 5""").collect
}
       

//Complex_StructUsingArray_TC_210
test("Complex_StructUsingArray_TC_210", Include) {
  sql(s"""select  cust_id,struct_of_array.sno[1] from STRUCT_OF_ARRAY_com where UPPER(struct_of_array.state[1]) == 'DUBLIN'""").collect
}
       

//Complex_StructUsingArray_TC_211
test("Complex_StructUsingArray_TC_211", Include) {
  sql(s"""select  cust_id,struct_of_array.sno[1] from STRUCT_OF_ARRAY_com where UPPER(struct_of_array.state[1]) in ('DUBLIN','England','SA')""").collect
}
       

//Complex_StructUsingArray_TC_212
test("Complex_StructUsingArray_TC_212", Include) {
  sql(s"""select  cust_id,struct_of_array.sno[1] from STRUCT_OF_ARRAY_com where UPPER(struct_of_array.state[1]) like '%A%'""").collect
}
       

//Complex_StructUsingArray_TC_213
test("Complex_StructUsingArray_TC_213", Include) {
  sql(s"""select count(struct_of_array.sno[1]) ,struct_of_array.state[1] from STRUCT_OF_ARRAY_com group by struct_of_array.state[1],struct_of_array.sno[1] having sum (struct_of_array.sno[1])= 9150500""").collect
}
       

//Complex_StructUsingArray_TC_214
test("Complex_StructUsingArray_TC_214", Include) {
  sql(s"""select count(struct_of_array.sno[1]) ,struct_of_array.state[1] from STRUCT_OF_ARRAY_com group by struct_of_array.state[1],struct_of_array.sno[1] having sum (struct_of_array.sno[1])>= 9150""").collect
}
       

//Complex_StructUsingArray_TC_215
test("Complex_StructUsingArray_TC_215", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1], struct_of_array.date1[1] ASC, struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_216
test("Complex_StructUsingArray_TC_216", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1], struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_218
test("Complex_StructUsingArray_TC_218", Include) {
  sql(s"""select struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY WHERE not(struct_of_array.state[1] = "") or not(struct_of_array.SNo[1]= "") or not(struct_of_array.check_date= "") GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1] ASC, struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_219
test("Complex_StructUsingArray_TC_219", Include) {
  sql(s"""select struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY WHERE (struct_of_array.state[1] > "") or (struct_of_array.SNo[1]>"") or (struct_of_array.check_date> "") GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1] ASC, struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_220
test("Complex_StructUsingArray_TC_220", Include) {
  sql(s"""select struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY WHERE (struct_of_array.state[1] >='""') or (struct_of_array.SNo[1]>= '""') or (struct_of_array.check_date>='""') GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1] ASC, struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_223
test("Complex_StructUsingArray_TC_223", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1], SUM(struct_of_array.sno[1]) AS Sum_array_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY where (struct_of_array.SNo[1] < 5700) or (struct_of_array.date1[0] like'%2015%') GROUP BY struct_of_array.sno[1], struct_of_array.date1[1] ORDER BY struct_of_array.sno[1] ASC, struct_of_array.date1[1] ASC""").collect
}
       

//Complex_StructUsingArray_TC_224
test("Complex_StructUsingArray_TC_224", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], AVG(struct_of_array.sno[1]) AS Avg FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_225
test("Complex_StructUsingArray_TC_225", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], SUM(struct_of_array.SNo[1]) AS sum FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_226
test("Complex_StructUsingArray_TC_226", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], count(struct_of_array.SNo[1]) AS count FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_227
test("Complex_StructUsingArray_TC_227", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], count(struct_of_array.state[1]) AS count FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_228
test("Complex_StructUsingArray_TC_228", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], count(struct_of_array.date1[1]) AS count FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_229
test("Complex_StructUsingArray_TC_229", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], min(struct_of_array.date1[1]) AS min_date,min(struct_of_array.state[0]) as min_string,min(struct_of_array.id) as min_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_230
test("Complex_StructUsingArray_TC_230", Include) {
  sql(s"""SELECT struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1], max(struct_of_array.date1[1]) AS max_date,max(struct_of_array.state[0]) as max_string,max(struct_of_array.id) as max_int FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY GROUP BY struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1] order by struct_of_array.sno[1], struct_of_array.date1[1],struct_of_array.state[1]""").collect
}
       

//Complex_StructUsingArray_TC_232
test("Complex_StructUsingArray_TC_232", Include) {
  sql(s"""select struct_of_array.date1[0],sum(struct_of_array.id+ struct_of_array.SNo[1]) as total from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>5700 and  struct_of_array.date1[0]like'%2015%' group by struct_of_array.date1[0]
""").collect
}
       

//Complex_StructUsingArray_TC_233
test("Complex_StructUsingArray_TC_233", Include) {
  sql(s"""select  struct_of_array.date1[0],sum( struct_of_array.id+ struct_of_array.SNo[1]) as total from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>5700 and  struct_of_array.date1[0]like'%2015%' group by struct_of_array.date1[0] having total > 10 order by total desc""").collect
}
       

//Complex_StructUsingArray_TC_234
test("Complex_StructUsingArray_TC_234", Include) {
  sql(s"""select  struct_of_array.date1[0],sum( struct_of_array.id+ struct_of_array.SNo[1]),count(distinct struct_of_array.SNo[1]) as total from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>5700 and  struct_of_array.date1[0]like'%2015%' group by struct_of_array.date1[0] having total > 10 order by total desc""").collect
}
       

//Complex_StructUsingArray_TC_235
test("Complex_StructUsingArray_TC_235", Include) {
  sql(s"""select struct_of_array.SNo[1],struct_of_array.state[0],struct_of_array.date1[1],count(distinct struct_of_array.SNo[1]) as count,sum( struct_of_array.id+ cust_id) as total from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dublin') group by struct_of_array.SNo[1],struct_of_array.state[0],struct_of_array.date1[1] order by total desc""").collect
}
       

//Complex_StructUsingArray_TC_236
test("Complex_StructUsingArray_TC_236", Include) {
  sql(s"""select count(distinct(year (struct_of_array.check_date))) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] like '%England%'""").collect
}
       

//Complex_StructUsingArray_TC_237
test("Complex_StructUsingArray_TC_237", Include) {
  sql(s"""select year (struct_of_array.check_date) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] like '%England%'""").collect
}
       

//Complex_StructUsingArray_TC_238
test("Complex_StructUsingArray_TC_238", Include) {
  sql(s"""select month (struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dubai')""").collect
}
       

//Complex_StructUsingArray_TC_239
test("Complex_StructUsingArray_TC_239", Include) {
  sql(s"""select day(struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dubai') group by struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_240
test("Complex_StructUsingArray_TC_240", Include) {
  sql(s"""select hour (struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dubai') group by struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_241
test("Complex_StructUsingArray_TC_241", Include) {
  sql(s"""select count (minute (struct_of_array.date1[1])) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dubai') group by struct_of_array.date1[1] order by struct_of_array.date1[1]""").collect
}
       

//Complex_StructUsingArray_TC_242
test("Complex_StructUsingArray_TC_242", Include) {
  sql(s"""select second (struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] in('England','Dubai') group by struct_of_array.date1[1] having count(struct_of_array.date1[1])>=2""").collect
}
       

//Complex_StructUsingArray_TC_243
test("Complex_StructUsingArray_TC_243", Include) {
  sql(s"""select WEEKOFYEAR(struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] like'%a%' group by struct_of_array.date1[1] having count(struct_of_array.date1[1])<=2""").collect
}
       

//Complex_StructUsingArray_TC_244
test("Complex_StructUsingArray_TC_244", Include) {
  sql(s"""select DATEDIFF(struct_of_array.date1[1] ,'2020-08-09 00:00:00')from STRUCT_OF_ARRAY_com where struct_of_array.state[1] like'%a%'""").collect
}
       

//Complex_StructUsingArray_TC_245
test("Complex_StructUsingArray_TC_245", Include) {
  sql(s"""select DATE_ADD(struct_of_array.date1[1] ,5) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] like'%a%'""").collect
}
       

//Complex_StructUsingArray_TC_246
test("Complex_StructUsingArray_TC_246", Include) {
  sql(s"""select DATE_SUB(struct_of_array.date1[1] ,5) from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1] like'%7%'""").collect
}
       

//Complex_StructUsingArray_TC_247
test("Complex_StructUsingArray_TC_247", Include) {
  sql(s"""select unix_timestamp(struct_of_array.date1[1]) from STRUCT_OF_ARRAY_com where struct_of_array.SNo[1]>=5702""").collect
}
       

//Complex_StructUsingArray_TC_248
test("Complex_StructUsingArray_TC_248", Include) {
  sql(s"""select substr(struct_of_array.state[0],3,4) from STRUCT_OF_ARRAY_com""").collect
}
       

//Complex_StructUsingArray_TC_249
test("Complex_StructUsingArray_TC_249", Include) {
  sql(s"""select substr(struct_of_array.state[0],3) from STRUCT_OF_ARRAY_com where struct_of_array.state[1] rlike 'a'""").collect
}
       

//Complex_StructUsingArray_TC_250
test("Complex_StructUsingArray_TC_250", Include) {
  sql(s"""SELECT struct_of_array.state[1],struct_of_array.date1[0],struct_of_array.date1[1],struct_of_array.state[0], SUM(struct_of_array.sno[1]) AS Sum FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY WHERE struct_of_array.SNo[1] > 5700 GROUP BY struct_of_array.state[1],struct_of_array.date1[0],struct_of_array.date1[1],struct_of_array.state[0] ORDER BY struct_of_array.state[1] asc,struct_of_array.date1[0] asc,struct_of_array.date1[1]asc, struct_of_array.state[0]asc""").collect
}
       

//Complex_StructUsingArray_TC_251
test("Complex_StructUsingArray_TC_251", Include) {
  sql(s"""SELECT struct_of_array.state[1],struct_of_array.date1[0],struct_of_array.date1[1],struct_of_array.state[0], SUM(struct_of_array.sno[1]) AS Sum FROM (select * from STRUCT_OF_ARRAY_com) SUB_QRY WHERE struct_of_array.state[1]='England' GROUP BY struct_of_array.state[1],struct_of_array.date1[0],struct_of_array.date1[1],struct_of_array.state[0] ORDER BY struct_of_array.state[1] asc,struct_of_array.date1[0] asc,struct_of_array.date1[1]asc, struct_of_array.state[0]asc""").collect
}
       
override def afterAll {
sql("drop table if exists STRUCT_OF_ARRAY_com")
sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
}
}
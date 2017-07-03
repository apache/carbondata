
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
 * Test Class for ARRAYOFSTRUCTcom to verify all scenerios
 */

class ARRAYOFSTRUCTCOMTestCase extends QueryTest with BeforeAndAfterAll {


//drop_ARRAY_OF_STRUCT_com
test("drop_ARRAY_OF_STRUCT_com", Include) {
  sql(s"""drop table  if exists ARRAY_OF_STRUCT_com""").collect

  sql(s"""drop table  if exists ARRAY_OF_STRUCT_com_hive""").collect

}


//Complex_ArrayUsingStruct_TC_CreateTable
test("Complex_ArrayUsingStruct_TC_CreateTable", Include) {
  sql(s""" create table ARRAY_OF_STRUCT_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_OF_STRUCT array<struct<ID:int,COUNTRY:string,STATE:string,CITI:string,CHECK_DATE:timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s""" create table ARRAY_OF_STRUCT_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_OF_STRUCT array<struct<ID:int,COUNTRY:string,STATE:string,CITI:string,CHECK_DATE:timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}


//Complex_ArrayUsingStruct_TC_DataLoad
test("Complex_ArrayUsingStruct_TC_DataLoad", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/arrayofstruct.csv' INTO table ARRAY_OF_STRUCT_com options ('DELIMITER'=',', 'QUOTECHAR'='"', 'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE,GENDER,EDUCATED,IS_MARRIED,ARRAY_OF_STRUCT,CARD_COUNT,DEBIT_COUNT,CREDIT_COUNT,DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$DOLLAR','COMPLEX_DELIMITER_LEVEL_2'='&')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/arrayofstruct.csv' INTO table ARRAY_OF_STRUCT_com_hive """).collect

}


//Complex_ArrayUsingStruct_TC_001
test("Complex_ArrayUsingStruct_TC_001", Include) {
  sql(s"""select array_of_struct.ID[1], sum(array_of_struct.ID[1]+ 10) as a from ARRAY_OF_STRUCT_com  group by  array_of_struct.ID[1] order by array_of_struct.ID[1]""").collect
}


//Complex_ArrayUsingStruct_TC_002
test("Complex_ArrayUsingStruct_TC_002", Include) {
  sql(s"""select array_of_struct.ID[1], sum(array_of_struct.ID[1]+ 10) as a from ARRAY_OF_STRUCT_com  group by  array_of_struct.ID[1]""").collect
}


//Complex_ArrayUsingStruct_TC_003
test("Complex_ArrayUsingStruct_TC_003", Include) {
  sql(s"""select sum(array_of_struct.ID[1]+ 10),array_of_struct.ID[1]+ 10 as a  from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1] """).collect
}


//Complex_ArrayUsingStruct_TC_004
test("Complex_ArrayUsingStruct_TC_004", Include) {
  sql(s"""select array_of_struct.ID[1]+ 10 as a  from ARRAY_OF_STRUCT_com   """).collect
}


//Complex_ArrayUsingStruct_TC_005
test("Complex_ArrayUsingStruct_TC_005", Include) {
  sql(s"""select array_of_struct.ID[1]+cust_id as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_006
test("Complex_ArrayUsingStruct_TC_006", Include) {
  sql(s"""select array_of_struct.ID[1]+age  as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_007
test("Complex_ArrayUsingStruct_TC_007", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.ID[1]+deposit  as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_008
test("Complex_ArrayUsingStruct_TC_008", Include) {
  sql(s"""select concat(array_of_struct.COUNTRY[0],'_',cust_id)  as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_009
test("Complex_ArrayUsingStruct_TC_009", Include) {
  sql(s"""select concat(array_of_struct.COUNTRY[2],'_',educated)  as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_010
test("Complex_ArrayUsingStruct_TC_010", Include) {
  sql(s"""select concat(array_of_struct.COUNTRY[2],'_',educated,'@',is_married)  as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_011
test("Complex_ArrayUsingStruct_TC_011", Include) {
  sql(s"""select array_of_struct.ID[1], avg(array_of_struct.ID[1]+ 10) as a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1]""").collect
}


//Complex_ArrayUsingStruct_TC_012
test("Complex_ArrayUsingStruct_TC_012", Include) {
  sql(s"""select array_of_struct.CITI[2],array_of_struct.ID[1],avg(deposit+ array_of_struct.ID[1]) Total from ARRAY_OF_STRUCT_com group by  array_of_struct.CITI[2],array_of_struct.ID[1] order by array_of_struct.CITI[2],array_of_struct.ID[1]""").collect
}


//Complex_ArrayUsingStruct_TC_013
test("Complex_ArrayUsingStruct_TC_013", Include) {
  sql(s"""select array_of_struct.CHECK_DATE[0], count(array_of_struct.ID[1]+deposit) Total from ARRAY_OF_STRUCT_com group by  array_of_struct.CHECK_DATE[0] order by array_of_struct.CHECK_DATE[0]""").collect
}


//Complex_ArrayUsingStruct_TC_014
test("Complex_ArrayUsingStruct_TC_014", Include) {
  sql(s"""select array_of_struct.CHECK_DATE[0], count(array_of_struct.ID[1]) Total from ARRAY_OF_STRUCT_com group by  array_of_struct.CHECK_DATE[0]""").collect
}


//Complex_ArrayUsingStruct_TC_059
test("Complex_ArrayUsingStruct_TC_059", Include) {
  sql(s"""select var_pop(array_of_struct.ID[1])  as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_060
test("Complex_ArrayUsingStruct_TC_060", Include) {
  sql(s"""select var_samp(array_of_struct.ID[1]) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_061
test("Complex_ArrayUsingStruct_TC_061", Include) {
  sql(s"""select stddev_pop(array_of_struct.ID[1]) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_062
test("Complex_ArrayUsingStruct_TC_062", Include) {
  sql(s"""select stddev_samp(array_of_struct.ID[1])  as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_063
test("Complex_ArrayUsingStruct_TC_063", Include) {
  sql(s"""select covar_pop(array_of_struct.ID[1],array_of_struct.ID[1]) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_064
test("Complex_ArrayUsingStruct_TC_064", Include) {
  sql(s"""select covar_samp(array_of_struct.ID[1],array_of_struct.ID[1]) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_065
test("Complex_ArrayUsingStruct_TC_065", Include) {
  sql(s"""select corr(array_of_struct.ID[1],array_of_struct.ID[1])  as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_066
test("Complex_ArrayUsingStruct_TC_066", Include) {
  sql(s"""select percentile(array_of_struct.ID[1],0.2) as  a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_067
test("Complex_ArrayUsingStruct_TC_067", Include) {
  sql(s"""select percentile(array_of_struct.ID[1],array(0,0.2,0.3,1))  as  a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_068
test("Complex_ArrayUsingStruct_TC_068", Include) {
  sql(s"""select percentile_approx(array_of_struct.ID[1],0.2) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_069
test("Complex_ArrayUsingStruct_TC_069", Include) {
  sql(s"""select percentile_approx(array_of_struct.ID[1],0.2,5) as a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_070
test("Complex_ArrayUsingStruct_TC_070", Include) {
  sql(s"""select percentile_approx(array_of_struct.ID[1],array(0.2,0.3,0.99))  as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_071
test("Complex_ArrayUsingStruct_TC_071", Include) {
  sql(s"""select percentile_approx(array_of_struct.ID[1],array(0.2,0.3,0.99),5) as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_072
test("Complex_ArrayUsingStruct_TC_072", Include) {
  sql(s"""select corr(array_of_struct.ID[1],array_of_struct.ID[1])  as a from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_077
test("Complex_ArrayUsingStruct_TC_077", Include) {
  sql(s"""select count(distinct array_of_struct.state[0]) a,array_of_struct.ID[1] from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1]""").collect
}


//Complex_ArrayUsingStruct_TC_079
test("Complex_ArrayUsingStruct_TC_079", Include) {
  sql(s"""select array_of_struct.state[0],sum(array_of_struct.ID[1]) a from ARRAY_OF_STRUCT_com group by array_of_struct.state[0] order by array_of_struct.state[0]""").collect
}


//Complex_ArrayUsingStruct_TC_080
test("Complex_ArrayUsingStruct_TC_080", Include) {
  sql(s"""select array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1],sum(array_of_struct.ID[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_081
test("Complex_ArrayUsingStruct_TC_081", Include) {
  sql(s"""select array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1],avg(array_of_struct.ID[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[0],array_of_struct.state[1],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_082
test("Complex_ArrayUsingStruct_TC_082", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],min(array_of_struct.state[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_083
test("Complex_ArrayUsingStruct_TC_083", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],min(array_of_struct.ID[1]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_084
test("Complex_ArrayUsingStruct_TC_084", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],min(array_of_struct.CHECK_DATE[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_085
test("Complex_ArrayUsingStruct_TC_085", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],max(array_of_struct.state[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_086
test("Complex_ArrayUsingStruct_TC_086", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],max(array_of_struct.ID[1]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_087
test("Complex_ArrayUsingStruct_TC_087", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1],max(array_of_struct.CHECK_DATE[0]) a from ARRAY_OF_STRUCT_com group by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1] order by array_of_struct.ID[1],array_of_struct.state[0],array_of_struct.CHECK_DATE[1]""").collect
}


//Complex_ArrayUsingStruct_TC_089
test("Complex_ArrayUsingStruct_TC_089", Include) {
  sql(s"""select array_of_struct.Country[1],array_of_struct.CHECK_DATE[0] from ARRAY_OF_STRUCT_com where array_of_struct.Country[1] IN ('England','India')""").collect
}


//Complex_ArrayUsingStruct_TC_090
test("Complex_ArrayUsingStruct_TC_090", Include) {
  sql(s"""select array_of_struct.Country[1],array_of_struct.CHECK_DATE[0] from ARRAY_OF_STRUCT_com where array_of_struct.Country[1] not IN ('England','India')""").collect
}


//Complex_ArrayUsingStruct_TC_093
test("Complex_ArrayUsingStruct_TC_093", Include) {
  sql(s"""select Upper(array_of_struct.Country[1]) a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_094
test("Complex_ArrayUsingStruct_TC_094", Include) {
  sql(s"""select Lower(array_of_struct.Country[0]) a  from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_099
test("Complex_ArrayUsingStruct_TC_099", Include) {
  sql(s"""select array_of_struct.COUNTRY[0],sum(array_of_struct.ID[2]) a from ARRAY_OF_STRUCT_com group by array_of_struct.COUNTRY[0] order by array_of_struct.COUNTRY[0] desc""").collect
}


//Complex_ArrayUsingStruct_TC_100
test("Complex_ArrayUsingStruct_TC_100", Include) {
  sql(s"""select  array_of_struct.CHECK_DATE[1],array_of_struct.COUNTRY[0],sum(array_of_struct.ID[1]) a from ARRAY_OF_STRUCT_com group by array_of_struct.COUNTRY[0], array_of_struct.CHECK_DATE[1] order by array_of_struct.COUNTRY[0], array_of_struct.CHECK_DATE[1] desc ,a desc""").collect
}


//Complex_ArrayUsingStruct_TC_101
test("Complex_ArrayUsingStruct_TC_101", Include) {
  sql(s"""select array_of_struct.CHECK_DATE[2],array_of_struct.ID[1],array_of_struct.Citi[2] as a from ARRAY_OF_STRUCT_com  order by a asc limit 10""").collect
}


//Complex_ArrayUsingStruct_TC_102
test("Complex_ArrayUsingStruct_TC_102", Include) {
  sql(s"""select array_of_struct.ID[2] from ARRAY_OF_STRUCT_com where  (array_of_struct.citi[2] =='Gaomi') and (array_of_struct.CHECK_DATE[2]=='2022-05-19 00:00:00.0')""").collect
}


//Complex_ArrayUsingStruct_TC_103
test("Complex_ArrayUsingStruct_TC_103", Include) {
  sql(s"""select array_of_struct.ID[2],array_of_struct.citi[2],array_of_struct.CHECK_DATE[2] from ARRAY_OF_STRUCT_com where  (array_of_struct.citi[2] == 'Allahabad') and (array_of_struct.ID[2]==123459228)""").collect
}


//Complex_ArrayUsingStruct_TC_104
test("Complex_ArrayUsingStruct_TC_104", Include) {
  sql(s"""select array_of_struct.ID[2],array_of_struct.citi[2],array_of_struct.CHECK_DATE[2] from ARRAY_OF_STRUCT_com where  (array_of_struct.citi[2] == 'Allahabad') or (array_of_struct.ID[2]==123459228)""").collect
}


//Complex_ArrayUsingStruct_TC_105
test("Complex_ArrayUsingStruct_TC_105", Include) {
  sql(s"""select array_of_struct.ID[2],array_of_struct.citi[2],array_of_struct.CHECK_DATE[2]  from ARRAY_OF_STRUCT_com where (array_of_struct.citi[2] == 'Allahabad' and array_of_struct.ID[2]==123459823) OR (array_of_struct.citi[2] =='Allahabad' or array_of_struct.CHECK_DATE[2]=='2020-11-14 00:00:00.0')""").collect
}


//Complex_ArrayUsingStruct_TC_106
test("Complex_ArrayUsingStruct_TC_106", Include) {
  sql(s"""select array_of_struct.ID[2],array_of_struct.citi[2],array_of_struct.CHECK_DATE[2]  from ARRAY_OF_STRUCT_com where (array_of_struct.citi[2] == 'Allahabad' and array_of_struct.ID[2]==123459823) OR (array_of_struct.citi[2] =='Allahabad' and array_of_struct.CHECK_DATE[2]=='2020-11-14 00:00:00.0')""").collect
}


//Complex_ArrayUsingStruct_TC_107
test("Complex_ArrayUsingStruct_TC_107", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.citi[2], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where array_of_struct.citi[2]!='Allahabad' and  array_of_struct.CHECK_DATE[2]=='2020-11-14 00:00:00.0'""").collect
}


//Complex_ArrayUsingStruct_TC_108
test("Complex_ArrayUsingStruct_TC_108", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[1], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[1]!='India'""").collect
}


//Complex_ArrayUsingStruct_TC_109
test("Complex_ArrayUsingStruct_TC_109", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.citi[2], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where array_of_struct.citi[2]!='Allahabad' or  array_of_struct.ID[2]!=123459604""").collect
}


//Complex_ArrayUsingStruct_TC_110
test("Complex_ArrayUsingStruct_TC_110", Include) {
  sql(s"""select array_of_struct.ID[1] as a from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]<=>cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_112
test("Complex_ArrayUsingStruct_TC_112", Include) {
  sql(s"""select array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] != cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_113
test("Complex_ArrayUsingStruct_TC_113", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]<cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_114
test("Complex_ArrayUsingStruct_TC_114", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]<=cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_115
test("Complex_ArrayUsingStruct_TC_115", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_116
test("Complex_ArrayUsingStruct_TC_116", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>=cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_117
test("Complex_ArrayUsingStruct_TC_117", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] NOT BETWEEN month AND  year""").collect
}


//Complex_ArrayUsingStruct_TC_118
test("Complex_ArrayUsingStruct_TC_118", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] BETWEEN month AND  year""").collect
}


//Complex_ArrayUsingStruct_TC_119
test("Complex_ArrayUsingStruct_TC_119", Include) {
  sql(s"""select array_of_struct.ID[1], array_of_struct.COUNTRY[1] from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[1] IS NULL""").collect
}


//Complex_ArrayUsingStruct_TC_120
test("Complex_ArrayUsingStruct_TC_120", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[0] from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[1] IS NOT NULL""").collect
}


//Complex_ArrayUsingStruct_TC_121
test("Complex_ArrayUsingStruct_TC_121", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[0] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] IS NOT NULL""").collect
}


//Complex_ArrayUsingStruct_TC_122
test("Complex_ArrayUsingStruct_TC_122", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[0], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where  array_of_struct.CHECK_DATE[1] IS NOT NULL""").collect
}


//Complex_ArrayUsingStruct_TC_123
test("Complex_ArrayUsingStruct_TC_123", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[0], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] NOT LIKE array_of_struct.COUNTRY[1] AND  array_of_struct.CHECK_DATE[1] NOT LIKE  cust_id""").collect
}


//Complex_ArrayUsingStruct_TC_124
test("Complex_ArrayUsingStruct_TC_124", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.COUNTRY[0], array_of_struct.CHECK_DATE[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]  LIKE array_of_struct.ID[1] and array_of_struct.COUNTRY[1] like  array_of_struct.CHECK_DATE[1] """).collect
}


//Complex_ArrayUsingStruct_TC_125
test("Complex_ArrayUsingStruct_TC_125", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] >505""").collect
}


//Complex_ArrayUsingStruct_TC_126
test("Complex_ArrayUsingStruct_TC_126", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] <505""").collect
}


//Complex_ArrayUsingStruct_TC_127
test("Complex_ArrayUsingStruct_TC_127", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.CHECK_DATE[1] <'2018-11-05 00:00:00.0'""").collect
}


//Complex_ArrayUsingStruct_TC_128
test("Complex_ArrayUsingStruct_TC_128", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.CHECK_DATE[1] >'2018-11-05 00:00:00.0'""").collect
}


//Complex_ArrayUsingStruct_TC_129
test("Complex_ArrayUsingStruct_TC_129", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] >=505""").collect
}


//Complex_ArrayUsingStruct_TC_130
test("Complex_ArrayUsingStruct_TC_130", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] <=505""").collect
}


//Complex_ArrayUsingStruct_TC_131
test("Complex_ArrayUsingStruct_TC_131", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] <=2""").collect
}


//Complex_ArrayUsingStruct_TC_132
test("Complex_ArrayUsingStruct_TC_132", Include) {
  sql(s"""select array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] >=2""").collect
}


//Complex_ArrayUsingStruct_TC_133
test("Complex_ArrayUsingStruct_TC_133", Include) {
  sql(s"""select sum(array_of_struct.ID[2]) a from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] >=10 OR (array_of_struct.ID[1] <=1 and array_of_struct.COUNTRY[0]='India')""").collect
}


//Complex_ArrayUsingStruct_TC_134
test("Complex_ArrayUsingStruct_TC_134", Include) {
  sql(s"""select * from (select array_of_struct.COUNTRY[0],if(array_of_struct.COUNTRY[0]='India',NULL,array_of_struct.COUNTRY[0]) a from ARRAY_OF_STRUCT_com) aa  where a IS not NULL""").collect
}


//Complex_ArrayUsingStruct_TC_135
test("Complex_ArrayUsingStruct_TC_135", Include) {
  sql(s"""select * from (select array_of_struct.COUNTRY[0],if(array_of_struct.COUNTRY[0]='India',NULL,array_of_struct.COUNTRY[0]) a from ARRAY_OF_STRUCT_com) aa  where a IS  NULL""").collect
}


//Complex_ArrayUsingStruct_TC_136
test("Complex_ArrayUsingStruct_TC_136", Include) {
  sql(s"""select array_of_struct.ID[2] from ARRAY_OF_STRUCT_com where  (array_of_struct.ID[1] == 123459604) or ( array_of_struct.CHECK_DATE[1]=='2018-11-05 00:00:00.0')""").collect
}


//Complex_ArrayUsingStruct_TC_137
test("Complex_ArrayUsingStruct_TC_137", Include) {
  sql(s"""select array_of_struct.ID[2] from ARRAY_OF_STRUCT_com where  (array_of_struct.ID[1] == 123459604) and ( array_of_struct.CHECK_DATE[1]=='2018-11-05 00:00:00.0')""").collect
}


//Complex_ArrayUsingStruct_TC_138
test("Complex_ArrayUsingStruct_TC_138", Include) {
  sql(s"""select cust_id,array_of_struct.CHECK_DATE[2], array_of_struct.COUNTRY[2],array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.ID[1] BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}


//Complex_ArrayUsingStruct_TC_139
test("Complex_ArrayUsingStruct_TC_139", Include) {
  sql(s"""select cust_id,array_of_struct.CHECK_DATE[2], array_of_struct.COUNTRY[2],array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.ID[1] BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}


//Complex_ArrayUsingStruct_TC_140
test("Complex_ArrayUsingStruct_TC_140", Include) {
  sql(s"""select cust_id,array_of_struct.CHECK_DATE[2], array_of_struct.COUNTRY[2],array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.ID[1] not BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}


//Complex_ArrayUsingStruct_TC_141
test("Complex_ArrayUsingStruct_TC_141", Include) {
  sql(s"""select cust_id,array_of_struct.CHECK_DATE[2], array_of_struct.COUNTRY[2],array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.COUNTRY[2] NOT LIKE '%r%' AND array_of_struct.ID[1] NOT LIKE 5 ORDER BY array_of_struct.CHECK_DATE[2] limit 5""").collect
}


//Complex_ArrayUsingStruct_TC_142
test("Complex_ArrayUsingStruct_TC_142", Include) {
  sql(s"""select cust_id,array_of_struct.CHECK_DATE[2], array_of_struct.COUNTRY[2],array_of_struct.ID[1] from ARRAY_OF_STRUCT_com where  array_of_struct.COUNTRY[2] RLIKE '%t%' ORDER BY array_of_struct.ID[1] limit 5""").collect
}


//Complex_ArrayUsingStruct_TC_144
test("Complex_ArrayUsingStruct_TC_144", Include) {
  sql(s"""select  cust_id,array_of_struct.ID[1]  from ARRAY_OF_STRUCT_com where UPPER(array_of_struct.COUNTRY[2]) == 'DUBLIN'""").collect
}


//Complex_ArrayUsingStruct_TC_145
test("Complex_ArrayUsingStruct_TC_145", Include) {
  sql(s"""select  cust_id,array_of_struct.ID[2] from ARRAY_OF_STRUCT_com where UPPER(array_of_struct.COUNTRY[2]) in ('DUBLIN','England','India')""").collect
}


//Complex_ArrayUsingStruct_TC_146
test("Complex_ArrayUsingStruct_TC_146", Include) {
  sql(s"""select  cust_id,array_of_struct.ID[2] from ARRAY_OF_STRUCT_com where UPPER(array_of_struct.COUNTRY[2]) like '%A%'""").collect
}


//Complex_ArrayUsingStruct_TC_147
test("Complex_ArrayUsingStruct_TC_147", Include) {
  sql(s"""select count(array_of_struct.ID[2]) ,array_of_struct.COUNTRY[2] from ARRAY_OF_STRUCT_com group by array_of_struct.COUNTRY[2],array_of_struct.ID[2] having sum (array_of_struct.ID[2])= 9150500""").collect
}


//Complex_ArrayUsingStruct_TC_148
test("Complex_ArrayUsingStruct_TC_148", Include) {
  sql(s"""select count(array_of_struct.ID[2]) ,array_of_struct.COUNTRY[2] from ARRAY_OF_STRUCT_com group by array_of_struct.COUNTRY[2],array_of_struct.ID[2] having sum (array_of_struct.ID[2])>= 9150""").collect
}


//Complex_ArrayUsingStruct_TC_149
test("Complex_ArrayUsingStruct_TC_149", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_151
test("Complex_ArrayUsingStruct_TC_151", Include) {
  sql(s"""select array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE (array_of_struct.COUNTRY[2] = "") or (array_of_struct.ID[1]= "") or (array_of_struct.CHECK_DATE[1]= "") GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_152
test("Complex_ArrayUsingStruct_TC_152", Include) {
  sql(s"""select array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE not(array_of_struct.COUNTRY[2] = "") or not(array_of_struct.ID[1]= "") or not(array_of_struct.CHECK_DATE[1]= "") GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_153
test("Complex_ArrayUsingStruct_TC_153", Include) {
  sql(s"""select array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE (array_of_struct.COUNTRY[2] > "") or (array_of_struct.ID[1]>"") or (array_of_struct.CHECK_DATE[1]> "") GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_154
test("Complex_ArrayUsingStruct_TC_154", Include) {
  sql(s"""select array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE (array_of_struct.COUNTRY[2] >="\"\"\") or (array_of_struct.ID[1]>= "\"\"") or (array_of_struct.CHECK_DATE[1]>="\"\"") GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_157
test("Complex_ArrayUsingStruct_TC_157", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2], SUM(array_of_struct.ID[2]) AS Sum_array_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY where (array_of_struct.ID[1] < 5700) or (array_of_struct.CHECK_DATE[0] like'%2018%') GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2] ORDER BY array_of_struct.ID[2] ASC, array_of_struct.CHECK_DATE[2] ASC""").collect
}


//Complex_ArrayUsingStruct_TC_158
test("Complex_ArrayUsingStruct_TC_158", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], AVG(array_of_struct.ID[2]) AS Avg FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_159
test("Complex_ArrayUsingStruct_TC_159", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], SUM(array_of_struct.ID[1]) AS sum FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_160
test("Complex_ArrayUsingStruct_TC_160", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], count(array_of_struct.ID[1]) AS count FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_161
test("Complex_ArrayUsingStruct_TC_161", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], count(array_of_struct.COUNTRY[2]) AS count FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_162
test("Complex_ArrayUsingStruct_TC_162", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], count(array_of_struct.CHECK_DATE[2]) AS count FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_163
test("Complex_ArrayUsingStruct_TC_163", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], min(array_of_struct.CHECK_DATE[2]) AS min_date,min(array_of_struct.CITI[0]) as min_string,min(array_of_struct.ID[0]) as min_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_164
test("Complex_ArrayUsingStruct_TC_164", Include) {
  sql(s"""SELECT array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2], max(array_of_struct.CHECK_DATE[2]) AS max_date,max(array_of_struct.CITI[0]) as max_string,max(array_of_struct.ID[0]) as max_int FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY GROUP BY array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2] order by array_of_struct.ID[2], array_of_struct.CHECK_DATE[2],array_of_struct.COUNTRY[2]""").collect
}


//Complex_ArrayUsingStruct_TC_166
test("Complex_ArrayUsingStruct_TC_166", Include) {
  sql(s"""select  array_of_struct.CHECK_DATE[0],sum( array_of_struct.ID[0]+ array_of_struct.ID[1]) as total from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>5700 and  array_of_struct.CHECK_DATE[0] like'%2015%' group by array_of_struct.CHECK_DATE[0]""").collect
}


//Complex_ArrayUsingStruct_TC_167
test("Complex_ArrayUsingStruct_TC_167", Include) {
  sql(s"""select  array_of_struct.CHECK_DATE[0],sum( array_of_struct.ID[0]+ array_of_struct.ID[1]) as total from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>5700 and  array_of_struct.CHECK_DATE[0] like'%2015%' group by array_of_struct.CHECK_DATE[0]  having total > 10 order by total desc""").collect
}


//Complex_ArrayUsingStruct_TC_168
test("Complex_ArrayUsingStruct_TC_168", Include) {
  sql(s"""select array_of_struct.CHECK_DATE[0],sum( array_of_struct.ID[0]+ array_of_struct.ID[1]),count(distinct array_of_struct.ID[1]) as total from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>5700 and  array_of_struct.CHECK_DATE[0]like'%2015%' group by array_of_struct.CHECK_DATE[0] having total > 10 order by total desc""").collect
}


//Complex_ArrayUsingStruct_TC_169
test("Complex_ArrayUsingStruct_TC_169", Include) {
  sql(s"""select array_of_struct.ID[1],array_of_struct.state[2],array_of_struct.CHECK_DATE[2],count(distinct array_of_struct.ID[1]) as count,sum( array_of_struct.ID[0]+ cust_id) as total from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dublin') group by array_of_struct.ID[1],array_of_struct.state[2],array_of_struct.CHECK_DATE[2] order by total desc""").collect
}


//Complex_ArrayUsingStruct_TC_170
test("Complex_ArrayUsingStruct_TC_170", Include) {
  sql(s"""select count(distinct(year (array_of_struct.CHECK_DATE[1]))) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] like '%England%'""").collect
}


//Complex_ArrayUsingStruct_TC_171
test("Complex_ArrayUsingStruct_TC_171", Include) {
  sql(s"""select year (array_of_struct.CHECK_DATE[1]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] like '%England%'""").collect
}


//Complex_ArrayUsingStruct_TC_172
test("Complex_ArrayUsingStruct_TC_172", Include) {
  sql(s"""select month (array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dubai')""").collect
}


//Complex_ArrayUsingStruct_TC_173
test("Complex_ArrayUsingStruct_TC_173", Include) {
  sql(s"""select day(array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dubai') group by array_of_struct.CHECK_DATE[2] order by array_of_struct.CHECK_DATE[2]""").collect
}


//Complex_ArrayUsingStruct_TC_174
test("Complex_ArrayUsingStruct_TC_174", Include) {
  sql(s"""select hour (array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dubai') group by array_of_struct.CHECK_DATE[2] order by array_of_struct.CHECK_DATE[2]""").collect
}


//Complex_ArrayUsingStruct_TC_175
test("Complex_ArrayUsingStruct_TC_175", Include) {
  sql(s"""select count (minute (array_of_struct.CHECK_DATE[2])) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dubai') group by array_of_struct.CHECK_DATE[2] order by array_of_struct.CHECK_DATE[2]""").collect
}


//Complex_ArrayUsingStruct_TC_176
test("Complex_ArrayUsingStruct_TC_176", Include) {
  sql(s"""select second (array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] in('England','Dubai') group by array_of_struct.CHECK_DATE[2] having count(array_of_struct.CHECK_DATE[2])>=2""").collect
}


//Complex_ArrayUsingStruct_TC_177
test("Complex_ArrayUsingStruct_TC_177", Include) {
  sql(s"""select WEEKOFYEAR(array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] like'%a%' group by array_of_struct.CHECK_DATE[2] having count(array_of_struct.CHECK_DATE[2])<=2""").collect
}


//Complex_ArrayUsingStruct_TC_178
test("Complex_ArrayUsingStruct_TC_178", Include) {
  sql(s"""select DATEDIFF(array_of_struct.CHECK_DATE[2] ,'2020-08-09 00:00:00')from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] like'%a%'""").collect
}


//Complex_ArrayUsingStruct_TC_179
test("Complex_ArrayUsingStruct_TC_179", Include) {
  sql(s"""select DATE_ADD(array_of_struct.CHECK_DATE[2] ,5) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] like'%a%'""").collect
}


//Complex_ArrayUsingStruct_TC_180
test("Complex_ArrayUsingStruct_TC_180", Include) {
  sql(s"""select DATE_SUB(array_of_struct.CHECK_DATE[2] ,5) from ARRAY_OF_STRUCT_com where array_of_struct.ID[1] like'%7%'""").collect
}


//Complex_ArrayUsingStruct_TC_181
test("Complex_ArrayUsingStruct_TC_181", Include) {
  sql(s"""select unix_timestamp(array_of_struct.CHECK_DATE[2]) from ARRAY_OF_STRUCT_com where array_of_struct.ID[1]>=5702""").collect
}


//Complex_ArrayUsingStruct_TC_182
test("Complex_ArrayUsingStruct_TC_182", Include) {
  sql(s"""select substr(array_of_struct.CITI[0],3,4) from ARRAY_OF_STRUCT_com""").collect
}


//Complex_ArrayUsingStruct_TC_183
test("Complex_ArrayUsingStruct_TC_183", Include) {
  sql(s"""select substr(array_of_struct.CITI[0],3) from ARRAY_OF_STRUCT_com where array_of_struct.COUNTRY[2] rlike 'a'""").collect
}


//Complex_ArrayUsingStruct_TC_184
test("Complex_ArrayUsingStruct_TC_184", Include) {
  sql(s"""SELECT array_of_struct.COUNTRY[2],array_of_struct.CHECK_DATE[0],array_of_struct.CHECK_DATE[2],array_of_struct.CITI[0], SUM(array_of_struct.ID[2]) AS Sum FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE array_of_struct.ID[1] > 5700 GROUP BY array_of_struct.COUNTRY[2],array_of_struct.CHECK_DATE[0],array_of_struct.CHECK_DATE[2],array_of_struct.CITI[0] ORDER BY array_of_struct.COUNTRY[2] asc,array_of_struct.CHECK_DATE[0] asc,array_of_struct.CHECK_DATE[2] asc, array_of_struct.CITI[0] asc""").collect
}


//Complex_ArrayUsingStruct_TC_185
test("Complex_ArrayUsingStruct_TC_185", Include) {
  sql(s"""SELECT array_of_struct.COUNTRY[2],array_of_struct.CHECK_DATE[0],array_of_struct.CHECK_DATE[2],array_of_struct.CITI[0], SUM(array_of_struct.ID[2]) AS Sum FROM (select * from ARRAY_OF_STRUCT_com) SUB_QRY WHERE array_of_struct.COUNTRY[2]='England' GROUP BY array_of_struct.COUNTRY[2],array_of_struct.CHECK_DATE[0],array_of_struct.CHECK_DATE[2],array_of_struct.CITI[0] ORDER BY array_of_struct.COUNTRY[2] asc,array_of_struct.CHECK_DATE[0] asc,array_of_struct.CHECK_DATE[2] asc, array_of_struct.CITI[0] asc""").collect
}

override def afterAll {
sql("drop table if exists ARRAY_OF_STRUCT_com")
sql("drop table if exists ARRAY_OF_STRUCT_com_hive")
}
}
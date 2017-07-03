
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
 * Test Class for Structcom to verify all scenerios
 */

class STRUCTCOMTestCase extends QueryTest with BeforeAndAfterAll {
         

//drop_Struct_com
test("drop_Struct_com", Include) {
  sql(s"""drop table if exists Struct_com""").collect

  sql(s"""drop table if exists Struct_com_hive""").collect

}
       

//Complex_Struct_TC_CreateTable
test("Complex_Struct_TC_CreateTable", Include) {
  sql(s"""create table Struct_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_INT_DOUBLE_STRING_DATE struct<ID:int,SALARY:double,COUNTRY:STRING,CHECK_DATE:timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table Struct_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_INT_DOUBLE_STRING_DATE struct<ID:int,SALARY:double,COUNTRY:STRING,CHECK_DATE:timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Complex_Struct_TC_DataLoad
test("Complex_Struct_TC_DataLoad", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/Struct.csv' INTO table Struct_com options ('DELIMITER'=',', 'QUOTECHAR'='"', 'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE,GENDER,EDUCATED,IS_MARRIED,STRUCT_INT_DOUBLE_STRING_DATE,CARD_COUNT,DEBIT_COUNT,CREDIT_COUNT,DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$DOLLAR')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/complex/Struct.csv' INTO table Struct_com_hive """).collect

}
       

//Complex_Struct_TC_003
test("Complex_Struct_TC_003", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.id+ 10 as a  from Struct_com """).collect
}
       

//Complex_Struct_TC_004
test("Complex_Struct_TC_004", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY+ 10 as a  from Struct_com    """).collect
}
       

//Complex_Struct_TC_005
test("Complex_Struct_TC_005", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.id+cust_id as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_006
test("Complex_Struct_TC_006", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.id+age  as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_007
test("Complex_Struct_TC_007", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.id+deposit  as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_008
test("Complex_Struct_TC_008", Include) {
  sql(s"""select concat(struct_int_double_string_date.COUNTRY,'_',cust_id)  as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_009
test("Complex_Struct_TC_009", Include) {
  sql(s"""select concat(struct_int_double_string_date.COUNTRY,'_',educated)  as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_010
test("Complex_Struct_TC_010", Include) {
  sql(s"""select concat(struct_int_double_string_date.COUNTRY,'_',educated,'@',is_married)  as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_011
test("Complex_Struct_TC_011", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, sum(struct_int_double_string_date.id+ 10) as a from Struct_com group by  struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_012
test("Complex_Struct_TC_012", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, sum(struct_int_double_string_date.id+ 10) as a from Struct_com group by  struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_013
test("Complex_Struct_TC_013", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE, sum(struct_int_double_string_date.id+ 10) as a from Struct_com  group by  struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_014
test("Complex_Struct_TC_014", Include) {
  sql(s"""select struct_int_double_string_date.id, sum(struct_int_double_string_date.SALARY+ 10) as a from Struct_com  group by  struct_int_double_string_date.id order by struct_int_double_string_date.id""").collect
}
       

//Complex_Struct_TC_015
test("Complex_Struct_TC_015", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.SALARY, avg(struct_int_double_string_date.id+ 10),avg(struct_int_double_string_date.salary+100)as a from Struct_com group by  struct_int_double_string_date.id,struct_int_double_string_date.SALARY""").collect
}
       

//Complex_Struct_TC_016
test("Complex_Struct_TC_016", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY,struct_int_double_string_date.SALARY,avg(struct_int_double_string_date.SALARY+ 10) Total from Struct_com group by  struct_int_double_string_date.COUNTRY,struct_int_double_string_date.SALARY order by struct_int_double_string_date.COUNTRY,struct_int_double_string_date.SALARY""").collect
}
       

//Complex_Struct_TC_017
test("Complex_Struct_TC_017", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE, avg(struct_int_double_string_date.id+struct_int_double_string_date.SALARY) Total from Struct_com group by  struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_018
test("Complex_Struct_TC_018", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE, count(struct_int_double_string_date.id)+10 as a  from Struct_com  group by  struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_019
test("Complex_Struct_TC_019", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.COUNTRY+ 10) Total from Struct_com group by  struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_020
test("Complex_Struct_TC_020", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.COUNTRY) Total from Struct_com group by  struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_021
test("Complex_Struct_TC_021", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY,struct_int_double_string_date.SALARY, count(struct_int_double_string_date.COUNTRY),count(struct_int_double_string_date.SALARY) Total from Struct_com group by  struct_int_double_string_date.SALARY,struct_int_double_string_date.COUNTRY having count(struct_int_double_string_date.COUNTRY)>2""").collect
}
       

//Complex_Struct_TC_022
test("Complex_Struct_TC_022", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.COUNTRY)+10  Total from Struct_com group by  struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_023
test("Complex_Struct_TC_023", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE, count(struct_int_double_string_date.CHECK_DATE)+10  Total from Struct_com group by  struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_025
test("Complex_Struct_TC_025", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY, min(struct_int_double_string_date.COUNTRY) Total from Struct_com group by  struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_026
test("Complex_Struct_TC_026", Include) {
  sql(s"""select  min(struct_int_double_string_date.CHECK_DATE) Total from Struct_com group by  struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_027
test("Complex_Struct_TC_027", Include) {
  sql(s"""select cust_id , min(struct_int_double_string_date.id+struct_int_double_string_date.id)  Total from Struct_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_Struct_TC_028
test("Complex_Struct_TC_028", Include) {
  sql(s"""select cust_id , max(struct_int_double_string_date.id+struct_int_double_string_date.id)  Total from Struct_com  group by  cust_id  order by cust_id """).collect
}
       

//Complex_Struct_TC_029
test("Complex_Struct_TC_029", Include) {
  sql(s"""select max(struct_int_double_string_date.CHECK_DATE) Total from Struct_com group by  struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_030
test("Complex_Struct_TC_030", Include) {
  sql(s"""select max(struct_int_double_string_date.Country) Total from Struct_com group by  struct_int_double_string_date.Country order by struct_int_double_string_date.Country""").collect
}
       

//Complex_Struct_TC_031
test("Complex_Struct_TC_031", Include) {
  sql(s"""select struct_int_double_string_date.id ,sum(distinct struct_int_double_string_date.id)+10,sum(distinct struct_int_double_string_date.salary)+10 from Struct_com group by struct_int_double_string_date.id""").collect
}
       

//Complex_Struct_TC_032
test("Complex_Struct_TC_032", Include) {
  sql(s"""select struct_int_double_string_date.Country ,sum(distinct struct_int_double_string_date.Country)+10 from Struct_com group by struct_int_double_string_date.Country""").collect
}
       

//Complex_Struct_TC_034
test("Complex_Struct_TC_034", Include) {
  sql(s"""select count(distinct struct_int_double_string_date.Country)+10 as a,struct_int_double_string_date.id from Struct_com group by struct_int_double_string_date.id""").collect
}
       

//Complex_Struct_TC_035
test("Complex_Struct_TC_035", Include) {
  sql(s"""select count(*) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_036
test("Complex_Struct_TC_036", Include) {
  sql(s"""Select count(1) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_037
test("Complex_Struct_TC_037", Include) {
  sql(s"""select count(struct_int_double_string_date.CHECK_DATE) as a   from Struct_com""").collect
}
       

//Complex_Struct_TC_038
test("Complex_Struct_TC_038", Include) {
  sql(s"""select count(struct_int_double_string_date.COUNTRY)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_039
test("Complex_Struct_TC_039", Include) {
  sql(s"""select count(DISTINCT struct_int_double_string_date.CHECK_DATE) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_040
test("Complex_Struct_TC_040", Include) {
  sql(s"""select count(DISTINCT struct_int_double_string_date.Country) as a from Struct_com""").collect
}
       

//Complex_Struct_TC_041
test("Complex_Struct_TC_041", Include) {
  sql(s"""select count (if(struct_int_double_string_date.CHECK_DATE>'100',NULL,struct_int_double_string_date.CHECK_DATE))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_042
test("Complex_Struct_TC_042", Include) {
  sql(s"""select count (if(struct_int_double_string_date.id>100,NULL,struct_int_double_string_date.id))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_043
test("Complex_Struct_TC_043", Include) {
  sql(s"""select count (if(struct_int_double_string_date.salary>100,NULL,struct_int_double_string_date.salary))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_044
test("Complex_Struct_TC_044", Include) {
  sql(s"""select count (if(struct_int_double_string_date.COUNTRY>100,NULL,struct_int_double_string_date.COUNTRY))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_045
test("Complex_Struct_TC_045", Include) {
  sql(s"""select sum(DISTINCT  struct_int_double_string_date.id) a  from Struct_com""").collect
}
       

//Complex_Struct_TC_046
test("Complex_Struct_TC_046", Include) {
  sql(s"""select sum(DISTINCT  struct_int_double_string_date.salary) a  from Struct_com""").collect
}
       

//Complex_Struct_TC_047
test("Complex_Struct_TC_047", Include) {
  sql(s"""select sum (if(struct_int_double_string_date.id>100,NULL,struct_int_double_string_date.id))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_048
test("Complex_Struct_TC_048", Include) {
  sql(s"""select sum (if(struct_int_double_string_date.Country>100,NULL,struct_int_double_string_date.Country))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_049
test("Complex_Struct_TC_049", Include) {
  sql(s"""select sum (if(struct_int_double_string_date.CHECK_DATE>'100',NULL,struct_int_double_string_date.Country))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_050
test("Complex_Struct_TC_050", Include) {
  sql(s"""select sum (if(struct_int_double_string_date.salary>'100',NULL,struct_int_double_string_date.salary))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_051
test("Complex_Struct_TC_051", Include) {
  sql(s"""select avg(struct_int_double_string_date.id) from Struct_com""").collect
}
       

//Complex_Struct_TC_052
test("Complex_Struct_TC_052", Include) {
  sql(s"""select avg(struct_int_double_string_date.salary) from Struct_com""").collect
}
       

//Complex_Struct_TC_053
test("Complex_Struct_TC_053", Include) {
  sql(s"""select sum(struct_int_double_string_date.id) from Struct_com""").collect
}
       

//Complex_Struct_TC_054
test("Complex_Struct_TC_054", Include) {
  sql(s"""select sum(struct_int_double_string_date.salary) from Struct_com""").collect
}
       

//Complex_Struct_TC_055
test("Complex_Struct_TC_055", Include) {
  sql(s"""select max(struct_int_double_string_date.Country) from Struct_com""").collect
}
       

//Complex_Struct_TC_056
test("Complex_Struct_TC_056", Include) {
  sql(s"""select max(struct_int_double_string_date.salary) from Struct_com""").collect
}
       

//Complex_Struct_TC_057
test("Complex_Struct_TC_057", Include) {
  sql(s"""select count(struct_int_double_string_date.Country) from Struct_com""").collect
}
       

//Complex_Struct_TC_058
test("Complex_Struct_TC_058", Include) {
  sql(s"""select count(struct_int_double_string_date.salary) from Struct_com""").collect
}
       

//Complex_Struct_TC_059
test("Complex_Struct_TC_059", Include) {
  sql(s"""select min(struct_int_double_string_date.CHECK_DATE) from Struct_com""").collect
}
       

//Complex_Struct_TC_060
test("Complex_Struct_TC_060", Include) {
  sql(s"""select min(struct_int_double_string_date.salary) from Struct_com""").collect
}
       

//Complex_Struct_TC_061
test("Complex_Struct_TC_061", Include) {
  sql(s"""select avg (if(struct_int_double_string_date.id>100,NULL,struct_int_double_string_date.id))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_062
test("Complex_Struct_TC_062", Include) {
  sql(s"""select avg (if(struct_int_double_string_date.COUNTRY>100,NULL,struct_int_double_string_date.COUNTRY))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_063
test("Complex_Struct_TC_063", Include) {
  sql(s"""select avg (if(struct_int_double_string_date.salary>100,NULL,struct_int_double_string_date.salary))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_064
test("Complex_Struct_TC_064", Include) {
  sql(s"""select min (if(struct_int_double_string_date.COUNTRY>100,NULL,struct_int_double_string_date.COUNTRY))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_065
test("Complex_Struct_TC_065", Include) {
  sql(s"""select min (if(struct_int_double_string_date.id>100,NULL,struct_int_double_string_date.id))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_066
test("Complex_Struct_TC_066", Include) {
  sql(s"""select min (if(struct_int_double_string_date.salary>100,NULL,struct_int_double_string_date.salary))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_067
test("Complex_Struct_TC_067", Include) {
  sql(s"""select max (if(struct_int_double_string_date.id>100,NULL,struct_int_double_string_date.id))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_068
test("Complex_Struct_TC_068", Include) {
  sql(s"""select max (if(struct_int_double_string_date.salary>100,NULL,struct_int_double_string_date.salary))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_069
test("Complex_Struct_TC_069", Include) {
  sql(s"""select max (if(struct_int_double_string_date.COUNTRY>100,NULL,struct_int_double_string_date.COUNTRY))  a from Struct_com""").collect
}
       

//Complex_Struct_TC_070
test("Complex_Struct_TC_070", Include) {
  sql(s"""select variance(struct_int_double_string_date.id) as a   from Struct_com""").collect
}
       

//Complex_Struct_TC_071
test("Complex_Struct_TC_071", Include) {
  sql(s"""select variance(struct_int_double_string_date.salary) as a   from Struct_com""").collect
}
       

//Complex_Struct_TC_072
test("Complex_Struct_TC_072", Include) {
  sql(s"""select var_pop(struct_int_double_string_date.id)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_073
test("Complex_Struct_TC_073", Include) {
  sql(s"""select var_pop(struct_int_double_string_date.salary)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_074
test("Complex_Struct_TC_074", Include) {
  sql(s"""select var_samp(struct_int_double_string_date.id) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_075
test("Complex_Struct_TC_075", Include) {
  sql(s"""select var_samp(struct_int_double_string_date.salary) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_076
test("Complex_Struct_TC_076", Include) {
  sql(s"""select stddev_pop(struct_int_double_string_date.salary) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_077
test("Complex_Struct_TC_077", Include) {
  sql(s"""select stddev_samp(struct_int_double_string_date.salary)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_078
test("Complex_Struct_TC_078", Include) {
  sql(s"""select covar_pop(struct_int_double_string_date.salary,struct_int_double_string_date.salary) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_079
test("Complex_Struct_TC_079", Include) {
  sql(s"""select covar_samp(struct_int_double_string_date.id,struct_int_double_string_date.id) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_080
test("Complex_Struct_TC_080", Include) {
  sql(s"""select covar_samp(struct_int_double_string_date.salary,struct_int_double_string_date.salary) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_081
test("Complex_Struct_TC_081", Include) {
  sql(s"""select corr(struct_int_double_string_date.id,struct_int_double_string_date.id)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_082
test("Complex_Struct_TC_082", Include) {
  sql(s"""select corr(struct_int_double_string_date.salary,struct_int_double_string_date.salary)  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_083
test("Complex_Struct_TC_083", Include) {
  sql(s"""select percentile(struct_int_double_string_date.id,0.2) as  a  from Struct_com""").collect
}
       

//Complex_Struct_TC_085
test("Complex_Struct_TC_085", Include) {
  sql(s"""select percentile(struct_int_double_string_date.id,array(0,0.2,0.3,1))  as  a from Struct_com""").collect
}
       

//Complex_Struct_TC_087
test("Complex_Struct_TC_087", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.id,0.2) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_088
test("Complex_Struct_TC_088", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.salary,0.2) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_089
test("Complex_Struct_TC_089", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.id,0.2,5) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_090
test("Complex_Struct_TC_090", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.salary,0.2,5) as a  from Struct_com""").collect
}
       

//Complex_Struct_TC_091
test("Complex_Struct_TC_091", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.id,array(0.2,0.3,0.99))  as a from Struct_com""").collect
}
       

//Complex_Struct_TC_092
test("Complex_Struct_TC_092", Include) {
  sql(s"""select percentile_approx(struct_int_double_string_date.id,array(0.2,0.3,0.99),5) as a from Struct_com""").collect
}
       

//Complex_Struct_TC_093
test("Complex_Struct_TC_093", Include) {
  sql(s"""select * from (select collect_set(struct_int_double_string_date.Country) as myseries from Struct_com) aa sort by myseries""").collect
}
       

//Complex_Struct_TC_097
test("Complex_Struct_TC_097", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE,count(struct_int_double_string_date.Country) a from Struct_com group by struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_098
test("Complex_Struct_TC_098", Include) {
  sql(s"""select struct_int_double_string_date.Country,count(struct_int_double_string_date.CHECK_DATE) a from Struct_com group by struct_int_double_string_date.Country order by a""").collect
}
       

//Complex_Struct_TC_099
test("Complex_Struct_TC_099", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY,count(struct_int_double_string_date.id)  a from Struct_com group by struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_100
test("Complex_Struct_TC_100", Include) {
  sql(s"""select count(distinct struct_int_double_string_date.Country) a,struct_int_double_string_date.id from Struct_com group by struct_int_double_string_date.id""").collect
}
       

//Complex_Struct_TC_102
test("Complex_Struct_TC_102", Include) {
  sql(s"""select struct_int_double_string_date.Country,sum(struct_int_double_string_date.id) a,sum(struct_int_double_string_date.salary) from Struct_com group by struct_int_double_string_date.Country order by struct_int_double_string_date.Country""").collect
}
       

//Complex_Struct_TC_103
test("Complex_Struct_TC_103", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,sum(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_104
test("Complex_Struct_TC_104", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,avg(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_105
test("Complex_Struct_TC_105", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,min(struct_int_double_string_date.Country) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_106
test("Complex_Struct_TC_106", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,min(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_107
test("Complex_Struct_TC_107", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,min(struct_int_double_string_date.salary) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_108
test("Complex_Struct_TC_108", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,min(struct_int_double_string_date.CHECK_DATE) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_109
test("Complex_Struct_TC_109", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,max(struct_int_double_string_date.Country) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_110
test("Complex_Struct_TC_110", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,max(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_111
test("Complex_Struct_TC_111", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,max(struct_int_double_string_date.CHECK_DATE) a from Struct_com group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_114
test("Complex_Struct_TC_114", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.COUNTRY IN ('England','Dublin')""").collect
}
       

//Complex_Struct_TC_115
test("Complex_Struct_TC_115", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.COUNTRY not IN ('England','Dublin')""").collect
}
       

//Complex_Struct_TC_118
test("Complex_Struct_TC_118", Include) {
  sql(s"""select Upper(struct_int_double_string_date.COUNTRY) a  from Struct_com""").collect
}
       

//Complex_Struct_TC_119
test("Complex_Struct_TC_119", Include) {
  sql(s"""select Lower(struct_int_double_string_date.Country) a  from Struct_com""").collect
}
       

//Complex_Struct_TC_124
test("Complex_Struct_TC_124", Include) {
  sql(s"""select struct_int_double_string_date.Country,sum(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.Country order by struct_int_double_string_date.Country desc""").collect
}
       

//Complex_Struct_TC_125
test("Complex_Struct_TC_125", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.Country,sum(struct_int_double_string_date.id) a from Struct_com group by struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE desc ,a desc""").collect
}
       

//Complex_Struct_TC_126
test("Complex_Struct_TC_126", Include) {
  sql(s"""select struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.id,struct_int_double_string_date.Country as a from Struct_com  order by a asc limit 10""").collect
}
       

//Complex_Struct_TC_127
test("Complex_Struct_TC_127", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where  (struct_int_double_string_date.Country =='Aasgaardstrand') and (struct_int_double_string_date.CHECK_DATE=='2013-11-26 00:00:00.0')""").collect
}
       

//Complex_Struct_TC_128
test("Complex_Struct_TC_128", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where  (struct_int_double_string_date.Country == 'Abu Dhabi') and (struct_int_double_string_date.id==9125)""").collect
}
       

//Complex_Struct_TC_129
test("Complex_Struct_TC_129", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where  (struct_int_double_string_date.Country == 'Abu Dhabi') or (struct_int_double_string_date.id==5702)""").collect
}
       

//Complex_Struct_TC_130
test("Complex_Struct_TC_130", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE  from Struct_com where (struct_int_double_string_date.Country == 'Abu Dhabi' and struct_int_double_string_date.id==5702) OR (struct_int_double_string_date.Country =='Aasgaardstrand' or struct_int_double_string_date.CHECK_DATE=='2013-11-26 00:00:00.0')""").collect
}
       

//Complex_Struct_TC_131
test("Complex_Struct_TC_131", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE  from Struct_com where (struct_int_double_string_date.Country == 'Abu Dhabi' and struct_int_double_string_date.id==5702) OR (struct_int_double_string_date.Country =='Aasgaardstrand' and struct_int_double_string_date.CHECK_DATE=='2013-11-26 00:00:00.0')""").collect
}
       

//Complex_Struct_TC_132
test("Complex_Struct_TC_132", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.Country!='Abu Dhabi' and  struct_int_double_string_date.CHECK_DATE=='2013-11-26 00:00:00.0'""").collect
}
       

//Complex_Struct_TC_133
test("Complex_Struct_TC_133", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.COUNTRY!='England'""").collect
}
       

//Complex_Struct_TC_135
test("Complex_Struct_TC_135", Include) {
  sql(s"""select struct_int_double_string_date.id as a from Struct_com where struct_int_double_string_date.id<=>cust_id""").collect
}
       

//Complex_Struct_TC_137
test("Complex_Struct_TC_137", Include) {
  sql(s"""select struct_int_double_string_date.id  from Struct_com where struct_int_double_string_date.id<>cust_id""").collect
}
       

//Complex_Struct_TC_138
test("Complex_Struct_TC_138", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id != cust_id""").collect
}
       

//Complex_Struct_TC_139
test("Complex_Struct_TC_139", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id<cust_id""").collect
}
       

//Complex_Struct_TC_140
test("Complex_Struct_TC_140", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id<=cust_id""").collect
}
       

//Complex_Struct_TC_141
test("Complex_Struct_TC_141", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id>cust_id""").collect
}
       

//Complex_Struct_TC_142
test("Complex_Struct_TC_142", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id>=cust_id""").collect
}
       

//Complex_Struct_TC_143
test("Complex_Struct_TC_143", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id NOT BETWEEN month AND  year""").collect
}
       

//Complex_Struct_TC_144
test("Complex_Struct_TC_144", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.id BETWEEN month AND  year""").collect
}
       

//Complex_Struct_TC_145
test("Complex_Struct_TC_145", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.COUNTRY from Struct_com where struct_int_double_string_date.COUNTRY IS NULL""").collect
}
       

//Complex_Struct_TC_146
test("Complex_Struct_TC_146", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country from Struct_com where struct_int_double_string_date.COUNTRY IS NOT NULL""").collect
}
       

//Complex_Struct_TC_147
test("Complex_Struct_TC_147", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country from Struct_com where struct_int_double_string_date.id IS NOT NULL""").collect
}
       

//Complex_Struct_TC_148
test("Complex_Struct_TC_148", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.CHECK_DATE IS NOT NULL""").collect
}
       

//Complex_Struct_TC_149
test("Complex_Struct_TC_149", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.id NOT LIKE struct_int_double_string_date.COUNTRY AND struct_int_double_string_date.CHECK_DATE NOT LIKE  cust_id""").collect
}
       

//Complex_Struct_TC_150
test("Complex_Struct_TC_150", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE from Struct_com where struct_int_double_string_date.id  LIKE struct_int_double_string_date.id and struct_int_double_string_date.COUNTRY like struct_int_double_string_date.CHECK_DATE """).collect
}
       

//Complex_Struct_TC_151
test("Complex_Struct_TC_151", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id >505""").collect
}
       

//Complex_Struct_TC_152
test("Complex_Struct_TC_152", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id <505""").collect
}
       

//Complex_Struct_TC_153
test("Complex_Struct_TC_153", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.CHECK_DATE <'2011-07-01 00:00:00'""").collect
}
       

//Complex_Struct_TC_154
test("Complex_Struct_TC_154", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.CHECK_DATE >'2011-07-01 00:00:00'""").collect
}
       

//Complex_Struct_TC_155
test("Complex_Struct_TC_155", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id >=505""").collect
}
       

//Complex_Struct_TC_156
test("Complex_Struct_TC_156", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id <=505""").collect
}
       

//Complex_Struct_TC_157
test("Complex_Struct_TC_157", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id <=2""").collect
}
       

//Complex_Struct_TC_158
test("Complex_Struct_TC_158", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where struct_int_double_string_date.id >=2""").collect
}
       

//Complex_Struct_TC_159
test("Complex_Struct_TC_159", Include) {
  sql(s"""select sum(struct_int_double_string_date.id) a from Struct_com where struct_int_double_string_date.id >=10 OR (struct_int_double_string_date.id <=1 and struct_int_double_string_date.Country='England')""").collect
}
       

//Complex_Struct_TC_160
test("Complex_Struct_TC_160", Include) {
  sql(s"""select * from (select struct_int_double_string_date.Country,if(struct_int_double_string_date.Country='England',NULL,struct_int_double_string_date.Country) a from Struct_com) aa  where a IS not NULL""").collect
}
       

//Complex_Struct_TC_161
test("Complex_Struct_TC_161", Include) {
  sql(s"""select * from (select struct_int_double_string_date.Country,if(struct_int_double_string_date.Country='England',NULL,struct_int_double_string_date.Country) a from Struct_com) aa  where a IS  NULL""").collect
}
       

//Complex_Struct_TC_162
test("Complex_Struct_TC_162", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where  (struct_int_double_string_date.id == 5754) or (struct_int_double_string_date.CHECK_DATE=='2016-09-06 00:00:00.0')""").collect
}
       

//Complex_Struct_TC_163
test("Complex_Struct_TC_163", Include) {
  sql(s"""select struct_int_double_string_date.id from Struct_com where  (struct_int_double_string_date.id == 5754) and (struct_int_double_string_date.CHECK_DATE=='2016-09-06 00:00:00.0')""").collect
}
       

//Complex_Struct_TC_164
test("Complex_Struct_TC_164", Include) {
  sql(s"""select struct_int_double_string_date.COUNTRY  from Struct_com where  (struct_int_double_string_date.COUNTRY like '%an%') OR ( struct_int_double_string_date.COUNTRY like '%t%') order by struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_165
test("Complex_Struct_TC_165", Include) {
  sql(s"""select cust_id,struct_int_double_string_date.CHECK_DATE, struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id from Struct_com where  struct_int_double_string_date.id BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_Struct_TC_166
test("Complex_Struct_TC_166", Include) {
  sql(s"""select cust_id,struct_int_double_string_date.CHECK_DATE, struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id from Struct_com where  struct_int_double_string_date.id BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_Struct_TC_167
test("Complex_Struct_TC_167", Include) {
  sql(s"""select cust_id,struct_int_double_string_date.CHECK_DATE, struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id from Struct_com where  struct_int_double_string_date.id not BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//Complex_Struct_TC_168
test("Complex_Struct_TC_168", Include) {
  sql(s"""select cust_id,struct_int_double_string_date.CHECK_DATE, struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id from Struct_com where  struct_int_double_string_date.COUNTRY NOT LIKE '%r%' AND struct_int_double_string_date.id NOT LIKE 5 ORDER BY struct_int_double_string_date.CHECK_DATE limit 5""").collect
}
       

//Complex_Struct_TC_169
test("Complex_Struct_TC_169", Include) {
  sql(s"""select cust_id,struct_int_double_string_date.CHECK_DATE, struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id from Struct_com where  struct_int_double_string_date.COUNTRY RLIKE '%t%' ORDER BY struct_int_double_string_date.id limit 5""").collect
}
       

//Complex_Struct_TC_171
test("Complex_Struct_TC_171", Include) {
  sql(s"""select  cust_id,struct_int_double_string_date.id from Struct_com where UPPER(struct_int_double_string_date.COUNTRY) == 'DUBLIN'""").collect
}
       

//Complex_Struct_TC_172
test("Complex_Struct_TC_172", Include) {
  sql(s"""select  cust_id,struct_int_double_string_date.id from Struct_com where UPPER(struct_int_double_string_date.COUNTRY) in ('DUBLIN','England','SA')""").collect
}
       

//Complex_Struct_TC_173
test("Complex_Struct_TC_173", Include) {
  sql(s"""select  cust_id,struct_int_double_string_date.id from Struct_com where UPPER(struct_int_double_string_date.COUNTRY) like '%A%'""").collect
}
       

//Complex_Struct_TC_174
test("Complex_Struct_TC_174", Include) {
  sql(s"""select count(struct_int_double_string_date.id) ,struct_int_double_string_date.COUNTRY from Struct_com group by struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id having sum (struct_int_double_string_date.id)= 9150500""").collect
}
       

//Complex_Struct_TC_175
test("Complex_Struct_TC_175", Include) {
  sql(s"""select count(struct_int_double_string_date.id) ,struct_int_double_string_date.COUNTRY from Struct_com group by struct_int_double_string_date.COUNTRY,struct_int_double_string_date.id having sum (struct_int_double_string_date.id)>= 9150""").collect
}
       

//Complex_Struct_TC_176
test("Complex_Struct_TC_176", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_177
test("Complex_Struct_TC_177", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_178
test("Complex_Struct_TC_178", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY WHERE (struct_int_double_string_date.COUNTRY = "") or (struct_int_double_string_date.id= "") or (struct_int_double_string_date.CHECK_DATE= "") GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_179
test("Complex_Struct_TC_179", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY WHERE not(struct_int_double_string_date.COUNTRY = "") or not(struct_int_double_string_date.id= "") or not(struct_int_double_string_date.CHECK_DATE= "") GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_180
test("Complex_Struct_TC_180", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY WHERE (struct_int_double_string_date.COUNTRY > "") or (struct_int_double_string_date.id>"") or (struct_int_double_string_date.CHECK_DATE> "") GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_181
test("Complex_Struct_TC_181", Include) {
  sql(s"""select struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY WHERE (struct_int_double_string_date.COUNTRY >='""') or (struct_int_double_string_date.id>= '""') or (struct_int_double_string_date.CHECK_DATE>='""') GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_182
test("Complex_Struct_TC_182", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY where (struct_int_double_string_date.id>5700) or (struct_int_double_string_date.CHECK_DATE like'%2015%') GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_184
test("Complex_Struct_TC_184", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE, SUM(struct_int_double_string_date.id) AS Sum_array_int FROM (select * from Struct_com) SUB_QRY where (struct_int_double_string_date.id < 5700) or (struct_int_double_string_date.CHECK_DATE like'%2015%') GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE ORDER BY struct_int_double_string_date.id ASC, struct_int_double_string_date.CHECK_DATE ASC""").collect
}
       

//Complex_Struct_TC_185
test("Complex_Struct_TC_185", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, AVG(struct_int_double_string_date.id) AS Avg FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_186
test("Complex_Struct_TC_186", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, SUM(struct_int_double_string_date.id) AS sum FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_187
test("Complex_Struct_TC_187", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.id) AS count FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_188
test("Complex_Struct_TC_188", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.COUNTRY) AS count FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_189
test("Complex_Struct_TC_189", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, count(struct_int_double_string_date.CHECK_DATE) AS count FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_190
test("Complex_Struct_TC_190", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, min(struct_int_double_string_date.CHECK_DATE) AS min_date,min(struct_int_double_string_date.Country) as min_string,min(struct_int_double_string_date.id) as min_int FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_191
test("Complex_Struct_TC_191", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, max(struct_int_double_string_date.CHECK_DATE) AS max_date,max(struct_int_double_string_date.Country) as max_string,max(struct_int_double_string_date.id) as max_int FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_192
test("Complex_Struct_TC_192", Include) {
  sql(s"""SELECT struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY, count(distinct struct_int_double_string_date.CHECK_DATE) AS count_date,count(distinct struct_int_double_string_date.Country) as count_string,count(distinct struct_int_double_string_date.id) as count_int FROM (select * from Struct_com) SUB_QRY GROUP BY struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY order by struct_int_double_string_date.id, struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.COUNTRY""").collect
}
       

//Complex_Struct_TC_193
test("Complex_Struct_TC_193", Include) {
  sql(s"""select  struct_int_double_string_date.CHECK_DATE,sum( struct_int_double_string_date.id+ struct_int_double_string_date.salary) as total from Struct_com where struct_int_double_string_date.id>5700 and  struct_int_double_string_date.CHECK_DATE like'%2015%' group by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_195
test("Complex_Struct_TC_195", Include) {
  sql(s"""select  struct_int_double_string_date.CHECK_DATE,sum( struct_int_double_string_date.id+ struct_int_double_string_date.salary),count(distinct struct_int_double_string_date.id) as total,count(distinct struct_int_double_string_date.salary) as total_Salary from Struct_com where struct_int_double_string_date.id>5700 and  struct_int_double_string_date.CHECK_DATE like'%2015%' group by struct_int_double_string_date.CHECK_DATE having total > 10 order by total desc""").collect
}
       

//Complex_Struct_TC_196
test("Complex_Struct_TC_196", Include) {
  sql(s"""select struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE,count(distinct struct_int_double_string_date.id) as count,sum( struct_int_double_string_date.id+ cust_id) as total,count(distinct struct_int_double_string_date.salary) as total_salary from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dublin') group by struct_int_double_string_date.id,struct_int_double_string_date.Country,struct_int_double_string_date.CHECK_DATE order by total desc""").collect
}
       

//Complex_Struct_TC_197
test("Complex_Struct_TC_197", Include) {
  sql(s"""select count(distinct(year (struct_int_double_string_date.CHECK_DATE))) from Struct_com where struct_int_double_string_date.COUNTRY like '%England%'""").collect
}
       

//Complex_Struct_TC_198
test("Complex_Struct_TC_198", Include) {
  sql(s"""select year (struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY like '%England%'""").collect
}
       

//Complex_Struct_TC_199
test("Complex_Struct_TC_199", Include) {
  sql(s"""select month (struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dubai')""").collect
}
       

//Complex_Struct_TC_200
test("Complex_Struct_TC_200", Include) {
  sql(s"""select day(struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dubai') group by struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_201
test("Complex_Struct_TC_201", Include) {
  sql(s"""select hour (struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dubai') group by struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_202
test("Complex_Struct_TC_202", Include) {
  sql(s"""select count (minute (struct_int_double_string_date.CHECK_DATE)) from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dubai') group by struct_int_double_string_date.CHECK_DATE order by struct_int_double_string_date.CHECK_DATE""").collect
}
       

//Complex_Struct_TC_203
test("Complex_Struct_TC_203", Include) {
  sql(s"""select second (struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY in('England','Dubai') group by struct_int_double_string_date.CHECK_DATE having count(struct_int_double_string_date.CHECK_DATE)>=2""").collect
}
       

//Complex_Struct_TC_204
test("Complex_Struct_TC_204", Include) {
  sql(s"""select WEEKOFYEAR(struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.COUNTRY like'%a%' group by struct_int_double_string_date.CHECK_DATE having count(struct_int_double_string_date.CHECK_DATE)<=2""").collect
}
       

//Complex_Struct_TC_205
test("Complex_Struct_TC_205", Include) {
  sql(s"""select DATEDIFF(struct_int_double_string_date.CHECK_DATE ,'2020-08-09 00:00:00')from Struct_com where struct_int_double_string_date.COUNTRY like'%a%'""").collect
}
       

//Complex_Struct_TC_206
test("Complex_Struct_TC_206", Include) {
  sql(s"""select DATE_ADD(struct_int_double_string_date.CHECK_DATE ,5) from Struct_com where struct_int_double_string_date.COUNTRY like'%a%'""").collect
}
       

//Complex_Struct_TC_207
test("Complex_Struct_TC_207", Include) {
  sql(s"""select DATE_SUB(struct_int_double_string_date.CHECK_DATE ,5) from Struct_com where struct_int_double_string_date.id like'%7%'""").collect
}
       

//Complex_Struct_TC_208
test("Complex_Struct_TC_208", Include) {
  sql(s"""select unix_timestamp(struct_int_double_string_date.CHECK_DATE) from Struct_com where struct_int_double_string_date.id>=5702""").collect
}
       

//Complex_Struct_TC_209
test("Complex_Struct_TC_209", Include) {
  sql(s"""select substr(struct_int_double_string_date.Country,3,4) from Struct_com""").collect
}
       

//Complex_Struct_TC_210
test("Complex_Struct_TC_210", Include) {
  sql(s"""select substr(struct_int_double_string_date.Country,3) from Struct_com where struct_int_double_string_date.COUNTRY rlike 'a'""").collect
}
       

//Complex_Struct_TC_211
test("Complex_Struct_TC_211", Include) {
  sql(s"""SELECT struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.Country, SUM(struct_int_double_string_date.id) AS Sum FROM (select * from Struct_com) SUB_QRY WHERE struct_int_double_string_date.id > 5700 GROUP BY struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.Country ORDER BY struct_int_double_string_date.COUNTRY asc,struct_int_double_string_date.CHECK_DATE asc,struct_int_double_string_date.CHECK_DATE asc, struct_int_double_string_date.Country asc""").collect
}
       

//Complex_Struct_TC_212
test("Complex_Struct_TC_212", Include) {
  sql(s"""SELECT struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.Country, SUM(struct_int_double_string_date.id) AS Sum FROM (select * from Struct_com) SUB_QRY WHERE struct_int_double_string_date.COUNTRY='England' GROUP BY struct_int_double_string_date.COUNTRY,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.CHECK_DATE,struct_int_double_string_date.Country ORDER BY struct_int_double_string_date.COUNTRY asc,struct_int_double_string_date.CHECK_DATE asc,struct_int_double_string_date.CHECK_DATE asc, struct_int_double_string_date.Country asc""").collect
}
       
override def afterAll {
sql("drop table if exists Struct_com")
sql("drop table if exists Struct_com_hive")
}
}
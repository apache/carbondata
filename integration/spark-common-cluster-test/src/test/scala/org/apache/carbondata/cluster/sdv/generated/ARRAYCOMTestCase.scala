
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
 * Test Class for Arraycom to verify all scenerios
 */

class ARRAYCOMTestCase extends QueryTest with BeforeAndAfterAll {
         



  override def beforeAll(): Unit = {
    sql(s"""drop table  if exists Array_com""").collect

    sql(s"""drop table  if exists Array_com_hive""").collect
  }
       

//COMPLEX_Array_Create Table
test("COMPLEX_Array_Create Table", Include) {
  sql(s"""create table Array_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table Array_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT double, HQ_DEPOSIT double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//COMPLEX_Array_DataLoad
test("COMPLEX_Array_DataLoad", Include) {
  sql(s""" LOAD DATA INPATH '$resourcesPath/Data/complex/Array.csv' INTO table Array_com  options ('DELIMITER'=',', 'QUOTECHAR'='"', 'FILEHEADER'='CUST_ID,YEAR,MONTH,AGE,GENDER,EDUCATED,IS_MARRIED,ARRAY_INT,ARRAY_STRING,ARRAY_DATE,CARD_COUNT,DEBIT_COUNT,CREDIT_COUNT,DEPOSIT,HQ_DEPOSIT','COMPLEX_DELIMITER_LEVEL_1'='$DOLLAR')
""").collect

  sql(s""" LOAD DATA INPATH '$resourcesPath/Data/complex/Array.csv' INTO table Array_com_hive  """).collect

}
       

//COMPLEX_Array_TC002
test("COMPLEX_Array_TC002", Include) {
  sql(s"""select array_int[0], array_int[0]+ 10 as a  from array_com    """).collect
}
       

//COMPLEX_Array_TC003
test("COMPLEX_Array_TC003", Include) {
  sql(s"""select array_int[0], array_string [0]+ 10 as a  from array_com    """).collect
}
       

//COMPLEX_Array_TC004
test("COMPLEX_Array_TC004", Include) {
  sql(s"""select array_int[1],array_int[1]+cust_id as a  from array_com""").collect
}
       

//COMPLEX_Array_TC005
test("COMPLEX_Array_TC005", Include) {
  sql(s"""select array_int[1],array_int[1]+age  as a  from array_com""").collect
}
       

//COMPLEX_Array_TC006
test("COMPLEX_Array_TC006", Include) {
  sql(s"""select array_int[1],array_int[1]+deposit  as a  from array_com""").collect
}
       

//COMPLEX_Array_TC007
test("COMPLEX_Array_TC007", Include) {
  sql(s"""select concat(array_string[1],'_',cust_id)  as a  from array_com""").collect
}
       

//COMPLEX_Array_TC008
test("COMPLEX_Array_TC008", Include) {
  sql(s"""select concat(array_string[1],'_',educated)  as a  from array_com""").collect
}
       

//COMPLEX_Array_TC009
test("COMPLEX_Array_TC009", Include) {
  sql(s"""select concat(array_string[1],'_',educated,'@',is_married)  as a  from array_com""").collect
}
       

//COMPLEX_Array_TC010
test("COMPLEX_Array_TC010", Include) {
  sql(s"""select array_string[1], sum(array_int[0]+ 10) as a from array_com group by  array_string[1]""").collect
}
       

//COMPLEX_Array_TC011
test("COMPLEX_Array_TC011", Include) {
  sql(s"""select array_string[1], sum(array_int[0]+ 10) as a from array_com group by  array_string[1] order by array_string[1]""").collect
}
       

//COMPLEX_Array_TC012
test("COMPLEX_Array_TC012", Include) {
  sql(s"""select array_date[1], sum(array_int[0]+ 10) as a from array_com  group by  array_date[1] order by array_date[1]""").collect
}
       

//COMPLEX_Array_TC014
test("COMPLEX_Array_TC014", Include) {
  sql(s"""select array_int[1], avg(array_int[1]+ 10) as a from array_com group by  array_int[1]""").collect
}
       

//COMPLEX_Array_TC016
test("COMPLEX_Array_TC016", Include) {
  sql(s"""select array_date[0], avg(array_int[0]+deposit) Total from array_com group by  array_date[0] order by array_date[0]""").collect
}
       

//COMPLEX_Array_TC017
test("COMPLEX_Array_TC017", Include) {
  sql(s"""select array_date[1], count(array_int[0])+10 as a  from array_com  group by  array_date[1]
""").collect
}
       

//COMPLEX_Array_TC018
test("COMPLEX_Array_TC018", Include) {
  sql(s"""select array_string[1], count(array_string[1]+ 10) Total from array_com group by  array_string[1] order by array_string[1]""").collect
}
       

//COMPLEX_Array_TC019
test("COMPLEX_Array_TC019", Include) {
  sql(s"""select array_string[1], count(array_string[1]) Total from array_com group by  array_string[1]""").collect
}
       

//COMPLEX_Array_TC020
test("COMPLEX_Array_TC020", Include) {
  sql(s"""select array_string[1], count(array_string[1]) Total from array_com group by  array_string[1] having count(array_string[1])>2""").collect
}
       

//COMPLEX_Array_TC024
test("COMPLEX_Array_TC024", Include) {
  sql(s"""select array_string[1], min(array_string[1]) Total from array_com group by  array_string[1] order by array_string[1]
""").collect
}
       

//COMPLEX_Array_TC025
test("COMPLEX_Array_TC025", Include) {
  sql(s"""select  min(array_date[1]) Total from array_com group by  array_date[1] order by array_date[1]""").collect
}
       

//COMPLEX_Array_TC026
test("COMPLEX_Array_TC026", Include) {
  sql(s"""select cust_id , min(array_int[0]+array_int[0])  Total from array_com  group by  cust_id  order by cust_id """).collect
}
       

//COMPLEX_Array_TC028
test("COMPLEX_Array_TC028", Include) {
  sql(s"""select max(array_date[1]) Total from array_com group by  array_date[1] order by array_date[1]""").collect
}
       

//COMPLEX_Array_TC029
test("COMPLEX_Array_TC029", Include) {
  sql(s"""select max(array_string[0]) Total from array_com group by  array_string[0] order by array_string[0]""").collect
}
       

//COMPLEX_Array_TC031
test("COMPLEX_Array_TC031", Include) {
  sql(s"""select array_string[0] ,sum(distinct array_string[0])+10 from array_com group by array_string[0]""").collect
}
       

//COMPLEX_Array_TC033
test("COMPLEX_Array_TC033", Include) {
  sql(s"""select count(distinct array_string[0])+10 as a,array_int[0] from array_com group by array_int[0]""").collect
}
       

//COMPLEX_Array_TC034
test("COMPLEX_Array_TC034", Include) {
  sql(s"""select count(*) as a  from array_com
""").collect
}
       

//COMPLEX_Array_TC035
test("COMPLEX_Array_TC035", Include) {
  sql(s"""Select count(1) as a  from array_com
""").collect
}
       

//COMPLEX_Array_TC036
test("COMPLEX_Array_TC036", Include) {
  sql(s"""select count(array_date[0]) as a   from array_com""").collect
}
       

//COMPLEX_Array_TC037
test("COMPLEX_Array_TC037", Include) {
  sql(s"""select count(array_string[1])  as a from array_com""").collect
}
       

//COMPLEX_Array_TC040
test("COMPLEX_Array_TC040", Include) {
  sql(s"""select count (if(array_date[1]>'100',NULL,array_date[1]))  a from array_Com""").collect
}
       

//COMPLEX_Array_TC041
test("COMPLEX_Array_TC041", Include) {
  sql(s"""select count (if(array_int[0]>100,NULL,array_int[0]))  a from array_com""").collect
}
       

//COMPLEX_Array_TC042
test("COMPLEX_Array_TC042", Include) {
  sql(s"""select count (if(array_string[1]>100,NULL,array_string[1]))  a from array_Com""").collect
}
       

//COMPLEX_Array_TC043
test("COMPLEX_Array_TC043", Include) {
  sql(s"""select sum(DISTINCT  array_int[0]) a  from array_com
""").collect
}
       

//COMPLEX_Array_TC045
test("COMPLEX_Array_TC045", Include) {
  sql(s"""select sum (if(array_String[0]>100,NULL,array_String[0]))  a from array_com""").collect
}
       

//COMPLEX_Array_TC046
test("COMPLEX_Array_TC046", Include) {
  sql(s"""select sum (if(array_date[0]>'100',NULL,array_String[0]))  a from array_com""").collect
}
       

//COMPLEX_Array_TC047
test("COMPLEX_Array_TC047", Include) {
  sql(s"""select avg(array_int[0]) from array_com
""").collect
}
       

//COMPLEX_Array_TC048
test("COMPLEX_Array_TC048", Include) {
  sql(s"""select sum(array_int[0]) from array_com
""").collect
}
       

//COMPLEX_Array_TC049
test("COMPLEX_Array_TC049", Include) {
  sql(s"""select max(array_string[0]) from array_com
""").collect
}
       

//COMPLEX_Array_TC050
test("COMPLEX_Array_TC050", Include) {
  sql(s"""select count(array_string[0]) from array_com
""").collect
}
       

//COMPLEX_Array_TC051
test("COMPLEX_Array_TC051", Include) {
  sql(s"""select min(array_date[1]) from array_com
""").collect
}
       

//COMPLEX_Array_TC052
test("COMPLEX_Array_TC052", Include) {
  sql(s"""select avg (if(array_int[0]>100,NULL,array_int[0]))  a from array_com""").collect
}
       

//COMPLEX_Array_TC053
test("COMPLEX_Array_TC053", Include) {
  sql(s"""select avg (if(array_string[1]>100,NULL,array_string[1]))  a from array_com
""").collect
}
       

//COMPLEX_Array_TC054
test("COMPLEX_Array_TC054", Include) {
  sql(s"""select min (if(array_string[1]>100,NULL,array_string[1]))  a from array_com""").collect
}
       

//COMPLEX_Array_TC055
test("COMPLEX_Array_TC055", Include) {
  sql(s"""select min (if(array_int[0]>100,NULL,array_int[0]))  a from array_com
""").collect
}
       

//COMPLEX_Array_TC056
test("COMPLEX_Array_TC056", Include) {
  sql(s"""select max (if(array_int[0]>100,NULL,array_int[0]))  a from array_com
""").collect
}
       

//COMPLEX_Array_TC058
test("COMPLEX_Array_TC058", Include) {
  sql(s"""select variance(array_int[1]) as a   from array_com""").collect
}
       

//COMPLEX_Array_TC059
test("COMPLEX_Array_TC059", Include) {
  sql(s"""select var_pop(array_int[1])  as a from array_com""").collect
}
       

//COMPLEX_Array_TC060
test("COMPLEX_Array_TC060", Include) {
  sql(s"""select var_samp(array_int[1]) as a  from array_com""").collect
}
       

//COMPLEX_Array_TC061
test("COMPLEX_Array_TC061", Include) {
  sql(s"""select stddev_pop(array_int[1]) as a  from array_com
""").collect
}
       

//COMPLEX_Array_TC062
test("COMPLEX_Array_TC062", Include) {
  sql(s"""select stddev_samp(array_int[1])  as a from array_com""").collect
}
       

//COMPLEX_Array_TC063
test("COMPLEX_Array_TC063", Include) {
  sql(s"""select covar_pop(array_int[1],array_int[1]) as a  from array_com""").collect
}
       

//COMPLEX_Array_TC064
test("COMPLEX_Array_TC064", Include) {
  sql(s"""select covar_samp(array_int[1],array_int[1]) as a  from array_com""").collect
}
       

//COMPLEX_Array_TC065
test("COMPLEX_Array_TC065", Include) {
  sql(s"""select corr(array_int[1],array_int[1])  as a from array_com""").collect
}
       

//COMPLEX_Array_TC066
test("COMPLEX_Array_TC066", Include) {
  sql(s"""select percentile(array_int[1],0.2) as  a  from array_com
""").collect
}
       

//COMPLEX_Array_TC067
test("COMPLEX_Array_TC067", Include) {
  sql(s"""select percentile(array_int[1],array(0,0.2,0.3,1))  as  a from array_com""").collect
}
       

//COMPLEX_Array_TC068
test("COMPLEX_Array_TC068", Include) {
  sql(s"""select percentile_approx(array_int[1],0.2) as a  from array_com""").collect
}
       

//COMPLEX_Array_TC069
test("COMPLEX_Array_TC069", Include) {
  sql(s"""select percentile_approx(array_int[1],0.2,5) as a  from array_com
""").collect
}
       

//COMPLEX_Array_TC070
test("COMPLEX_Array_TC070", Include) {
  sql(s"""select percentile_approx(array_int[1],array(0.2,0.3,0.99))  as a from array_com""").collect
}
       

//COMPLEX_Array_TC071
test("COMPLEX_Array_TC071", Include) {
  sql(s"""select percentile_approx(array_int[1],array(0.2,0.3,0.99),5) as a from array_com""").collect
}
       

//COMPLEX_Array_TC072
test("COMPLEX_Array_TC072", Include) {
  sql(s"""select * from (select collect_set(array_string[0]) as myseries from array_com) aa sort by myseries
""").collect
}
       

//COMPLEX_Array_TC076
test("COMPLEX_Array_TC076", Include) {
  sql(s"""select array_date[1],count(array_string[0]) a from array_Com group by array_date[1] order by array_date[1]""").collect
}
       

//COMPLEX_Array_TC077
test("COMPLEX_Array_TC077", Include) {
  sql(s"""select array_string[0],count(array_date[1]) a from array_Com group by array_string[0] order by a
""").collect
}
       

//COMPLEX_Array_TC078
test("COMPLEX_Array_TC078", Include) {
  sql(s"""select array_date[1],array_string[1],count(array_int[1])  a from array_Com group by array_string[1],array_date[1] order by array_date[1],array_string[1]""").collect
}
       

//COMPLEX_Array_TC079
test("COMPLEX_Array_TC079", Include) {
  sql(s"""select count(distinct array_string[0]) a,array_int[1] from array_Com group by array_int[1]""").collect
}
       

//COMPLEX_Array_TC081
test("COMPLEX_Array_TC081", Include) {
  sql(s"""select array_string[0],sum(array_int[1]) a from array_com group by array_string[0] order by array_string[0]""").collect
}
       

//COMPLEX_Array_TC082
test("COMPLEX_Array_TC082", Include) {
  sql(s"""select array_int[0],array_string[1],array_date[1],sum(array_int[0]) a from array_com group by array_int[0],array_string[1],array_date[1] order by array_int[0],array_string[1],array_date[1]""").collect
}
       

//COMPLEX_Array_TC085
test("COMPLEX_Array_TC085", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1],min(array_int[1]) a from array_com group by array_int[1],array_string[0],array_date[1] order by array_int[1],array_string[0],array_date[1]""").collect
}
       

//COMPLEX_Array_TC086
test("COMPLEX_Array_TC086", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1],min(array_date[0]) a from array_com group by array_int[1],array_string[0],array_date[1] order by array_int[1],array_string[0],array_date[1]""").collect
}
       

//COMPLEX_Array_TC087
test("COMPLEX_Array_TC087", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1],max(array_string[0]) a from array_com group by array_int[1],array_string[0],array_date[1] order by array_int[1],array_string[0],array_date[1]""").collect
}
       

//COMPLEX_Array_TC088
test("COMPLEX_Array_TC088", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1],max(array_int[1]) a from array_com group by array_int[1],array_string[0],array_date[1] order by array_int[1],array_string[0],array_date[1]""").collect
}
       

//COMPLEX_Array_TC089
test("COMPLEX_Array_TC089", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1],max(array_date[0]) a from array_com group by array_int[1],array_string[0],array_date[1] order by array_int[1],array_string[0],array_date[1]""").collect
}
       

//COMPLEX_Array_TC091
test("COMPLEX_Array_TC091", Include) {
  sql(s"""select array_string[1],array_date[0] from array_com where array_string[1] IN ('England','Dublin')""").collect
}
       

//COMPLEX_Array_TC092
test("COMPLEX_Array_TC092", Include) {
  sql(s"""select array_string[1],array_date[0] from array_com where array_string[1] not IN ('England','Dublin')""").collect
}
       

//COMPLEX_Array_TC093
test("COMPLEX_Array_TC093", Include) {
  sql(s"""select array_string[1] from array_com where array_string[1] in (select  array_string[1] from array_com) order by array_string[1]""").collect
}
       

//COMPLEX_Array_TC094
test("COMPLEX_Array_TC094", Include) {
  sql(s"""select  array_string[1], sum(array_int[0]) as a  from array_com where array_int[0] in (select array_int[0]  from array_com) group by array_int[0],array_string[1]""").collect
}
       

//COMPLEX_Array_TC095
test("COMPLEX_Array_TC095", Include) {
  sql(s"""select Upper(array_string[1]) a  from array_com""").collect
}
       

//COMPLEX_Array_TC096
test("COMPLEX_Array_TC096", Include) {
  sql(s"""select Lower(array_string[0]) a  from array_com""").collect
}
       

//COMPLEX_Array_TC101
test("COMPLEX_Array_TC101", Include) {
  sql(s"""select array_string[0],sum(array_int[2]) a from array_com group by array_string[0] order by array_string[0] desc""").collect
}
       

//COMPLEX_Array_TC102
test("COMPLEX_Array_TC102", Include) {
  sql(s"""select array_date[1],array_string[0],sum(array_int[1]) a from array_com group by array_string[0],array_date[1] order by array_string[0],array_date[1] desc ,a desc
""").collect
}
       

//COMPLEX_Array_TC103
test("COMPLEX_Array_TC103", Include) {
  sql(s"""select array_date[2],array_int[1],array_string[2] as a from array_com  order by a asc limit 10
""").collect
}
       

//COMPLEX_Array_TC104
test("COMPLEX_Array_TC104", Include) {
  sql(s"""select array_int[2] from array_com where  (array_string[2] =='Aasgaardstrand') and (array_date[2]=='2013-11-26 00:00:00.0')
""").collect
}
       

//COMPLEX_Array_TC105
test("COMPLEX_Array_TC105", Include) {
  sql(s"""select array_int[2],array_string[2],array_date[2] from array_com where  (array_string[2] == 'Abu Dhabi') and (array_int[2]==9125)
""").collect
}
       

//COMPLEX_Array_TC106
test("COMPLEX_Array_TC106", Include) {
  sql(s"""select array_int[2],array_string[2],array_date[2] from array_com where  (array_string[2] == 'Abu Dhabi') or (array_int[2]==5702)""").collect
}
       

//COMPLEX_Array_TC107
test("COMPLEX_Array_TC107", Include) {
  sql(s"""select array_int[2],array_string[2],array_date[2]  from array_Com where (array_string[2] == 'Abu Dhabi' and array_int[2]==5702) OR (array_string[2] =='Aasgaardstrand' or array_date[2]=='2013-11-26 00:00:00.0')
""").collect
}
       

//COMPLEX_Array_TC108
test("COMPLEX_Array_TC108", Include) {
  sql(s"""select array_int[2],array_string[2],array_date[2]  from array_Com where (array_string[2] == 'Abu Dhabi' and array_int[2]==5702) OR (array_string[2] =='Aasgaardstrand' and array_date[2]=='2013-11-26 00:00:00.0')

""").collect
}
       

//COMPLEX_Array_TC109
test("COMPLEX_Array_TC109", Include) {
  sql(s"""select array_int[1],array_string[2],array_date[0] from array_Com where array_string[2]!='Abu Dhabi' and  array_date[2]=='2013-11-26 00:00:00.0'""").collect
}
       

//COMPLEX_Array_TC110
test("COMPLEX_Array_TC110", Include) {
  sql(s"""select array_int[1],array_string[1],array_date[0] from array_Com where array_string[1]!='England'""").collect
}
       

//COMPLEX_Array_TC112
test("COMPLEX_Array_TC112", Include) {
  sql(s"""select array_int[1] as a from array_com where array_int[1]<=>cust_id""").collect
}
       

//COMPLEX_Array_TC114
test("COMPLEX_Array_TC114", Include) {
  sql(s"""select array_int[1]  from array_com where array_int[1]<>cust_id""").collect
}
       

//COMPLEX_Array_TC115
test("COMPLEX_Array_TC115", Include) {
  sql(s"""select array_string[1] from array_com where array_int[1] != cust_id""").collect
}
       

//COMPLEX_Array_TC116
test("COMPLEX_Array_TC116", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_int[1]<cust_id""").collect
}
       

//COMPLEX_Array_TC117
test("COMPLEX_Array_TC117", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_int[1]<=cust_id""").collect
}
       

//COMPLEX_Array_TC119
test("COMPLEX_Array_TC119", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_int[1]>=cust_id""").collect
}
       

//COMPLEX_Array_TC120
test("COMPLEX_Array_TC120", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_int[1] NOT BETWEEN month AND  year""").collect
}
       

//COMPLEX_Array_TC121
test("COMPLEX_Array_TC121", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_int[1] BETWEEN month AND  year""").collect
}
       

//COMPLEX_Array_TC122
test("COMPLEX_Array_TC122", Include) {
  sql(s"""select array_int[1], array_string[1] from array_com where array_string[1] IS NULL""").collect
}
       

//COMPLEX_Array_TC123
test("COMPLEX_Array_TC123", Include) {
  sql(s"""select array_int[1],array_string[0] from array_com where array_string[1] IS NOT NULL""").collect
}
       

//COMPLEX_Array_TC124
test("COMPLEX_Array_TC124", Include) {
  sql(s"""select array_int[1],array_string[0] from array_com where array_int[1] IS NOT NULL""").collect
}
       

//COMPLEX_Array_TC125
test("COMPLEX_Array_TC125", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1] from array_com where array_date[1] IS NOT NULL
""").collect
}
       

//COMPLEX_Array_TC126
test("COMPLEX_Array_TC126", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1] from array_com where array_int[1] NOT LIKE array_string[1] AND array_date[1] NOT LIKE  cust_id
""").collect
}
       

//COMPLEX_Array_TC127
test("COMPLEX_Array_TC127", Include) {
  sql(s"""select array_int[1],array_string[0],array_date[1] from array_com where array_int[1]  LIKE array_int[1] and array_string[1] like array_date[1] """).collect
}
       

//COMPLEX_Array_TC128
test("COMPLEX_Array_TC128", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] >505""").collect
}
       

//COMPLEX_Array_TC129
test("COMPLEX_Array_TC129", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] <505""").collect
}
       

//COMPLEX_Array_TC130
test("COMPLEX_Array_TC130", Include) {
  sql(s"""select array_int[1] from array_com where array_date[1] <'2011-07-01 00:00:00'""").collect
}
       

//COMPLEX_Array_TC131
test("COMPLEX_Array_TC131", Include) {
  sql(s"""select array_int[1] from array_com where array_date[1] >'2011-07-01 00:00:00'""").collect
}
       

//COMPLEX_Array_TC132
test("COMPLEX_Array_TC132", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] >=505""").collect
}
       

//COMPLEX_Array_TC133
test("COMPLEX_Array_TC133", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] <=505""").collect
}
       

//COMPLEX_Array_TC134
test("COMPLEX_Array_TC134", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] <=2""").collect
}
       

//COMPLEX_Array_TC135
test("COMPLEX_Array_TC135", Include) {
  sql(s"""select array_int[1] from array_com where array_int[1] >=2""").collect
}
       

//COMPLEX_Array_TC136
test("COMPLEX_Array_TC136", Include) {
  sql(s"""select sum(array_int[2]) a from array_com where array_int[1] >=10 OR (array_int[1] <=1 and array_string[0]='England')""").collect
}
       

//COMPLEX_Array_TC137
test("COMPLEX_Array_TC137", Include) {
  sql(s"""select * from (select array_string[0],if(array_string[0]='England',NULL,array_string[0]) a from array_com) aa  where a IS not NULL""").collect
}
       

//COMPLEX_Array_TC138
test("COMPLEX_Array_TC138", Include) {
  sql(s"""select * from (select array_string[0],if(array_string[0]='England',NULL,array_string[0]) a from array_com) aa  where a IS  NULL""").collect
}
       

//COMPLEX_Array_TC139
test("COMPLEX_Array_TC139", Include) {
  sql(s"""select array_int[2] from array_com where  (array_int[1] == 5754) or (array_date[1]=='2016-09-06 00:00:00.0')""").collect
}
       

//COMPLEX_Array_TC140
test("COMPLEX_Array_TC140", Include) {
  sql(s"""select array_int[2] from array_com where  (array_int[1] == 5754) and (array_date[1]=='2016-09-06 00:00:00.0')""").collect
}
       

//COMPLEX_Array_TC141
test("COMPLEX_Array_TC141", Include) {
  sql(s"""select array_string[1]  from array_com where  (array_string[1] like '%an%') OR ( array_string[1] like '%t%') order by array_string[1]""").collect
}
       

//COMPLEX_Array_TC142
test("COMPLEX_Array_TC142", Include) {
  sql(s"""select cust_id,array_date[1], array_string[1],array_int[1] from array_com where  array_int[1] BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//COMPLEX_Array_TC143
test("COMPLEX_Array_TC143", Include) {
  sql(s"""select cust_id,array_date[1], array_string[1],array_int[1] from array_com where  array_int[0] BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//COMPLEX_Array_TC144
test("COMPLEX_Array_TC144", Include) {
  sql(s"""select cust_id,array_date[1], array_string[1],array_int[1] from array_com where  array_int[0] not BETWEEN 4 AND 5902 ORDER BY cust_id limit 5""").collect
}
       

//COMPLEX_Array_TC145
test("COMPLEX_Array_TC145", Include) {
  sql(s"""select cust_id,array_date[1], array_string[1],array_int[1] from array_com where  array_string[1] NOT LIKE '%r%' AND array_int[1] NOT LIKE 5 ORDER BY array_date[1] limit 5""").collect
}
       

//COMPLEX_Array_TC146
test("COMPLEX_Array_TC146", Include) {
  sql(s"""select cust_id,array_date[1], array_string[1],array_int[1] from array_com where  array_string[1] RLIKE '%t%' ORDER BY array_int[1] limit 5""").collect
}
       

//COMPLEX_Array_TC148
test("COMPLEX_Array_TC148", Include) {
  sql(s"""select  cust_id,array_int[2] from array_com where UPPER(array_string[1]) == 'DUBLIN'""").collect
}
       

//COMPLEX_Array_TC149
test("COMPLEX_Array_TC149", Include) {
  sql(s"""select  cust_id,array_int[2] from array_com where UPPER(array_string[1]) in ('DUBLIN','England','SA')""").collect
}
       

//COMPLEX_Array_TC150
test("COMPLEX_Array_TC150", Include) {
  sql(s"""select  cust_id,array_int[2] from array_com where UPPER(array_string[1]) like '%A%'""").collect
}
       

//COMPLEX_Array_TC151
test("COMPLEX_Array_TC151", Include) {
  sql(s"""select count(array_int[2]) ,array_string[1] from array_com group by array_string[1],array_int[2] having sum (array_int[2])= 9150500""").collect
}
       

//COMPLEX_Array_TC152
test("COMPLEX_Array_TC152", Include) {
  sql(s"""select count(array_int[2]) ,array_string[1] from array_com group by array_string[1],array_int[2] having sum (array_int[2])>= 9150""").collect
}
       

//COMPLEX_Array_TC153
test("COMPLEX_Array_TC153", Include) {
  sql(s"""SELECT array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2] ORDER BY array_int[2], array_date[2] ASC, array_date[2] ASC
 """).collect
}
       

//COMPLEX_Array_TC155
test("COMPLEX_Array_TC155", Include) {
  sql(s"""select array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY WHERE (array_string[1] = "") or (array_int[1]= "") or (array_date[1]= "") GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC""").collect
}
       

//COMPLEX_Array_TC156
test("COMPLEX_Array_TC156", Include) {
  sql(s"""select array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY WHERE not(array_string[1] = "") or not(array_int[1]= "") or not(array_date[1]= "") GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC""").collect
}
       

//COMPLEX_Array_TC157
test("COMPLEX_Array_TC157", Include) {
  sql(s"""select array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY WHERE (array_string[1] > "") or (array_int[1]>"") or (array_date[1]> "") GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC""").collect
}
       

//COMPLEX_Array_TC158
test("COMPLEX_Array_TC158", Include) {
  sql(s"""select array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY WHERE (array_string[1] >="") or (array_int[1]>= "") or (array_date[1]>="") GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC
""").collect
}
       

//COMPLEX_Array_TC159
test("COMPLEX_Array_TC159", Include) {
  sql(s"""SELECT array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY where (array_int[1]>5700) or (array_date[0]like'%2015%')
 GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC""").collect
}
       

//COMPLEX_Array_TC160
test("COMPLEX_Array_TC160", Include) {
  sql(s"""SELECT array_int[2], array_date[2], SUM(array_int[2]) AS Sum_array_int FROM (select * from array_com) SUB_QRY where (array_int[1] < 5700) or (array_date[0] like'%2015%') GROUP BY array_int[2], array_date[2] ORDER BY array_int[2] ASC, array_date[2] ASC""").collect
}
       

//COMPLEX_Array_TC161
test("COMPLEX_Array_TC161", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], AVG(array_int[2]) AS Avg FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC162
test("COMPLEX_Array_TC162", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], SUM(array_int[1]) AS sum FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC163
test("COMPLEX_Array_TC163", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], count(array_int[1]) AS count FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC164
test("COMPLEX_Array_TC164", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], count(array_string[1]) AS count FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC165
test("COMPLEX_Array_TC165", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], count(array_date[2]) AS count FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC166
test("COMPLEX_Array_TC166", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], min(array_date[2]) AS min_date,min(array_string[2]) as min_string,min(array_int[0]) as min_int FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC167
test("COMPLEX_Array_TC167", Include) {
  sql(s"""SELECT array_int[2], array_date[2],array_string[1], max(array_date[2]) AS max_date,max(array_string[2]) as max_string,max(array_int[0]) as max_int FROM (select * from array_com) SUB_QRY GROUP BY array_int[2], array_date[2],array_string[1] order by array_int[2], array_date[2],array_string[1]""").collect
}
       

//COMPLEX_Array_TC169
test("COMPLEX_Array_TC169", Include) {
  sql(s"""select  array_date,sum( array_int[0]+ array_int[1]) as total from array_com where array_int[1]>5700 and  array_date[0]like'%2015%' group by array_date""").collect
}
       

//COMPLEX_Array_TC171
test("COMPLEX_Array_TC171", Include) {
  sql(s"""select  array_date,sum( array_int[0]+ array_int[1]),count(distinct array_int[1]) as total from array_com where array_int[1]>5700 and  array_date[0]like'%2015%' group by array_date having total > 10 order by total desc
""").collect
}
       

//COMPLEX_Array_TC172
test("COMPLEX_Array_TC172", Include) {
  sql(s"""select array_int[1],array_String[0],array_date[2],count(distinct array_int[1]) as count,sum( array_int[0]+ cust_id) as total from array_com where array_string[1] in('England','Dublin') group by array_int[1],array_String[0],array_date[2] order by total desc""").collect
}
       

//COMPLEX_Array_TC173
test("COMPLEX_Array_TC173", Include) {
  sql(s"""select count(distinct(year (array_date[1]))) from array_com where array_String[1] like '%England%'""").collect
}
       

//COMPLEX_Array_TC174
test("COMPLEX_Array_TC174", Include) {
  sql(s"""select year (array_date[1]) from array_com where array_String[1] like '%England%'""").collect
}
       

//COMPLEX_Array_TC175
test("COMPLEX_Array_TC175", Include) {
  sql(s"""select month (array_date[2]) from array_com where array_String[1] in('England','Dubai')""").collect
}
       

//COMPLEX_Array_TC176
test("COMPLEX_Array_TC176", Include) {
  sql(s"""select day(array_date[2]) from array_com where array_String[1] in('England','Dubai') group by array_date[2] order by array_date[2]""").collect
}
       

//COMPLEX_Array_TC177
test("COMPLEX_Array_TC177", Include) {
  sql(s"""select hour (array_date[2]) from array_com where array_String[1] in('England','Dubai') group by array_date[2] order by array_date[2]""").collect
}
       

//COMPLEX_Array_TC178
test("COMPLEX_Array_TC178", Include) {
  sql(s"""select count (minute (array_date[2])) from array_com where array_String[1] in('England','Dubai') group by array_date[2] order by array_date[2]""").collect
}
       

//COMPLEX_Array_TC179
test("COMPLEX_Array_TC179", Include) {
  sql(s"""select second (array_date[2]) from array_com where array_String[1] in('England','Dubai') group by array_date[2] having count(array_date[2])>=2""").collect
}
       

//COMPLEX_Array_TC180
test("COMPLEX_Array_TC180", Include) {
  sql(s"""select WEEKOFYEAR(array_date[2]) from array_com where array_String[1] like'%a%' group by array_date[2] having count(array_date[2])<=2""").collect
}
       

//COMPLEX_Array_TC181
test("COMPLEX_Array_TC181", Include) {
  sql(s"""select DATEDIFF(array_date[2] ,'2020-08-09 00:00:00')from array_com where array_String[1] like'%a%'""").collect
}
       

//COMPLEX_Array_TC182
test("COMPLEX_Array_TC182", Include) {
  sql(s"""select DATE_ADD(array_date[2] ,5) from array_com where array_String[1] like'%a%'""").collect
}
       

//COMPLEX_Array_TC183
test("COMPLEX_Array_TC183", Include) {
  sql(s"""select DATE_SUB(array_date[2] ,5) from array_com where array_int[1] like'%7%'""").collect
}
       

//COMPLEX_Array_TC184
test("COMPLEX_Array_TC184", Include) {
  sql(s"""select unix_timestamp(array_date[2]) from array_com where array_int[1]>=5702""").collect
}
       

//COMPLEX_Array_TC185
test("COMPLEX_Array_TC185", Include) {
  sql(s"""select substr(array_string[2],3,4) from array_com""").collect
}
       

//COMPLEX_Array_TC186
test("COMPLEX_Array_TC186", Include) {
  sql(s"""select substr(array_string[2],3) from array_com where array_String[1] rlike 'a'""").collect
}
       

//COMPLEX_Array_TC187
test("COMPLEX_Array_TC187", Include) {
  sql(s"""SELECT array_string[1],array_date[0],array_date[2],array_string[2], SUM(array_int[2]) AS Sum FROM (select * from array_com) SUB_QRY WHERE array_int[1] > 5700 GROUP BY array_string[1],array_date[0],array_date[2],array_string[2] ORDER BY array_string[1] asc,array_date[0] asc,array_date[2]asc, array_string[2]asc""").collect
}
       

//COMPLEX_Array_TC188
test("COMPLEX_Array_TC188", Include) {
  sql(s"""SELECT array_string[1],array_date[0],array_date[2],array_string[2], SUM(array_int[2]) AS Sum FROM (select * from array_com) SUB_QRY WHERE array_string[1]='England' GROUP BY array_string[1],array_date[0],array_date[2],array_string[2] ORDER BY array_string[1] asc,array_date[0] asc,array_date[2]asc, array_string[2]asc""").collect
}
       
override def afterAll {
sql("drop table if exists Array_com")
sql("drop table if exists Array_com_hive")
}
}
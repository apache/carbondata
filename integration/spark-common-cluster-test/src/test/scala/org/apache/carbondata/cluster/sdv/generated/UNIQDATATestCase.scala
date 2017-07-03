
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
 * Test Class for uniqdata to verify all scenerios
 */

class UNIQDATATestCase extends QueryTest with BeforeAndAfterAll {
         

//UNIQDATA_CreateTable_Drop
test("UNIQDATA_CreateTable_Drop", Include) {
  sql(s"""drop table if exists  uniqdata""").collect

  sql(s"""drop table if exists  UNIQDATA_hive""").collect

}
       

//UNIQDATA_CreateTable
test("UNIQDATA_CreateTable", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE UNIQDATA_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//UNIQDATA_DataLoad1
test("UNIQDATA_DataLoad1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table UNIQDATA_hive """).collect

}
       

//UNIQDATA_DataLoad3
test("UNIQDATA_DataLoad3", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//UNIQDATA_DataLoad5
test("UNIQDATA_DataLoad5", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table UNIQDATA_hive""").collect

}
       

//UNIQDATA_DataLoad6
test("UNIQDATA_DataLoad6", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table UNIQDATA_hive""").collect

}
       

//UNIQDATA_DataLoad7
test("UNIQDATA_DataLoad7", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_1_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_1_UniqData.csv' into table UNIQDATA_hive""").collect

}
       

//UNIQDATA_DEFAULT_TC001
test("UNIQDATA_DEFAULT_TC001", Include) {
  checkAnswer(s"""select count(CUST_NAME) from UNIQDATA""",
    s"""select count(CUST_NAME) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC002
test("UNIQDATA_DEFAULT_TC002", Include) {
  sql(s"""select count(DISTINCT CUST_NAME) as a from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC003
test("UNIQDATA_DEFAULT_TC003", Include) {
  sql(s"""select sum(INTEGER_COLUMN1)+10 as a ,CUST_NAME  from UNIQDATA group by CUST_NAME order by CUST_NAME""").collect
}
       

//UNIQDATA_DEFAULT_TC004
test("UNIQDATA_DEFAULT_TC004", Include) {
  sql(s"""select max(CUST_NAME),min(CUST_NAME) from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC005
test("UNIQDATA_DEFAULT_TC005", Include) {
  checkAnswer(s"""select min(CUST_NAME), max(CUST_NAME) Total from UNIQDATA group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(CUST_NAME), max(CUST_NAME) Total from UNIQDATA_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_DEFAULT_TC006
test("UNIQDATA_DEFAULT_TC006", Include) {
  checkAnswer(s"""select last(CUST_NAME) a from UNIQDATA  group by CUST_NAME order by CUST_NAME limit 1""",
    s"""select last(CUST_NAME) a from UNIQDATA_hive  group by CUST_NAME order by CUST_NAME limit 1""")
}
       

//UNIQDATA_DEFAULT_TC007
test("UNIQDATA_DEFAULT_TC007", Include) {
  checkAnswer(s"""select FIRST(CUST_NAME) a from UNIQDATA group by CUST_NAME order by CUST_NAME limit 1""",
    s"""select FIRST(CUST_NAME) a from UNIQDATA_hive group by CUST_NAME order by CUST_NAME limit 1""")
}
       

//UNIQDATA_DEFAULT_TC008
test("UNIQDATA_DEFAULT_TC008", Include) {
  checkAnswer(s"""select CUST_NAME,count(CUST_NAME) a from UNIQDATA group by CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME,count(CUST_NAME) a from UNIQDATA_hive group by CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC009
test("UNIQDATA_DEFAULT_TC009", Include) {
  checkAnswer(s"""select Lower(CUST_NAME) a  from UNIQDATA order by CUST_NAME""",
    s"""select Lower(CUST_NAME) a  from UNIQDATA_hive order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC010
test("UNIQDATA_DEFAULT_TC010", Include) {
  checkAnswer(s"""select distinct CUST_NAME from UNIQDATA order by CUST_NAME""",
    s"""select distinct CUST_NAME from UNIQDATA_hive order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC011
test("UNIQDATA_DEFAULT_TC011", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA order by CUST_NAME limit 101 """,
    s"""select CUST_NAME from UNIQDATA_hive order by CUST_NAME limit 101 """)
}
       

//UNIQDATA_DEFAULT_TC012
test("UNIQDATA_DEFAULT_TC012", Include) {
  checkAnswer(s"""select CUST_NAME as a from UNIQDATA  order by a asc limit 10""",
    s"""select CUST_NAME as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC013
test("UNIQDATA_DEFAULT_TC013", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select CUST_NAME from UNIQDATA_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_DEFAULT_TC014
test("UNIQDATA_DEFAULT_TC014", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME !='CUST_NAME_01844' order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME !='CUST_NAME_01844' order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC015
test("UNIQDATA_DEFAULT_TC015", Include) {
  checkAnswer(s"""select CUST_NAME  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_NAME  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC016
test("UNIQDATA_DEFAULT_TC016", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC017
test("UNIQDATA_DEFAULT_TC017", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME >'CUST_NAME_01840' order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME >'CUST_NAME_01840' order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC018
test("UNIQDATA_DEFAULT_TC018", Include) {
  checkAnswer(s"""select CUST_NAME  from UNIQDATA where CUST_NAME<>CUST_NAME""",
    s"""select CUST_NAME  from UNIQDATA_hive where CUST_NAME<>CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC019
test("UNIQDATA_DEFAULT_TC019", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME != BIGINT_COLUMN2 order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME != BIGINT_COLUMN2 order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC020
test("UNIQDATA_DEFAULT_TC020", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where BIGINT_COLUMN2<CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where BIGINT_COLUMN2<CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC021
test("UNIQDATA_DEFAULT_TC021", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where DECIMAL_COLUMN1<=CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where DECIMAL_COLUMN1<=CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC022
test("UNIQDATA_DEFAULT_TC022", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME <'CUST_NAME_01844' order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME <'CUST_NAME_01844' order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC023
test("UNIQDATA_DEFAULT_TC023", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where CUST_NAME IS NULL""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where CUST_NAME IS NULL""")
}
       

//UNIQDATA_DEFAULT_TC024
test("UNIQDATA_DEFAULT_TC024", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where CUST_NAME IS NOT NULL order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where CUST_NAME IS NOT NULL order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC025
test("UNIQDATA_DEFAULT_TC025", Include) {
  checkAnswer(s"""Select count(CUST_NAME),min(CUST_NAME) from UNIQDATA """,
    s"""Select count(CUST_NAME),min(CUST_NAME) from UNIQDATA_hive """)
}
       

//UNIQDATA_DEFAULT_TC026
test("UNIQDATA_DEFAULT_TC026", Include) {
  checkAnswer(s"""select count(DISTINCT CUST_NAME,DECIMAL_COLUMN1) as a from UNIQDATA""",
    s"""select count(DISTINCT CUST_NAME,DECIMAL_COLUMN1) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC027
test("UNIQDATA_DEFAULT_TC027", Include) {
  checkAnswer(s"""select max(CUST_NAME),min(CUST_NAME),count(CUST_NAME) from UNIQDATA""",
    s"""select max(CUST_NAME),min(CUST_NAME),count(CUST_NAME) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC028
test("UNIQDATA_DEFAULT_TC028", Include) {
  checkAnswer(s"""select sum(CUST_NAME),avg(CUST_NAME),count(CUST_NAME) a  from UNIQDATA""",
    s"""select sum(CUST_NAME),avg(CUST_NAME),count(CUST_NAME) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC029
test("UNIQDATA_DEFAULT_TC029", Include) {
  sql(s"""select last(CUST_NAME),Min(CUST_NAME),max(CUST_NAME)  a from UNIQDATA  order by a""").collect
}
       

//UNIQDATA_DEFAULT_TC030
test("UNIQDATA_DEFAULT_TC030", Include) {
  sql(s"""select FIRST(CUST_NAME),Last(CUST_NAME) a from UNIQDATA group by CUST_NAME order by CUST_NAME limit 1""").collect
}
       

//UNIQDATA_DEFAULT_TC031
test("UNIQDATA_DEFAULT_TC031", Include) {
  checkAnswer(s"""select CUST_NAME,count(CUST_NAME) a from UNIQDATA group by CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME,count(CUST_NAME) a from UNIQDATA_hive group by CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC032
test("UNIQDATA_DEFAULT_TC032", Include) {
  checkAnswer(s"""select Lower(CUST_NAME),upper(CUST_NAME)  a  from UNIQDATA order by CUST_NAME""",
    s"""select Lower(CUST_NAME),upper(CUST_NAME)  a  from UNIQDATA_hive order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC033
test("UNIQDATA_DEFAULT_TC033", Include) {
  checkAnswer(s"""select CUST_NAME as a from UNIQDATA  order by a asc limit 10""",
    s"""select CUST_NAME as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC034
test("UNIQDATA_DEFAULT_TC034", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""",
    s"""select CUST_NAME from UNIQDATA_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""")
}
       

//UNIQDATA_DEFAULT_TC035
test("UNIQDATA_DEFAULT_TC035", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""")
}
       

//UNIQDATA_DEFAULT_TC036
test("UNIQDATA_DEFAULT_TC036", Include) {
  checkAnswer(s"""select CUST_NAME  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_NAME  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC037
test("UNIQDATA_DEFAULT_TC037", Include) {
  checkAnswer(s"""Select count(BIGINT_COLUMN1) from UNIQDATA""",
    s"""Select count(BIGINT_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC038
test("UNIQDATA_DEFAULT_TC038", Include) {
  checkAnswer(s"""select count(DISTINCT BIGINT_COLUMN1) as a from UNIQDATA""",
    s"""select count(DISTINCT BIGINT_COLUMN1) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC039
test("UNIQDATA_DEFAULT_TC039", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)+10 as a ,BIGINT_COLUMN1  from UNIQDATA group by BIGINT_COLUMN1""",
    s"""select sum(BIGINT_COLUMN1)+10 as a ,BIGINT_COLUMN1  from UNIQDATA_hive group by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC040
test("UNIQDATA_DEFAULT_TC040", Include) {
  checkAnswer(s"""select max(BIGINT_COLUMN1),min(BIGINT_COLUMN1) from UNIQDATA""",
    s"""select max(BIGINT_COLUMN1),min(BIGINT_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC041
test("UNIQDATA_DEFAULT_TC041", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1) a  from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC042
test("UNIQDATA_DEFAULT_TC042", Include) {
  checkAnswer(s"""select avg(BIGINT_COLUMN1) a  from UNIQDATA""",
    s"""select avg(BIGINT_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC043
test("UNIQDATA_DEFAULT_TC043", Include) {
  checkAnswer(s"""select min(BIGINT_COLUMN1) a  from UNIQDATA""",
    s"""select min(BIGINT_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC044
test("UNIQDATA_DEFAULT_TC044", Include) {
  sql(s"""select variance(BIGINT_COLUMN1) as a   from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC045
test("UNIQDATA_DEFAULT_TC045", Include) {
  sql(s"""select var_pop(BIGINT_COLUMN1)  as a from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC046
test("UNIQDATA_DEFAULT_TC046", Include) {
  sql(s"""select var_samp(BIGINT_COLUMN1) as a  from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC047
test("UNIQDATA_DEFAULT_TC047", Include) {
  sql(s"""select stddev_pop(BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC048
test("UNIQDATA_DEFAULT_TC048", Include) {
  sql(s"""select stddev_samp(BIGINT_COLUMN1)  as a from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC049
test("UNIQDATA_DEFAULT_TC049", Include) {
  sql(s"""select covar_pop(BIGINT_COLUMN1,BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC050
test("UNIQDATA_DEFAULT_TC050", Include) {
  sql(s"""select covar_samp(BIGINT_COLUMN1,BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC051
test("UNIQDATA_DEFAULT_TC051", Include) {
  sql(s"""select corr(BIGINT_COLUMN1,BIGINT_COLUMN1)  as a from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC052
test("UNIQDATA_DEFAULT_TC052", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,0.2) as a  from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC053
test("UNIQDATA_DEFAULT_TC053", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,0.2,5) as a  from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC054
test("UNIQDATA_DEFAULT_TC054", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,array(0.2,0.3,0.99))  as a from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC055
test("UNIQDATA_DEFAULT_TC055", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,array(0.2,0.3,0.99),5) as a from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC056
test("UNIQDATA_DEFAULT_TC056", Include) {
  sql(s"""select histogram_numeric(BIGINT_COLUMN1,2)  as a from (select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC057
test("UNIQDATA_DEFAULT_TC057", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1+ 10 as a  from UNIQDATA order by a""",
    s"""select BIGINT_COLUMN1+ 10 as a  from UNIQDATA_hive order by a""")
}
       

//UNIQDATA_DEFAULT_TC058
test("UNIQDATA_DEFAULT_TC058", Include) {
  checkAnswer(s"""select min(BIGINT_COLUMN1), max(BIGINT_COLUMN1+ 10) Total from UNIQDATA group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(BIGINT_COLUMN1), max(BIGINT_COLUMN1+ 10) Total from UNIQDATA_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_DEFAULT_TC059
test("UNIQDATA_DEFAULT_TC059", Include) {
  sql(s"""select last(BIGINT_COLUMN1) a from UNIQDATA  order by a""").collect
}
       

//UNIQDATA_DEFAULT_TC060
test("UNIQDATA_DEFAULT_TC060", Include) {
  sql(s"""select FIRST(BIGINT_COLUMN1) a from UNIQDATA order by a""").collect
}
       

//UNIQDATA_DEFAULT_TC061
test("UNIQDATA_DEFAULT_TC061", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1,count(BIGINT_COLUMN1) a from UNIQDATA group by BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1,count(BIGINT_COLUMN1) a from UNIQDATA_hive group by BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC062
test("UNIQDATA_DEFAULT_TC062", Include) {
  checkAnswer(s"""select Lower(BIGINT_COLUMN1) a  from UNIQDATA order by BIGINT_COLUMN1""",
    s"""select Lower(BIGINT_COLUMN1) a  from UNIQDATA_hive order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC063
test("UNIQDATA_DEFAULT_TC063", Include) {
  checkAnswer(s"""select distinct BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1""",
    s"""select distinct BIGINT_COLUMN1 from UNIQDATA_hive order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC064
test("UNIQDATA_DEFAULT_TC064", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA order by BIGINT_COLUMN1 limit 101""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive order by BIGINT_COLUMN1 limit 101""")
}
       

//UNIQDATA_DEFAULT_TC065
test("UNIQDATA_DEFAULT_TC065", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 as a from UNIQDATA  order by a asc limit 10""",
    s"""select BIGINT_COLUMN1 as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC066
test("UNIQDATA_DEFAULT_TC066", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""")
}
       

//UNIQDATA_DEFAULT_TC067
test("UNIQDATA_DEFAULT_TC067", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC068
test("UNIQDATA_DEFAULT_TC068", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840') order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840') order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC069
test("UNIQDATA_DEFAULT_TC069", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC070
test("UNIQDATA_DEFAULT_TC070", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 >123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 >123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC071
test("UNIQDATA_DEFAULT_TC071", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from UNIQDATA where BIGINT_COLUMN1<>BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from UNIQDATA_hive where BIGINT_COLUMN1<>BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC072
test("UNIQDATA_DEFAULT_TC072", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 != BIGINT_COLUMN2 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 != BIGINT_COLUMN2 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC073
test("UNIQDATA_DEFAULT_TC073", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN2<BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN2<BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC074
test("UNIQDATA_DEFAULT_TC074", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1<=BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1<=BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC075
test("UNIQDATA_DEFAULT_TC075", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 <1000 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 <1000 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC076
test("UNIQDATA_DEFAULT_TC076", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 >1000 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 >1000 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC077
test("UNIQDATA_DEFAULT_TC077", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from UNIQDATA where BIGINT_COLUMN1 IS NULL order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from UNIQDATA_hive where BIGINT_COLUMN1 IS NULL order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC078
test("UNIQDATA_DEFAULT_TC078", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from UNIQDATA where DECIMAL_COLUMN1 IS NOT NULL order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from UNIQDATA_hive where DECIMAL_COLUMN1 IS NOT NULL order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC079
test("UNIQDATA_DEFAULT_TC079", Include) {
  checkAnswer(s"""Select count(DECIMAL_COLUMN1) from UNIQDATA""",
    s"""Select count(DECIMAL_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC080
test("UNIQDATA_DEFAULT_TC080", Include) {
  checkAnswer(s"""select count(DISTINCT DECIMAL_COLUMN1) as a from UNIQDATA""",
    s"""select count(DISTINCT DECIMAL_COLUMN1) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC081
test("UNIQDATA_DEFAULT_TC081", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)+10 as a ,DECIMAL_COLUMN1  from UNIQDATA group by DECIMAL_COLUMN1 order by a""",
    s"""select sum(DECIMAL_COLUMN1)+10 as a ,DECIMAL_COLUMN1  from UNIQDATA_hive group by DECIMAL_COLUMN1 order by a""")
}
       

//UNIQDATA_DEFAULT_TC082
test("UNIQDATA_DEFAULT_TC082", Include) {
  checkAnswer(s"""select max(DECIMAL_COLUMN1),min(DECIMAL_COLUMN1) from UNIQDATA""",
    s"""select max(DECIMAL_COLUMN1),min(DECIMAL_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC083
test("UNIQDATA_DEFAULT_TC083", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1) a  from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC084
test("UNIQDATA_DEFAULT_TC084", Include) {
  checkAnswer(s"""select avg(DECIMAL_COLUMN1) a  from UNIQDATA""",
    s"""select avg(DECIMAL_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC085
test("UNIQDATA_DEFAULT_TC085", Include) {
  checkAnswer(s"""select min(DECIMAL_COLUMN1) a  from UNIQDATA""",
    s"""select min(DECIMAL_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC086
test("UNIQDATA_DEFAULT_TC086", Include) {
  sql(s"""select variance(DECIMAL_COLUMN1) as a   from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC087
test("UNIQDATA_DEFAULT_TC087", Include) {
  sql(s"""select var_pop(DECIMAL_COLUMN1)  as a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC088
test("UNIQDATA_DEFAULT_TC088", Include) {
  sql(s"""select var_samp(DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC089
test("UNIQDATA_DEFAULT_TC089", Include) {
  sql(s"""select stddev_pop(DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC090
test("UNIQDATA_DEFAULT_TC090", Include) {
  sql(s"""select stddev_samp(DECIMAL_COLUMN1)  as a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC091
test("UNIQDATA_DEFAULT_TC091", Include) {
  sql(s"""select covar_pop(DECIMAL_COLUMN1,DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC092
test("UNIQDATA_DEFAULT_TC092", Include) {
  sql(s"""select covar_samp(DECIMAL_COLUMN1,DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC093
test("UNIQDATA_DEFAULT_TC093", Include) {
  checkAnswer(s"""select corr(DECIMAL_COLUMN1,DECIMAL_COLUMN1)  as a from UNIQDATA""",
    s"""select corr(DECIMAL_COLUMN1,DECIMAL_COLUMN1)  as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC094
test("UNIQDATA_DEFAULT_TC094", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,0.2) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC095
test("UNIQDATA_DEFAULT_TC095", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,0.2,5) as a  from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC096
test("UNIQDATA_DEFAULT_TC096", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,array(0.2,0.3,0.99))  as a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC097
test("UNIQDATA_DEFAULT_TC097", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,array(0.2,0.3,0.99),5) as a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC098
test("UNIQDATA_DEFAULT_TC098", Include) {
  sql(s"""select histogram_numeric(DECIMAL_COLUMN1,2)  as a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC099
test("UNIQDATA_DEFAULT_TC099", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1+ 10 as a  from UNIQDATA order by a""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1+ 10 as a  from UNIQDATA_hive order by a""")
}
       

//UNIQDATA_DEFAULT_TC100
test("UNIQDATA_DEFAULT_TC100", Include) {
  checkAnswer(s"""select min(DECIMAL_COLUMN1), max(DECIMAL_COLUMN1+ 10) Total from UNIQDATA group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(DECIMAL_COLUMN1), max(DECIMAL_COLUMN1+ 10) Total from UNIQDATA_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_DEFAULT_TC101
test("UNIQDATA_DEFAULT_TC101", Include) {
  sql(s"""select last(DECIMAL_COLUMN1) a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC102
test("UNIQDATA_DEFAULT_TC102", Include) {
  sql(s"""select FIRST(DECIMAL_COLUMN1) a from (select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC103
test("UNIQDATA_DEFAULT_TC103", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1,count(DECIMAL_COLUMN1) a from UNIQDATA group by DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1,count(DECIMAL_COLUMN1) a from UNIQDATA_hive group by DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC104
test("UNIQDATA_DEFAULT_TC104", Include) {
  checkAnswer(s"""select Lower(DECIMAL_COLUMN1) a  from UNIQDATA order by a""",
    s"""select Lower(DECIMAL_COLUMN1) a  from UNIQDATA_hive order by a""")
}
       

//UNIQDATA_DEFAULT_TC105
test("UNIQDATA_DEFAULT_TC105", Include) {
  checkAnswer(s"""select distinct DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1""",
    s"""select distinct DECIMAL_COLUMN1 from UNIQDATA_hive order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC106
test("UNIQDATA_DEFAULT_TC106", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA order by DECIMAL_COLUMN1 limit 101""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive order by DECIMAL_COLUMN1 limit 101""")
}
       

//UNIQDATA_DEFAULT_TC107
test("UNIQDATA_DEFAULT_TC107", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 as a from UNIQDATA  order by a asc limit 10""",
    s"""select DECIMAL_COLUMN1 as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC108
test("UNIQDATA_DEFAULT_TC108", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where  (DECIMAL_COLUMN1 == 12345680745.1234000000)  and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where  (DECIMAL_COLUMN1 == 12345680745.1234000000)  and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_DEFAULT_TC109
test("UNIQDATA_DEFAULT_TC109", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC110
test("UNIQDATA_DEFAULT_TC110", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC111
test("UNIQDATA_DEFAULT_TC111", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC112
test("UNIQDATA_DEFAULT_TC112", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 >12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 >12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC113
test("UNIQDATA_DEFAULT_TC113", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where DECIMAL_COLUMN1<>DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where DECIMAL_COLUMN1<>DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC114
test("UNIQDATA_DEFAULT_TC114", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 != BIGINT_COLUMN2 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 != BIGINT_COLUMN2 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC115
test("UNIQDATA_DEFAULT_TC115", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from UNIQDATA where BIGINT_COLUMN2<DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN2<DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC116
test("UNIQDATA_DEFAULT_TC116", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1<=DECIMAL_COLUMN1  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1<=DECIMAL_COLUMN1  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC117
test("UNIQDATA_DEFAULT_TC117", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 <1000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 <1000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC118
test("UNIQDATA_DEFAULT_TC118", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 >1000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 >1000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC119
test("UNIQDATA_DEFAULT_TC119", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where DECIMAL_COLUMN1 IS NULL  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where DECIMAL_COLUMN1 IS NULL  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC120
test("UNIQDATA_DEFAULT_TC120", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from UNIQDATA where DECIMAL_COLUMN1 IS NOT NULL  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from UNIQDATA_hive where DECIMAL_COLUMN1 IS NOT NULL  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC121
test("UNIQDATA_DEFAULT_TC121", Include) {
  checkAnswer(s"""Select count(Double_COLUMN1) from UNIQDATA""",
    s"""Select count(Double_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC122
test("UNIQDATA_DEFAULT_TC122", Include) {
  checkAnswer(s"""select count(DISTINCT Double_COLUMN1) as a from UNIQDATA""",
    s"""select count(DISTINCT Double_COLUMN1) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC123
test("UNIQDATA_DEFAULT_TC123", Include) {
  sql(s"""select sum(Double_COLUMN1)+10 as a ,Double_COLUMN1  from UNIQDATA group by Double_COLUMN1 order by a""").collect
}
       

//UNIQDATA_DEFAULT_TC124
test("UNIQDATA_DEFAULT_TC124", Include) {
  checkAnswer(s"""select max(Double_COLUMN1),min(Double_COLUMN1) from UNIQDATA""",
    s"""select max(Double_COLUMN1),min(Double_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC125
test("UNIQDATA_DEFAULT_TC125", Include) {
  sql(s"""select sum(Double_COLUMN1) a  from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC126
test("UNIQDATA_DEFAULT_TC126", Include) {
  sql(s"""select avg(Double_COLUMN1) a  from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC127
test("UNIQDATA_DEFAULT_TC127", Include) {
  checkAnswer(s"""select min(Double_COLUMN1) a  from UNIQDATA""",
    s"""select min(Double_COLUMN1) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC128
test("UNIQDATA_DEFAULT_TC128", Include) {
  sql(s"""select variance(Double_COLUMN1) as a   from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC129
test("UNIQDATA_DEFAULT_TC129", Include) {
  sql(s"""select var_pop(Double_COLUMN1)  as a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC130
test("UNIQDATA_DEFAULT_TC130", Include) {
  sql(s"""select var_samp(Double_COLUMN1) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC131
test("UNIQDATA_DEFAULT_TC131", Include) {
  sql(s"""select stddev_pop(Double_COLUMN1) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC132
test("UNIQDATA_DEFAULT_TC132", Include) {
  sql(s"""select stddev_samp(Double_COLUMN1)  as a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC133
test("UNIQDATA_DEFAULT_TC133", Include) {
  sql(s"""select covar_pop(Double_COLUMN1,Double_COLUMN1) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC134
test("UNIQDATA_DEFAULT_TC134", Include) {
  sql(s"""select covar_samp(Double_COLUMN1,Double_COLUMN1) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC135
test("UNIQDATA_DEFAULT_TC135", Include) {
  checkAnswer(s"""select corr(Double_COLUMN1,Double_COLUMN1)  as a from UNIQDATA""",
    s"""select corr(Double_COLUMN1,Double_COLUMN1)  as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC136
test("UNIQDATA_DEFAULT_TC136", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,0.2) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC137
test("UNIQDATA_DEFAULT_TC137", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,0.2,5) as a  from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC138
test("UNIQDATA_DEFAULT_TC138", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,array(0.2,0.3,0.99))  as a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC139
test("UNIQDATA_DEFAULT_TC139", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,array(0.2,0.3,0.99),5) as a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC140
test("UNIQDATA_DEFAULT_TC140", Include) {
  sql(s"""select histogram_numeric(Double_COLUMN1,2)  as a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC141
test("UNIQDATA_DEFAULT_TC141", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1+ 10 as a  from UNIQDATA order by a""",
    s"""select Double_COLUMN1, Double_COLUMN1+ 10 as a  from UNIQDATA_hive order by a""")
}
       

//UNIQDATA_DEFAULT_TC142
test("UNIQDATA_DEFAULT_TC142", Include) {
  checkAnswer(s"""select min(Double_COLUMN1), max(Double_COLUMN1+ 10) Total from UNIQDATA group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(Double_COLUMN1), max(Double_COLUMN1+ 10) Total from UNIQDATA_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_DEFAULT_TC143
test("UNIQDATA_DEFAULT_TC143", Include) {
  sql(s"""select last(Double_COLUMN1) a from (select Double_COLUMN1 from UNIQDATA order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_DEFAULT_TC144
test("UNIQDATA_DEFAULT_TC144", Include) {
  checkAnswer(s"""select FIRST(Double_COLUMN1) a from UNIQDATA order by a""",
    s"""select FIRST(Double_COLUMN1) a from UNIQDATA_hive order by a""")
}
       

//UNIQDATA_DEFAULT_TC145
test("UNIQDATA_DEFAULT_TC145", Include) {
  checkAnswer(s"""select Double_COLUMN1,count(Double_COLUMN1) a from UNIQDATA group by Double_COLUMN1 order by Double_COLUMN1""",
    s"""select Double_COLUMN1,count(Double_COLUMN1) a from UNIQDATA_hive group by Double_COLUMN1 order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC146
test("UNIQDATA_DEFAULT_TC146", Include) {
  checkAnswer(s"""select Lower(Double_COLUMN1) a  from UNIQDATA order by Double_COLUMN1""",
    s"""select Lower(Double_COLUMN1) a  from UNIQDATA_hive order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC147
test("UNIQDATA_DEFAULT_TC147", Include) {
  checkAnswer(s"""select distinct Double_COLUMN1 from UNIQDATA order by Double_COLUMN1""",
    s"""select distinct Double_COLUMN1 from UNIQDATA_hive order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC148
test("UNIQDATA_DEFAULT_TC148", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA  order by Double_COLUMN1 limit 101""",
    s"""select Double_COLUMN1 from UNIQDATA_hive  order by Double_COLUMN1 limit 101""")
}
       

//UNIQDATA_DEFAULT_TC149
test("UNIQDATA_DEFAULT_TC149", Include) {
  checkAnswer(s"""select Double_COLUMN1 as a from UNIQDATA  order by a asc limit 10""",
    s"""select Double_COLUMN1 as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC150
test("UNIQDATA_DEFAULT_TC150", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where  (Double_COLUMN1 == 12345680745.1234000000) and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where  (Double_COLUMN1 == 12345680745.1234000000) and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_DEFAULT_TC151
test("UNIQDATA_DEFAULT_TC151", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 !=12345680745.1234000000  order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 !=12345680745.1234000000  order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC152
test("UNIQDATA_DEFAULT_TC152", Include) {
  checkAnswer(s"""select Double_COLUMN1  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select Double_COLUMN1  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC153
test("UNIQDATA_DEFAULT_TC153", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 !=12345680745.1234000000""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 !=12345680745.1234000000""")
}
       

//UNIQDATA_DEFAULT_TC154
test("UNIQDATA_DEFAULT_TC154", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 >12345680745.1234000000""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 >12345680745.1234000000""")
}
       

//UNIQDATA_DEFAULT_TC155
test("UNIQDATA_DEFAULT_TC155", Include) {
  checkAnswer(s"""select Double_COLUMN1  from UNIQDATA where Double_COLUMN1<>Double_COLUMN1""",
    s"""select Double_COLUMN1  from UNIQDATA_hive where Double_COLUMN1<>Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC156
test("UNIQDATA_DEFAULT_TC156", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 != BIGINT_COLUMN2  order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 != BIGINT_COLUMN2  order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC157
test("UNIQDATA_DEFAULT_TC157", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1 from UNIQDATA where BIGINT_COLUMN2<Double_COLUMN1  order by Double_COLUMN1""",
    s"""select Double_COLUMN1, Double_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN2<Double_COLUMN1  order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC158
test("UNIQDATA_DEFAULT_TC158", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1 from UNIQDATA where Double_COLUMN1<=Double_COLUMN1  order by Double_COLUMN1""",
    s"""select Double_COLUMN1, Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1<=Double_COLUMN1  order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC159
test("UNIQDATA_DEFAULT_TC159", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 <1000 order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 <1000 order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC160
test("UNIQDATA_DEFAULT_TC160", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 >1000 order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 >1000 order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC161
test("UNIQDATA_DEFAULT_TC161", Include) {
  checkAnswer(s"""select Double_COLUMN1  from UNIQDATA where Double_COLUMN1 IS NULL order by Double_COLUMN1""",
    s"""select Double_COLUMN1  from UNIQDATA_hive where Double_COLUMN1 IS NULL order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC162
test("UNIQDATA_DEFAULT_TC162", Include) {
  checkAnswer(s"""select Double_COLUMN1  from UNIQDATA where Double_COLUMN1 IS NOT NULL order by Double_COLUMN1""",
    s"""select Double_COLUMN1  from UNIQDATA_hive where Double_COLUMN1 IS NOT NULL order by Double_COLUMN1""")
}
       

//UNIQDATA_DEFAULT_TC163
test("UNIQDATA_DEFAULT_TC163", Include) {
  checkAnswer(s"""Select count(DOB) from UNIQDATA""",
    s"""Select count(DOB) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC164
test("UNIQDATA_DEFAULT_TC164", Include) {
  checkAnswer(s"""select count(DISTINCT DOB) as a from UNIQDATA""",
    s"""select count(DISTINCT DOB) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC165
test("UNIQDATA_DEFAULT_TC165", Include) {
  checkAnswer(s"""select sum(DOB)+10 as a ,DOB  from UNIQDATA group by DOB order by DOB""",
    s"""select sum(DOB)+10 as a ,DOB  from UNIQDATA_hive group by DOB order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC166
test("UNIQDATA_DEFAULT_TC166", Include) {
  checkAnswer(s"""select max(DOB),min(DOB) from UNIQDATA""",
    s"""select max(DOB),min(DOB) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC167
test("UNIQDATA_DEFAULT_TC167", Include) {
  checkAnswer(s"""select sum(DOB) a  from UNIQDATA""",
    s"""select sum(DOB) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC168
test("UNIQDATA_DEFAULT_TC168", Include) {
  checkAnswer(s"""select avg(DOB) a  from UNIQDATA""",
    s"""select avg(DOB) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC169
test("UNIQDATA_DEFAULT_TC169", Include) {
  checkAnswer(s"""select min(DOB) a  from UNIQDATA""",
    s"""select min(DOB) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC182
test("UNIQDATA_DEFAULT_TC182", Include) {
  sql(s"""select histogram_numeric(DOB,2)  as a from (select DOB from UNIQDATA order by DOB) t""").collect
}
       

//UNIQDATA_DEFAULT_TC183
test("UNIQDATA_DEFAULT_TC183", Include) {
  sql(s"""select last(DOB) a from (select DOB from UNIQDATA order by DOB) t""").collect
}
       

//UNIQDATA_DEFAULT_TC184
test("UNIQDATA_DEFAULT_TC184", Include) {
  sql(s"""select FIRST(DOB) a from (select DOB from UNIQDATA order by DOB) t""").collect
}
       

//UNIQDATA_DEFAULT_TC185
test("UNIQDATA_DEFAULT_TC185", Include) {
  checkAnswer(s"""select DOB,count(DOB) a from UNIQDATA group by DOB order by DOB""",
    s"""select DOB,count(DOB) a from UNIQDATA_hive group by DOB order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC186
test("UNIQDATA_DEFAULT_TC186", Include) {
  checkAnswer(s"""select Lower(DOB) a  from UNIQDATA order by DOB""",
    s"""select Lower(DOB) a  from UNIQDATA_hive order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC187
test("UNIQDATA_DEFAULT_TC187", Include) {
  checkAnswer(s"""select distinct DOB from UNIQDATA order by DOB""",
    s"""select distinct DOB from UNIQDATA_hive order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC188
test("UNIQDATA_DEFAULT_TC188", Include) {
  sql(s"""select DOB from UNIQDATA order by DOB limit 101""").collect
}
       

//UNIQDATA_DEFAULT_TC189
test("UNIQDATA_DEFAULT_TC189", Include) {
  sql(s"""select DOB as a from UNIQDATA  order by a asc limit 10""").collect
}
       

//UNIQDATA_DEFAULT_TC190
test("UNIQDATA_DEFAULT_TC190", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where  (DOB == '1970-06-19 01:00:03') and (DOJ=='1970-06-19 02:00:03')""",
    s"""select DOB from UNIQDATA_hive where  (DOB == '1970-06-19 01:00:03') and (DOJ=='1970-06-19 02:00:03')""")
}
       

//UNIQDATA_DEFAULT_TC191
test("UNIQDATA_DEFAULT_TC191", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB !='1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOB !='1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC192
test("UNIQDATA_DEFAULT_TC192", Include) {
  checkAnswer(s"""select DOB  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select DOB  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC193
test("UNIQDATA_DEFAULT_TC193", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB !='1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOB !='1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC194
test("UNIQDATA_DEFAULT_TC194", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB >'1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOB >'1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC195
test("UNIQDATA_DEFAULT_TC195", Include) {
  checkAnswer(s"""select DOB  from UNIQDATA where DOB<>DOB order by DOB""",
    s"""select DOB  from UNIQDATA_hive where DOB<>DOB order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC196
test("UNIQDATA_DEFAULT_TC196", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB != DOJ order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOB != DOJ order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC197
test("UNIQDATA_DEFAULT_TC197", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOJ<DOB order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOJ<DOB order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC198
test("UNIQDATA_DEFAULT_TC198", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB<=DOB order by DOB""",
    s"""select DOB from UNIQDATA_hive where DOB<=DOB order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC199
test("UNIQDATA_DEFAULT_TC199", Include) {
  sql(s"""select DOB from UNIQDATA where DOB <'1970-06-19 01:00:03' order by DOB""").collect
}
       

//UNIQDATA_DEFAULT_TC200
test("UNIQDATA_DEFAULT_TC200", Include) {
  checkAnswer(s"""select DOB  from UNIQDATA where DOB IS NULL""",
    s"""select DOB  from UNIQDATA_hive where DOB IS NULL""")
}
       

//UNIQDATA_DEFAULT_TC201
test("UNIQDATA_DEFAULT_TC201", Include) {
  checkAnswer(s"""select DOB  from UNIQDATA where DOB IS NOT NULL order by DOB""",
    s"""select DOB  from UNIQDATA_hive where DOB IS NOT NULL order by DOB""")
}
       

//UNIQDATA_DEFAULT_TC202
test("UNIQDATA_DEFAULT_TC202", Include) {
  checkAnswer(s"""Select count(CUST_ID) from UNIQDATA""",
    s"""Select count(CUST_ID) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC203
test("UNIQDATA_DEFAULT_TC203", Include) {
  checkAnswer(s"""select count(DISTINCT CUST_ID) as a from UNIQDATA""",
    s"""select count(DISTINCT CUST_ID) as a from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC204
test("UNIQDATA_DEFAULT_TC204", Include) {
  checkAnswer(s"""select sum(CUST_ID)+10 as a ,CUST_ID  from UNIQDATA group by CUST_ID order by CUST_ID""",
    s"""select sum(CUST_ID)+10 as a ,CUST_ID  from UNIQDATA_hive group by CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC205
test("UNIQDATA_DEFAULT_TC205", Include) {
  checkAnswer(s"""select max(CUST_ID),min(CUST_ID) from UNIQDATA""",
    s"""select max(CUST_ID),min(CUST_ID) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC206
test("UNIQDATA_DEFAULT_TC206", Include) {
  checkAnswer(s"""select sum(CUST_ID) a  from UNIQDATA""",
    s"""select sum(CUST_ID) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC207
test("UNIQDATA_DEFAULT_TC207", Include) {
  checkAnswer(s"""select avg(CUST_ID) a  from UNIQDATA""",
    s"""select avg(CUST_ID) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC208
test("UNIQDATA_DEFAULT_TC208", Include) {
  checkAnswer(s"""select min(CUST_ID) a  from UNIQDATA""",
    s"""select min(CUST_ID) a  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC209
test("UNIQDATA_DEFAULT_TC209", Include) {
  sql(s"""select variance(CUST_ID) as a   from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC210
test("UNIQDATA_DEFAULT_TC210", Include) {
  sql(s"""select var_pop(CUST_ID)  as a from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC211
test("UNIQDATA_DEFAULT_TC211", Include) {
  sql(s"""select var_samp(CUST_ID) as a  from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC212
test("UNIQDATA_DEFAULT_TC212", Include) {
  sql(s"""select stddev_pop(CUST_ID) as a  from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC213
test("UNIQDATA_DEFAULT_TC213", Include) {
  sql(s"""select stddev_samp(CUST_ID)  as a from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC214
test("UNIQDATA_DEFAULT_TC214", Include) {
  sql(s"""select covar_pop(CUST_ID,CUST_ID) as a  from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC215
test("UNIQDATA_DEFAULT_TC215", Include) {
  sql(s"""select covar_samp(CUST_ID,CUST_ID) as a  from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC216
test("UNIQDATA_DEFAULT_TC216", Include) {
  sql(s"""select corr(CUST_ID,CUST_ID)  as a from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC217
test("UNIQDATA_DEFAULT_TC217", Include) {
  sql(s"""select percentile_approx(CUST_ID,0.2) as a  from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC218
test("UNIQDATA_DEFAULT_TC218", Include) {
  sql(s"""select percentile_approx(CUST_ID,0.2,5) as a  from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC219
test("UNIQDATA_DEFAULT_TC219", Include) {
  sql(s"""select percentile_approx(CUST_ID,array(0.2,0.3,0.99))  as a from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC220
test("UNIQDATA_DEFAULT_TC220", Include) {
  sql(s"""select percentile_approx(CUST_ID,array(0.2,0.3,0.99),5) as a from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC221
test("UNIQDATA_DEFAULT_TC221", Include) {
  sql(s"""select histogram_numeric(CUST_ID,2)  as a from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC222
test("UNIQDATA_DEFAULT_TC222", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID+ 10 as a  from UNIQDATA order by CUST_ID""",
    s"""select CUST_ID, CUST_ID+ 10 as a  from UNIQDATA_hive order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC223
test("UNIQDATA_DEFAULT_TC223", Include) {
  checkAnswer(s"""select min(CUST_ID), max(CUST_ID+ 10) Total from UNIQDATA group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(CUST_ID), max(CUST_ID+ 10) Total from UNIQDATA_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_DEFAULT_TC224
test("UNIQDATA_DEFAULT_TC224", Include) {
  sql(s"""select last(CUST_ID) a from (select CUST_ID from UNIQDATA order by CUST_ID) t""").collect
}
       

//UNIQDATA_DEFAULT_TC225
test("UNIQDATA_DEFAULT_TC225", Include) {
  sql(s"""select FIRST(CUST_ID) a from UNIQDATA order by a""").collect
}
       

//UNIQDATA_DEFAULT_TC226
test("UNIQDATA_DEFAULT_TC226", Include) {
  checkAnswer(s"""select CUST_ID,count(CUST_ID) a from UNIQDATA group by CUST_ID order by CUST_ID""",
    s"""select CUST_ID,count(CUST_ID) a from UNIQDATA_hive group by CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC227
test("UNIQDATA_DEFAULT_TC227", Include) {
  checkAnswer(s"""select Lower(CUST_ID) a  from UNIQDATA order by CUST_ID""",
    s"""select Lower(CUST_ID) a  from UNIQDATA_hive order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC228
test("UNIQDATA_DEFAULT_TC228", Include) {
  checkAnswer(s"""select distinct CUST_ID from UNIQDATA order by CUST_ID""",
    s"""select distinct CUST_ID from UNIQDATA_hive order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC229
test("UNIQDATA_DEFAULT_TC229", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA order by CUST_ID limit 101""",
    s"""select CUST_ID from UNIQDATA_hive order by CUST_ID limit 101""")
}
       

//UNIQDATA_DEFAULT_TC230
test("UNIQDATA_DEFAULT_TC230", Include) {
  checkAnswer(s"""select CUST_ID as a from UNIQDATA  order by a asc limit 10""",
    s"""select CUST_ID as a from UNIQDATA_hive  order by a asc limit 10""")
}
       

//UNIQDATA_DEFAULT_TC231
test("UNIQDATA_DEFAULT_TC231", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where  (CUST_ID == 100084) and (CUST_ID==100084)""",
    s"""select CUST_ID from UNIQDATA_hive where  (CUST_ID == 100084) and (CUST_ID==100084)""")
}
       

//UNIQDATA_DEFAULT_TC232
test("UNIQDATA_DEFAULT_TC232", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID !='100084' order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID !='100084' order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC233
test("UNIQDATA_DEFAULT_TC233", Include) {
  checkAnswer(s"""select CUST_ID  from UNIQDATA where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_ID  from UNIQDATA_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_DEFAULT_TC234
test("UNIQDATA_DEFAULT_TC234", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID !=100084 order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID !=100084 order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC235
test("UNIQDATA_DEFAULT_TC235", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID >100084 order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID >100084 order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC236
test("UNIQDATA_DEFAULT_TC236", Include) {
  checkAnswer(s"""select CUST_ID  from UNIQDATA where CUST_ID<>CUST_ID order by CUST_ID""",
    s"""select CUST_ID  from UNIQDATA_hive where CUST_ID<>CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC237
test("UNIQDATA_DEFAULT_TC237", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID != BIGINT_COLUMN2 order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID != BIGINT_COLUMN2 order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC238
test("UNIQDATA_DEFAULT_TC238", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID from UNIQDATA where BIGINT_COLUMN2<CUST_ID order by CUST_ID""",
    s"""select CUST_ID, CUST_ID from UNIQDATA_hive where BIGINT_COLUMN2<CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC239
test("UNIQDATA_DEFAULT_TC239", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID from UNIQDATA where CUST_ID<=CUST_ID order by CUST_ID""",
    s"""select CUST_ID, CUST_ID from UNIQDATA_hive where CUST_ID<=CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC240
test("UNIQDATA_DEFAULT_TC240", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID <1000 order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID <1000 order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC241
test("UNIQDATA_DEFAULT_TC241", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID >1000 order by CUST_ID""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID >1000 order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC242
test("UNIQDATA_DEFAULT_TC242", Include) {
  checkAnswer(s"""select CUST_ID  from UNIQDATA where CUST_ID IS NULL order by CUST_ID""",
    s"""select CUST_ID  from UNIQDATA_hive where CUST_ID IS NULL order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC243
test("UNIQDATA_DEFAULT_TC243", Include) {
  checkAnswer(s"""select CUST_ID  from UNIQDATA where CUST_ID IS NOT NULL order by CUST_ID""",
    s"""select CUST_ID  from UNIQDATA_hive where CUST_ID IS NOT NULL order by CUST_ID""")
}
       

//UNIQDATA_DEFAULT_TC244
test("UNIQDATA_DEFAULT_TC244", Include) {
  checkAnswer(s"""select sum(CUST_NAME)+10 as a   from UNIQDATA""",
    s"""select sum(CUST_NAME)+10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC245
test("UNIQDATA_DEFAULT_TC245", Include) {
  checkAnswer(s"""select sum(CUST_NAME)*10 as a   from UNIQDATA""",
    s"""select sum(CUST_NAME)*10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC246
test("UNIQDATA_DEFAULT_TC246", Include) {
  checkAnswer(s"""select sum(CUST_NAME)/10 as a   from UNIQDATA""",
    s"""select sum(CUST_NAME)/10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC247
test("UNIQDATA_DEFAULT_TC247", Include) {
  checkAnswer(s"""select sum(CUST_NAME)-10 as a   from UNIQDATA""",
    s"""select sum(CUST_NAME)-10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC248
test("UNIQDATA_DEFAULT_TC248", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)+10 as a   from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1)+10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC249
test("UNIQDATA_DEFAULT_TC249", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)*10 as a   from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1)*10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC250
test("UNIQDATA_DEFAULT_TC250", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)/10 as a   from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1)/10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC251
test("UNIQDATA_DEFAULT_TC251", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)-10 as a   from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1)-10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC252
test("UNIQDATA_DEFAULT_TC252", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)+10 as a   from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1)+10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC253
test("UNIQDATA_DEFAULT_TC253", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)*10 as a   from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1)*10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC254
test("UNIQDATA_DEFAULT_TC254", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)/10 as a   from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1)/10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC255
test("UNIQDATA_DEFAULT_TC255", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)-10 as a   from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1)-10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC256
test("UNIQDATA_DEFAULT_TC256", Include) {
  sql(s"""select sum(Double_COLUMN1)+10 as a   from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC257
test("UNIQDATA_DEFAULT_TC257", Include) {
  sql(s"""select sum(Double_COLUMN1)*10 as a   from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC258
test("UNIQDATA_DEFAULT_TC258", Include) {
  sql(s"""select sum(Double_COLUMN1)/10 as a   from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC259
test("UNIQDATA_DEFAULT_TC259", Include) {
  sql(s"""select sum(Double_COLUMN1)-10 as a   from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC260
test("UNIQDATA_DEFAULT_TC260", Include) {
  checkAnswer(s"""select sum(DOB)+10 as a   from UNIQDATA""",
    s"""select sum(DOB)+10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC261
test("UNIQDATA_DEFAULT_TC261", Include) {
  checkAnswer(s"""select sum(DOB)*10 as a   from UNIQDATA""",
    s"""select sum(DOB)*10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC262
test("UNIQDATA_DEFAULT_TC262", Include) {
  checkAnswer(s"""select sum(DOB)/10 as a   from UNIQDATA""",
    s"""select sum(DOB)/10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC263
test("UNIQDATA_DEFAULT_TC263", Include) {
  checkAnswer(s"""select sum(DOB)-10 as a   from UNIQDATA""",
    s"""select sum(DOB)-10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC264
test("UNIQDATA_DEFAULT_TC264", Include) {
  checkAnswer(s"""select sum(CUST_ID)+10 as a   from UNIQDATA""",
    s"""select sum(CUST_ID)+10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC265
test("UNIQDATA_DEFAULT_TC265", Include) {
  checkAnswer(s"""select sum(CUST_ID)*10 as a   from UNIQDATA""",
    s"""select sum(CUST_ID)*10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC266
test("UNIQDATA_DEFAULT_TC266", Include) {
  checkAnswer(s"""select sum(CUST_ID)/10 as a   from UNIQDATA""",
    s"""select sum(CUST_ID)/10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC267
test("UNIQDATA_DEFAULT_TC267", Include) {
  checkAnswer(s"""select sum(CUST_ID)-10 as a   from UNIQDATA""",
    s"""select sum(CUST_ID)-10 as a   from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC292
test("UNIQDATA_DEFAULT_TC292", Include) {
  checkAnswer(s"""SELECT DOB from UNIQDATA where DOB LIKE '2015-09-30%'""",
    s"""SELECT DOB from UNIQDATA_hive where DOB LIKE '2015-09-30%'""")
}
       

//UNIQDATA_DEFAULT_TC293
test("UNIQDATA_DEFAULT_TC293", Include) {
  checkAnswer(s"""SELECT DOB from UNIQDATA where DOB LIKE '% %'""",
    s"""SELECT DOB from UNIQDATA_hive where DOB LIKE '% %'""")
}
       

//UNIQDATA_DEFAULT_TC294
test("UNIQDATA_DEFAULT_TC294", Include) {
  checkAnswer(s"""SELECT DOB from UNIQDATA where DOB LIKE '%01:00:03'""",
    s"""SELECT DOB from UNIQDATA_hive where DOB LIKE '%01:00:03'""")
}
       

//UNIQDATA_DEFAULT_TC295
test("UNIQDATA_DEFAULT_TC295", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 like '123372038%' """,
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 like '123372038%' """)
}
       

//UNIQDATA_DEFAULT_TC296
test("UNIQDATA_DEFAULT_TC296", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 like '%2038'""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 like '%2038'""")
}
       

//UNIQDATA_DEFAULT_TC297
test("UNIQDATA_DEFAULT_TC297", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 like '%2038%'""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 like '%2038%'""")
}
       

//UNIQDATA_DEFAULT_TC298
test("UNIQDATA_DEFAULT_TC298", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 like '123456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 like '123456790%'""")
}
       

//UNIQDATA_DEFAULT_TC299
test("UNIQDATA_DEFAULT_TC299", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 like '%456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 like '%456790%'""")
}
       

//UNIQDATA_DEFAULT_TC300
test("UNIQDATA_DEFAULT_TC300", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 like '123456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 like '123456790%'""")
}
       

//UNIQDATA_DEFAULT_TC301
test("UNIQDATA_DEFAULT_TC301", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from UNIQDATA where Double_COLUMN1 like '1.123456748%'""",
    s"""SELECT Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 like '1.123456748%'""")
}
       

//UNIQDATA_DEFAULT_TC302
test("UNIQDATA_DEFAULT_TC302", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from UNIQDATA where Double_COLUMN1 like '%23456%'""",
    s"""SELECT Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 like '%23456%'""")
}
       

//UNIQDATA_DEFAULT_TC303
test("UNIQDATA_DEFAULT_TC303", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from UNIQDATA where Double_COLUMN1 like '%976E10'""",
    s"""SELECT Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 like '%976E10'""")
}
       

//UNIQDATA_DEFAULT_TC304
test("UNIQDATA_DEFAULT_TC304", Include) {
  checkAnswer(s"""SELECT CUST_ID from UNIQDATA where CUST_ID like '1000%'""",
    s"""SELECT CUST_ID from UNIQDATA_hive where CUST_ID like '1000%'""")
}
       

//UNIQDATA_DEFAULT_TC305
test("UNIQDATA_DEFAULT_TC305", Include) {
  checkAnswer(s"""SELECT CUST_ID from UNIQDATA where CUST_ID like '%00%'""",
    s"""SELECT CUST_ID from UNIQDATA_hive where CUST_ID like '%00%'""")
}
       

//UNIQDATA_DEFAULT_TC306
test("UNIQDATA_DEFAULT_TC306", Include) {
  checkAnswer(s"""SELECT CUST_ID from UNIQDATA where CUST_ID like '%0084'""",
    s"""SELECT CUST_ID from UNIQDATA_hive where CUST_ID like '%0084'""")
}
       

//UNIQDATA_DEFAULT_TC307
test("UNIQDATA_DEFAULT_TC307", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME like 'CUST_NAME_0184%'""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME like 'CUST_NAME_0184%'""")
}
       

//UNIQDATA_DEFAULT_TC308
test("UNIQDATA_DEFAULT_TC308", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME like '%E_01%'""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME like '%E_01%'""")
}
       

//UNIQDATA_DEFAULT_TC309
test("UNIQDATA_DEFAULT_TC309", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME like '%ST_NAME_018'""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME like '%ST_NAME_018'""")
}
       

//UNIQDATA_DEFAULT_TC310
test("UNIQDATA_DEFAULT_TC310", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""")
}
       

//UNIQDATA_DEFAULT_TC311
test("UNIQDATA_DEFAULT_TC311", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME not in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME not in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""")
}
       

//UNIQDATA_DEFAULT_TC312
test("UNIQDATA_DEFAULT_TC312", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID in (9137,10137,14137)""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID in (9137,10137,14137)""")
}
       

//UNIQDATA_DEFAULT_TC313
test("UNIQDATA_DEFAULT_TC313", Include) {
  sql(s"""select CUST_ID from UNIQDATA where CUST_ID not in (9137,10137,14137)""").collect
}
       

//UNIQDATA_DEFAULT_TC314
test("UNIQDATA_DEFAULT_TC314", Include) {
  sql(s"""select DOB from UNIQDATA where DOB in ('2015-10-04 01:00:03','2015-10-07%','2015-10-07 01:00:03')""").collect
}
       

//UNIQDATA_DEFAULT_TC315
test("UNIQDATA_DEFAULT_TC315", Include) {
  sql(s"""select DOB from UNIQDATA where DOB not in ('2015-10-04 01:00:03','2015-10-07 01:00:03')""").collect
}
       

//UNIQDATA_DEFAULT_TC316
test("UNIQDATA_DEFAULT_TC316", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""")
}
       

//UNIQDATA_DEFAULT_TC317
test("UNIQDATA_DEFAULT_TC317", Include) {
  sql(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 not in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""").collect
}
       

//UNIQDATA_DEFAULT_TC318
test("UNIQDATA_DEFAULT_TC318", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 in (22345680745.1234000000,22345680741.1234000000)""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 in (22345680745.1234000000,22345680741.1234000000)""")
}
       

//UNIQDATA_DEFAULT_TC319
test("UNIQDATA_DEFAULT_TC319", Include) {
  sql(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 not in (22345680745.1234000000,22345680741.1234000000)""").collect
}
       

//UNIQDATA_DEFAULT_TC322
test("UNIQDATA_DEFAULT_TC322", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME !='CUST_NAME_0184400077'""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME !='CUST_NAME_0184400077'""")
}
       

//UNIQDATA_DEFAULT_TC323
test("UNIQDATA_DEFAULT_TC323", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME NOT LIKE 'CUST_NAME_0184400077'""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME NOT LIKE 'CUST_NAME_0184400077'""")
}
       

//UNIQDATA_DEFAULT_TC324
test("UNIQDATA_DEFAULT_TC324", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID !=100078""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID !=100078""")
}
       

//UNIQDATA_DEFAULT_TC325
test("UNIQDATA_DEFAULT_TC325", Include) {
  sql(s"""select CUST_ID from UNIQDATA where CUST_ID NOT LIKE 100079""").collect
}
       

//UNIQDATA_DEFAULT_TC326
test("UNIQDATA_DEFAULT_TC326", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB !='2015-10-07 01:00:03'""",
    s"""select DOB from UNIQDATA_hive where DOB !='2015-10-07 01:00:03'""")
}
       

//UNIQDATA_DEFAULT_TC327
test("UNIQDATA_DEFAULT_TC327", Include) {
  sql(s"""select DOB from UNIQDATA where DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_DEFAULT_TC328
test("UNIQDATA_DEFAULT_TC328", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 !=11234569489.7976000000""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 !=11234569489.7976000000""")
}
       

//UNIQDATA_DEFAULT_TC329
test("UNIQDATA_DEFAULT_TC329", Include) {
  sql(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 NOT LIKE 11234569489.7976000000""").collect
}
       

//UNIQDATA_DEFAULT_TC330
test("UNIQDATA_DEFAULT_TC330", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 != 22345680741.1234000000""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 != 22345680741.1234000000""")
}
       

//UNIQDATA_DEFAULT_TC331
test("UNIQDATA_DEFAULT_TC331", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 NOT LIKE 22345680741.1234000000""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 NOT LIKE 22345680741.1234000000""")
}
       

//UNIQDATA_DEFAULT_TC335
test("UNIQDATA_DEFAULT_TC335", Include) {
  checkAnswer(s"""SELECT DOB,CUST_NAME from UNIQDATA where CUST_NAME RLIKE 'CUST_NAME_0184400077'""",
    s"""SELECT DOB,CUST_NAME from UNIQDATA_hive where CUST_NAME RLIKE 'CUST_NAME_0184400077'""")
}
       

//UNIQDATA_DEFAULT_TC336
test("UNIQDATA_DEFAULT_TC336", Include) {
  checkAnswer(s"""SELECT CUST_ID from UNIQDATA where CUST_ID RLIKE '100079'""",
    s"""SELECT CUST_ID from UNIQDATA_hive where CUST_ID RLIKE '100079'""")
}
       

//UNIQDATA_DEFAULT_TC337
test("UNIQDATA_DEFAULT_TC337", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from UNIQDATA where Double_COLUMN1 RLIKE '11234569489.7976000000'""",
    s"""SELECT Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 RLIKE '11234569489.7976000000'""")
}
       

//UNIQDATA_DEFAULT_TC338
test("UNIQDATA_DEFAULT_TC338", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 RLIKE '1234567890123550.0000000000'""",
    s"""SELECT DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 RLIKE '1234567890123550.0000000000'""")
}
       

//UNIQDATA_DEFAULT_TC339
test("UNIQDATA_DEFAULT_TC339", Include) {
  checkAnswer(s"""SELECT BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 RLIKE '123372038698'""",
    s"""SELECT BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 RLIKE '123372038698'""")
}
       

//UNIQDATA_DEFAULT_TC340
test("UNIQDATA_DEFAULT_TC340", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.DOB=b.DOB and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_DEFAULT_TC341
test("UNIQDATA_DEFAULT_TC341", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.CUST_ID=b.CUST_ID and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_DEFAULT_TC342
test("UNIQDATA_DEFAULT_TC342", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.CUST_NAME=b.CUST_NAME and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_DEFAULT_TC343
test("UNIQDATA_DEFAULT_TC343", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.Double_COLUMN1=b.Double_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA_hive a join UNIQDATA_hive b on a.Double_COLUMN1=b.Double_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_DEFAULT_TC344
test("UNIQDATA_DEFAULT_TC344", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.DECIMAL_COLUMN1=b.DECIMAL_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA_hive a join UNIQDATA_hive b on a.DECIMAL_COLUMN1=b.DECIMAL_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_DEFAULT_TC345
test("UNIQDATA_DEFAULT_TC345", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA a join UNIQDATA b on a.BIGINT_COLUMN1=b.BIGINT_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from UNIQDATA_hive a join UNIQDATA_hive b on a.BIGINT_COLUMN1=b.BIGINT_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_DEFAULT_TC346
test("UNIQDATA_DEFAULT_TC346", Include) {
  checkAnswer(s"""select count( BIGINT_COLUMN1 ),sum( BIGINT_COLUMN1 ),count(distinct BIGINT_COLUMN1 ),avg( BIGINT_COLUMN1 ),max( BIGINT_COLUMN1 ),min( BIGINT_COLUMN1 ),1 from UNIQDATA""",
    s"""select count( BIGINT_COLUMN1 ),sum( BIGINT_COLUMN1 ),count(distinct BIGINT_COLUMN1 ),avg( BIGINT_COLUMN1 ),max( BIGINT_COLUMN1 ),min( BIGINT_COLUMN1 ),1 from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC347
test("UNIQDATA_DEFAULT_TC347", Include) {
  checkAnswer(s"""select count( DECIMAL_COLUMN1 ),sum( DECIMAL_COLUMN1 ),count(distinct DECIMAL_COLUMN1 ),avg( DECIMAL_COLUMN1 ),max( DECIMAL_COLUMN1 ),min( DECIMAL_COLUMN1 ),1 from UNIQDATA""",
    s"""select count( DECIMAL_COLUMN1 ),sum( DECIMAL_COLUMN1 ),count(distinct DECIMAL_COLUMN1 ),avg( DECIMAL_COLUMN1 ),max( DECIMAL_COLUMN1 ),min( DECIMAL_COLUMN1 ),1 from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC348
test("UNIQDATA_DEFAULT_TC348", Include) {
  sql(s"""select count( Double_COLUMN1),sum( Double_COLUMN1 ),count(distinct Double_COLUMN1 ),avg(Double_COLUMN1),max(Double_COLUMN1),min(Double_COLUMN1),1 from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC349
test("UNIQDATA_DEFAULT_TC349", Include) {
  checkAnswer(s"""select count(CUST_ID),sum(CUST_ID),count(CUST_ID),avg(CUST_ID),max(CUST_ID),min(CUST_ID),1 from UNIQDATA""",
    s"""select count(CUST_ID),sum(CUST_ID),count(CUST_ID),avg(CUST_ID),max(CUST_ID),min(CUST_ID),1 from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC350
test("UNIQDATA_DEFAULT_TC350", Include) {
  checkAnswer(s"""select count(DOB),sum(DOB),count(distinct DOB ),avg(DOB),max(DOB ),min(DOB),1 from UNIQDATA""",
    s"""select count(DOB),sum(DOB),count(distinct DOB ),avg(DOB),max(DOB ),min(DOB),1 from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC351
test("UNIQDATA_DEFAULT_TC351", Include) {
  checkAnswer(s"""select count(CUST_NAME ),sum(CUST_NAME ),count(distinct CUST_NAME ),avg(CUST_NAME ),max(CUST_NAME ),min(CUST_NAME ),1 from UNIQDATA""",
    s"""select count(CUST_NAME ),sum(CUST_NAME ),count(distinct CUST_NAME ),avg(CUST_NAME ),max(CUST_NAME ),min(CUST_NAME ),1 from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC352
test("UNIQDATA_DEFAULT_TC352", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1),count(BIGINT_COLUMN1),avg(BIGINT_COLUMN1),sum(BIGINT_COLUMN1)/count(BIGINT_COLUMN1) from UNIQDATA""",
    s"""select sum(BIGINT_COLUMN1),count(BIGINT_COLUMN1),avg(BIGINT_COLUMN1),sum(BIGINT_COLUMN1)/count(BIGINT_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC353
test("UNIQDATA_DEFAULT_TC353", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1),count(DECIMAL_COLUMN1),avg(DECIMAL_COLUMN1),sum(DECIMAL_COLUMN1)/count(DECIMAL_COLUMN1) from UNIQDATA""",
    s"""select sum(DECIMAL_COLUMN1),count(DECIMAL_COLUMN1),avg(DECIMAL_COLUMN1),sum(DECIMAL_COLUMN1)/count(DECIMAL_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC354
test("UNIQDATA_DEFAULT_TC354", Include) {
  sql(s"""select sum(Double_COLUMN1),count(Double_COLUMN1),avg(Double_COLUMN1),sum(Double_COLUMN1)/count(Double_COLUMN1) from UNIQDATA""").collect
}
       

//UNIQDATA_DEFAULT_TC355
test("UNIQDATA_DEFAULT_TC355", Include) {
  checkAnswer(s"""select sum(CUST_ID),count(CUST_ID),avg(CUST_ID),sum(CUST_ID)/count(CUST_ID) from UNIQDATA""",
    s"""select sum(CUST_ID),count(CUST_ID),avg(CUST_ID),sum(CUST_ID)/count(CUST_ID) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC356
test("UNIQDATA_DEFAULT_TC356", Include) {
  checkAnswer(s"""select sum(CUST_NAME),count(CUST_NAME),avg(CUST_NAME),sum(CUST_NAME)/count(CUST_NAME) from UNIQDATA""",
    s"""select sum(CUST_NAME),count(CUST_NAME),avg(CUST_NAME),sum(CUST_NAME)/count(CUST_NAME) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC357
test("UNIQDATA_DEFAULT_TC357", Include) {
  checkAnswer(s"""select sum(DOB),count(DOB),avg(DOB),sum(DOB)/count(DOB) from UNIQDATA""",
    s"""select sum(DOB),count(DOB),avg(DOB),sum(DOB)/count(DOB) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC358
test("UNIQDATA_DEFAULT_TC358", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,DOB,CUST_ID,CUST_NAME  from UNIQDATA""",
    s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,DOB,CUST_ID,CUST_NAME  from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC359
test("UNIQDATA_DEFAULT_TC359", Include) {
  checkAnswer(s"""select count(DECIMAL_COLUMN2) from UNIQDATA""",
    s"""select count(DECIMAL_COLUMN2) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC360
test("UNIQDATA_DEFAULT_TC360", Include) {
  checkAnswer(s"""select count(Double_COLUMN1) from UNIQDATA""",
    s"""select count(Double_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC361
test("UNIQDATA_DEFAULT_TC361", Include) {
  checkAnswer(s"""select count(BIGINT_COLUMN1) from UNIQDATA""",
    s"""select count(BIGINT_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC362
test("UNIQDATA_DEFAULT_TC362", Include) {
  checkAnswer(s"""select count(DECIMAL_COLUMN1) from UNIQDATA""",
    s"""select count(DECIMAL_COLUMN1) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC363
test("UNIQDATA_DEFAULT_TC363", Include) {
  checkAnswer(s"""select count(DOB) from UNIQDATA""",
    s"""select count(DOB) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC364
test("UNIQDATA_DEFAULT_TC364", Include) {
  checkAnswer(s"""select count(CUST_ID) from UNIQDATA""",
    s"""select count(CUST_ID) from UNIQDATA_hive""")
}
       

//UNIQDATA_DEFAULT_TC365
test("UNIQDATA_DEFAULT_TC365", Include) {
  sql(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  BIGINT_COLUMN1  != '123372038698'""").collect
}
       

//UNIQDATA_DEFAULT_TC366
test("UNIQDATA_DEFAULT_TC366", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  DECIMAL_COLUMN1  != '1234567890123480.0000000000' order by CUST_ID limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  DECIMAL_COLUMN1  != '1234567890123480.0000000000' order by CUST_ID limit 5""")
}
       

//UNIQDATA_DEFAULT_TC367
test("UNIQDATA_DEFAULT_TC367", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  Double_COLUMN1  != '11234569489.7976000000' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  Double_COLUMN1  != '11234569489.7976000000' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_DEFAULT_TC368
test("UNIQDATA_DEFAULT_TC368", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  DOB  != '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  DOB  != '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_DEFAULT_TC369
test("UNIQDATA_DEFAULT_TC369", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  CUST_ID  != '100075' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  CUST_ID  != '100075' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_DEFAULT_TC370
test("UNIQDATA_DEFAULT_TC370", Include) {
  sql(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  BIGINT_COLUMN1  not like '123372038698' order by  CUST_ID limit 5""").collect
}
       

//UNIQDATA_DEFAULT_TC371
test("UNIQDATA_DEFAULT_TC371", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  DECIMAL_COLUMN1  not like '11234569489.79760000000' order by CUST_ID limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  DECIMAL_COLUMN1  not like '11234569489.79760000000' order by CUST_ID limit 5""")
}
       

//UNIQDATA_DEFAULT_TC372
test("UNIQDATA_DEFAULT_TC372", Include) {
  sql(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  Double_COLUMN1  not like '11234569489.7976000000' order by CUST_NAME limit 5""").collect
}
       

//UNIQDATA_DEFAULT_TC373
test("UNIQDATA_DEFAULT_TC373", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  DOB  not like '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA_hive where  DOB  not like '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_DEFAULT_TC374
test("UNIQDATA_DEFAULT_TC374", Include) {
  sql(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from UNIQDATA where  CUST_ID  not like '100075' order by CUST_NAME limit 5""").collect
}
       

//UNIQDATA_DEFAULT_TC375
test("UNIQDATA_DEFAULT_TC375", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME is not null""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME is not null""")
}
       

//UNIQDATA_DEFAULT_TC376
test("UNIQDATA_DEFAULT_TC376", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 is not null""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 is not null""")
}
       

//UNIQDATA_DEFAULT_TC377
test("UNIQDATA_DEFAULT_TC377", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 is not null""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 is not null""")
}
       

//UNIQDATA_DEFAULT_TC378
test("UNIQDATA_DEFAULT_TC378", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 is not null""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 is not null""")
}
       

//UNIQDATA_DEFAULT_TC379
test("UNIQDATA_DEFAULT_TC379", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB is not null""",
    s"""select DOB from UNIQDATA_hive where DOB is not null""")
}
       

//UNIQDATA_DEFAULT_TC380
test("UNIQDATA_DEFAULT_TC380", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID is not null""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID is not null""")
}
       

//UNIQDATA_DEFAULT_TC381
test("UNIQDATA_DEFAULT_TC381", Include) {
  checkAnswer(s"""select CUST_NAME from UNIQDATA where CUST_NAME is  null""",
    s"""select CUST_NAME from UNIQDATA_hive where CUST_NAME is  null""")
}
       

//UNIQDATA_DEFAULT_TC382
test("UNIQDATA_DEFAULT_TC382", Include) {
  checkAnswer(s"""select Double_COLUMN1 from UNIQDATA where Double_COLUMN1 is  null""",
    s"""select Double_COLUMN1 from UNIQDATA_hive where Double_COLUMN1 is  null""")
}
       

//UNIQDATA_DEFAULT_TC383
test("UNIQDATA_DEFAULT_TC383", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from UNIQDATA where BIGINT_COLUMN1 is  null""",
    s"""select BIGINT_COLUMN1 from UNIQDATA_hive where BIGINT_COLUMN1 is  null""")
}
       

//UNIQDATA_DEFAULT_TC384
test("UNIQDATA_DEFAULT_TC384", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from UNIQDATA where DECIMAL_COLUMN1 is  null""",
    s"""select DECIMAL_COLUMN1 from UNIQDATA_hive where DECIMAL_COLUMN1 is  null""")
}
       

//UNIQDATA_DEFAULT_TC385
test("UNIQDATA_DEFAULT_TC385", Include) {
  checkAnswer(s"""select DOB from UNIQDATA where DOB is  null""",
    s"""select DOB from UNIQDATA_hive where DOB is  null""")
}
       

//UNIQDATA_DEFAULT_TC386
test("UNIQDATA_DEFAULT_TC386", Include) {
  checkAnswer(s"""select CUST_ID from UNIQDATA where CUST_ID is  null""",
    s"""select CUST_ID from UNIQDATA_hive where CUST_ID is  null""")
}
       

//UNIQDATA_DEFAULT_TC387
test("UNIQDATA_DEFAULT_TC387", Include) {
  checkAnswer(s"""select count(*) from UNIQDATA where CUST_NAME = 'CUST_NAME_01844'""",
    s"""select count(*) from UNIQDATA_hive where CUST_NAME = 'CUST_NAME_01844'""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_001
test("UNIQDATA_DEFAULT_MultiBitSet_TC_001", Include) {
  sql(s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata where cust_id=9056 and INTEGER_COLUMN1=57  and BIGINT_COLUMN1=123372036910 and  BIGINT_COLUMN2=-223372036798 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00056' and cust_name='CUST_NAME_00056' and cust_id!=1 and ACTIVE_EMUI_VERSION!='abc' """).collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_002
test("UNIQDATA_DEFAULT_MultiBitSet_TC_002", Include) {
  checkAnswer(s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata where DECIMAL_COLUMN1=12345680900.1234 and DECIMAL_COLUMN2=22345680900.1234 and Double_COLUMN1=1.12345674897976E10 and Double_COLUMN2=-4.8E-4 and dob='1975-06-23 01:00:03'  and doj='1975-06-23 02:00:03' and dob!='1970-03-29 01:00:03' and doj!='1970-04-03 02:00:03' and Double_COLUMN2!=12345678987.1234""",
    s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_hive where DECIMAL_COLUMN1=12345680900.1234 and DECIMAL_COLUMN2=22345680900.1234 and Double_COLUMN1=1.12345674897976E10 and Double_COLUMN2=-4.8E-4 and dob='1975-06-23 01:00:03'  and doj='1975-06-23 02:00:03' and dob!='1970-03-29 01:00:03' and doj!='1970-04-03 02:00:03' and Double_COLUMN2!=12345678987.1234""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_003
test("UNIQDATA_DEFAULT_MultiBitSet_TC_003", Include) {
  sql(s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata where cust_id>=9000 and CUST_ID<=10000 and INTEGER_COLUMN1>=1 and INTEGER_COLUMN1<=400 and BIGINT_COLUMN1>=123372036854 and BIGINT_COLUMN1<=123372037254 and BIGINT_COLUMN2>=-223372036854  and BIGINT_COLUMN2<=-223372036454""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_004
test("UNIQDATA_DEFAULT_MultiBitSet_TC_004", Include) {
  sql(s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata where DECIMAL_COLUMN1>=12345678901.1234 and DECIMAL_COLUMN1<=12345679301.12344 and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976 and Double_COLUMN1<=1.12345674897976E10 and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<= -4.8E-4 and dob>='1970-01-02 01:00:03' and dob<= '1971-02-05 01:00:03' and doj>='1970-01-02 02:00:03' and doj<='1971-02-05 02:00:03'""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_005
test("UNIQDATA_DEFAULT_MultiBitSet_TC_005", Include) {
  sql(s"""select distinct INTEGER_COLUMN1,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,cust_name from uniqdata where INTEGER_COLUMN1<cust_id and BIGINT_COLUMN2<BIGINT_COLUMN1 and substr(cust_name,10,length(cust_name))=substr(ACTIVE_EMUI_VERSION,20,length(ACTIVE_EMUI_VERSION))""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_006
test("UNIQDATA_DEFAULT_MultiBitSet_TC_006", Include) {
  sql(s"""select distinct dob,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata where  DECIMAL_COLUMN1<DECIMAL_COLUMN2 and Double_COLUMN1>Double_COLUMN2  and day(dob)=day(doj)""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_007
test("UNIQDATA_DEFAULT_MultiBitSet_TC_007", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata where cust_id in(9011,9012,9013,9014,9015,9016) and INTEGER_COLUMN1 in (12,13,14,15,16,17,18)and ACTIVE_EMUI_VERSION  in('ACTIVE_EMUI_VERSION_00011','ACTIVE_EMUI_VERSION_00012','ACTIVE_EMUI_VERSION_00013','ACTIVE_EMUI_VERSION_00014','ACTIVE_EMUI_VERSION_00015') and CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') and DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03')""",
    s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata_hive where cust_id in(9011,9012,9013,9014,9015,9016) and INTEGER_COLUMN1 in (12,13,14,15,16,17,18)and ACTIVE_EMUI_VERSION  in('ACTIVE_EMUI_VERSION_00011','ACTIVE_EMUI_VERSION_00012','ACTIVE_EMUI_VERSION_00013','ACTIVE_EMUI_VERSION_00014','ACTIVE_EMUI_VERSION_00015') and CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') and DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03')""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_008
test("UNIQDATA_DEFAULT_MultiBitSet_TC_008", Include) {
  sql(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,BIGINT_COLUMN2 from uniqdata where BIGINT_COLUMN1 in (123372036869,123372036870,123372036871,123372036872) and BIGINT_COLUMN2 not in (-223372034862,-223372034889,-223372034902,-223372034860) and DECIMAL_COLUMN1 not in(12345678916.1234,12345678917.1234,12345678918.1234,2345678919.1234,2345678920.1234) and DECIMAL_COLUMN2 not in (22345680900.1234,22345680895.1234,22345680892.1234)  and Double_COLUMN2 not in(1234567890,6789076,11234567489.7976)""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_009
test("UNIQDATA_DEFAULT_MultiBitSet_TC_009", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata where (cust_id in(9011,9012,9013,9014,9015,9016) or INTEGER_COLUMN1 in (17,18,19,20))and (ACTIVE_EMUI_VERSION not in('ACTIVE_EMUI_VERSION_00028','ACTIVE_EMUI_VERSION_00029','ACTIVE_EMUI_VERSION_00030') or CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') )and (DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03') or doj not in('1970-01-30 02:00:03','1970-01-31 02:00:03','1970-02-01 02:00:03','1970-02-02 02:00:03','1970-02-03 02:00:03','1970-02-04 02:00:03','1970-02-05 02:00:03'))""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_010
test("UNIQDATA_DEFAULT_MultiBitSet_TC_010", Include) {
  sql(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,BIGINT_COLUMN2 from uniqdata where (BIGINT_COLUMN1 in (123372036869,123372036870,123372036871,123372036872) or BIGINT_COLUMN2 not in (-223372034862,-223372034889,-223372034902,-223372034860) )and (DECIMAL_COLUMN1 in(12345678916.1234,12345678917.1234,12345678918.1234,2345678919.1234,2345678920.1234) or DECIMAL_COLUMN2 not in (22345680900.1234,22345680895.1234,22345680892.1234)) and (Double_COLUMN1 in (11234567489.7976,11234567489.7976,11234567489.7976,11234567489.7976) or Double_COLUMN2 not in(1234567890,6789076,11234567489.7976))""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_011
test("UNIQDATA_DEFAULT_MultiBitSet_TC_011", Include) {
  sql(s"""select cust_id,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,BIGINT_COLUMN2 from uniqdata where INTEGER_COLUMN1>=50 and INTEGER_COLUMN1<=59 and BIGINT_COLUMN2>=-223372036805 and BIGINT_COLUMN2<=-223372036796 and ACTIVE_EMUI_VERSION rlike 'ACTIVE'
""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_012
test("UNIQDATA_DEFAULT_MultiBitSet_TC_012", Include) {
  sql(s"""select dob,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata where doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and 
Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_013
test("UNIQDATA_DEFAULT_MultiBitSet_TC_013", Include) {
  sql(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata where cust_id rlike 9 and 
BIGINT_COLUMN1 rlike 12 and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and CUST_NAME rlike 'CUST' and DOB rlike '19' and DECIMAL_COLUMN1 rlike 1234 and Double_COLUMN1 >=1111 and integer_column1 is not null and BIGINT_COLUMN2 is not null and CUST_NAME is not null and DOJ is not null and  DECIMAL_COLUMN2 is not null and Double_COLUMN2 is not null""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_014
test("UNIQDATA_DEFAULT_MultiBitSet_TC_014", Include) {
  checkAnswer(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata where integer_column1 like '5%' and BIGINT_COLUMN2 like '-22%' and CUST_NAME like 'CUST%' and ACTIVE_EMUI_VERSION like 'ACTIVE%' and dob like '19%' and decimal_column2 like '22%' and  Double_COLUMN2 >=-111""",
    s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_hive where integer_column1 like '5%' and BIGINT_COLUMN2 like '-22%' and CUST_NAME like 'CUST%' and ACTIVE_EMUI_VERSION like 'ACTIVE%' and dob like '19%' and decimal_column2 like '22%' and  Double_COLUMN2 >=-111""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_015
test("UNIQDATA_DEFAULT_MultiBitSet_TC_015", Include) {
  sql(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata where integer_column1 is null and BIGINT_COLUMN2 is null and cust_id is null and dob  is null and decimal_column2 is null and  Double_COLUMN2 is null""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_017
test("UNIQDATA_DEFAULT_MultiBitSet_TC_017", Include) {
  sql(s"""select avg(cust_id),avg(integer_column1),avg(BIGINT_COLUMN1),avg(BIGINT_COLUMN2),avg(DECIMAL_COLUMN1),avg(Double_COLUMN1),avg(Double_COLUMN2),count(cust_id),count(integer_column1),count(ACTIVE_EMUI_VERSION),count(CUST_NAME),count(CUST_NAME),count(DOB),count(doj),count(BIGINT_COLUMN2),count(DECIMAL_COLUMN1),count(Double_COLUMN1),count(Double_COLUMN2),avg(DECIMAL_COLUMN2),sum(cust_id),sum(integer_column1),sum(BIGINT_COLUMN1),sum(BIGINT_COLUMN2),sum(DECIMAL_COLUMN1),sum(Double_COLUMN1),sum(DECIMAL_COLUMN2),min(cust_id),min(integer_column1),min(ACTIVE_EMUI_VERSION),min(CUST_NAME),min(CUST_NAME),min(DOB),min(doj),min(BIGINT_COLUMN2),min(DECIMAL_COLUMN1),min(Double_COLUMN1),min(Double_COLUMN2),max(cust_id),max(integer_column1),max(ACTIVE_EMUI_VERSION),max(CUST_NAME),max(CUST_NAME),max(DOB),max(doj),max(BIGINT_COLUMN2),max(DECIMAL_COLUMN1),max(Double_COLUMN1),max(Double_COLUMN2)from uniqdata where cust_id between 9000 and 9100 and dob between '1970-01-01 01:00:03' and '1972-09-27 02:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1972-09-27 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and 
Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_019
test("UNIQDATA_DEFAULT_MultiBitSet_TC_019", Include) {
  sql(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata where cust_name not like '%abc%' and cust_id in (9002,9003,9004,9005,9006,9007,9008,9009,9010,9011,9012,9013,9014,9015,9016,9017,9018,9019,9021,9022,9023,9024,9025,9027,9028,9029,9030) and ACTIVE_EMUI_VERSION like 'ACTIVE%' group by CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 having count(DECIMAL_COLUMN1) <=2  order by CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 
""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_020
test("UNIQDATA_DEFAULT_MultiBitSet_TC_020", Include) {
  sql(s"""select (avg(DECIMAL_COLUMN1)),(avg(CUST_ID)),(avg(BIGINT_COLUMN1)), (avg(Double_COLUMN1)) from uniqdata  where cust_id between 9000 and  9100 and dob between '1970-01-01 01:00:03' and '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976 
""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_021
test("UNIQDATA_DEFAULT_MultiBitSet_TC_021", Include) {
  sql(s"""select (sum(DECIMAL_COLUMN2)),(sum(integer_column1)),(sum(BIGINT_COLUMN2)),(sum(Double_COLUMN2)) from uniqdata  where cust_id between 9000 and  9100 and dob between '1970-01-01 01:00:03' and '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_022
test("UNIQDATA_DEFAULT_MultiBitSet_TC_022", Include) {
  sql(s"""select distinct(count(Double_COLUMN2)),(count(integer_column1)),(count(BIGINT_COLUMN2)),(count(DECIMAL_COLUMN2)),count(ACTIVE_EMUI_VERSION),count(dob) from uniqdata where doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) """).collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_024
test("UNIQDATA_DEFAULT_MultiBitSet_TC_024", Include) {
  sql(s"""select distinct(max(BIGINT_COLUMN2)),(max(integer_column1)),(max(DECIMAL_COLUMN2)),(max(Double_COLUMN2)),max(ACTIVE_EMUI_VERSION),max(dob) from uniqdata where cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03' and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 )
""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_025
test("UNIQDATA_DEFAULT_MultiBitSet_TC_025", Include) {
  sql(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata where cust_id between 9006 and 9080 and cust_name not rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) group by CUST_ID,dob,ACTIVE_EMUI_VERSION order by CUST_ID,dob,ACTIVE_EMUI_VERSION""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_026
test("UNIQDATA_DEFAULT_MultiBitSet_TC_026", Include) {
  checkAnswer(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""",
    s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_hive where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_027
test("UNIQDATA_DEFAULT_MultiBitSet_TC_027", Include) {
  checkAnswer(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490 )group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""",
    s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_hive where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490 )group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_028
test("UNIQDATA_DEFAULT_MultiBitSet_TC_028", Include) {
  sql(s"""select distinct(instr(ACTIVE_EMUI_VERSION,'Active')),length(cust_name),locate(ACTIVE_EMUI_VERSION,'Active'),lower(cust_name),ltrim(cust_name),repeat(ACTIVE_EMUI_VERSION,1),reverse(cust_name),rpad(ACTIVE_EMUI_VERSION,5,'Cust'),rtrim(cust_name),space(ACTIVE_EMUI_VERSION),split(cust_name,2),substr(ACTIVE_EMUI_VERSION,5,length(ACTIVE_EMUI_VERSION)),trim(cust_name),unbase64(ACTIVE_EMUI_VERSION),Upper(cust_name),initcap(ACTIVE_EMUI_VERSION),soundex(cust_name) from uniqdata where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-04-06 02:00:03' and doj<='1971-02-05 02:00:03' and  BIGINT_COLUMN2 between -223372036759  and -223372036454   and cust_id not in(9500,9501,9506,9600,9700,9800,10000,9788)""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_029
test("UNIQDATA_DEFAULT_MultiBitSet_TC_029", Include) {
  sql(s"""select(instr(ACTIVE_EMUI_VERSION,'Active')),length(cust_name),locate(ACTIVE_EMUI_VERSION,'Active'),lower(cust_name),ltrim(cust_name),repeat(ACTIVE_EMUI_VERSION,1),reverse(cust_name),rpad(ACTIVE_EMUI_VERSION,5,'Cust'),rtrim(cust_name),space(ACTIVE_EMUI_VERSION),split(cust_name,2),substr(ACTIVE_EMUI_VERSION,5,length(ACTIVE_EMUI_VERSION)),trim(cust_name),unbase64(ACTIVE_EMUI_VERSION),Upper(cust_name),initcap(ACTIVE_EMUI_VERSION),soundex(cust_name) from uniqdata where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-04-06 02:00:03'
and doj<='1971-02-05 02:00:03' and  BIGINT_COLUMN2 between -223372036759  and -223372036454   and cust_id not in(9500,9501,9506,9600,9700,9800,10000,9788) group by ACTIVE_EMUI_VERSION,cust_name having count(length(cust_name))>=1 and min(lower(cust_name))not like '%abc%'""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_030
test("UNIQDATA_DEFAULT_MultiBitSet_TC_030", Include) {
  sql(s"""select cust_id,cust_name,to_date(doj),quarter(dob),month(doj),day(dob),hour(doj),minute(dob),second(doj),weekofyear(dob),datediff(doj,current_date),date_add(dob,4),date_sub(doj,1),to_utc_timestamp(doj,current_date),add_months(dob,5),last_day(doj),months_between(doj,current_date),date_format(dob,current_date) from uniqdata where substr(cust_name,0,4)='CUST' and length(cust_name)in(15,14,13,16) and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and month(dob)=01  and minute(dob)=0  group by doj,cust_id,cust_name,ACTIVE_EMUI_VERSION,dob having  max(cust_id)=10830 and count(distinct(cust_id))<=2001 and max(cust_name) not like '%def%' order by cust_id,cust_name""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_032
test("UNIQDATA_DEFAULT_MultiBitSet_TC_032", Include) {
  sql(s"""select variance(Double_COLUMN2),var_pop(Double_COLUMN1),var_samp(BIGINT_COLUMN2),stddev_pop(Double_COLUMN2),stddev_samp(DECIMAL_COLUMN2),covar_pop(DECIMAL_COLUMN2,DECIMAL_COLUMN1),covar_samp(DECIMAL_COLUMN2,DECIMAL_COLUMN1),corr(Double_COLUMN2,Double_COLUMN1),corr(DECIMAL_COLUMN2,DECIMAL_COLUMN1),percentile(cust_id,0.25),percentile_approx(BIGINT_COLUMN1,0.25,5) from uniqdata where cust_id not in (9002,9003,9004,9005,9006,9007,9008,9009,9010,9011,9012,9013,9014,9015,9016,9017,9018,9019,9021,9022,9023,9024,9025,9027,9028,9029,9030) and cust_name like 'CUST%' and dob> '1970-01-01 01:00:03' and dob< '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION not like '%abc%' """).collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_036
test("UNIQDATA_DEFAULT_MultiBitSet_TC_036", Include) {
  checkAnswer(s"""select covar_pop(Double_COLUMN2,Double_COLUMN2) as a  from (select Double_COLUMN2 from uniqdata where cust_id between 9000 and 10000 and cust_name like 'Cust%' and dob>='1970-01-01 00:00:00'  order by Double_COLUMN2 ) t""",
    s"""select covar_pop(Double_COLUMN2,Double_COLUMN2) as a  from (select Double_COLUMN2 from uniqdata_hive where cust_id between 9000 and 10000 and cust_name like 'Cust%' and dob>='1970-01-01 00:00:00'  order by Double_COLUMN2 ) t""")
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_037
test("UNIQDATA_DEFAULT_MultiBitSet_TC_037", Include) {
  sql(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 as a  from (select * from uniqdata order by Double_COLUMN2)t where cust_name in ('CUST_NAME_01987','CUST_NAME_01988','CUST_NAME_01989','CUST_NAME_01990','CUST_NAME_01991' ,'CUST_NAME_01992')
""").collect
}
       

//UNIQDATA_DEFAULT_MultiBitSet_TC_038
test("UNIQDATA_DEFAULT_MultiBitSet_TC_038", Include) {
  sql(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from uniqdata where  ACTIVE_EMUI_VERSION rlike 'ACTIVE' and dob between '1970-10-15 01:00:03' and '1971-04-29 01:00:03' and doj between '1970-10-15 02:00:03' and '1971-04-29 02:00:03' and cust_name like 'CUST%'
""").collect
}
       

//PushUP_FILTER_uniqdata_TC001
test("PushUP_FILTER_uniqdata_TC001", Include) {
  sql(s"""select to_date(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC002
test("PushUP_FILTER_uniqdata_TC002", Include) {
  checkAnswer(s"""select max(to_date(DOB)),min(to_date(DOB)),count(to_date(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(to_date(DOB)),min(to_date(DOB)),count(to_date(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC003
test("PushUP_FILTER_uniqdata_TC003", Include) {
  checkAnswer(s"""select max(to_date(DOB)),min(to_date(DOB)),count(to_date(DOB)) from uniqdata where to_date(DOB)='1975-06-11' or to_date(DOB)='1975-06-23' """,
    s"""select max(to_date(DOB)),min(to_date(DOB)),count(to_date(DOB)) from uniqdata_hive where to_date(DOB)='1975-06-11' or to_date(DOB)='1975-06-23' """)
}
       

//PushUP_FILTER_uniqdata_TC004
test("PushUP_FILTER_uniqdata_TC004", Include) {
  checkAnswer(s"""select year(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select year(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC005
test("PushUP_FILTER_uniqdata_TC005", Include) {
  checkAnswer(s"""select max(year(DOB)),min(year(DOB)),count(year(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(year(DOB)),min(year(DOB)),count(year(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC006
test("PushUP_FILTER_uniqdata_TC006", Include) {
  checkAnswer(s"""select max(year(DOB)),min(year(DOB)),count(year(DOB)) from uniqdata where year(DOB)=1975 or year(DOB) is NOT NULL or year(DOB) IS NULL""",
    s"""select max(year(DOB)),min(year(DOB)),count(year(DOB)) from uniqdata_hive where year(DOB)=1975 or year(DOB) is NOT NULL or year(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC007
test("PushUP_FILTER_uniqdata_TC007", Include) {
  checkAnswer(s"""select month(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select month(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC008
test("PushUP_FILTER_uniqdata_TC008", Include) {
  checkAnswer(s"""select max(month(DOB)),min(month(DOB)),count(month(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(month(DOB)),min(month(DOB)),count(month(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC009
test("PushUP_FILTER_uniqdata_TC009", Include) {
  checkAnswer(s"""select max(month(DOB)),min(month(DOB)),count(month(DOB)) from uniqdata where month(DOB)=6 or month(DOB) is NOT NULL or month(DOB) IS NULL""",
    s"""select max(month(DOB)),min(month(DOB)),count(month(DOB)) from uniqdata_hive where month(DOB)=6 or month(DOB) is NOT NULL or month(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC010
test("PushUP_FILTER_uniqdata_TC010", Include) {
  checkAnswer(s"""select day(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select day(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC011
test("PushUP_FILTER_uniqdata_TC011", Include) {
  checkAnswer(s"""select max(day(DOB)),min(day(DOB)),count(day(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(day(DOB)),min(day(DOB)),count(day(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC012
test("PushUP_FILTER_uniqdata_TC012", Include) {
  checkAnswer(s"""select max(day(DOB)),min(day(DOB)),count(day(DOB)) from uniqdata where day(DOB)=23 or day(DOB) is NOT NULL or day(DOB) IS NULL""",
    s"""select max(day(DOB)),min(day(DOB)),count(day(DOB)) from uniqdata_hive where day(DOB)=23 or day(DOB) is NOT NULL or day(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC013
test("PushUP_FILTER_uniqdata_TC013", Include) {
  checkAnswer(s"""select hour(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select hour(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC014
test("PushUP_FILTER_uniqdata_TC014", Include) {
  checkAnswer(s"""select max(hour(DOB)),min(hour(DOB)),count(hour(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(hour(DOB)),min(hour(DOB)),count(hour(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC015
test("PushUP_FILTER_uniqdata_TC015", Include) {
  checkAnswer(s"""select max(hour(DOB)),min(hour(DOB)),count(hour(DOB)) from uniqdata where hour(DOB)=1 or hour(DOB) is NOT NULL or hour(DOB) IS NULL""",
    s"""select max(hour(DOB)),min(hour(DOB)),count(hour(DOB)) from uniqdata_hive where hour(DOB)=1 or hour(DOB) is NOT NULL or hour(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC016
test("PushUP_FILTER_uniqdata_TC016", Include) {
  checkAnswer(s"""select minute(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select minute(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC017
test("PushUP_FILTER_uniqdata_TC017", Include) {
  checkAnswer(s"""select max(minute(DOB)),min(minute(DOB)),count(minute(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(minute(DOB)),min(minute(DOB)),count(minute(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC018
test("PushUP_FILTER_uniqdata_TC018", Include) {
  checkAnswer(s"""select max(minute(DOB)),min(minute(DOB)),count(minute(DOB)) from uniqdata where minute(DOB)=23 or minute(DOB) is NOT NULL or minute(DOB) IS NULL""",
    s"""select max(minute(DOB)),min(minute(DOB)),count(minute(DOB)) from uniqdata_hive where minute(DOB)=23 or minute(DOB) is NOT NULL or minute(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC019
test("PushUP_FILTER_uniqdata_TC019", Include) {
  checkAnswer(s"""select second(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select second(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC020
test("PushUP_FILTER_uniqdata_TC020", Include) {
  checkAnswer(s"""select max(second(DOB)),min(second(DOB)),count(second(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(second(DOB)),min(second(DOB)),count(second(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC021
test("PushUP_FILTER_uniqdata_TC021", Include) {
  checkAnswer(s"""select max(second(DOB)),min(second(DOB)),count(second(DOB)) from uniqdata where second(DOB)=3 or second(DOB) is NOT NULL or second(DOB) IS NULL""",
    s"""select max(second(DOB)),min(second(DOB)),count(second(DOB)) from uniqdata_hive where second(DOB)=3 or second(DOB) is NOT NULL or second(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC022
test("PushUP_FILTER_uniqdata_TC022", Include) {
  checkAnswer(s"""select weekofyear(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select weekofyear(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC023
test("PushUP_FILTER_uniqdata_TC023", Include) {
  checkAnswer(s"""select max(weekofyear(DOB)),min(weekofyear(DOB)),count(weekofyear(DOB)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(weekofyear(DOB)),min(weekofyear(DOB)),count(weekofyear(DOB)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC024
test("PushUP_FILTER_uniqdata_TC024", Include) {
  checkAnswer(s"""select max(weekofyear(DOB)),min(weekofyear(DOB)),count(weekofyear(DOB)) from uniqdata where weekofyear(DOB)=26 or weekofyear(DOB) is NOT NULL or weekofyear(DOB) IS NULL""",
    s"""select max(weekofyear(DOB)),min(weekofyear(DOB)),count(weekofyear(DOB)) from uniqdata_hive where weekofyear(DOB)=26 or weekofyear(DOB) is NOT NULL or weekofyear(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC025
test("PushUP_FILTER_uniqdata_TC025", Include) {
  checkAnswer(s"""select datediff(DOB,DOJ) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select datediff(DOB,DOJ) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC026
test("PushUP_FILTER_uniqdata_TC026", Include) {
  checkAnswer(s"""select max(datediff(DOB,DOJ)),min(datediff(DOB,DOJ)),count(datediff(DOB,DOJ)) from uniqdata where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(datediff(DOB,DOJ)),min(datediff(DOB,DOJ)),count(datediff(DOB,DOJ)) from uniqdata_hive where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC027
test("PushUP_FILTER_uniqdata_TC027", Include) {
  sql(s"""select max(weekofyear(DOB)),min(weekofyear(DOB)),count(weekofyear(DOB)) from uniqdata where datediff(DOB,DOJ)=0 or datediff(DOB,DOJ) is NOT NULL or datediff(DOB,DOJ) IS NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC028
test("PushUP_FILTER_uniqdata_TC028", Include) {
  checkAnswer(s"""select date_add(DOB,1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select date_add(DOB,1) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC029
test("PushUP_FILTER_uniqdata_TC029", Include) {
  checkAnswer(s"""select max(date_add(DOB,1)),min(date_add(DOB,1)),count(date_add(DOB,1)) from uniqdata where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(date_add(DOB,1)),min(date_add(DOB,1)),count(date_add(DOB,1)) from uniqdata_hive where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC030
test("PushUP_FILTER_uniqdata_TC030", Include) {
  checkAnswer(s"""select max(date_add(DOB,1)),min(date_add(DOB,1)),count(date_add(DOB,1)) from uniqdata where date_add(DOB,1)='1975-06-12' or date_add(DOB,1) is NOT NULL or date_add(DOB,1) IS NULL""",
    s"""select max(date_add(DOB,1)),min(date_add(DOB,1)),count(date_add(DOB,1)) from uniqdata_hive where date_add(DOB,1)='1975-06-12' or date_add(DOB,1) is NOT NULL or date_add(DOB,1) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC031
test("PushUP_FILTER_uniqdata_TC031", Include) {
  checkAnswer(s"""select date_sub(DOB,1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select date_sub(DOB,1) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC032
test("PushUP_FILTER_uniqdata_TC032", Include) {
  checkAnswer(s"""select max(date_sub(DOB,1)),min(date_sub(DOB,1)),count(date_sub(DOB,1)) from uniqdata where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(date_sub(DOB,1)),min(date_sub(DOB,1)),count(date_sub(DOB,1)) from uniqdata_hive where CUST_ID IS NULL or DOJ IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC033
test("PushUP_FILTER_uniqdata_TC033", Include) {
  checkAnswer(s"""select max(date_sub(DOB,1)),min(date_sub(DOB,1)),count(date_sub(DOB,1)) from uniqdata where date_sub(DOB,1)='1975-06-12' or date_sub(DOB,1) is NOT NULL or date_sub(DOB,1) IS NULL""",
    s"""select max(date_sub(DOB,1)),min(date_sub(DOB,1)),count(date_sub(DOB,1)) from uniqdata_hive where date_sub(DOB,1)='1975-06-12' or date_sub(DOB,1) is NOT NULL or date_sub(DOB,1) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC034
test("PushUP_FILTER_uniqdata_TC034", Include) {
  checkAnswer(s"""select concat(CUST_ID),concat(CUST_NAME),concat(DOB),concat(BIGINT_COLUMN1),concat(DECIMAL_COLUMN1),concat(Double_COLUMN1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select concat(CUST_ID),concat(CUST_NAME),concat(DOB),concat(BIGINT_COLUMN1),concat(DECIMAL_COLUMN1),concat(Double_COLUMN1) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC035
test("PushUP_FILTER_uniqdata_TC035", Include) {
  checkAnswer(s"""select max(concat(CUST_ID)),max(concat(CUST_NAME)),max(concat(DOB)),max(concat(BIGINT_COLUMN1)),max(concat(DECIMAL_COLUMN1)),max(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(concat(CUST_ID)),max(concat(CUST_NAME)),max(concat(DOB)),max(concat(BIGINT_COLUMN1)),max(concat(DECIMAL_COLUMN1)),max(concat(Double_COLUMN1)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC036
test("PushUP_FILTER_uniqdata_TC036", Include) {
  checkAnswer(s"""select min(concat(CUST_ID)),min(concat(CUST_NAME)),min(concat(DOB)),min(concat(BIGINT_COLUMN1)),min(concat(DECIMAL_COLUMN1)),min(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select min(concat(CUST_ID)),min(concat(CUST_NAME)),min(concat(DOB)),min(concat(BIGINT_COLUMN1)),min(concat(DECIMAL_COLUMN1)),min(concat(Double_COLUMN1)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC037
test("PushUP_FILTER_uniqdata_TC037", Include) {
  sql(s"""select sum(concat(CUST_ID)),sum(concat(CUST_NAME)),sum(concat(DOB)),sum(concat(BIGINT_COLUMN1)),sum(concat(DECIMAL_COLUMN1)),sum(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC038
test("PushUP_FILTER_uniqdata_TC038", Include) {
  checkAnswer(s"""select count(concat(CUST_ID)),count(concat(CUST_NAME)),count(concat(DOB)),count(concat(BIGINT_COLUMN1)),count(concat(DECIMAL_COLUMN1)),count(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select count(concat(CUST_ID)),count(concat(CUST_NAME)),count(concat(DOB)),count(concat(BIGINT_COLUMN1)),count(concat(DECIMAL_COLUMN1)),count(concat(Double_COLUMN1)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC039
test("PushUP_FILTER_uniqdata_TC039", Include) {
  sql(s"""select avg(concat(CUST_ID)),avg(concat(CUST_NAME)),avg(concat(DOB)),avg(concat(BIGINT_COLUMN1)),avg(concat(DECIMAL_COLUMN1)),avg(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC040
test("PushUP_FILTER_uniqdata_TC040", Include) {
  sql(s"""select variance(concat(CUST_ID)),variance(concat(CUST_NAME)),variance(concat(DOB)),variance(concat(BIGINT_COLUMN1)),variance(concat(DECIMAL_COLUMN1)),variance(concat(Double_COLUMN1)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC041
test("PushUP_FILTER_uniqdata_TC041", Include) {
  sql(s"""select max(concat(CUST_ID)),max(concat(CUST_NAME)),max(concat(DOB)),max(concat(BIGINT_COLUMN1)),max(concat(DECIMAL_COLUMN1)),max(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC042
test("PushUP_FILTER_uniqdata_TC042", Include) {
  sql(s"""select min(concat(CUST_ID)),min(concat(CUST_NAME)),min(concat(DOB)),min(concat(BIGINT_COLUMN1)),min(concat(DECIMAL_COLUMN1)),min(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC043
test("PushUP_FILTER_uniqdata_TC043", Include) {
  sql(s"""select sum(concat(CUST_ID)),sum(concat(CUST_NAME)),sum(concat(DOB)),sum(concat(BIGINT_COLUMN1)),sum(concat(DECIMAL_COLUMN1)),sum(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC044
test("PushUP_FILTER_uniqdata_TC044", Include) {
  sql(s"""select count(concat(CUST_ID)),count(concat(CUST_NAME)),count(concat(DOB)),count(concat(BIGINT_COLUMN1)),count(concat(DECIMAL_COLUMN1)),count(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC045
test("PushUP_FILTER_uniqdata_TC045", Include) {
  sql(s"""select avg(concat(CUST_ID)),avg(concat(CUST_NAME)),avg(concat(DOB)),avg(concat(BIGINT_COLUMN1)),avg(concat(DECIMAL_COLUMN1)),avg(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC046
test("PushUP_FILTER_uniqdata_TC046", Include) {
  sql(s"""select variance(concat(CUST_ID)),variance(concat(CUST_NAME)),variance(concat(DOB)),variance(concat(BIGINT_COLUMN1)),variance(concat(DECIMAL_COLUMN1)),variance(concat(Double_COLUMN1)) from uniqdata where concat(CUST_ID)=10999 or concat(CUST_NAME)='CUST_NAME_01987' or concat(CUST_NAME)='1975-06-11 01:00:03' or  concat(BIGINT_COLUMN1)=123372038844 or concat(DECIMAL_COLUMN1)=12345680888.1234000000 or concat(Double_COLUMN1)=1.12345674897976E10""").collect
}
       

//PushUP_FILTER_uniqdata_TC047
test("PushUP_FILTER_uniqdata_TC047", Include) {
  checkAnswer(s"""select length(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select length(CUST_NAME) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC048
test("PushUP_FILTER_uniqdata_TC048", Include) {
  sql(s"""select max(length(CUST_NAME)),min(length(CUST_NAME)),avg(length(CUST_NAME)),count(length(CUST_NAME)),sum(length(CUST_NAME)),variance(length(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC049
test("PushUP_FILTER_uniqdata_TC049", Include) {
  sql(s"""select max(length(CUST_NAME)),min(length(CUST_NAME)),avg(length(CUST_NAME)),count(length(CUST_NAME)),sum(length(CUST_NAME)),variance(length(CUST_NAME)) from uniqdata where length(CUST_NAME)=15 or length(CUST_NAME) is NULL or length(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC050
test("PushUP_FILTER_uniqdata_TC050", Include) {
  checkAnswer(s"""select lower(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select lower(CUST_NAME) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC051
test("PushUP_FILTER_uniqdata_TC051", Include) {
  checkAnswer(s"""select max(lower(CUST_NAME)),min(lower(CUST_NAME)),avg(lower(CUST_NAME)),count(lower(CUST_NAME)),sum(lower(CUST_NAME)),variance(lower(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(lower(CUST_NAME)),min(lower(CUST_NAME)),avg(lower(CUST_NAME)),count(lower(CUST_NAME)),sum(lower(CUST_NAME)),variance(lower(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC052
test("PushUP_FILTER_uniqdata_TC052", Include) {
  sql(s"""select max(lower(CUST_NAME)),min(lower(CUST_NAME)),avg(lower(CUST_NAME)),count(lower(CUST_NAME)),sum(lower(CUST_NAME)),variance(lower(CUST_NAME)) from uniqdata where lower(CUST_NAME)=15 or lower(CUST_NAME) is NULL or lower(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC053
test("PushUP_FILTER_uniqdata_TC053", Include) {
  checkAnswer(s"""select lcase(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select lcase(CUST_NAME) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC054
test("PushUP_FILTER_uniqdata_TC054", Include) {
  checkAnswer(s"""select max(lcase(CUST_NAME)),min(lcase(CUST_NAME)),avg(lcase(CUST_NAME)),count(lcase(CUST_NAME)),sum(lcase(CUST_NAME)),variance(lcase(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(lcase(CUST_NAME)),min(lcase(CUST_NAME)),avg(lcase(CUST_NAME)),count(lcase(CUST_NAME)),sum(lcase(CUST_NAME)),variance(lcase(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC055
test("PushUP_FILTER_uniqdata_TC055", Include) {
  sql(s"""select max(lcase(CUST_NAME)),min(lcase(CUST_NAME)),avg(lcase(CUST_NAME)),count(lcase(CUST_NAME)),sum(lcase(CUST_NAME)),variance(lcase(CUST_NAME)) from uniqdata where lcase(CUST_NAME)=15 or lcase(CUST_NAME) is NULL or lcase(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC056
test("PushUP_FILTER_uniqdata_TC056", Include) {
  checkAnswer(s"""select regexp_replace(CUST_NAME,'a','b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select regexp_replace(CUST_NAME,'a','b') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC057
test("PushUP_FILTER_uniqdata_TC057", Include) {
  checkAnswer(s"""select substr(CUST_NAME,1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select substr(CUST_NAME,1,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC058
test("PushUP_FILTER_uniqdata_TC058", Include) {
  checkAnswer(s"""select max(substr(CUST_NAME,1,2)),min(substr(CUST_NAME,1,2)),avg(substr(CUST_NAME,1,2)),count(substr(CUST_NAME,1,2)),sum(substr(CUST_NAME,1,2)),variance(substr(CUST_NAME,1,2)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(substr(CUST_NAME,1,2)),min(substr(CUST_NAME,1,2)),avg(substr(CUST_NAME,1,2)),count(substr(CUST_NAME,1,2)),sum(substr(CUST_NAME,1,2)),variance(substr(CUST_NAME,1,2)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC059
test("PushUP_FILTER_uniqdata_TC059", Include) {
  sql(s"""select max(substr(CUST_NAME,1,2)),min(substr(CUST_NAME,1,2)),avg(substr(CUST_NAME,1,2)),count(substr(CUST_NAME,1,2)),sum(substr(CUST_NAME,1,2)),variance(substr(CUST_NAME,1,2)) from uniqdata where substr(CUST_NAME,1,2)='CU' or substr(CUST_NAME,1,2) is NULL or substr(CUST_NAME,1,2) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC060
test("PushUP_FILTER_uniqdata_TC060", Include) {
  checkAnswer(s"""select substring(CUST_NAME,1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select substring(CUST_NAME,1,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC061
test("PushUP_FILTER_uniqdata_TC061", Include) {
  checkAnswer(s"""select max(substring(CUST_NAME,1,2)),min(substring(CUST_NAME,1,2)),avg(substring(CUST_NAME,1,2)),count(substring(CUST_NAME,1,2)),sum(substring(CUST_NAME,1,2)),variance(substring(CUST_NAME,1,2)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(substring(CUST_NAME,1,2)),min(substring(CUST_NAME,1,2)),avg(substring(CUST_NAME,1,2)),count(substring(CUST_NAME,1,2)),sum(substring(CUST_NAME,1,2)),variance(substring(CUST_NAME,1,2)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC062
test("PushUP_FILTER_uniqdata_TC062", Include) {
  sql(s"""select max(substring(CUST_NAME,1,2)),min(substring(CUST_NAME,1,2)),avg(substring(CUST_NAME,1,2)),count(substring(CUST_NAME,1,2)),sum(substring(CUST_NAME,1,2)),variance(substring(CUST_NAME,1,2)) from uniqdata where substring(CUST_NAME,1,2)='CU' or substring(CUST_NAME,1,2) is NULL or substring(CUST_NAME,1,2) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC063
test("PushUP_FILTER_uniqdata_TC063", Include) {
  checkAnswer(s"""select upper(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select upper(CUST_NAME) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC064
test("PushUP_FILTER_uniqdata_TC064", Include) {
  checkAnswer(s"""select max(upper(CUST_NAME)),min(upper(CUST_NAME)),avg(upper(CUST_NAME)),count(upper(CUST_NAME)),sum(upper(CUST_NAME)),variance(upper(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select max(upper(CUST_NAME)),min(upper(CUST_NAME)),avg(upper(CUST_NAME)),count(upper(CUST_NAME)),sum(upper(CUST_NAME)),variance(upper(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC065
test("PushUP_FILTER_uniqdata_TC065", Include) {
  sql(s"""select max(upper(CUST_NAME)),min(upper(CUST_NAME)),avg(upper(CUST_NAME)),count(upper(CUST_NAME)),sum(upper(CUST_NAME)),variance(upper(CUST_NAME)) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC066
test("PushUP_FILTER_uniqdata_TC066", Include) {
  checkAnswer(s"""select var_pop(upper(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select var_pop(upper(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC067
test("PushUP_FILTER_uniqdata_TC067", Include) {
  checkAnswer(s"""select var_samp(upper(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select var_samp(upper(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC068
test("PushUP_FILTER_uniqdata_TC068", Include) {
  checkAnswer(s"""select var_samp(upper(CUST_NAME)) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""",
    s"""select var_samp(upper(CUST_NAME)) from uniqdata_hive where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""")
}
       

//PushUP_FILTER_uniqdata_TC069
test("PushUP_FILTER_uniqdata_TC069", Include) {
  checkAnswer(s"""select stddev_pop(upper(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select stddev_pop(upper(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC070
test("PushUP_FILTER_uniqdata_TC070", Include) {
  checkAnswer(s"""select stddev_pop(upper(CUST_NAME)) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""",
    s"""select stddev_pop(upper(CUST_NAME)) from uniqdata_hive where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""")
}
       

//PushUP_FILTER_uniqdata_TC071
test("PushUP_FILTER_uniqdata_TC071", Include) {
  checkAnswer(s"""select stddev_samp(upper(CUST_NAME)) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select stddev_samp(upper(CUST_NAME)) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC072
test("PushUP_FILTER_uniqdata_TC072", Include) {
  checkAnswer(s"""select stddev_samp(upper(CUST_NAME)) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""",
    s"""select stddev_samp(upper(CUST_NAME)) from uniqdata_hive where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""")
}
       

//PushUP_FILTER_uniqdata_TC073
test("PushUP_FILTER_uniqdata_TC073", Include) {
  sql(s"""select covar_pop(1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC074
test("PushUP_FILTER_uniqdata_TC074", Include) {
  sql(s"""select covar_pop(1,2) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC075
test("PushUP_FILTER_uniqdata_TC075", Include) {
  sql(s"""select covar_samp(1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC076
test("PushUP_FILTER_uniqdata_TC076", Include) {
  sql(s"""select covar_samp(1,2) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC077
test("PushUP_FILTER_uniqdata_TC077", Include) {
  sql(s"""select corr(1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC078
test("PushUP_FILTER_uniqdata_TC078", Include) {
  sql(s"""select corr(1,2) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC079
test("PushUP_FILTER_uniqdata_TC079", Include) {
  checkAnswer(s"""select histogram_numeric(1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """,
    s"""select histogram_numeric(1,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC080
test("PushUP_FILTER_uniqdata_TC080", Include) {
  sql(s"""select histogram_numeric(1,2) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC081
test("PushUP_FILTER_uniqdata_TC081", Include) {
  sql(s"""select collect_set(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC082
test("PushUP_FILTER_uniqdata_TC082", Include) {
  sql(s"""select collect_set(CUST_NAME) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC083
test("PushUP_FILTER_uniqdata_TC083", Include) {
  sql(s"""select collect_list(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC084
test("PushUP_FILTER_uniqdata_TC084", Include) {
  sql(s"""select collect_list(CUST_NAME) from uniqdata where upper(CUST_NAME)=15 or upper(CUST_NAME) is NULL or upper(CUST_NAME) is NOT NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC085
test("PushUP_FILTER_uniqdata_TC085", Include) {
  sql(s"""select bin(CUST_ID),bin(CUST_NAME),bin(BIGINT_COLUMN1),bin(DECIMAL_COLUMN1),bin(Double_COLUMN1),bin(INTEGER_COLUMN1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 <=> 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL """).collect
}
       

//PushUP_FILTER_uniqdata_TC087
test("PushUP_FILTER_uniqdata_TC087", Include) {
  checkAnswer(s"""select hex(CUST_ID),hex(CUST_NAME),hex(BIGINT_COLUMN1),hex(DECIMAL_COLUMN1),hex(Double_COLUMN1),hex(INTEGER_COLUMN1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL or hex(DOB)='313937352D30362D31312030313A30303A3033' """,
    s"""select hex(CUST_ID),hex(CUST_NAME),hex(BIGINT_COLUMN1),hex(DECIMAL_COLUMN1),hex(Double_COLUMN1),hex(INTEGER_COLUMN1) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL or hex(DOB)='313937352D30362D31312030313A30303A3033' """)
}
       

//PushUP_FILTER_uniqdata_TC088
test("PushUP_FILTER_uniqdata_TC088", Include) {
  sql(s"""select hex(CUST_ID),hex(CUST_NAME),hex(BIGINT_COLUMN1),hex(DECIMAL_COLUMN1),hex(Double_COLUMN1),hex(INTEGER_COLUMN1) from uniqdata where hex(CUST_ID)='2AEF' or hex(CUST_NAME) is NULL or hex(BIGINT_COLUMN1)='1CB98BEABA' or hex(Double_COLUMN1)='29DA1E541' or hex(INTEGER_COLUMN1)='7C7'""").collect
}
       

//PushUP_FILTER_uniqdata_TC089
test("PushUP_FILTER_uniqdata_TC089", Include) {
  sql(s"""select unhex(CUST_ID),unhex(CUST_NAME),unhex(BIGINT_COLUMN1),unhex(DECIMAL_COLUMN1),unhex(Double_COLUMN1),unhex(INTEGER_COLUMN1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC090
test("PushUP_FILTER_uniqdata_TC090", Include) {
  sql(s"""select unhex(CUST_ID),unhex(CUST_NAME),unhex(BIGINT_COLUMN1),unhex(DECIMAL_COLUMN1),unhex(Double_COLUMN1),unhex(INTEGER_COLUMN1) from uniqdata where unhex(CUST_NAME) is NULL or unhex(BIGINT_COLUMN1) IS NOT NULL or unhex(Double_COLUMN1) IS NULL or unhex(DOB) IS NULL""").collect
}
       

//PushUP_FILTER_uniqdata_TC091
test("PushUP_FILTER_uniqdata_TC091", Include) {
  checkAnswer(s"""select conv(CUST_ID,1,2),conv(CUST_NAME,1,2),conv(BIGINT_COLUMN1,1,2),conv(DECIMAL_COLUMN1,1,2),conv(Double_COLUMN1,1,2),conv(INTEGER_COLUMN1,1,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select conv(CUST_ID,1,2),conv(CUST_NAME,1,2),conv(BIGINT_COLUMN1,1,2),conv(DECIMAL_COLUMN1,1,2),conv(Double_COLUMN1,1,2),conv(INTEGER_COLUMN1,1,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC092
test("PushUP_FILTER_uniqdata_TC092", Include) {
  checkAnswer(s"""select conv(CUST_ID,1,2),conv(CUST_NAME,1,2),conv(BIGINT_COLUMN1,1,2),conv(DECIMAL_COLUMN1,1,2),conv(Double_COLUMN1,1,2),conv(INTEGER_COLUMN1,1,2) from uniqdata where conv(CUST_NAME,1,2) is NULL or conv(BIGINT_COLUMN1,1,2) IS NOT NULL or conv(Double_COLUMN1,1,2) IS NULL or conv(DOB,1,2) IS NULL""",
    s"""select conv(CUST_ID,1,2),conv(CUST_NAME,1,2),conv(BIGINT_COLUMN1,1,2),conv(DECIMAL_COLUMN1,1,2),conv(Double_COLUMN1,1,2),conv(INTEGER_COLUMN1,1,2) from uniqdata_hive where conv(CUST_NAME,1,2) is NULL or conv(BIGINT_COLUMN1,1,2) IS NOT NULL or conv(Double_COLUMN1,1,2) IS NULL or conv(DOB,1,2) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC093
test("PushUP_FILTER_uniqdata_TC093", Include) {
  checkAnswer(s"""select abs(CUST_ID),abs(BIGINT_COLUMN1),abs(DECIMAL_COLUMN1),abs(Double_COLUMN1),abs(INTEGER_COLUMN1) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select abs(CUST_ID),abs(BIGINT_COLUMN1),abs(DECIMAL_COLUMN1),abs(Double_COLUMN1),abs(INTEGER_COLUMN1) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC094
test("PushUP_FILTER_uniqdata_TC094", Include) {
  checkAnswer(s"""select abs(CUST_ID),abs(BIGINT_COLUMN1),abs(DECIMAL_COLUMN1),abs(Double_COLUMN1),abs(INTEGER_COLUMN1) from uniqdata where abs(BIGINT_COLUMN1) IS NOT NULL or abs(Double_COLUMN1) IS NULL """,
    s"""select abs(CUST_ID),abs(BIGINT_COLUMN1),abs(DECIMAL_COLUMN1),abs(Double_COLUMN1),abs(INTEGER_COLUMN1) from uniqdata_hive where abs(BIGINT_COLUMN1) IS NOT NULL or abs(Double_COLUMN1) IS NULL """)
}
       

//PushUP_FILTER_uniqdata_TC095
test("PushUP_FILTER_uniqdata_TC095", Include) {
  checkAnswer(s"""select unix_timestamp(CUST_NAME,'a'),unix_timestamp(DOB,'a') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select unix_timestamp(CUST_NAME,'a'),unix_timestamp(DOB,'a') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC096
test("PushUP_FILTER_uniqdata_TC096", Include) {
  checkAnswer(s"""select unix_timestamp(CUST_NAME,'a'),unix_timestamp(DOB,'a')from uniqdata where unix_timestamp(CUST_NAME,'a') is not null or unix_timestamp(DOB,'a') IS NOT NULL""",
    s"""select unix_timestamp(CUST_NAME,'a'),unix_timestamp(DOB,'a')from uniqdata_hive where unix_timestamp(CUST_NAME,'a') is not null or unix_timestamp(DOB,'a') IS NOT NULL""")
}
       

//PushUP_FILTER_uniqdata_TC097
test("PushUP_FILTER_uniqdata_TC097", Include) {
  checkAnswer(s"""select quarter(CUST_NAME),quarter(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select quarter(CUST_NAME),quarter(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC098
test("PushUP_FILTER_uniqdata_TC098", Include) {
  checkAnswer(s"""select quarter(CUST_NAME),quarter(DOB)from uniqdata where quarter(CUST_NAME) is not null or quarter(DOB) =2""",
    s"""select quarter(CUST_NAME),quarter(DOB)from uniqdata_hive where quarter(CUST_NAME) is not null or quarter(DOB) =2""")
}
       

//PushUP_FILTER_uniqdata_TC099
test("PushUP_FILTER_uniqdata_TC099", Include) {
  checkAnswer(s"""select from_utc_timestamp(CUST_NAME,'a'),from_utc_timestamp(DOB,'a') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select from_utc_timestamp(CUST_NAME,'a'),from_utc_timestamp(DOB,'a') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC100
test("PushUP_FILTER_uniqdata_TC100", Include) {
  checkAnswer(s"""select from_utc_timestamp(CUST_NAME,'a'),from_utc_timestamp(DOB,'a')from uniqdata where from_utc_timestamp(CUST_NAME,'a') is not null or from_utc_timestamp(DOB,'a') ='1975-06-15 01:00:03.0'""",
    s"""select from_utc_timestamp(CUST_NAME,'a'),from_utc_timestamp(DOB,'a')from uniqdata_hive where from_utc_timestamp(CUST_NAME,'a') is not null or from_utc_timestamp(DOB,'a') ='1975-06-15 01:00:03.0'""")
}
       

//PushUP_FILTER_uniqdata_TC101
test("PushUP_FILTER_uniqdata_TC101", Include) {
  checkAnswer(s"""select to_utc_timestamp(CUST_NAME,'a'),to_utc_timestamp(DOB,'a') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select to_utc_timestamp(CUST_NAME,'a'),to_utc_timestamp(DOB,'a') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC102
test("PushUP_FILTER_uniqdata_TC102", Include) {
  checkAnswer(s"""select to_utc_timestamp(CUST_NAME,'a'),to_utc_timestamp(DOB,'a')from uniqdata where to_utc_timestamp(CUST_NAME,'a') is not null or to_utc_timestamp(DOB,'a') ='1975-06-15 01:00:03.0'""",
    s"""select to_utc_timestamp(CUST_NAME,'a'),to_utc_timestamp(DOB,'a')from uniqdata_hive where to_utc_timestamp(CUST_NAME,'a') is not null or to_utc_timestamp(DOB,'a') ='1975-06-15 01:00:03.0'""")
}
       

//PushUP_FILTER_uniqdata_TC103
test("PushUP_FILTER_uniqdata_TC103", Include) {
  checkAnswer(s"""select add_months(CUST_NAME,1),add_months(DOB,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select add_months(CUST_NAME,1),add_months(DOB,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC104
test("PushUP_FILTER_uniqdata_TC104", Include) {
  checkAnswer(s"""select add_months(CUST_NAME,1),add_months(DOB,2)from uniqdata where add_months(CUST_NAME,1) is not null or add_months(DOB,2) ='1975-08-19'""",
    s"""select add_months(CUST_NAME,1),add_months(DOB,2)from uniqdata_hive where add_months(CUST_NAME,1) is not null or add_months(DOB,2) ='1975-08-19'""")
}
       

//PushUP_FILTER_uniqdata_TC105
test("PushUP_FILTER_uniqdata_TC105", Include) {
  checkAnswer(s"""select last_day(CUST_NAME),last_day(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select last_day(CUST_NAME),last_day(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC106
test("PushUP_FILTER_uniqdata_TC106", Include) {
  checkAnswer(s"""select last_day(CUST_NAME),last_day(DOB)from uniqdata where last_day(CUST_NAME) is not null or last_day(DOB) ='1975-06-30'""",
    s"""select last_day(CUST_NAME),last_day(DOB)from uniqdata_hive where last_day(CUST_NAME) is not null or last_day(DOB) ='1975-06-30'""")
}
       

//PushUP_FILTER_uniqdata_TC107
test("PushUP_FILTER_uniqdata_TC107", Include) {
  checkAnswer(s"""select next_day(CUST_NAME,1),next_day(DOB,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select next_day(CUST_NAME,1),next_day(DOB,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC108
test("PushUP_FILTER_uniqdata_TC108", Include) {
  checkAnswer(s"""select next_day(CUST_NAME,1),next_day(DOB,2)from uniqdata where next_day(CUST_NAME,1) is not null or next_day(DOB,2) IS NULL""",
    s"""select next_day(CUST_NAME,1),next_day(DOB,2)from uniqdata_hive where next_day(CUST_NAME,1) is not null or next_day(DOB,2) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC109
test("PushUP_FILTER_uniqdata_TC109", Include) {
  checkAnswer(s"""select months_between(CUST_NAME,'a'),months_between(DOB,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select months_between(CUST_NAME,'a'),months_between(DOB,'b') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC110
test("PushUP_FILTER_uniqdata_TC110", Include) {
  checkAnswer(s"""select months_between(CUST_NAME,'a'),months_between(DOB,'b')from uniqdata where months_between(CUST_NAME,'a') is not null or months_between(DOB,'b') IS NULL""",
    s"""select months_between(CUST_NAME,'a'),months_between(DOB,'b')from uniqdata_hive where months_between(CUST_NAME,'a') is not null or months_between(DOB,'b') IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC112
test("PushUP_FILTER_uniqdata_TC112", Include) {
  checkAnswer(s"""select ascii(CUST_NAME),ascii(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select ascii(CUST_NAME),ascii(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC113
test("PushUP_FILTER_uniqdata_TC113", Include) {
  checkAnswer(s"""select ascii(CUST_NAME),ascii(DOB)from uniqdata where ascii(CUST_NAME) =67 or ascii(DOB) IS NULL""",
    s"""select ascii(CUST_NAME),ascii(DOB)from uniqdata_hive where ascii(CUST_NAME) =67 or ascii(DOB) IS NULL""")
}
       

//PushUP_FILTER_uniqdata_TC114
test("PushUP_FILTER_uniqdata_TC114", Include) {
  checkAnswer(s"""select base64(CUST_NAME) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select base64(CUST_NAME) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC115
test("PushUP_FILTER_uniqdata_TC115", Include) {
  checkAnswer(s"""select base64(CUST_NAME)from uniqdata where base64(CUST_NAME) ='Q1VTVF9OQU1FXzAxOTg3' or base64(CUST_NAME) is not null""",
    s"""select base64(CUST_NAME)from uniqdata_hive where base64(CUST_NAME) ='Q1VTVF9OQU1FXzAxOTg3' or base64(CUST_NAME) is not null""")
}
       

//PushUP_FILTER_uniqdata_TC116
test("PushUP_FILTER_uniqdata_TC116", Include) {
  checkAnswer(s"""select concat_ws(CUST_NAME),concat_ws(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select concat_ws(CUST_NAME),concat_ws(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC117
test("PushUP_FILTER_uniqdata_TC117", Include) {
  checkAnswer(s"""select concat_ws(CUST_NAME)from uniqdata where concat_ws(CUST_NAME) IS NOT NULL or concat_ws(DOB) is null""",
    s"""select concat_ws(CUST_NAME)from uniqdata_hive where concat_ws(CUST_NAME) IS NOT NULL or concat_ws(DOB) is null""")
}
       

//PushUP_FILTER_uniqdata_TC118
test("PushUP_FILTER_uniqdata_TC118", Include) {
  checkAnswer(s"""select find_in_set(CUST_NAME,'a') , find_in_set(DOB,'a')from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select find_in_set(CUST_NAME,'a') , find_in_set(DOB,'a')from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC119
test("PushUP_FILTER_uniqdata_TC119", Include) {
  checkAnswer(s"""select find_in_set(CUST_NAME,'a')from uniqdata where find_in_set(CUST_NAME,'a') =0 or find_in_set(DOB,'b') is null""",
    s"""select find_in_set(CUST_NAME,'a')from uniqdata_hive where find_in_set(CUST_NAME,'a') =0 or find_in_set(DOB,'b') is null""")
}
       

//PushUP_FILTER_uniqdata_TC120
test("PushUP_FILTER_uniqdata_TC120", Include) {
  checkAnswer(s"""select format_number(CUST_ID,1)from uniqdata where format_number(CUST_ID,1) ='10,987.0' or format_number(BIGINT_COLUMN1,2) = '123,372,038,841.00' or format_number(DECIMAL_COLUMN1,3)='12,345,680,888.123'""",
    s"""select format_number(CUST_ID,1)from uniqdata_hive where format_number(CUST_ID,1) ='10,987.0' or format_number(BIGINT_COLUMN1,2) = '123,372,038,841.00' or format_number(DECIMAL_COLUMN1,3)='12,345,680,888.123'""")
}
       

//PushUP_FILTER_uniqdata_TC121
test("PushUP_FILTER_uniqdata_TC121", Include) {
  checkAnswer(s"""select get_json_object(CUST_NAME,'a') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select get_json_object(CUST_NAME,'a') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC122
test("PushUP_FILTER_uniqdata_TC122", Include) {
  checkAnswer(s"""select get_json_object(CUST_NAME,'a')from uniqdata where get_json_object(CUST_NAME,'a') is null or get_json_object(CUST_NAME,'a') is not null""",
    s"""select get_json_object(CUST_NAME,'a')from uniqdata_hive where get_json_object(CUST_NAME,'a') is null or get_json_object(CUST_NAME,'a') is not null""")
}
       

//PushUP_FILTER_uniqdata_TC123
test("PushUP_FILTER_uniqdata_TC123", Include) {
  checkAnswer(s"""select instr(CUST_NAME,'a'),instr(DOB,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select instr(CUST_NAME,'a'),instr(DOB,'b') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC124
test("PushUP_FILTER_uniqdata_TC124", Include) {
  checkAnswer(s"""select instr(CUST_NAME,'a')from uniqdata where instr(CUST_NAME,'a') =0 or instr(DOB,'b') is null""",
    s"""select instr(CUST_NAME,'a')from uniqdata_hive where instr(CUST_NAME,'a') =0 or instr(DOB,'b') is null""")
}
       

//PushUP_FILTER_uniqdata_TC125
test("PushUP_FILTER_uniqdata_TC125", Include) {
  checkAnswer(s"""select locate(CUST_NAME,'a',1),locate(DOB,'b',2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select locate(CUST_NAME,'a',1),locate(DOB,'b',2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC126
test("PushUP_FILTER_uniqdata_TC126", Include) {
  checkAnswer(s"""select locate(CUST_NAME,'a',1)from uniqdata where locate(CUST_NAME,'a',1) =1 or locate(DOB,'b',2) is null""",
    s"""select locate(CUST_NAME,'a',1)from uniqdata_hive where locate(CUST_NAME,'a',1) =1 or locate(DOB,'b',2) is null""")
}
       

//PushUP_FILTER_uniqdata_TC127
test("PushUP_FILTER_uniqdata_TC127", Include) {
  checkAnswer(s"""select lpad(CUST_NAME,1,'a'),lpad(DOB,2,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select lpad(CUST_NAME,1,'a'),lpad(DOB,2,'b') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC128
test("PushUP_FILTER_uniqdata_TC128", Include) {
  checkAnswer(s"""select lpad(CUST_NAME,1,'a')from uniqdata where lpad(CUST_NAME,1,'a') =1 or lpad(DOB,2,'b') is null""",
    s"""select lpad(CUST_NAME,1,'a')from uniqdata_hive where lpad(CUST_NAME,1,'a') =1 or lpad(DOB,2,'b') is null""")
}
       

//PushUP_FILTER_uniqdata_TC129
test("PushUP_FILTER_uniqdata_TC129", Include) {
  checkAnswer(s"""select ltrim(CUST_NAME),ltrim(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select ltrim(CUST_NAME),ltrim(DOB) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC130
test("PushUP_FILTER_uniqdata_TC130", Include) {
  checkAnswer(s"""select ltrim(CUST_NAME)from uniqdata where ltrim(CUST_NAME) =1 or ltrim(DOB) is null""",
    s"""select ltrim(CUST_NAME)from uniqdata_hive where ltrim(CUST_NAME) =1 or ltrim(DOB) is null""")
}
       

//PushUP_FILTER_uniqdata_TC133
test("PushUP_FILTER_uniqdata_TC133", Include) {
  checkAnswer(s"""select printf(CUST_NAME,'a'),printf(DOB,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select printf(CUST_NAME,'a'),printf(DOB,'b') from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC134
test("PushUP_FILTER_uniqdata_TC134", Include) {
  sql(s"""select printf(CUST_NAME,'a')from uniqdata where printf(CUST_NAME,'b') ='CUST_NAME_01987' or printf(DOB,'b')= '1975-06-11 01:00:03' or printf(DOB,'b') is null""").collect
}
       

//PushUP_FILTER_uniqdata_TC135
test("PushUP_FILTER_uniqdata_TC135", Include) {
  checkAnswer(s"""select regexp_extract(CUST_NAME,'a',1),regexp_extract(DOB,'b',2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select regexp_extract(CUST_NAME,'a',1),regexp_extract(DOB,'b',2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC136
test("PushUP_FILTER_uniqdata_TC136", Include) {
  checkAnswer(s"""select regexp_extract(CUST_NAME,'a',1)from uniqdata where regexp_extract(CUST_NAME,'a',1) IS NULL or regexp_extract(DOB,'b',2) is NULL""",
    s"""select regexp_extract(CUST_NAME,'a',1)from uniqdata_hive where regexp_extract(CUST_NAME,'a',1) IS NULL or regexp_extract(DOB,'b',2) is NULL""")
}
       

//PushUP_FILTER_uniqdata_TC137
test("PushUP_FILTER_uniqdata_TC137", Include) {
  checkAnswer(s"""select repeat(CUST_NAME,1),repeat(DOB,2) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select repeat(CUST_NAME,1),repeat(DOB,2) from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC138
test("PushUP_FILTER_uniqdata_TC138", Include) {
  sql(s"""select repeat(CUST_NAME,1)from uniqdata where repeat(CUST_NAME,1) ='CUST_NAME_01987' or repeat(DOB,2) ='1975-06-11 01:00:031975-06-11 01:00:03'""").collect
}
       

//PushUP_FILTER_uniqdata_TC139
test("PushUP_FILTER_uniqdata_TC139", Include) {
  sql(s"""select reverse(CUST_NAME),reverse(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC140
test("PushUP_FILTER_uniqdata_TC140", Include) {
  sql(s"""select reverse(CUST_NAME)from uniqdata where reverse(CUST_NAME) ='78910_EMAN_TSUC' or reverse(DOB) ='30:00:10 11-60-5791'""").collect
}
       

//PushUP_FILTER_uniqdata_TC141
test("PushUP_FILTER_uniqdata_TC141", Include) {
  sql(s"""select rpad(CUST_NAME,1,'a'),rpad(DOB,2,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC142
test("PushUP_FILTER_uniqdata_TC142", Include) {
  sql(s"""select rpad(CUST_NAME,1,'a')from uniqdata where rpad(CUST_NAME,1,'a') ='a' or rpad(DOB,2,'b') =19""").collect
}
       

//PushUP_FILTER_uniqdata_TC143
test("PushUP_FILTER_uniqdata_TC143", Include) {
  sql(s"""select rtrim(CUST_NAME),rtrim(DOB) from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC144
test("PushUP_FILTER_uniqdata_TC144", Include) {
  sql(s"""select rtrim(CUST_NAME)from uniqdata where rtrim(CUST_NAME) ='CUST_NAME_01987' or rtrim(DOB) ='1975-06-11 01:00:03'""").collect
}
       

//PushUP_FILTER_uniqdata_TC145
test("PushUP_FILTER_uniqdata_TC145", Include) {
  sql(s"""select sentences(CUST_NAME,'a','b'),sentences(DOB,'a','b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC146
test("PushUP_FILTER_uniqdata_TC146", Include) {
  sql(s"""select sentences(CUST_NAME,'a','b')from uniqdata where sentences(CUST_NAME,'a','b') IS NULL or sentences(DOB,'a','b') is not null""").collect
}
       

//PushUP_FILTER_uniqdata_TC147
test("PushUP_FILTER_uniqdata_TC147", Include) {
  sql(s"""select split(CUST_NAME,'a'),split(DOB,'b') from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC148
test("PushUP_FILTER_uniqdata_TC148", Include) {
  sql(s"""select split(CUST_NAME,'a')from uniqdata where split(CUST_NAME,'a') IS NULL or split(DOB,'b') is not null""").collect
}
       

//PushUP_FILTER_uniqdata_TC149
test("PushUP_FILTER_uniqdata_TC149", Include) {
  sql(s"""select str_to_map(CUST_NAME)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC150
test("PushUP_FILTER_uniqdata_TC150", Include) {
  sql(s"""select substring_index(CUST_NAME,'a',1),substring_index(DOB,'1975',2)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC151
test("PushUP_FILTER_uniqdata_TC151", Include) {
  sql(s"""select substring_index(CUST_NAME,'a',1)from uniqdata where substring_index(CUST_NAME,'a',1) ='CUST_NAME_01987' or  substring_index(DOB,'1975',2)='1975-06-11 01:00:03'""").collect
}
       

//PushUP_FILTER_uniqdata_TC152
test("PushUP_FILTER_uniqdata_TC152", Include) {
  sql(s"""select translate('test','t','s'),translate(CUST_NAME,'a','b')from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC153
test("PushUP_FILTER_uniqdata_TC153", Include) {
  sql(s"""select translate('test','t','s')from uniqdata where translate(CUST_NAME,'a','b') ='CUST_NAME_01987' or translate('test','t','s')='sess'""").collect
}
       

//PushUP_FILTER_uniqdata_TC154
test("PushUP_FILTER_uniqdata_TC154", Include) {
  sql(s"""select trim(CUST_NAME),trim(DOB)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC155
test("PushUP_FILTER_uniqdata_TC155", Include) {
  sql(s"""select trim(CUST_NAME)from uniqdata where trim(CUST_NAME) ='CUST_NAME_01988' or trim(DOB) is null""").collect
}
       

//PushUP_FILTER_uniqdata_TC156
test("PushUP_FILTER_uniqdata_TC156", Include) {
  sql(s"""select unbase64(CUST_NAME),unbase64(DOB)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushUP_FILTER_uniqdata_TC157
test("PushUP_FILTER_uniqdata_TC157", Include) {
  sql(s"""select unbase64(CUST_NAME)from uniqdata where unbase64(CUST_NAME) is not null or unbase64(DOB) is null""").collect
}
       

//PushUP_FILTER_uniqdata_TC158
test("PushUP_FILTER_uniqdata_TC158", Include) {
  checkAnswer(s"""select ucase(CUST_NAME),ucase(DOB)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select ucase(CUST_NAME),ucase(DOB)from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC159
test("PushUP_FILTER_uniqdata_TC159", Include) {
  checkAnswer(s"""select ucase(CUST_NAME)from uniqdata where ucase(CUST_NAME) ='CUST_NAME_01987' or ucase(DOB) is null""",
    s"""select ucase(CUST_NAME)from uniqdata_hive where ucase(CUST_NAME) ='CUST_NAME_01987' or ucase(DOB) is null""")
}
       

//PushUP_FILTER_uniqdata_TC160
test("PushUP_FILTER_uniqdata_TC160", Include) {
  checkAnswer(s"""select initcap(CUST_NAME),initcap(DOB)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select initcap(CUST_NAME),initcap(DOB)from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC161
test("PushUP_FILTER_uniqdata_TC161", Include) {
  checkAnswer(s"""select initcap(CUST_NAME)from uniqdata where initcap(CUST_NAME) ='CUST_NAME_01987' or initcap(DOB) is null""",
    s"""select initcap(CUST_NAME)from uniqdata_hive where initcap(CUST_NAME) ='CUST_NAME_01987' or initcap(DOB) is null""")
}
       

//PushUP_FILTER_uniqdata_TC162
test("PushUP_FILTER_uniqdata_TC162", Include) {
  checkAnswer(s"""select levenshtein(CUST_NAME,'a'),levenshtein(DOB,'b')from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select levenshtein(CUST_NAME,'a'),levenshtein(DOB,'b')from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC163
test("PushUP_FILTER_uniqdata_TC163", Include) {
  checkAnswer(s"""select levenshtein(CUST_NAME,'a')from uniqdata where levenshtein(CUST_NAME,'a') =1 or levenshtein(DOB,'b') is null""",
    s"""select levenshtein(CUST_NAME,'a')from uniqdata_hive where levenshtein(CUST_NAME,'a') =1 or levenshtein(DOB,'b') is null""")
}
       

//PushUP_FILTER_uniqdata_TC164
test("PushUP_FILTER_uniqdata_TC164", Include) {
  checkAnswer(s"""select soundex(CUST_NAME)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select soundex(CUST_NAME)from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC165
test("PushUP_FILTER_uniqdata_TC165", Include) {
  checkAnswer(s"""select soundex(CUST_NAME)from uniqdata where soundex(CUST_NAME) is null or soundex(CUST_NAME) is not null""",
    s"""select soundex(CUST_NAME)from uniqdata_hive where soundex(CUST_NAME) is null or soundex(CUST_NAME) is not null""")
}
       

//PushUP_FILTER_uniqdata_TC167
test("PushUP_FILTER_uniqdata_TC167", Include) {
  checkAnswer(s"""select first_value(CUST_ID),first_value(CUST_NAME),first_value(DOB),first_value(BIGINT_COLUMN1),first_value(DECIMAL_COLUMN1),first_value(Double_COLUMN2)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """,
    s"""select first_value(CUST_ID),first_value(CUST_NAME),first_value(DOB),first_value(BIGINT_COLUMN1),first_value(DECIMAL_COLUMN1),first_value(Double_COLUMN2)from uniqdata_hive where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """)
}
       

//PushUP_FILTER_uniqdata_TC168
test("PushUP_FILTER_uniqdata_TC168", Include) {
  sql(s"""select last_value(CUST_ID),last_value(CUST_NAME),last_value(DOB),last_value(BIGINT_COLUMN1),last_value(DECIMAL_COLUMN1),last_value(Double_COLUMN2)from uniqdata where CUST_ID IS NULL or DOB IS NOT NULL or BIGINT_COLUMN1 =1233720368578 or DECIMAL_COLUMN1 = 12345678901.1234000058 or Double_COLUMN1 = 1.12345674897976E10 or INTEGER_COLUMN1 IS NULL  """).collect
}
       

//PushDown_INSERT_uniqdata_TC001
test("PushDown_INSERT_uniqdata_TC001", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  and  BIGINT_COLUMN1 in (123372037874,123372037884,123372037886,123372037889,'',NULL,' ')
""").collect
}
       

//PushDown_INSERT_uniqdata_TC002
test("PushDown_INSERT_uniqdata_TC002", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  and  BIGINT_COLUMN1 not in (123372037874,123372037884,123372037886,123372037889,'',NULL,' ')""",
    s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata_hive where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  and  BIGINT_COLUMN1 not in (123372037874,123372037884,123372037886,123372037889,'',NULL,' ')""")
}
       

//PushDown_INSERT_uniqdata_TC003
test("PushDown_INSERT_uniqdata_TC003", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  or  BIGINT_COLUMN1 in (123372037874,123372037884,123372037886,123372037889,'',NULL,' ')""").collect
}
       

//PushDown_INSERT_uniqdata_TC004
test("PushDown_INSERT_uniqdata_TC004", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  or  BIGINT_COLUMN1 not in (123372037874,123372037884,123372037886,123372037889,'',NULL,' ')""").collect
}
       

//PushDown_INSERT_uniqdata_TC008
test("PushDown_INSERT_uniqdata_TC008", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,INTEGER_COLUMN1 from uniqdata where CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  and  INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ') or Double_COLUMN1 in ('1.12345674897976E10','',NULL,' ')""").collect
}
       

//PushDown_INSERT_uniqdata_TC009
test("PushDown_INSERT_uniqdata_TC009", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2 from uniqdata where (CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  and  INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ')) or (Double_COLUMN1 in ('1.12345674897976E10','',NULL,' ') and  DECIMAL_COLUMN2 in ('22345679921.1234000000','',NULL,' '))""").collect
}
       

//PushDown_INSERT_uniqdata_TC014
test("PushDown_INSERT_uniqdata_TC014", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2 from uniqdata where  CUST_NAME in ((lower('CUST_NAME_01020')),(upper('cust_name_01040')),(upper('')),(upper('NULL')),(upper(' '))) and  ACTIVE_EMUI_VERSION in ((upper('ACTIVE_EMUI_VERSION_01026')),(upper('active_emui_version_01020')))""",
    s"""select CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2 from uniqdata_hive where  CUST_NAME in ((lower('CUST_NAME_01020')),(upper('cust_name_01040')),(upper('')),(upper('NULL')),(upper(' '))) and  ACTIVE_EMUI_VERSION in ((upper('ACTIVE_EMUI_VERSION_01026')),(upper('active_emui_version_01020')))""")
}
       

//PushDown_INSERT_uniqdata_TC017
test("PushDown_INSERT_uniqdata_TC017", Include) {
  sql(s"""SELECT CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2 from (select * from uniqdata) SUB_QRY WHERE (CUST_ID in ('10020','10030','10032','10035','10040','10060','',NULL,' ')  or  INTEGER_COLUMN1 in (1021,1031,1032,1033,'',NULL,' ')) and (Double_COLUMN1 in ('1.12345674897976E10','',NULL,' ') or  DECIMAL_COLUMN2 in ('22345679921.1234000000','',NULL,' '))""").collect
}
       

//UNIQDATA_CreateTable_2_Drop
test("UNIQDATA_CreateTable_2_Drop", Include) {
  sql(s"""drop table if exists  uniqdata""").collect

  sql(s"""drop table if exists  uniqdata_hive""").collect

}
       

//Union
test("Union", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""CREATE TABLE uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Union_01
test("Union_01", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Union_02
test("Union_02", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Union_03_Drop
test("Union_03_Drop", Include) {
  sql(s"""drop table if exists  uniqdata1""").collect

  sql(s"""drop table if exists  uniqdata1_hive""").collect

}
       

//Union_03
test("Union_03", Include) {
  sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""CREATE TABLE uniqdata1_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Union_04
test("Union_04", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata1_hive """).collect

}
       

//Union_05
test("Union_05", Include) {
  sql(s"""Select * from uniqdata union all select * from uniqdata1""").collect
}
       

//Standard Deviation
test("Standard Deviation", Include) {
  sql(s"""select stddev(Double_COLUMN1) as a  from UNIQDATA""").collect
}
       

//Standard Deviation_01
test("Standard Deviation_01", Include) {
  sql(s"""select cust_name, sum(integer_column1) OVER w from uniqdata WINDOW w AS (PARTITION BY double_column2)""").collect
}
       

//Window
test("Window", Include) {
  sql(s"""select cust_name, sum(integer_column1) OVER w from uniqdata WINDOW w AS (PARTITION BY double_column2 order by cust_name)""").collect
}
       

//Rollup
test("Rollup", Include) {
  sql(s"""select cust_name, sum(integer_column1) from uniqdata group by cust_name with rollup""").collect
}
       

//regexp with regular expression
test("regexp with regular expression", Include) {
  sql(s"""SELECT cust_name,cust_name REGEXP '^[a-d]' from uniqdata group by cust_name""").collect
}
       

//Compaction_Sequence_01
test("Compaction_Sequence_01", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_02
test("Compaction_Sequence_02", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_03
test("Compaction_Sequence_03", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_04
test("Compaction_Sequence_04", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_05
test("Compaction_Sequence_05", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_09
test("Compaction_Sequence_09", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_10
test("Compaction_Sequence_10", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_11
test("Compaction_Sequence_11", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_12
test("Compaction_Sequence_12", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_13
test("Compaction_Sequence_13", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_16
test("Compaction_Sequence_16", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_17
test("Compaction_Sequence_17", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_21
test("Compaction_Sequence_21", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_24
test("Compaction_Sequence_24", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_27
test("Compaction_Sequence_27", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_28
test("Compaction_Sequence_28", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_31
test("Compaction_Sequence_31", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_34
test("Compaction_Sequence_34", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_35
test("Compaction_Sequence_35", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_36
test("Compaction_Sequence_36", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_37
test("Compaction_Sequence_37", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Compaction_Sequence_38
test("Compaction_Sequence_38", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Drop_01
test("Drop_01", Include) {
  sql(s"""drop table uniqdata""").collect

  sql(s"""drop table uniqdata_hive""").collect

}


//Insert_01
test("Insert_01", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Insert_08
test("Insert_08", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table uniqdata options('DELIMITER'=',', 'FILEHEADER'='CUST_ID, CUST_NAME, ACTIVE_EMUI_VERSION, DOB, DOJ, BIGINT_COLUMN1, BIGINT_COLUMN2, DECIMAL_COLUMN1, DECIMAL_COLUMN2, Double_COLUMN1, Double_COLUMN2, INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/uniqdata/2000_UniqData.csv' INTO table uniqdata_hive """).collect

}
       

//Insert_48a
test("Insert_48a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_49a
test("Insert_49a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_50a
test("Insert_50a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_51a
test("Insert_51a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_52a
test("Insert_52a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_53a
test("Insert_53a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_54a
test("Insert_54a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_55a
test("Insert_55a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_56a
test("Insert_56a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_57a
test("Insert_57a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_58a
test("Insert_58a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Insert_61a
test("Insert_61a", Include) {
  sql(s"""Select * from  uniqdata""").collect
}
       

//Drop_75
test("Drop_75", Include) {
  sql(s"""drop table if exists uniqdata""").collect

  sql(s"""drop table if exists uniqdata_hive""").collect

}
       

//Insert_75
test("Insert_75", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Insert_76_b
test("Insert_76_b", Include) {
  sql(s"""Select * from uniqdata""").collect
}
       

//drop table_insert
test("drop table_insert", Include) {
  sql(s"""drop table if exists uniqdata""").collect

  sql(s"""drop table if exists uniqdata_hive""").collect

}
       

//drop_uniqdata1_alter
test("drop_uniqdata1_alter", Include) {
  sql(s"""drop table if exists uniqdata1""").collect

  sql(s"""drop table if exists uniqdata1_hive""").collect

}
       

//Create Table
test("Create Table", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Load 1
test("Load 1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Load 2
test("Load 2", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Load 3
test("Load 3", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Load 4
test("Load 4", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/5000_UniqData.csv' into table uniqdata_hive """).collect

}

  test("Alter") {
    sql(s"""ALTER TABLE uniqdata RENAME TO uniqdata1""").collect()
  }

  test("Desc Table1") {
    sql(s"""desc uniqdata1""").collect()
  }

  test("Alter1") {
    sql(s"""alter table uniqdata1 add columns(dict int) TBLPROPERTIES('DICTIONARY_INCLUDE'='dict','DEFAULT.VALUE.dict'= '9999')""").collect()
  }

//Select query1b
test("Select query1b", Include) {
  sql(s"""select distinct(dict) from uniqdata1 """).collect
}



  test("Alter2") {
    sql(s"""alter table uniqdata1 add columns(nodict string) TBLPROPERTIES('DICTIONARY_EXCLUDE'='nodict', 'DEFAULT.VALUE.NoDict'= 'abcd')""").collect()
  }

  //Select query2b
  test("Select query2b", Include) {
    sql(s"""select distinct(nodict) from uniqdata1 """).collect
  }

  test("Alter3a") {
    sql(s"""alter table uniqdata1 add columns(tmpstmp timestamp) TBLPROPERTIES('DEFAULT.VALUE.tmpstmp'= '2017-01-01 00:00:00')""").collect()
  }

  //Select query3a
  test("Select query3a", Include) {
    sql(s"""select distinct(tmpstmp) from uniqdata1""").collect
  }

  test("Alter3b") {
    sql(s"""alter table uniqdata1 add columns(date1 date) TBLPROPERTIES('DEFAULT.VALUE.tmpstmp'= '2017-01-01')""").collect()
  }

  test("Select query3b", Include) {
    sql(s"""select distinct(date1) from uniqdata1""").collect
  }

  test("Alter4") {
    sql(s"""alter table uniqdata1 add columns(msrField decimal(5,2))TBLPROPERTIES('DEFAULT.VALUE.msrfield'= '123.45')""").collect()
  }

//Select query4b
test("Select query4b", Include) {
  sql(s"""select msrField from uniqdata1""").collect
}

  test("Alter5") {
    sql(s"""alter table uniqdata1 add columns(strfld string, datefld date, tptfld timestamp, shortFld smallInt, intFld int, longFld bigint, dblFld double,dcml decimal(5,4)) TBLPROPERTIES('DICTIONARY_INCLUDE'='datefld,shortFld,intFld,longFld,dblFld,dcml', 'DEFAULT.VALUE.dblFld'= '12345')""").collect()
  }

  test("Select query5", Include) {
    sql(s"""select distinct(dblFld) from uniqdata1 """).collect
  }

  test("Desc1 alter", Include) {
    sql(s"""desc table uniqdata1 """).collect
  }

  test("Alter6a") {
    sql(s"""alter table uniqdata1 add columns(dcmlfld decimal(5,4))""").collect()
  }


  ignore("Alter6b") {
    sql(s"""alter table uniqdata1 add columns(dcmlfld string)""").collect()
  }

  test("Alter7a") {
    sql(s"""alter table uniqdata1 add columns(dimfld string)""").collect()
  }

  ignore("Alter7b") {
    sql(s"""alter table uniqdata1 add columns(dimfld decimal(5,4))""").collect()
  }

  test("Alter8a") {
    sql(s"""alter table uniqdata1 add columns(dimfld1 string, msrCol double)""").collect()
  }

  ignore("Alter8b") {
    sql(s"""alter table uniqdata1 add columns(dimfld1 int) TBLPROPERTIES('DICTIONARY_INCLUDE'='dimfld1')""").collect()
  }

  test("Alter8c") {
    sql(s"""alter table uniqdata1 add columns(msrCol decimal(5,3)) """).collect()
  }

  test("Alter9") {
    sql(s"""alter table uniqdata1 add columns(dimfld2 double) TBLPROPERTIES('DICTIONARY_EXCLUDE'='dimfld2')""").collect()
  }

  test("Alter10") {
    sql(s"""alter table uniqdata1 add columns(arr array<string>)""").collect()
  }

  test("Alter11a") {
    sql(s"""alter table uniqdata1 drop columns(CUST_NAME)""").collect()
  }

  test("Alter11b") {
    sql(s"""alter table uniqdata1 add columns(CUST_NAME int) TBLPROPERTIES('DICTIONARY_INCLUDE'='CUST_NAME', 'DEFAULT.VALUE.CUST_NAME'='12345')""").collect()
  }

//Alter11c
test("Alter11c", Include) {
  sql(s"""select distinct(CUST_NAME) from uniqdata1 """).collect
}
       

//Alter11d
test("Alter11d", Include) {
  sql(s"""select count(CUST_NAME) from uniqdata1 """).collect
}

  test("Alter12a") {
    sql(s"""alter table uniqdata1 drop columns(CUST_NAME)""").collect()
  }

  test("Alter12b", Include) {
    sql(s"""select distinct(CUST_NAME) from uniqdata1 """).collect
  }

  test("Alter12c") {
    sql(s"""alter table uniqdata1 add columns(CUST_NAME string) TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME', 'DEFAULT.VALUE.CUST_NAME'='testuser') """).collect()
  }

//Alter12d
test("Alter12d", Include) {
  sql(s"""select distinct(CUST_NAME) from uniqdata1 """).collect
}
       

//Alter12e
test("Alter12e", Include) {
  sql(s"""select count(CUST_NAME) from uniqdata1 """).collect
}

  test("Alter13") {
    sql(s"""alter table uniqdata1 add columns(newField string, newField int) """).collect()
  }

  test("Alter14") {
    sql(s"""alter table uniqdata1 drop columns(CUST_NAME, CUST_NAME)""").collect()
  }

  test("Alter15") {
    sql(s"""alter table uniqdata1 drop columns(abcd)""").collect()
  }

  test("Alter16a") {
    sql(s"""alter table default.uniqdata1 drop columns(CUST_ID ,ACTIVE_EMUI_VERSION, DOB)""").collect()
  }

  test("Alter16b") {
    sql(s"""desc uniqdata1 """).collect()
  }
       

//Alter16c
test("Alter16c", Include) {
  sql(s"""select * from uniqdata1 """).collect
}

  test("Alter16d", Include) {
    sql(s"""alter table uniqdata1 add columns(CUST_ID int, ACTIVE_EMUI_VERSION string, DOB timestamp)""").collect
  }

  test("Alter17a", Include) {
    sql(s"""alter table uniqdata1 drop columns(ACTIVE_EMUI_VERSION)""").collect
  }

  test("Alter17b", Include) {
    sql(s"""alter table default.uniqdata1 add columns(ACTIVE_EMUI_VERSION int) TBLPROPERTIES('DICTIONARY_INCLUDE'='ACTIVE_EMUI_VERSION','DEFAULT.VALUE.ACTIVE_EMUI_VERSION'='12345')""").collect
  }

//Alter17c
test("Alter17c", Include) {
  sql(s"""select distinct(ACTIVE_EMUI_VERSION) from uniqdata1""").collect
}

  test("Alter17d", Include) {
    sql(s"""alter table uniqdata1 drop columns(ACTIVE_EMUI_VERSION)""").collect
  }

  test("Alter17e", Include) {
    sql(s"""alter table uniqdata1 add columns(ACTIVE_EMUI_VERSION string) TBLPROPERTIES('DICTIONARY_EXCLUDE'='ACTIVE_EMUI_VERSION', 'DEFAULT.VALUE.(ACTIVE_EMUI_VERSION'='abcd')""").collect
  }
       

//Alter17f
test("Alter17f", Include) {
  sql(s"""select distinct(ACTIVE_EMUI_VERSION) from uniqdata1 """).collect
}

  test("Alter17g", Include) {
    sql(s"""alter table uniqdata1 drop columns(ACTIVE_EMUI_VERSION) """).collect
  }

  test("Alter17h", Include) {
    sql(s"""alter table uniqdata1 add columns(ACTIVE_EMUI_VERSION timestamp) TBLPROPERTIES ('DEFAULT.VALUE.ACTIVE_EMUI_VERSION'= '2017-01-01 00:00:00')""").collect
  }


       

//Alter17i
test("Alter17i", Include) {
  sql(s"""select distinct(ACTIVE_EMUI_VERSION) from uniqdata1 """).collect
}

  test("Alter17j", Include) {
    sql(s"""alter table uniqdata1 drop columns(ACTIVE_EMUI_VERSION)""").collect
  }

  test("Alter17k", Include) {
    sql(s"""alter table default.uniqdata1 add columns(ACTIVE_EMUI_VERSION int) TBLPROPERTIES('DEFAULT.VALUE.ACTIVE_EMUI_VERSION'='67890')""").collect
  }
       

//Alter17l
test("Alter17l", Include) {
  sql(s"""select distinct(ACTIVE_EMUI_VERSION) from uniqdata1 """).collect
}

  test("Alter18a", Include) {
    sql(s"""alter table uniqdata1 add columns(intfield int, decimalfield decimal(10,2))""").collect
  }

  test("Alter18b", Include) {
    sql(s"""alter table default.uniqdata1 change intfield intField bigint""").collect
  }

  test("Alter18c", Include) {
    sql(s"""desc table uniqdata1 """).collect
  }

  test("Alter18d", Include) {
    sql(s"""alter table default.uniqdata1 change decimalfield deciMalfield Decimal(11,3)""").collect
  }

  test("Alter19", Include) {
    sql(s"""alter table uniqdata1 change CUST_NAME CUST_NAME bigint""").collect
  }

  test("Alter20", Include) {
    sql(s"""alter table uniqdata1 change CUST_ID CUST_ID string """).collect
  }

  test("Alter21", Include) {
    sql(s"""alter table uniqdata1 change abcd abcd string """).collect
  }

  test("Alter22a", Include) {
    sql(s"""alter table uniqdata1 add columns(decField decimal(10,2))""").collect
  }

  test("Alter22b", Include) {
    sql(s"""desc uniqdata1 """).collect
  }

  test("Alter22b1", Include) {
    sql(s"""alter table uniqdata1 change decField decField decimal(10,1)""").collect
  }

  test("Alter22c", Include) {
    sql(s"""alter table uniqdata1 change decField decField decimal(5,3)""").collect
  }
       

//Drop-1
test("Drop-1", Include) {
  sql(s"""DROP TABLE IF EXISTS uniqdata1""").collect

  sql(s"""DROP TABLE IF EXISTS uniqdata1_hive""").collect

}
       

//Range_Filter_00
test("Range_Filter_00", Include) {
  sql(s"""CREATE TABLE uniqdata1 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata1_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Range_Filter_01
test("Range_Filter_01", Include) {
  sql(s"""CREATE TABLE uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ("TABLE_BLOCKSIZE"= "256 MB")""").collect

  sql(s"""CREATE TABLE uniqdata_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Range_Filter_02
test("Range_Filter_02", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_hive """).collect

}
       

//Range_Filter_002
test("Range_Filter_002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata1_hive """).collect

}
       

//Range_Filter_03
test("Range_Filter_03", Include) {
  checkAnswer(s"""select cust_id from uniqdata where cust_id >10987 and cust_id <10999""",
    s"""select cust_id from uniqdata_hive where cust_id >10987 and cust_id <10999""")
}
       

//Range_Filter_04
test("Range_Filter_04", Include) {
  checkAnswer(s"""select Cust_id from uniqdata where cust_id >= 10997 and cust_id < 10999""",
    s"""select Cust_id from uniqdata_hive where cust_id >= 10997 and cust_id < 10999""")
}
       

//Range_Filter_05
test("Range_Filter_05", Include) {
  checkAnswer(s"""select Cust_id from uniqdata where cust_id >10997 and cust_id <= 10999""",
    s"""select Cust_id from uniqdata_hive where cust_id >10997 and cust_id <= 10999""")
}
       

//Range_Filter_06
test("Range_Filter_06", Include) {
  checkAnswer(s"""select Cust_id from uniqdata where cust_id >=10997 and cust_id <= 10999""",
    s"""select Cust_id from uniqdata_hive where cust_id >=10997 and cust_id <= 10999""")
}
       

//Range_Filter_07
test("Range_Filter_07", Include) {
  checkAnswer(s"""select Cust_id from uniqdata where cust_id >=10997""",
    s"""select Cust_id from uniqdata_hive where cust_id >=10997""")
}
       

//Range_Filter_08
test("Range_Filter_08", Include) {
  checkAnswer(s"""select Cust_id from uniqdata where cust_id >=10997 and cust_id <= 10999""",
    s"""select Cust_id from uniqdata_hive where cust_id >=10997 and cust_id <= 10999""")
}
       

//Range_Filter_09
test("Range_Filter_09", Include) {
  sql(s"""select dob from uniqdata where dob>='1972-12-23 01:00:03.0' and dob <= '1972-12-31 01:00:03.0'""").collect
}
       

//Range_Filter_10
test("Range_Filter_10", Include) {
  sql(s"""select dob from uniqdata where dob>='1972-12-23 01:00:03.0' and dob <= '1972-12-31 01:00:03.0'""").collect
}
       

//Range_Filter_11
test("Range_Filter_11", Include) {
  sql(s"""select dob from uniqdata where dob > '1972-12-23 01:00:03.0' and dob < '1972-12-31 01:00:03.0'""").collect
}
       

//Range_Filter_12
test("Range_Filter_12", Include) {
  sql(s"""select dob from uniqdata where dob >= '1973-12-25 01:00:03.0' and dob < '1974-01-01 01:00:03.0'""").collect
}
       

//Range_Filter_13
test("Range_Filter_13", Include) {
  sql(s"""select dob from uniqdata where dob > '1974-12-25 01:00:03.0' and dob < '1975-01-01 01:00:03.0'""").collect
}
       

//Range_Filter_14
test("Range_Filter_14", Include) {
  checkAnswer(s"""select dob from uniqdata where dob > '1975-06-03 01:00:03.0' and dob <= '1975-06-11 01:00:03.0'""",
    s"""select dob from uniqdata_hive where dob > '1975-06-03 01:00:03.0' and dob <= '1975-06-11 01:00:03.0'""")
}
       

//Range_Filter_15
test("Range_Filter_15", Include) {
  checkAnswer(s"""select dob from uniqdata where dob > '1975-06-11 01:00:03.0'""",
    s"""select dob from uniqdata_hive where dob > '1975-06-11 01:00:03.0'""")
}
       

//Range_Filter_16
test("Range_Filter_16", Include) {
  sql(s"""select dob from uniqdata where dob < '1970-01-11 01:00:03.0'""").collect
}
       

//Range_Filter_17
test("Range_Filter_17", Include) {
  sql(s"""select dob from uniqdata where dob >= '1975-06-11 01:00:03.0'""").collect
}
       

//Range_Filter_18
test("Range_Filter_18", Include) {
  sql(s"""select doj from uniqdata where doj < '1970-01-11 01:00:03.0'""").collect
}
       

//Range_Filter_19
test("Range_Filter_19", Include) {
  checkAnswer(s"""select doj from uniqdata where doj > '1974-12-25 01:00:03.0' and doj < '1975-01-01 01:00:03.0'""",
    s"""select doj from uniqdata_hive where doj > '1974-12-25 01:00:03.0' and doj < '1975-01-01 01:00:03.0'""")
}
       

//Range_Filter_20
test("Range_Filter_20", Include) {
  checkAnswer(s"""select doj from uniqdata where doj > '1972-12-23 01:00:03.0' and doj < '1972-12-31 01:00:03.0'""",
    s"""select doj from uniqdata_hive where doj > '1972-12-23 01:00:03.0' and doj < '1972-12-31 01:00:03.0'""")
}
       

//Range_Filter_21
test("Range_Filter_21", Include) {
  checkAnswer(s"""select doj from uniqdata where doj>='1972-12-23 01:00:03.0' and doj <= '1972-12-31 01:00:03.0'""",
    s"""select doj from uniqdata_hive where doj>='1972-12-23 01:00:03.0' and doj <= '1972-12-31 01:00:03.0'""")
}
       

//Range_Filter_22
test("Range_Filter_22", Include) {
  checkAnswer(s"""select doj from uniqdata where doj>='1972-12-23 01:00:03.0' and doj <= '1972-12-31 01:00:03.0'""",
    s"""select doj from uniqdata_hive where doj>='1972-12-23 01:00:03.0' and doj <= '1972-12-31 01:00:03.0'""")
}
       

//Range_Filter_23
test("Range_Filter_23", Include) {
  checkAnswer(s"""select bigint_column1 from uniqdata where bigint_column1 > 123372038841 and bigint_column1< 123372038853""",
    s"""select bigint_column1 from uniqdata_hive where bigint_column1 > 123372038841 and bigint_column1< 123372038853""")
}
       

//Range_Filter_24
test("Range_Filter_24", Include) {
  checkAnswer(s"""select bigint_column1 from uniqdata where bigint_column1 >= 123372038841 and bigint_column1< 123372038853""",
    s"""select bigint_column1 from uniqdata_hive where bigint_column1 >= 123372038841 and bigint_column1< 123372038853""")
}
       

//Range_Filter_25
test("Range_Filter_25", Include) {
  checkAnswer(s"""select bigint_column1 from uniqdata where bigint_column1 > 123372038841 and bigint_column1<= 123372038853""",
    s"""select bigint_column1 from uniqdata_hive where bigint_column1 > 123372038841 and bigint_column1<= 123372038853""")
}
       

//Range_Filter_26
test("Range_Filter_26", Include) {
  checkAnswer(s"""select bigint_column1 from uniqdata where bigint_column1 >= 123372038841 and bigint_column1<= 123372038853""",
    s"""select bigint_column1 from uniqdata_hive where bigint_column1 >= 123372038841 and bigint_column1<= 123372038853""")
}
       

//Range_Filter_27
test("Range_Filter_27", Include) {
  checkAnswer(s"""select bigint_column2 from uniqdata where bigint_column2 > -223372034867 and bigint_column2 <-223372034857""",
    s"""select bigint_column2 from uniqdata_hive where bigint_column2 > -223372034867 and bigint_column2 <-223372034857""")
}
       

//Range_Filter_28
test("Range_Filter_28", Include) {
  checkAnswer(s"""select bigint_column2 from uniqdata where bigint_column2 >= -223372034867 and bigint_column2 <=-223372034857""",
    s"""select bigint_column2 from uniqdata_hive where bigint_column2 >= -223372034867 and bigint_column2 <=-223372034857""")
}
       

//Range_Filter_29
test("Range_Filter_29", Include) {
  checkAnswer(s"""select decimal_column1 from uniqdata where decimal_column1 >12345680888.1234000000 and decimal_column1 < 12345680899.1234000000""",
    s"""select decimal_column1 from uniqdata_hive where decimal_column1 >12345680888.1234000000 and decimal_column1 < 12345680899.1234000000""")
}
       

//Range_Filter_30
test("Range_Filter_30", Include) {
  checkAnswer(s"""select decimal_column1 from uniqdata where decimal_column1 >=12345680888.1234000000 and decimal_column1 <= 12345680899.1234000000""",
    s"""select decimal_column1 from uniqdata_hive where decimal_column1 >=12345680888.1234000000 and decimal_column1 <= 12345680899.1234000000""")
}
       

//Range_Filter_31
test("Range_Filter_31", Include) {
  checkAnswer(s"""select decimal_column2 from uniqdata where decimal_column2 >22345680888.1234000000 and decimal_column2 < 22345680897.1234000000""",
    s"""select decimal_column2 from uniqdata_hive where decimal_column2 >22345680888.1234000000 and decimal_column2 < 22345680897.1234000000""")
}
       

//Range_Filter_32
test("Range_Filter_32", Include) {
  checkAnswer(s"""select decimal_column2 from uniqdata where decimal_column2 >=22345680888.1234000000 and decimal_column2 <= 22345680897.1234000000""",
    s"""select decimal_column2 from uniqdata_hive where decimal_column2 >=22345680888.1234000000 and decimal_column2 <= 22345680897.1234000000""")
}
       

//Range_Filter_33
test("Range_Filter_33", Include) {
  checkAnswer(s"""select max(double_column1) from uniqdata """,
    s"""select max(double_column1) from uniqdata_hive """)
}
       

//Range_Filter_34
test("Range_Filter_34", Include) {
  checkAnswer(s"""select double_column1 from uniqdata where double_column1 >1.12345674897976E10""",
    s"""select double_column1 from uniqdata_hive where double_column1 >1.12345674897976E10""")
}
       

//Range_Filter_35
test("Range_Filter_35", Include) {
  checkAnswer(s"""select min(double_column1) from uniqdata """,
    s"""select min(double_column1) from uniqdata_hive """)
}
       

//Range_Filter_36
test("Range_Filter_36", Include) {
  checkAnswer(s"""select count(double_column1) from uniqdata """,
    s"""select count(double_column1) from uniqdata_hive """)
}
       

//Range_Filter_37
test("Range_Filter_37", Include) {
  checkAnswer(s"""select double_column1 from uniqdata where double_column1 >=1.12345674897976E10""",
    s"""select double_column1 from uniqdata_hive where double_column1 >=1.12345674897976E10""")
}
       

//Range_Filter_38
test("Range_Filter_38", Include) {
  checkAnswer(s"""select min(double_column2) from uniqdata """,
    s"""select min(double_column2) from uniqdata_hive """)
}
       

//Range_Filter_39
test("Range_Filter_39", Include) {
  checkAnswer(s"""select max(double_column2) from uniqdata """,
    s"""select max(double_column2) from uniqdata_hive """)
}
       

//Range_Filter_40
test("Range_Filter_40", Include) {
  checkAnswer(s"""select count(double_column2) from uniqdata """,
    s"""select count(double_column2) from uniqdata_hive """)
}
       

//Range_Filter_41
test("Range_Filter_41", Include) {
  checkAnswer(s"""select double_column2 from uniqdata where double_column2 <-1.12345674897976E10""",
    s"""select double_column2 from uniqdata_hive where double_column2 <-1.12345674897976E10""")
}
       

//Range_Filter_42
test("Range_Filter_42", Include) {
  checkAnswer(s"""select integer_column1 from uniqdata where integer_column1>=1985 and integer_column1 <= 1990""",
    s"""select integer_column1 from uniqdata_hive where integer_column1>=1985 and integer_column1 <= 1990""")
}
       

//Range_Filter_43
test("Range_Filter_43", Include) {
  checkAnswer(s"""select integer_column1 from uniqdata where integer_column1 > 1985 and integer_column1 <1990 """,
    s"""select integer_column1 from uniqdata_hive where integer_column1 > 1985 and integer_column1 <1990 """)
}
       

//Range_Filter_44
test("Range_Filter_44", Include) {
  checkAnswer(s"""select Cust_id from default.uniqdata where cust_id >=10997 and cust_id <= 10999""",
    s"""select Cust_id from default.uniqdata_hive where cust_id >=10997 and cust_id <= 10999""")
}
       

//Range_Filter_45
test("Range_Filter_45", Include) {
  sql(s"""select uniqdata.dob, uniqdata1.dob from uniqdata inner join uniqdata1 on (uniqdata.dob=uniqdata1.dob)""").collect
}
       

//Range_Filter_46
test("Range_Filter_46", Include) {
  sql(s"""select uniqdata.cust_id, uniqdata.dob, uniqdata1.dob from uniqdata left outer join uniqdata1 on (uniqdata.dob=uniqdata1.dob) order by cust_id""").collect
}
       

//Range_Filter_47
test("Range_Filter_47", Include) {
  sql(s"""select uniqdata.cust_id, uniqdata.dob, uniqdata1.dob from uniqdata right outer join uniqdata1 on (uniqdata.dob=uniqdata1.dob) order by cust_id""").collect
}
       

//Range_Filter_48
test("Range_Filter_48", Include) {
  sql(s"""select uniqdata.cust_id, uniqdata.dob, uniqdata1.dob from uniqdata full outer join uniqdata1 on (uniqdata.dob=uniqdata1.dob) order by cust_id""").collect
}
       
override def afterAll {
sql("drop table if exists uniqdata")
sql("drop table if exists UNIQDATA_hive")
sql("drop table if exists uniqdata1")
sql("drop table if exists uniqdata1_hive")
}
}
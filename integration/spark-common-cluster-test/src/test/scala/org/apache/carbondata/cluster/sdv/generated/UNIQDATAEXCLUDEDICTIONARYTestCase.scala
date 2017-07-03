
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
 * Test Class for uniqdataexcludedictionary to verify all scenerios
 */

class UNIQDATAEXCLUDEDICTIONARYTestCase extends QueryTest with BeforeAndAfterAll {
         

//UNIQDATA_EXCLUDEDICTIONARY_CreteTable_Drop
test("UNIQDATA_EXCLUDEDICTIONARY_CreteTable_Drop", Include) {
  sql(s"""drop table if exists  uniqdata_excludedictionary""").collect

  sql(s"""drop table if exists  uniqdata_excludedictionary_hive""").collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_CreteTable
test("UNIQDATA_EXCLUDEDICTIONARY_CreteTable", Include) {
  sql(s""" CREATE TABLE uniqdata_EXCLUDEDICTIONARY (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='CUST_NAME,ACTIVE_EMUI_VERSION')""").collect

  sql(s""" CREATE TABLE uniqdata_EXCLUDEDICTIONARY_hive (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_DataLoad1
test("UNIQDATA_EXCLUDEDICTIONARY_DataLoad1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/2000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY_hive """).collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_DataLoad3
test("UNIQDATA_EXCLUDEDICTIONARY_DataLoad3", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/4000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY_hive """).collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_DataLoad5
test("UNIQDATA_EXCLUDEDICTIONARY_DataLoad5", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/6000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY_hive """).collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_DataLoad6
test("UNIQDATA_EXCLUDEDICTIONARY_DataLoad6", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/7000_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY_hive """).collect

}
       

//UNIQDATA_EXCLUDEDICTIONARY_DataLoad7
test("UNIQDATA_EXCLUDEDICTIONARY_DataLoad7", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_1_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/uniqdata/3000_1_UniqData.csv' into table uniqdata_EXCLUDEDICTIONARY_hive """).collect

}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC001
test("UNIQDATA_EXCLUDE_DICTIONARY_TC001", Include) {
  sql(s"""Select count(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC002
test("UNIQDATA_EXCLUDE_DICTIONARY_TC002", Include) {
  sql(s"""select count(DISTINCT CUST_NAME) as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC003
test("UNIQDATA_EXCLUDE_DICTIONARY_TC003", Include) {
  sql(s"""select sum(INTEGER_COLUMN1)+10 as a ,CUST_NAME  from uniqdata_EXCLUDEDICTIONARY group by CUST_NAME order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC004
test("UNIQDATA_EXCLUDE_DICTIONARY_TC004", Include) {
  checkAnswer(s"""select max(CUST_NAME),min(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(CUST_NAME),min(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC005
test("UNIQDATA_EXCLUDE_DICTIONARY_TC005", Include) {
  sql(s"""select min(CUST_NAME), max(CUST_NAME) Total from uniqdata_EXCLUDEDICTIONARY group by  ACTIVE_EMUI_VERSION order by Total""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC006
test("UNIQDATA_EXCLUDE_DICTIONARY_TC006", Include) {
  checkAnswer(s"""select last(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY  group by CUST_NAME order by CUST_NAME limit 1""",
    s"""select last(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY_hive  group by CUST_NAME order by CUST_NAME limit 1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC007
test("UNIQDATA_EXCLUDE_DICTIONARY_TC007", Include) {
  checkAnswer(s"""select FIRST(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY group by CUST_NAME order by CUST_NAME limit 1""",
    s"""select FIRST(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY_hive group by CUST_NAME order by CUST_NAME limit 1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC008
test("UNIQDATA_EXCLUDE_DICTIONARY_TC008", Include) {
  sql(s"""select CUST_NAME,count(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY group by CUST_NAME order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC009
test("UNIQDATA_EXCLUDE_DICTIONARY_TC009", Include) {
  sql(s"""select Lower(CUST_NAME) a  from uniqdata_EXCLUDEDICTIONARY order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC010
test("UNIQDATA_EXCLUDE_DICTIONARY_TC010", Include) {
  sql(s"""select distinct CUST_NAME from uniqdata_EXCLUDEDICTIONARY order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC011
test("UNIQDATA_EXCLUDE_DICTIONARY_TC011", Include) {
  sql(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY order by CUST_NAME limit 101 """).collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC012
test("UNIQDATA_EXCLUDE_DICTIONARY_TC012", Include) {
  checkAnswer(s"""select CUST_NAME as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select CUST_NAME as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC013
test("UNIQDATA_EXCLUDE_DICTIONARY_TC013", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC014
test("UNIQDATA_EXCLUDE_DICTIONARY_TC014", Include) {
  sql(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME !='CUST_NAME_01844' order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC015
test("UNIQDATA_EXCLUDE_DICTIONARY_TC015", Include) {
  checkAnswer(s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC016
test("UNIQDATA_EXCLUDE_DICTIONARY_TC016", Include) {
  sql(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC017
test("UNIQDATA_EXCLUDE_DICTIONARY_TC017", Include) {
  sql(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME >'CUST_NAME_01840' order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC018
test("UNIQDATA_EXCLUDE_DICTIONARY_TC018", Include) {
  checkAnswer(s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY where CUST_NAME<>CUST_NAME""",
    s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME<>CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC019
test("UNIQDATA_EXCLUDE_DICTIONARY_TC019", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME != BIGINT_COLUMN2 order by CUST_NAME""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME != BIGINT_COLUMN2 order by CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC020
test("UNIQDATA_EXCLUDE_DICTIONARY_TC020", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN2<CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN2<CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC021
test("UNIQDATA_EXCLUDE_DICTIONARY_TC021", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1<=CUST_NAME order by CUST_NAME""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1<=CUST_NAME order by CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC022
test("UNIQDATA_EXCLUDE_DICTIONARY_TC022", Include) {
  sql(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME <'CUST_NAME_01844' order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC023
test("UNIQDATA_EXCLUDE_DICTIONARY_TC023", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where CUST_NAME IS NULL""",
    s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME IS NULL""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC024
test("UNIQDATA_EXCLUDE_DICTIONARY_TC024", Include) {
  sql(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where CUST_NAME IS NOT NULL order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC025
test("UNIQDATA_EXCLUDE_DICTIONARY_TC025", Include) {
  sql(s"""Select count(CUST_NAME),min(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY """).collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC026
test("UNIQDATA_EXCLUDE_DICTIONARY_TC026", Include) {
  sql(s"""select count(DISTINCT CUST_NAME,DECIMAL_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC027
test("UNIQDATA_EXCLUDE_DICTIONARY_TC027", Include) {
  sql(s"""select max(CUST_NAME),min(CUST_NAME),count(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC028
test("UNIQDATA_EXCLUDE_DICTIONARY_TC028", Include) {
  sql(s"""select sum(CUST_NAME),avg(CUST_NAME),count(CUST_NAME) a  from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC029
test("UNIQDATA_EXCLUDE_DICTIONARY_TC029", Include) {
  sql(s"""select last(CUST_NAME),Min(CUST_NAME),max(CUST_NAME)  a from uniqdata_EXCLUDEDICTIONARY  order by a""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC030
test("UNIQDATA_EXCLUDE_DICTIONARY_TC030", Include) {
  checkAnswer(s"""select FIRST(CUST_NAME),Last(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY group by CUST_NAME order by CUST_NAME limit 1""",
    s"""select FIRST(CUST_NAME),Last(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY_hive group by CUST_NAME order by CUST_NAME limit 1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC031
test("UNIQDATA_EXCLUDE_DICTIONARY_TC031", Include) {
  sql(s"""select CUST_NAME,count(CUST_NAME) a from uniqdata_EXCLUDEDICTIONARY group by CUST_NAME order by CUST_NAME""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC032
test("UNIQDATA_EXCLUDE_DICTIONARY_TC032", Include) {
  checkAnswer(s"""select Lower(CUST_NAME),upper(CUST_NAME)  a  from uniqdata_EXCLUDEDICTIONARY order by CUST_NAME""",
    s"""select Lower(CUST_NAME),upper(CUST_NAME)  a  from uniqdata_EXCLUDEDICTIONARY_hive order by CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC033
test("UNIQDATA_EXCLUDE_DICTIONARY_TC033", Include) {
  checkAnswer(s"""select CUST_NAME as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select CUST_NAME as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC034
test("UNIQDATA_EXCLUDE_DICTIONARY_TC034", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC035
test("UNIQDATA_EXCLUDE_DICTIONARY_TC035", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME !='CUST_NAME_01840' order by CUST_NAME""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC036
test("UNIQDATA_EXCLUDE_DICTIONARY_TC036", Include) {
  checkAnswer(s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_NAME  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC037
test("UNIQDATA_EXCLUDE_DICTIONARY_TC037", Include) {
  checkAnswer(s"""Select count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""Select count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC038
test("UNIQDATA_EXCLUDE_DICTIONARY_TC038", Include) {
  checkAnswer(s"""select count(DISTINCT BIGINT_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DISTINCT BIGINT_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC039
test("UNIQDATA_EXCLUDE_DICTIONARY_TC039", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)+10 as a ,BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY group by BIGINT_COLUMN1""",
    s"""select sum(BIGINT_COLUMN1)+10 as a ,BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive group by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC040
test("UNIQDATA_EXCLUDE_DICTIONARY_TC040", Include) {
  checkAnswer(s"""select max(BIGINT_COLUMN1),min(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(BIGINT_COLUMN1),min(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC041
test("UNIQDATA_EXCLUDE_DICTIONARY_TC041", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC042
test("UNIQDATA_EXCLUDE_DICTIONARY_TC042", Include) {
  checkAnswer(s"""select avg(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select avg(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC043
test("UNIQDATA_EXCLUDE_DICTIONARY_TC043", Include) {
  checkAnswer(s"""select min(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select min(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC044
test("UNIQDATA_EXCLUDE_DICTIONARY_TC044", Include) {
  sql(s"""select variance(BIGINT_COLUMN1) as a   from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC045
test("UNIQDATA_EXCLUDE_DICTIONARY_TC045", Include) {
  sql(s"""select var_pop(BIGINT_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC046
test("UNIQDATA_EXCLUDE_DICTIONARY_TC046", Include) {
  sql(s"""select var_samp(BIGINT_COLUMN1) as a  from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC047
test("UNIQDATA_EXCLUDE_DICTIONARY_TC047", Include) {
  sql(s"""select stddev_pop(BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC048
test("UNIQDATA_EXCLUDE_DICTIONARY_TC048", Include) {
  sql(s"""select stddev_samp(BIGINT_COLUMN1)  as a from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC049
test("UNIQDATA_EXCLUDE_DICTIONARY_TC049", Include) {
  sql(s"""select covar_pop(BIGINT_COLUMN1,BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC050
test("UNIQDATA_EXCLUDE_DICTIONARY_TC050", Include) {
  sql(s"""select covar_samp(BIGINT_COLUMN1,BIGINT_COLUMN1) as a  from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC051
test("UNIQDATA_EXCLUDE_DICTIONARY_TC051", Include) {
  checkAnswer(s"""select corr(BIGINT_COLUMN1,BIGINT_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select corr(BIGINT_COLUMN1,BIGINT_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC052
test("UNIQDATA_EXCLUDE_DICTIONARY_TC052", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,0.2) as a  from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC053
test("UNIQDATA_EXCLUDE_DICTIONARY_TC053", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,0.2,5) as a  from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC054
test("UNIQDATA_EXCLUDE_DICTIONARY_TC054", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,array(0.2,0.3,0.99))  as a from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC055
test("UNIQDATA_EXCLUDE_DICTIONARY_TC055", Include) {
  sql(s"""select percentile_approx(BIGINT_COLUMN1,array(0.2,0.3,0.99),5) as a from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC056
test("UNIQDATA_EXCLUDE_DICTIONARY_TC056", Include) {
  sql(s"""select histogram_numeric(BIGINT_COLUMN1,2)  as a from (select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC057
test("UNIQDATA_EXCLUDE_DICTIONARY_TC057", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select BIGINT_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC058
test("UNIQDATA_EXCLUDE_DICTIONARY_TC058", Include) {
  checkAnswer(s"""select min(BIGINT_COLUMN1), max(BIGINT_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(BIGINT_COLUMN1), max(BIGINT_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC059
test("UNIQDATA_EXCLUDE_DICTIONARY_TC059", Include) {
  sql(s"""select last(BIGINT_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY  order by a""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC060
test("UNIQDATA_EXCLUDE_DICTIONARY_TC060", Include) {
  checkAnswer(s"""select FIRST(BIGINT_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select FIRST(BIGINT_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC061
test("UNIQDATA_EXCLUDE_DICTIONARY_TC061", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1,count(BIGINT_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY group by BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1,count(BIGINT_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY_hive group by BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC062
test("UNIQDATA_EXCLUDE_DICTIONARY_TC062", Include) {
  checkAnswer(s"""select Lower(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1""",
    s"""select Lower(BIGINT_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC063
test("UNIQDATA_EXCLUDE_DICTIONARY_TC063", Include) {
  checkAnswer(s"""select distinct BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1""",
    s"""select distinct BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC064
test("UNIQDATA_EXCLUDE_DICTIONARY_TC064", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by BIGINT_COLUMN1 limit 101""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by BIGINT_COLUMN1 limit 101""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC065
test("UNIQDATA_EXCLUDE_DICTIONARY_TC065", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select BIGINT_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC066
test("UNIQDATA_EXCLUDE_DICTIONARY_TC066", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where  (BIGINT_COLUMN1 == 123372038698) and (CUST_NAME=='CUST_NAME_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC067
test("UNIQDATA_EXCLUDE_DICTIONARY_TC067", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC068
test("UNIQDATA_EXCLUDE_DICTIONARY_TC068", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840') order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840') order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC069
test("UNIQDATA_EXCLUDE_DICTIONARY_TC069", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 !=123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC070
test("UNIQDATA_EXCLUDE_DICTIONARY_TC070", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 >123372038698 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 >123372038698 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC071
test("UNIQDATA_EXCLUDE_DICTIONARY_TC071", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1<>BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1<>BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC072
test("UNIQDATA_EXCLUDE_DICTIONARY_TC072", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 != BIGINT_COLUMN2 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 != BIGINT_COLUMN2 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC073
test("UNIQDATA_EXCLUDE_DICTIONARY_TC073", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN2<BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN2<BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC074
test("UNIQDATA_EXCLUDE_DICTIONARY_TC074", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1<=BIGINT_COLUMN1 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1, BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1<=BIGINT_COLUMN1 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC075
test("UNIQDATA_EXCLUDE_DICTIONARY_TC075", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 <1000 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 <1000 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC076
test("UNIQDATA_EXCLUDE_DICTIONARY_TC076", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 >1000 order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 >1000 order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC077
test("UNIQDATA_EXCLUDE_DICTIONARY_TC077", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 IS NULL order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 IS NULL order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC078
test("UNIQDATA_EXCLUDE_DICTIONARY_TC078", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 IS NOT NULL order by BIGINT_COLUMN1""",
    s"""select BIGINT_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 IS NOT NULL order by BIGINT_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC079
test("UNIQDATA_EXCLUDE_DICTIONARY_TC079", Include) {
  checkAnswer(s"""Select count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""Select count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC080
test("UNIQDATA_EXCLUDE_DICTIONARY_TC080", Include) {
  checkAnswer(s"""select count(DISTINCT DECIMAL_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DISTINCT DECIMAL_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC081
test("UNIQDATA_EXCLUDE_DICTIONARY_TC081", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)+10 as a ,DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY group by DECIMAL_COLUMN1 order by a""",
    s"""select sum(DECIMAL_COLUMN1)+10 as a ,DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive group by DECIMAL_COLUMN1 order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC082
test("UNIQDATA_EXCLUDE_DICTIONARY_TC082", Include) {
  checkAnswer(s"""select max(DECIMAL_COLUMN1),min(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(DECIMAL_COLUMN1),min(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC083
test("UNIQDATA_EXCLUDE_DICTIONARY_TC083", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC084
test("UNIQDATA_EXCLUDE_DICTIONARY_TC084", Include) {
  checkAnswer(s"""select avg(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select avg(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC085
test("UNIQDATA_EXCLUDE_DICTIONARY_TC085", Include) {
  checkAnswer(s"""select min(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select min(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC086
test("UNIQDATA_EXCLUDE_DICTIONARY_TC086", Include) {
  sql(s"""select variance(DECIMAL_COLUMN1) as a   from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC087
test("UNIQDATA_EXCLUDE_DICTIONARY_TC087", Include) {
  sql(s"""select var_pop(DECIMAL_COLUMN1)  as a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC088
test("UNIQDATA_EXCLUDE_DICTIONARY_TC088", Include) {
  sql(s"""select var_samp(DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC089
test("UNIQDATA_EXCLUDE_DICTIONARY_TC089", Include) {
  sql(s"""select stddev_pop(DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC090
test("UNIQDATA_EXCLUDE_DICTIONARY_TC090", Include) {
  sql(s"""select stddev_samp(DECIMAL_COLUMN1)  as a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC091
test("UNIQDATA_EXCLUDE_DICTIONARY_TC091", Include) {
  sql(s"""select covar_pop(DECIMAL_COLUMN1,DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC092
test("UNIQDATA_EXCLUDE_DICTIONARY_TC092", Include) {
  sql(s"""select covar_samp(DECIMAL_COLUMN1,DECIMAL_COLUMN1) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC093
test("UNIQDATA_EXCLUDE_DICTIONARY_TC093", Include) {
  checkAnswer(s"""select corr(DECIMAL_COLUMN1,DECIMAL_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select corr(DECIMAL_COLUMN1,DECIMAL_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC094
test("UNIQDATA_EXCLUDE_DICTIONARY_TC094", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,0.2) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC095
test("UNIQDATA_EXCLUDE_DICTIONARY_TC095", Include) {
  checkAnswer(s"""select percentile_approx(DECIMAL_COLUMN1,0.2,5) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""",
    s"""select percentile_approx(DECIMAL_COLUMN1,0.2,5) as a  from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by DECIMAL_COLUMN1) t""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC096
test("UNIQDATA_EXCLUDE_DICTIONARY_TC096", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,array(0.2,0.3,0.99))  as a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC097
test("UNIQDATA_EXCLUDE_DICTIONARY_TC097", Include) {
  sql(s"""select percentile_approx(DECIMAL_COLUMN1,array(0.2,0.3,0.99),5) as a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC098
test("UNIQDATA_EXCLUDE_DICTIONARY_TC098", Include) {
  sql(s"""select histogram_numeric(DECIMAL_COLUMN1,2)  as a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC099
test("UNIQDATA_EXCLUDE_DICTIONARY_TC099", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC100
test("UNIQDATA_EXCLUDE_DICTIONARY_TC100", Include) {
  checkAnswer(s"""select min(DECIMAL_COLUMN1), max(DECIMAL_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(DECIMAL_COLUMN1), max(DECIMAL_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC101
test("UNIQDATA_EXCLUDE_DICTIONARY_TC101", Include) {
  sql(s"""select last(DECIMAL_COLUMN1) a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC102
test("UNIQDATA_EXCLUDE_DICTIONARY_TC102", Include) {
  sql(s"""select FIRST(DECIMAL_COLUMN1) a from (select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC103
test("UNIQDATA_EXCLUDE_DICTIONARY_TC103", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1,count(DECIMAL_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY group by DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1,count(DECIMAL_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY_hive group by DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC104
test("UNIQDATA_EXCLUDE_DICTIONARY_TC104", Include) {
  checkAnswer(s"""select Lower(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select Lower(DECIMAL_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC105
test("UNIQDATA_EXCLUDE_DICTIONARY_TC105", Include) {
  checkAnswer(s"""select distinct DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1""",
    s"""select distinct DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC106
test("UNIQDATA_EXCLUDE_DICTIONARY_TC106", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by DECIMAL_COLUMN1 limit 101""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by DECIMAL_COLUMN1 limit 101""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC107
test("UNIQDATA_EXCLUDE_DICTIONARY_TC107", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select DECIMAL_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC108
test("UNIQDATA_EXCLUDE_DICTIONARY_TC108", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where  (DECIMAL_COLUMN1 == 12345680745.1234000000)  and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where  (DECIMAL_COLUMN1 == 12345680745.1234000000)  and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC109
test("UNIQDATA_EXCLUDE_DICTIONARY_TC109", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC110
test("UNIQDATA_EXCLUDE_DICTIONARY_TC110", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC111
test("UNIQDATA_EXCLUDE_DICTIONARY_TC111", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 !=12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC112
test("UNIQDATA_EXCLUDE_DICTIONARY_TC112", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 >12345680745.1234000000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 >12345680745.1234000000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC113
test("UNIQDATA_EXCLUDE_DICTIONARY_TC113", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1<>DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1<>DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC114
test("UNIQDATA_EXCLUDE_DICTIONARY_TC114", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 != BIGINT_COLUMN2 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 != BIGINT_COLUMN2 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC115
test("UNIQDATA_EXCLUDE_DICTIONARY_TC115", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN2<DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN2<DECIMAL_COLUMN1 order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC116
test("UNIQDATA_EXCLUDE_DICTIONARY_TC116", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1<=DECIMAL_COLUMN1  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1, DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1<=DECIMAL_COLUMN1  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC117
test("UNIQDATA_EXCLUDE_DICTIONARY_TC117", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 <1000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 <1000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC118
test("UNIQDATA_EXCLUDE_DICTIONARY_TC118", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 >1000  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 >1000  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC119
test("UNIQDATA_EXCLUDE_DICTIONARY_TC119", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 IS NULL  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 IS NULL  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC120
test("UNIQDATA_EXCLUDE_DICTIONARY_TC120", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 IS NOT NULL  order by DECIMAL_COLUMN1""",
    s"""select DECIMAL_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 IS NOT NULL  order by DECIMAL_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC121
test("UNIQDATA_EXCLUDE_DICTIONARY_TC121", Include) {
  checkAnswer(s"""Select count(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""Select count(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC122
test("UNIQDATA_EXCLUDE_DICTIONARY_TC122", Include) {
  checkAnswer(s"""select count(DISTINCT Double_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DISTINCT Double_COLUMN1) as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC123
test("UNIQDATA_EXCLUDE_DICTIONARY_TC123", Include) {
  sql(s"""select sum(Double_COLUMN1)+10 as a ,Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY group by Double_COLUMN1 order by a""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC124
test("UNIQDATA_EXCLUDE_DICTIONARY_TC124", Include) {
  checkAnswer(s"""select max(Double_COLUMN1),min(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(Double_COLUMN1),min(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC125
test("UNIQDATA_EXCLUDE_DICTIONARY_TC125", Include) {
  sql(s"""select sum(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC126
test("UNIQDATA_EXCLUDE_DICTIONARY_TC126", Include) {
  sql(s"""select avg(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC127
test("UNIQDATA_EXCLUDE_DICTIONARY_TC127", Include) {
  checkAnswer(s"""select min(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select min(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC128
test("UNIQDATA_EXCLUDE_DICTIONARY_TC128", Include) {
  sql(s"""select variance(Double_COLUMN1) as a   from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC129
test("UNIQDATA_EXCLUDE_DICTIONARY_TC129", Include) {
  sql(s"""select var_pop(Double_COLUMN1)  as a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC130
test("UNIQDATA_EXCLUDE_DICTIONARY_TC130", Include) {
  sql(s"""select var_samp(Double_COLUMN1) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC131
test("UNIQDATA_EXCLUDE_DICTIONARY_TC131", Include) {
  sql(s"""select stddev_pop(Double_COLUMN1) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC132
test("UNIQDATA_EXCLUDE_DICTIONARY_TC132", Include) {
  sql(s"""select stddev_samp(Double_COLUMN1)  as a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC133
test("UNIQDATA_EXCLUDE_DICTIONARY_TC133", Include) {
  sql(s"""select covar_pop(Double_COLUMN1,Double_COLUMN1) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC134
test("UNIQDATA_EXCLUDE_DICTIONARY_TC134", Include) {
  sql(s"""select covar_samp(Double_COLUMN1,Double_COLUMN1) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC135
test("UNIQDATA_EXCLUDE_DICTIONARY_TC135", Include) {
  sql(s"""select corr(Double_COLUMN1,Double_COLUMN1)  as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC136
test("UNIQDATA_EXCLUDE_DICTIONARY_TC136", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,0.2) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC137
test("UNIQDATA_EXCLUDE_DICTIONARY_TC137", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,0.2,5) as a  from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC138
test("UNIQDATA_EXCLUDE_DICTIONARY_TC138", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,array(0.2,0.3,0.99))  as a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC139
test("UNIQDATA_EXCLUDE_DICTIONARY_TC139", Include) {
  sql(s"""select percentile_approx(Double_COLUMN1,array(0.2,0.3,0.99),5) as a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC140
test("UNIQDATA_EXCLUDE_DICTIONARY_TC140", Include) {
  sql(s"""select histogram_numeric(Double_COLUMN1,2)  as a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC141
test("UNIQDATA_EXCLUDE_DICTIONARY_TC141", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select Double_COLUMN1, Double_COLUMN1+ 10 as a  from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC142
test("UNIQDATA_EXCLUDE_DICTIONARY_TC142", Include) {
  checkAnswer(s"""select min(Double_COLUMN1), max(Double_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(Double_COLUMN1), max(Double_COLUMN1+ 10) Total from uniqdata_EXCLUDEDICTIONARY_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC143
test("UNIQDATA_EXCLUDE_DICTIONARY_TC143", Include) {
  sql(s"""select last(Double_COLUMN1) a from (select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC144
test("UNIQDATA_EXCLUDE_DICTIONARY_TC144", Include) {
  checkAnswer(s"""select FIRST(Double_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY order by a""",
    s"""select FIRST(Double_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY_hive order by a""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC145
test("UNIQDATA_EXCLUDE_DICTIONARY_TC145", Include) {
  checkAnswer(s"""select Double_COLUMN1,count(Double_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY group by Double_COLUMN1 order by Double_COLUMN1""",
    s"""select Double_COLUMN1,count(Double_COLUMN1) a from uniqdata_EXCLUDEDICTIONARY_hive group by Double_COLUMN1 order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC146
test("UNIQDATA_EXCLUDE_DICTIONARY_TC146", Include) {
  checkAnswer(s"""select Lower(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1""",
    s"""select Lower(Double_COLUMN1) a  from uniqdata_EXCLUDEDICTIONARY_hive order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC147
test("UNIQDATA_EXCLUDE_DICTIONARY_TC147", Include) {
  checkAnswer(s"""select distinct Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN1""",
    s"""select distinct Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC148
test("UNIQDATA_EXCLUDE_DICTIONARY_TC148", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY  order by Double_COLUMN1 limit 101""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive  order by Double_COLUMN1 limit 101""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC149
test("UNIQDATA_EXCLUDE_DICTIONARY_TC149", Include) {
  checkAnswer(s"""select Double_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select Double_COLUMN1 as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC150
test("UNIQDATA_EXCLUDE_DICTIONARY_TC150", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where  (Double_COLUMN1 == 12345680745.1234000000) and (CUST_NAME=='CUST_NAME_01844')""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where  (Double_COLUMN1 == 12345680745.1234000000) and (CUST_NAME=='CUST_NAME_01844')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC151
test("UNIQDATA_EXCLUDE_DICTIONARY_TC151", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 !=12345680745.1234000000  order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 !=12345680745.1234000000  order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC152
test("UNIQDATA_EXCLUDE_DICTIONARY_TC152", Include) {
  checkAnswer(s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC153
test("UNIQDATA_EXCLUDE_DICTIONARY_TC153", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 !=12345680745.1234000000""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 !=12345680745.1234000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC154
test("UNIQDATA_EXCLUDE_DICTIONARY_TC154", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 >12345680745.1234000000""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 >12345680745.1234000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC155
test("UNIQDATA_EXCLUDE_DICTIONARY_TC155", Include) {
  checkAnswer(s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1<>Double_COLUMN1""",
    s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1<>Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC156
test("UNIQDATA_EXCLUDE_DICTIONARY_TC156", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 != BIGINT_COLUMN2  order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 != BIGINT_COLUMN2  order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC157
test("UNIQDATA_EXCLUDE_DICTIONARY_TC157", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN2<Double_COLUMN1  order by Double_COLUMN1""",
    s"""select Double_COLUMN1, Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN2<Double_COLUMN1  order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC158
test("UNIQDATA_EXCLUDE_DICTIONARY_TC158", Include) {
  checkAnswer(s"""select Double_COLUMN1, Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1<=Double_COLUMN1  order by Double_COLUMN1""",
    s"""select Double_COLUMN1, Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1<=Double_COLUMN1  order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC159
test("UNIQDATA_EXCLUDE_DICTIONARY_TC159", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 <1000 order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 <1000 order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC160
test("UNIQDATA_EXCLUDE_DICTIONARY_TC160", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 >1000 order by Double_COLUMN1""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 >1000 order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC161
test("UNIQDATA_EXCLUDE_DICTIONARY_TC161", Include) {
  checkAnswer(s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 IS NULL order by Double_COLUMN1""",
    s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 IS NULL order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC162
test("UNIQDATA_EXCLUDE_DICTIONARY_TC162", Include) {
  checkAnswer(s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 IS NOT NULL order by Double_COLUMN1""",
    s"""select Double_COLUMN1  from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 IS NOT NULL order by Double_COLUMN1""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC163
test("UNIQDATA_EXCLUDE_DICTIONARY_TC163", Include) {
  checkAnswer(s"""Select count(DOB) from uniqdata_EXCLUDEDICTIONARY""",
    s"""Select count(DOB) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC164
test("UNIQDATA_EXCLUDE_DICTIONARY_TC164", Include) {
  checkAnswer(s"""select count(DISTINCT DOB) as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DISTINCT DOB) as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC165
test("UNIQDATA_EXCLUDE_DICTIONARY_TC165", Include) {
  checkAnswer(s"""select sum(DOB)+10 as a ,DOB  from uniqdata_EXCLUDEDICTIONARY group by DOB order by DOB""",
    s"""select sum(DOB)+10 as a ,DOB  from uniqdata_EXCLUDEDICTIONARY_hive group by DOB order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC166
test("UNIQDATA_EXCLUDE_DICTIONARY_TC166", Include) {
  checkAnswer(s"""select max(DOB),min(DOB) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(DOB),min(DOB) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC167
test("UNIQDATA_EXCLUDE_DICTIONARY_TC167", Include) {
  checkAnswer(s"""select sum(DOB) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC168
test("UNIQDATA_EXCLUDE_DICTIONARY_TC168", Include) {
  checkAnswer(s"""select avg(DOB) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select avg(DOB) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC169
test("UNIQDATA_EXCLUDE_DICTIONARY_TC169", Include) {
  checkAnswer(s"""select min(DOB) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select min(DOB) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC182
test("UNIQDATA_EXCLUDE_DICTIONARY_TC182", Include) {
  sql(s"""select histogram_numeric(DOB,2)  as a from (select DOB from uniqdata_EXCLUDEDICTIONARY order by DOB) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC183
test("UNIQDATA_EXCLUDE_DICTIONARY_TC183", Include) {
  sql(s"""select last(DOB) a from (select DOB from uniqdata_EXCLUDEDICTIONARY order by DOB) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC184
test("UNIQDATA_EXCLUDE_DICTIONARY_TC184", Include) {
  sql(s"""select FIRST(DOB) a from (select DOB from uniqdata_EXCLUDEDICTIONARY order by DOB) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC185
test("UNIQDATA_EXCLUDE_DICTIONARY_TC185", Include) {
  checkAnswer(s"""select DOB,count(DOB) a from uniqdata_EXCLUDEDICTIONARY group by DOB order by DOB""",
    s"""select DOB,count(DOB) a from uniqdata_EXCLUDEDICTIONARY_hive group by DOB order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC186
test("UNIQDATA_EXCLUDE_DICTIONARY_TC186", Include) {
  checkAnswer(s"""select Lower(DOB) a  from uniqdata_EXCLUDEDICTIONARY order by DOB""",
    s"""select Lower(DOB) a  from uniqdata_EXCLUDEDICTIONARY_hive order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC187
test("UNIQDATA_EXCLUDE_DICTIONARY_TC187", Include) {
  checkAnswer(s"""select distinct DOB from uniqdata_EXCLUDEDICTIONARY order by DOB""",
    s"""select distinct DOB from uniqdata_EXCLUDEDICTIONARY_hive order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC188
test("UNIQDATA_EXCLUDE_DICTIONARY_TC188", Include) {
  sql(s"""select DOB from uniqdata_EXCLUDEDICTIONARY order by DOB limit 101""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC189
test("UNIQDATA_EXCLUDE_DICTIONARY_TC189", Include) {
  sql(s"""select DOB as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC190
test("UNIQDATA_EXCLUDE_DICTIONARY_TC190", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where  (DOB == '1970-06-19 01:00:03') and (DOJ=='1970-06-19 02:00:03')""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where  (DOB == '1970-06-19 01:00:03') and (DOJ=='1970-06-19 02:00:03')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC191
test("UNIQDATA_EXCLUDE_DICTIONARY_TC191", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB !='1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB !='1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC192
test("UNIQDATA_EXCLUDE_DICTIONARY_TC192", Include) {
  checkAnswer(s"""select DOB  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select DOB  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC193
test("UNIQDATA_EXCLUDE_DICTIONARY_TC193", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB !='1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB !='1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC194
test("UNIQDATA_EXCLUDE_DICTIONARY_TC194", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB >'1970-06-19 01:00:03' order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB >'1970-06-19 01:00:03' order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC195
test("UNIQDATA_EXCLUDE_DICTIONARY_TC195", Include) {
  checkAnswer(s"""select DOB  from uniqdata_EXCLUDEDICTIONARY where DOB<>DOB order by DOB""",
    s"""select DOB  from uniqdata_EXCLUDEDICTIONARY_hive where DOB<>DOB order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC196
test("UNIQDATA_EXCLUDE_DICTIONARY_TC196", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB != DOJ order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB != DOJ order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC197
test("UNIQDATA_EXCLUDE_DICTIONARY_TC197", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOJ<DOB order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOJ<DOB order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC198
test("UNIQDATA_EXCLUDE_DICTIONARY_TC198", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB<=DOB order by DOB""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB<=DOB order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC199
test("UNIQDATA_EXCLUDE_DICTIONARY_TC199", Include) {
  sql(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB <'1970-06-19 01:00:03' order by DOB""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC200
test("UNIQDATA_EXCLUDE_DICTIONARY_TC200", Include) {
  checkAnswer(s"""select DOB  from uniqdata_EXCLUDEDICTIONARY where DOB IS NULL""",
    s"""select DOB  from uniqdata_EXCLUDEDICTIONARY_hive where DOB IS NULL""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC201
test("UNIQDATA_EXCLUDE_DICTIONARY_TC201", Include) {
  checkAnswer(s"""select DOB  from uniqdata_EXCLUDEDICTIONARY where DOB IS NOT NULL order by DOB""",
    s"""select DOB  from uniqdata_EXCLUDEDICTIONARY_hive where DOB IS NOT NULL order by DOB""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC202
test("UNIQDATA_EXCLUDE_DICTIONARY_TC202", Include) {
  checkAnswer(s"""Select count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY""",
    s"""Select count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC203
test("UNIQDATA_EXCLUDE_DICTIONARY_TC203", Include) {
  checkAnswer(s"""select count(DISTINCT CUST_ID) as a from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DISTINCT CUST_ID) as a from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC204
test("UNIQDATA_EXCLUDE_DICTIONARY_TC204", Include) {
  checkAnswer(s"""select sum(CUST_ID)+10 as a ,CUST_ID  from uniqdata_EXCLUDEDICTIONARY group by CUST_ID order by CUST_ID""",
    s"""select sum(CUST_ID)+10 as a ,CUST_ID  from uniqdata_EXCLUDEDICTIONARY_hive group by CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC205
test("UNIQDATA_EXCLUDE_DICTIONARY_TC205", Include) {
  checkAnswer(s"""select max(CUST_ID),min(CUST_ID) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select max(CUST_ID),min(CUST_ID) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC206
test("UNIQDATA_EXCLUDE_DICTIONARY_TC206", Include) {
  checkAnswer(s"""select sum(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC207
test("UNIQDATA_EXCLUDE_DICTIONARY_TC207", Include) {
  checkAnswer(s"""select avg(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select avg(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC208
test("UNIQDATA_EXCLUDE_DICTIONARY_TC208", Include) {
  checkAnswer(s"""select min(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select min(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC209
test("UNIQDATA_EXCLUDE_DICTIONARY_TC209", Include) {
  sql(s"""select variance(CUST_ID) as a   from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC210
test("UNIQDATA_EXCLUDE_DICTIONARY_TC210", Include) {
  sql(s"""select var_pop(CUST_ID)  as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC211
test("UNIQDATA_EXCLUDE_DICTIONARY_TC211", Include) {
  sql(s"""select var_samp(CUST_ID) as a  from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC212
test("UNIQDATA_EXCLUDE_DICTIONARY_TC212", Include) {
  sql(s"""select stddev_pop(CUST_ID) as a  from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC213
test("UNIQDATA_EXCLUDE_DICTIONARY_TC213", Include) {
  sql(s"""select stddev_samp(CUST_ID)  as a from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC214
test("UNIQDATA_EXCLUDE_DICTIONARY_TC214", Include) {
  sql(s"""select covar_pop(CUST_ID,CUST_ID) as a  from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC215
test("UNIQDATA_EXCLUDE_DICTIONARY_TC215", Include) {
  sql(s"""select covar_samp(CUST_ID,CUST_ID) as a  from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC216
test("UNIQDATA_EXCLUDE_DICTIONARY_TC216", Include) {
  sql(s"""select corr(CUST_ID,CUST_ID)  as a from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC217
test("UNIQDATA_EXCLUDE_DICTIONARY_TC217", Include) {
  sql(s"""select percentile_approx(CUST_ID,0.2) as a  from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC218
test("UNIQDATA_EXCLUDE_DICTIONARY_TC218", Include) {
  sql(s"""select percentile_approx(CUST_ID,0.2,5) as a  from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC219
test("UNIQDATA_EXCLUDE_DICTIONARY_TC219", Include) {
  sql(s"""select percentile_approx(CUST_ID,array(0.2,0.3,0.99))  as a from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC220
test("UNIQDATA_EXCLUDE_DICTIONARY_TC220", Include) {
  sql(s"""select percentile_approx(CUST_ID,array(0.2,0.3,0.99),5) as a from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC221
test("UNIQDATA_EXCLUDE_DICTIONARY_TC221", Include) {
  sql(s"""select histogram_numeric(CUST_ID,2)  as a from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC222
test("UNIQDATA_EXCLUDE_DICTIONARY_TC222", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID+ 10 as a  from uniqdata_EXCLUDEDICTIONARY order by CUST_ID""",
    s"""select CUST_ID, CUST_ID+ 10 as a  from uniqdata_EXCLUDEDICTIONARY_hive order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC223
test("UNIQDATA_EXCLUDE_DICTIONARY_TC223", Include) {
  checkAnswer(s"""select min(CUST_ID), max(CUST_ID+ 10) Total from uniqdata_EXCLUDEDICTIONARY group by  ACTIVE_EMUI_VERSION order by Total""",
    s"""select min(CUST_ID), max(CUST_ID+ 10) Total from uniqdata_EXCLUDEDICTIONARY_hive group by  ACTIVE_EMUI_VERSION order by Total""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC224
test("UNIQDATA_EXCLUDE_DICTIONARY_TC224", Include) {
  sql(s"""select last(CUST_ID) a from (select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID) t""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC225
test("UNIQDATA_EXCLUDE_DICTIONARY_TC225", Include) {
  sql(s"""select FIRST(CUST_ID) a from uniqdata_EXCLUDEDICTIONARY order by a""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC226
test("UNIQDATA_EXCLUDE_DICTIONARY_TC226", Include) {
  checkAnswer(s"""select CUST_ID,count(CUST_ID) a from uniqdata_EXCLUDEDICTIONARY group by CUST_ID order by CUST_ID""",
    s"""select CUST_ID,count(CUST_ID) a from uniqdata_EXCLUDEDICTIONARY_hive group by CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC227
test("UNIQDATA_EXCLUDE_DICTIONARY_TC227", Include) {
  checkAnswer(s"""select Lower(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY order by CUST_ID""",
    s"""select Lower(CUST_ID) a  from uniqdata_EXCLUDEDICTIONARY_hive order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC228
test("UNIQDATA_EXCLUDE_DICTIONARY_TC228", Include) {
  checkAnswer(s"""select distinct CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID""",
    s"""select distinct CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC229
test("UNIQDATA_EXCLUDE_DICTIONARY_TC229", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY order by CUST_ID limit 101""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive order by CUST_ID limit 101""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC230
test("UNIQDATA_EXCLUDE_DICTIONARY_TC230", Include) {
  checkAnswer(s"""select CUST_ID as a from uniqdata_EXCLUDEDICTIONARY  order by a asc limit 10""",
    s"""select CUST_ID as a from uniqdata_EXCLUDEDICTIONARY_hive  order by a asc limit 10""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC231
test("UNIQDATA_EXCLUDE_DICTIONARY_TC231", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where  (CUST_ID == 100084) and (CUST_ID==100084)""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where  (CUST_ID == 100084) and (CUST_ID==100084)""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC232
test("UNIQDATA_EXCLUDE_DICTIONARY_TC232", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID !='100084' order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID !='100084' order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC233
test("UNIQDATA_EXCLUDE_DICTIONARY_TC233", Include) {
  checkAnswer(s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""",
    s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY_hive where (CUST_ID=10844 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01844') OR (CUST_ID=10840 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_01840')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC234
test("UNIQDATA_EXCLUDE_DICTIONARY_TC234", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID !=100084 order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID !=100084 order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC235
test("UNIQDATA_EXCLUDE_DICTIONARY_TC235", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID >100084 order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID >100084 order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC236
test("UNIQDATA_EXCLUDE_DICTIONARY_TC236", Include) {
  checkAnswer(s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY where CUST_ID<>CUST_ID order by CUST_ID""",
    s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID<>CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC237
test("UNIQDATA_EXCLUDE_DICTIONARY_TC237", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID != BIGINT_COLUMN2 order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID != BIGINT_COLUMN2 order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC238
test("UNIQDATA_EXCLUDE_DICTIONARY_TC238", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN2<CUST_ID order by CUST_ID""",
    s"""select CUST_ID, CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN2<CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC239
test("UNIQDATA_EXCLUDE_DICTIONARY_TC239", Include) {
  checkAnswer(s"""select CUST_ID, CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID<=CUST_ID order by CUST_ID""",
    s"""select CUST_ID, CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID<=CUST_ID order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC240
test("UNIQDATA_EXCLUDE_DICTIONARY_TC240", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID <1000 order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID <1000 order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC241
test("UNIQDATA_EXCLUDE_DICTIONARY_TC241", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID >1000 order by CUST_ID""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID >1000 order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC242
test("UNIQDATA_EXCLUDE_DICTIONARY_TC242", Include) {
  checkAnswer(s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY where CUST_ID IS NULL order by CUST_ID""",
    s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID IS NULL order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC243
test("UNIQDATA_EXCLUDE_DICTIONARY_TC243", Include) {
  checkAnswer(s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY where CUST_ID IS NOT NULL order by CUST_ID""",
    s"""select CUST_ID  from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID IS NOT NULL order by CUST_ID""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC244
test("UNIQDATA_EXCLUDE_DICTIONARY_TC244", Include) {
  checkAnswer(s"""select sum(CUST_NAME)+10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_NAME)+10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC245
test("UNIQDATA_EXCLUDE_DICTIONARY_TC245", Include) {
  checkAnswer(s"""select sum(CUST_NAME)*10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_NAME)*10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC246
test("UNIQDATA_EXCLUDE_DICTIONARY_TC246", Include) {
  checkAnswer(s"""select sum(CUST_NAME)/10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_NAME)/10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC247
test("UNIQDATA_EXCLUDE_DICTIONARY_TC247", Include) {
  checkAnswer(s"""select sum(CUST_NAME)-10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_NAME)-10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC248
test("UNIQDATA_EXCLUDE_DICTIONARY_TC248", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)+10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1)+10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC249
test("UNIQDATA_EXCLUDE_DICTIONARY_TC249", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)*10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1)*10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC250
test("UNIQDATA_EXCLUDE_DICTIONARY_TC250", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)/10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1)/10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC251
test("UNIQDATA_EXCLUDE_DICTIONARY_TC251", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1)-10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1)-10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC252
test("UNIQDATA_EXCLUDE_DICTIONARY_TC252", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)+10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1)+10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC253
test("UNIQDATA_EXCLUDE_DICTIONARY_TC253", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)*10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1)*10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC254
test("UNIQDATA_EXCLUDE_DICTIONARY_TC254", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)/10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1)/10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC255
test("UNIQDATA_EXCLUDE_DICTIONARY_TC255", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1)-10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1)-10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC256
test("UNIQDATA_EXCLUDE_DICTIONARY_TC256", Include) {
  sql(s"""select sum(Double_COLUMN1)+10 as a   from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC257
test("UNIQDATA_EXCLUDE_DICTIONARY_TC257", Include) {
  sql(s"""select sum(Double_COLUMN1)*10 as a   from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC258
test("UNIQDATA_EXCLUDE_DICTIONARY_TC258", Include) {
  sql(s"""select sum(Double_COLUMN1)/10 as a   from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC259
test("UNIQDATA_EXCLUDE_DICTIONARY_TC259", Include) {
  sql(s"""select sum(Double_COLUMN1)-10 as a   from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC260
test("UNIQDATA_EXCLUDE_DICTIONARY_TC260", Include) {
  checkAnswer(s"""select sum(DOB)+10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB)+10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC261
test("UNIQDATA_EXCLUDE_DICTIONARY_TC261", Include) {
  checkAnswer(s"""select sum(DOB)*10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB)*10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC262
test("UNIQDATA_EXCLUDE_DICTIONARY_TC262", Include) {
  checkAnswer(s"""select sum(DOB)/10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB)/10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC263
test("UNIQDATA_EXCLUDE_DICTIONARY_TC263", Include) {
  checkAnswer(s"""select sum(DOB)-10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB)-10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC264
test("UNIQDATA_EXCLUDE_DICTIONARY_TC264", Include) {
  checkAnswer(s"""select sum(CUST_ID)+10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID)+10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC265
test("UNIQDATA_EXCLUDE_DICTIONARY_TC265", Include) {
  checkAnswer(s"""select sum(CUST_ID)*10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID)*10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC266
test("UNIQDATA_EXCLUDE_DICTIONARY_TC266", Include) {
  checkAnswer(s"""select sum(CUST_ID)/10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID)/10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC267
test("UNIQDATA_EXCLUDE_DICTIONARY_TC267", Include) {
  checkAnswer(s"""select sum(CUST_ID)-10 as a   from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID)-10 as a   from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC292
test("UNIQDATA_EXCLUDE_DICTIONARY_TC292", Include) {
  checkAnswer(s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY where DOB LIKE '2015-09-30%'""",
    s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB LIKE '2015-09-30%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC293
test("UNIQDATA_EXCLUDE_DICTIONARY_TC293", Include) {
  checkAnswer(s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY where DOB LIKE '% %'""",
    s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB LIKE '% %'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC294
test("UNIQDATA_EXCLUDE_DICTIONARY_TC294", Include) {
  checkAnswer(s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY where DOB LIKE '%01:00:03'""",
    s"""SELECT DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB LIKE '%01:00:03'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC295
test("UNIQDATA_EXCLUDE_DICTIONARY_TC295", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 like '123372038%' """,
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 like '123372038%' """)
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC296
test("UNIQDATA_EXCLUDE_DICTIONARY_TC296", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 like '%2038'""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 like '%2038'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC297
test("UNIQDATA_EXCLUDE_DICTIONARY_TC297", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 like '%2038%'""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 like '%2038%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC298
test("UNIQDATA_EXCLUDE_DICTIONARY_TC298", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 like '123456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 like '123456790%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC299
test("UNIQDATA_EXCLUDE_DICTIONARY_TC299", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 like '%456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 like '%456790%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC300
test("UNIQDATA_EXCLUDE_DICTIONARY_TC300", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 like '123456790%'""",
    s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 like '123456790%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC301
test("UNIQDATA_EXCLUDE_DICTIONARY_TC301", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 like '1.123456748%'""",
    s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 like '1.123456748%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC302
test("UNIQDATA_EXCLUDE_DICTIONARY_TC302", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 like '%23456%'""",
    s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 like '%23456%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC303
test("UNIQDATA_EXCLUDE_DICTIONARY_TC303", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 like '%976E10'""",
    s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 like '%976E10'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC304
test("UNIQDATA_EXCLUDE_DICTIONARY_TC304", Include) {
  checkAnswer(s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID like '1000%'""",
    s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID like '1000%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC305
test("UNIQDATA_EXCLUDE_DICTIONARY_TC305", Include) {
  checkAnswer(s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID like '%00%'""",
    s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID like '%00%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC306
test("UNIQDATA_EXCLUDE_DICTIONARY_TC306", Include) {
  checkAnswer(s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID like '%0084'""",
    s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID like '%0084'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC307
test("UNIQDATA_EXCLUDE_DICTIONARY_TC307", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME like 'CUST_NAME_0184%'""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME like 'CUST_NAME_0184%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC308
test("UNIQDATA_EXCLUDE_DICTIONARY_TC308", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME like '%E_01%'""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME like '%E_01%'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC309
test("UNIQDATA_EXCLUDE_DICTIONARY_TC309", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME like '%ST_NAME_018'""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME like '%ST_NAME_018'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC310
test("UNIQDATA_EXCLUDE_DICTIONARY_TC310", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC311
test("UNIQDATA_EXCLUDE_DICTIONARY_TC311", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME not in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME not in ('CUST_NAME_0184400074','CUST_NAME_0184400075','CUST_NAME_0184400077')""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC312
test("UNIQDATA_EXCLUDE_DICTIONARY_TC312", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID in (9137,10137,14137)""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID in (9137,10137,14137)""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC313
test("UNIQDATA_EXCLUDE_DICTIONARY_TC313", Include) {
  sql(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID not in (9137,10137,14137)""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC314
test("UNIQDATA_EXCLUDE_DICTIONARY_TC314", Include) {
  sql(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB in ('2015-10-04 01:00:03','2015-10-07%','2015-10-07 01:00:03')""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC315
test("UNIQDATA_EXCLUDE_DICTIONARY_TC315", Include) {
  sql(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB not in ('2015-10-04 01:00:03','2015-10-07 01:00:03')""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC316
test("UNIQDATA_EXCLUDE_DICTIONARY_TC316", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC317
test("UNIQDATA_EXCLUDE_DICTIONARY_TC317", Include) {
  sql(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 not in (11234567489.7976000000,11234567589.7976000000,11234569489.7976000000)""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC318
test("UNIQDATA_EXCLUDE_DICTIONARY_TC318", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 in (22345680745.1234000000,22345680741.1234000000)""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 in (22345680745.1234000000,22345680741.1234000000)""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC319
test("UNIQDATA_EXCLUDE_DICTIONARY_TC319", Include) {
  sql(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 not in (22345680745.1234000000,22345680741.1234000000)""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC322
test("UNIQDATA_EXCLUDE_DICTIONARY_TC322", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME !='CUST_NAME_0184400077'""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME !='CUST_NAME_0184400077'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC323
test("UNIQDATA_EXCLUDE_DICTIONARY_TC323", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME NOT LIKE 'CUST_NAME_0184400077'""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME NOT LIKE 'CUST_NAME_0184400077'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC324
test("UNIQDATA_EXCLUDE_DICTIONARY_TC324", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID !=100078""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID !=100078""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC325
test("UNIQDATA_EXCLUDE_DICTIONARY_TC325", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID NOT LIKE 100079""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID NOT LIKE 100079""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC326
test("UNIQDATA_EXCLUDE_DICTIONARY_TC326", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB !='2015-10-07 01:00:03'""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB !='2015-10-07 01:00:03'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC327
test("UNIQDATA_EXCLUDE_DICTIONARY_TC327", Include) {
  sql(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC328
test("UNIQDATA_EXCLUDE_DICTIONARY_TC328", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 !=11234569489.7976000000""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 !=11234569489.7976000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC329
test("UNIQDATA_EXCLUDE_DICTIONARY_TC329", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 NOT LIKE 11234569489.7976000000""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 NOT LIKE 11234569489.7976000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC330
test("UNIQDATA_EXCLUDE_DICTIONARY_TC330", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 != 22345680741.1234000000""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 != 22345680741.1234000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC331
test("UNIQDATA_EXCLUDE_DICTIONARY_TC331", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 NOT LIKE 22345680741.1234000000""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 NOT LIKE 22345680741.1234000000""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC335
test("UNIQDATA_EXCLUDE_DICTIONARY_TC335", Include) {
  checkAnswer(s"""SELECT DOB,CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME RLIKE 'CUST_NAME_0184400077'""",
    s"""SELECT DOB,CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME RLIKE 'CUST_NAME_0184400077'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC336
test("UNIQDATA_EXCLUDE_DICTIONARY_TC336", Include) {
  checkAnswer(s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID RLIKE '100079'""",
    s"""SELECT CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID RLIKE '100079'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC337
test("UNIQDATA_EXCLUDE_DICTIONARY_TC337", Include) {
  checkAnswer(s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 RLIKE '11234569489.7976000000'""",
    s"""SELECT Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 RLIKE '11234569489.7976000000'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC338
test("UNIQDATA_EXCLUDE_DICTIONARY_TC338", Include) {
  checkAnswer(s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 RLIKE '1234567890123550.0000000000'""",
    s"""SELECT DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 RLIKE '1234567890123550.0000000000'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC339
test("UNIQDATA_EXCLUDE_DICTIONARY_TC339", Include) {
  checkAnswer(s"""SELECT BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 RLIKE '123372038698'""",
    s"""SELECT BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 RLIKE '123372038698'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC340
test("UNIQDATA_EXCLUDE_DICTIONARY_TC340", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.DOB=b.DOB and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC341
test("UNIQDATA_EXCLUDE_DICTIONARY_TC341", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.CUST_ID=b.CUST_ID and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC342
test("UNIQDATA_EXCLUDE_DICTIONARY_TC342", Include) {
  sql(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.CUST_NAME=b.CUST_NAME and a.DOB NOT LIKE '2015-10-07 01:00:03'""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC343
test("UNIQDATA_EXCLUDE_DICTIONARY_TC343", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.Double_COLUMN1=b.Double_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive a join uniqdata_EXCLUDEDICTIONARY_hive b on a.Double_COLUMN1=b.Double_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC344
test("UNIQDATA_EXCLUDE_DICTIONARY_TC344", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.DECIMAL_COLUMN1=b.DECIMAL_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive a join uniqdata_EXCLUDEDICTIONARY_hive b on a.DECIMAL_COLUMN1=b.DECIMAL_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC345
test("UNIQDATA_EXCLUDE_DICTIONARY_TC345", Include) {
  checkAnswer(s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY a join uniqdata_EXCLUDEDICTIONARY b on a.BIGINT_COLUMN1=b.BIGINT_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""",
    s"""select  b.BIGINT_COLUMN1,b.DECIMAL_COLUMN1,b.Double_COLUMN1,b.DOB,b.CUST_ID,b.CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive a join uniqdata_EXCLUDEDICTIONARY_hive b on a.BIGINT_COLUMN1=b.BIGINT_COLUMN1 and a.DECIMAL_COLUMN1 RLIKE '12345678901.1234000058'""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC346
test("UNIQDATA_EXCLUDE_DICTIONARY_TC346", Include) {
  checkAnswer(s"""select count( BIGINT_COLUMN1 ),sum( BIGINT_COLUMN1 ),count(distinct BIGINT_COLUMN1 ),avg( BIGINT_COLUMN1 ),max( BIGINT_COLUMN1 ),min( BIGINT_COLUMN1 ),1 from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count( BIGINT_COLUMN1 ),sum( BIGINT_COLUMN1 ),count(distinct BIGINT_COLUMN1 ),avg( BIGINT_COLUMN1 ),max( BIGINT_COLUMN1 ),min( BIGINT_COLUMN1 ),1 from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC347
test("UNIQDATA_EXCLUDE_DICTIONARY_TC347", Include) {
  checkAnswer(s"""select count( DECIMAL_COLUMN1 ),sum( DECIMAL_COLUMN1 ),count(distinct DECIMAL_COLUMN1 ),avg( DECIMAL_COLUMN1 ),max( DECIMAL_COLUMN1 ),min( DECIMAL_COLUMN1 ),1 from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count( DECIMAL_COLUMN1 ),sum( DECIMAL_COLUMN1 ),count(distinct DECIMAL_COLUMN1 ),avg( DECIMAL_COLUMN1 ),max( DECIMAL_COLUMN1 ),min( DECIMAL_COLUMN1 ),1 from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC348
test("UNIQDATA_EXCLUDE_DICTIONARY_TC348", Include) {
  sql(s"""select count( Double_COLUMN1),sum( Double_COLUMN1 ),count(distinct Double_COLUMN1 ),avg(Double_COLUMN1),max(Double_COLUMN1),min(Double_COLUMN1),1 from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC349
test("UNIQDATA_EXCLUDE_DICTIONARY_TC349", Include) {
  checkAnswer(s"""select count(CUST_ID),sum(CUST_ID),count(CUST_ID),avg(CUST_ID),max(CUST_ID),min(CUST_ID),1 from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(CUST_ID),sum(CUST_ID),count(CUST_ID),avg(CUST_ID),max(CUST_ID),min(CUST_ID),1 from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC350
test("UNIQDATA_EXCLUDE_DICTIONARY_TC350", Include) {
  checkAnswer(s"""select count(DOB),sum(DOB),count(distinct DOB ),avg(DOB),max(DOB ),min(DOB),1 from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DOB),sum(DOB),count(distinct DOB ),avg(DOB),max(DOB ),min(DOB),1 from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC351
test("UNIQDATA_EXCLUDE_DICTIONARY_TC351", Include) {
  checkAnswer(s"""select count(CUST_NAME ),sum(CUST_NAME ),count(distinct CUST_NAME ),avg(CUST_NAME ),max(CUST_NAME ),min(CUST_NAME ),1 from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(CUST_NAME ),sum(CUST_NAME ),count(distinct CUST_NAME ),avg(CUST_NAME ),max(CUST_NAME ),min(CUST_NAME ),1 from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC352
test("UNIQDATA_EXCLUDE_DICTIONARY_TC352", Include) {
  checkAnswer(s"""select sum(BIGINT_COLUMN1),count(BIGINT_COLUMN1),avg(BIGINT_COLUMN1),sum(BIGINT_COLUMN1)/count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(BIGINT_COLUMN1),count(BIGINT_COLUMN1),avg(BIGINT_COLUMN1),sum(BIGINT_COLUMN1)/count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC353
test("UNIQDATA_EXCLUDE_DICTIONARY_TC353", Include) {
  checkAnswer(s"""select sum(DECIMAL_COLUMN1),count(DECIMAL_COLUMN1),avg(DECIMAL_COLUMN1),sum(DECIMAL_COLUMN1)/count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DECIMAL_COLUMN1),count(DECIMAL_COLUMN1),avg(DECIMAL_COLUMN1),sum(DECIMAL_COLUMN1)/count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC354
test("UNIQDATA_EXCLUDE_DICTIONARY_TC354", Include) {
  sql(s"""select sum(Double_COLUMN1),count(Double_COLUMN1),avg(Double_COLUMN1),sum(Double_COLUMN1)/count(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC355
test("UNIQDATA_EXCLUDE_DICTIONARY_TC355", Include) {
  checkAnswer(s"""select sum(CUST_ID),count(CUST_ID),avg(CUST_ID),sum(CUST_ID)/count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_ID),count(CUST_ID),avg(CUST_ID),sum(CUST_ID)/count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC356
test("UNIQDATA_EXCLUDE_DICTIONARY_TC356", Include) {
  checkAnswer(s"""select sum(CUST_NAME),count(CUST_NAME),avg(CUST_NAME),sum(CUST_NAME)/count(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(CUST_NAME),count(CUST_NAME),avg(CUST_NAME),sum(CUST_NAME)/count(CUST_NAME) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC357
test("UNIQDATA_EXCLUDE_DICTIONARY_TC357", Include) {
  checkAnswer(s"""select sum(DOB),count(DOB),avg(DOB),sum(DOB)/count(DOB) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select sum(DOB),count(DOB),avg(DOB),sum(DOB)/count(DOB) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC358
test("UNIQDATA_EXCLUDE_DICTIONARY_TC358", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,DOB,CUST_ID,CUST_NAME  from uniqdata_EXCLUDEDICTIONARY""",
    s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1,DOB,CUST_ID,CUST_NAME  from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC359
test("UNIQDATA_EXCLUDE_DICTIONARY_TC359", Include) {
  checkAnswer(s"""select count(DECIMAL_COLUMN2) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DECIMAL_COLUMN2) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC360
test("UNIQDATA_EXCLUDE_DICTIONARY_TC360", Include) {
  checkAnswer(s"""select count(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(Double_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC361
test("UNIQDATA_EXCLUDE_DICTIONARY_TC361", Include) {
  checkAnswer(s"""select count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(BIGINT_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC362
test("UNIQDATA_EXCLUDE_DICTIONARY_TC362", Include) {
  checkAnswer(s"""select count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DECIMAL_COLUMN1) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC363
test("UNIQDATA_EXCLUDE_DICTIONARY_TC363", Include) {
  checkAnswer(s"""select count(DOB) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(DOB) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC364
test("UNIQDATA_EXCLUDE_DICTIONARY_TC364", Include) {
  checkAnswer(s"""select count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY""",
    s"""select count(CUST_ID) from uniqdata_EXCLUDEDICTIONARY_hive""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC365
test("UNIQDATA_EXCLUDE_DICTIONARY_TC365", Include) {
  sql(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  BIGINT_COLUMN1  != '123372038698'""").collect
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC366
test("UNIQDATA_EXCLUDE_DICTIONARY_TC366", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  DECIMAL_COLUMN1  != '1234567890123480.0000000000' order by CUST_ID limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  DECIMAL_COLUMN1  != '1234567890123480.0000000000' order by CUST_ID limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC367
test("UNIQDATA_EXCLUDE_DICTIONARY_TC367", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  Double_COLUMN1  != '11234569489.7976000000' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  Double_COLUMN1  != '11234569489.7976000000' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC368
test("UNIQDATA_EXCLUDE_DICTIONARY_TC368", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  DOB  != '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  DOB  != '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC369
test("UNIQDATA_EXCLUDE_DICTIONARY_TC369", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  CUST_ID  != '100075' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  CUST_ID  != '100075' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC370
test("UNIQDATA_EXCLUDE_DICTIONARY_TC370", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  BIGINT_COLUMN1  not like '123372038698' order by  CUST_ID limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  BIGINT_COLUMN1  not like '123372038698' order by  CUST_ID limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC371
test("UNIQDATA_EXCLUDE_DICTIONARY_TC371", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  DECIMAL_COLUMN1  not like '11234569489.79760000000' order by CUST_ID limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  DECIMAL_COLUMN1  not like '11234569489.79760000000' order by CUST_ID limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC372
test("UNIQDATA_EXCLUDE_DICTIONARY_TC372", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  Double_COLUMN1  not like '11234569489.7976000000' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  Double_COLUMN1  not like '11234569489.7976000000' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC373
test("UNIQDATA_EXCLUDE_DICTIONARY_TC373", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  DOB  not like '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  DOB  not like '2015-09-18 01:00:03.0' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC374
test("UNIQDATA_EXCLUDE_DICTIONARY_TC374", Include) {
  checkAnswer(s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where  CUST_ID  not like '100075' order by CUST_NAME limit 5""",
    s"""select CUST_NAME,CUST_ID,DECIMAL_COLUMN2,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where  CUST_ID  not like '100075' order by CUST_NAME limit 5""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC375
test("UNIQDATA_EXCLUDE_DICTIONARY_TC375", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME is not null""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC376
test("UNIQDATA_EXCLUDE_DICTIONARY_TC376", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 is not null""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC377
test("UNIQDATA_EXCLUDE_DICTIONARY_TC377", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 is not null""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC378
test("UNIQDATA_EXCLUDE_DICTIONARY_TC378", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 is not null""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC379
test("UNIQDATA_EXCLUDE_DICTIONARY_TC379", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB is not null""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC380
test("UNIQDATA_EXCLUDE_DICTIONARY_TC380", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID is not null""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID is not null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC381
test("UNIQDATA_EXCLUDE_DICTIONARY_TC381", Include) {
  checkAnswer(s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY where CUST_NAME is  null""",
    s"""select CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC382
test("UNIQDATA_EXCLUDE_DICTIONARY_TC382", Include) {
  checkAnswer(s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where Double_COLUMN1 is  null""",
    s"""select Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where Double_COLUMN1 is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC383
test("UNIQDATA_EXCLUDE_DICTIONARY_TC383", Include) {
  checkAnswer(s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 is  null""",
    s"""select BIGINT_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where BIGINT_COLUMN1 is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC384
test("UNIQDATA_EXCLUDE_DICTIONARY_TC384", Include) {
  checkAnswer(s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1 is  null""",
    s"""select DECIMAL_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1 is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC385
test("UNIQDATA_EXCLUDE_DICTIONARY_TC385", Include) {
  checkAnswer(s"""select DOB from uniqdata_EXCLUDEDICTIONARY where DOB is  null""",
    s"""select DOB from uniqdata_EXCLUDEDICTIONARY_hive where DOB is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC386
test("UNIQDATA_EXCLUDE_DICTIONARY_TC386", Include) {
  checkAnswer(s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY where CUST_ID is  null""",
    s"""select CUST_ID from uniqdata_EXCLUDEDICTIONARY_hive where CUST_ID is  null""")
}
       

//UNIQDATA_EXCLUDE_DICTIONARY_TC387
test("UNIQDATA_EXCLUDE_DICTIONARY_TC387", Include) {
  checkAnswer(s"""select count(*) from uniqdata_EXCLUDEDICTIONARY where CUST_NAME = 'CUST_NAME_01844'""",
    s"""select count(*) from uniqdata_EXCLUDEDICTIONARY_hive where CUST_NAME = 'CUST_NAME_01844'""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_001
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_001", Include) {
  checkAnswer(s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata_EXCLUDEDICTIONARY where cust_id=9056 and INTEGER_COLUMN1=57  and BIGINT_COLUMN1=123372036910 and  BIGINT_COLUMN2=-223372036798 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00056' and cust_name='CUST_NAME_00056' and cust_id!=1 and ACTIVE_EMUI_VERSION!='abc' """,
    s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where cust_id=9056 and INTEGER_COLUMN1=57  and BIGINT_COLUMN1=123372036910 and  BIGINT_COLUMN2=-223372036798 and ACTIVE_EMUI_VERSION='ACTIVE_EMUI_VERSION_00056' and cust_name='CUST_NAME_00056' and cust_id!=1 and ACTIVE_EMUI_VERSION!='abc' """)
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_002
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_002", Include) {
  checkAnswer(s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1=12345680900.1234 and DECIMAL_COLUMN2=22345680900.1234 and Double_COLUMN1=1.12345674897976E10 and Double_COLUMN2=-4.8E-4 and dob='1975-06-23 01:00:03'  and doj='1975-06-23 02:00:03' and dob!='1970-03-29 01:00:03' and doj!='1970-04-03 02:00:03' and Double_COLUMN2!=12345678987.1234""",
    s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY_hive where DECIMAL_COLUMN1=12345680900.1234 and DECIMAL_COLUMN2=22345680900.1234 and Double_COLUMN1=1.12345674897976E10 and Double_COLUMN2=-4.8E-4 and dob='1975-06-23 01:00:03'  and doj='1975-06-23 02:00:03' and dob!='1970-03-29 01:00:03' and doj!='1970-04-03 02:00:03' and Double_COLUMN2!=12345678987.1234""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_003
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_003", Include) {
  checkAnswer(s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata_EXCLUDEDICTIONARY where cust_id>=9000 and CUST_ID<=10000 and INTEGER_COLUMN1>=1 and INTEGER_COLUMN1<=400 and BIGINT_COLUMN1>=123372036854 and BIGINT_COLUMN1<=123372037254 and BIGINT_COLUMN2>=-223372036854  and BIGINT_COLUMN2<=-223372036454""",
    s"""select CUST_ID,INTEGER_COLUMN1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME from uniqdata_EXCLUDEDICTIONARY_hive where cust_id>=9000 and CUST_ID<=10000 and INTEGER_COLUMN1>=1 and INTEGER_COLUMN1<=400 and BIGINT_COLUMN1>=123372036854 and BIGINT_COLUMN1<=123372037254 and BIGINT_COLUMN2>=-223372036854  and BIGINT_COLUMN2<=-223372036454""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_004
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_004", Include) {
  sql(s"""select DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where DECIMAL_COLUMN1>=12345678901.1234 and DECIMAL_COLUMN1<=12345679301.12344 and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976 and Double_COLUMN1<=1.12345674897976E10 and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<= -4.8E-4 and dob>='1970-01-02 01:00:03' and dob<= '1971-02-05 01:00:03' and doj>='1970-01-02 02:00:03' and doj<='1971-02-05 02:00:03'""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_005
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_005", Include) {
  checkAnswer(s"""select distinct INTEGER_COLUMN1,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,cust_name from uniqdata_EXCLUDEDICTIONARY where INTEGER_COLUMN1<cust_id and BIGINT_COLUMN2<BIGINT_COLUMN1 and substr(cust_name,10,length(cust_name))=substr(ACTIVE_EMUI_VERSION,20,length(ACTIVE_EMUI_VERSION))""",
    s"""select distinct INTEGER_COLUMN1,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,cust_name from uniqdata_EXCLUDEDICTIONARY_hive where INTEGER_COLUMN1<cust_id and BIGINT_COLUMN2<BIGINT_COLUMN1 and substr(cust_name,10,length(cust_name))=substr(ACTIVE_EMUI_VERSION,20,length(ACTIVE_EMUI_VERSION))""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_006
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_006", Include) {
  checkAnswer(s"""select distinct dob,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where  DECIMAL_COLUMN1<DECIMAL_COLUMN2 and Double_COLUMN1>Double_COLUMN2  and day(dob)=day(doj)""",
    s"""select distinct dob,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where  DECIMAL_COLUMN1<DECIMAL_COLUMN2 and Double_COLUMN1>Double_COLUMN2  and day(dob)=day(doj)""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_007
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_007", Include) {
  sql(s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where cust_id in(9011,9012,9013,9014,9015,9016) and INTEGER_COLUMN1 in (12,13,14,15,16,17,18)and ACTIVE_EMUI_VERSION  in('ACTIVE_EMUI_VERSION_00011','ACTIVE_EMUI_VERSION_00012','ACTIVE_EMUI_VERSION_00013','ACTIVE_EMUI_VERSION_00014','ACTIVE_EMUI_VERSION_00015') and CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') and DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03')""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_008
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_008", Include) {
  sql(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,BIGINT_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where BIGINT_COLUMN1 in (123372036869,123372036870,123372036871,123372036872) and BIGINT_COLUMN2 not in (-223372034862,-223372034889,-223372034902,-223372034860) and DECIMAL_COLUMN1 not in(12345678916.1234,12345678917.1234,12345678918.1234,2345678919.1234,2345678920.1234) and DECIMAL_COLUMN2 not in (22345680900.1234,22345680895.1234,22345680892.1234)  and Double_COLUMN2 not in(1234567890,6789076,11234567489.7976)""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_009
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_009", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where (cust_id in(9011,9012,9013,9014,9015,9016) or INTEGER_COLUMN1 in (17,18,19,20))and (ACTIVE_EMUI_VERSION not in('ACTIVE_EMUI_VERSION_00028','ACTIVE_EMUI_VERSION_00029','ACTIVE_EMUI_VERSION_00030') or CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') )and (DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03') or doj not in('1970-01-30 02:00:03','1970-01-31 02:00:03','1970-02-01 02:00:03','1970-02-02 02:00:03','1970-02-03 02:00:03','1970-02-04 02:00:03','1970-02-05 02:00:03'))""",
    s"""select CUST_ID,CUST_NAME,DOB,DOJ,INTEGER_COLUMN1,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where (cust_id in(9011,9012,9013,9014,9015,9016) or INTEGER_COLUMN1 in (17,18,19,20))and (ACTIVE_EMUI_VERSION not in('ACTIVE_EMUI_VERSION_00028','ACTIVE_EMUI_VERSION_00029','ACTIVE_EMUI_VERSION_00030') or CUST_NAME in ('CUST_NAME_00011','CUST_NAME_00012','CUST_NAME_00013','CUST_NAME_00016') )and (DOB in('1970-01-12 01:00:03','1970-01-13 01:00:03','1970-01-14 01:00:03','1970-01-15 01:00:03') or doj not in('1970-01-30 02:00:03','1970-01-31 02:00:03','1970-02-01 02:00:03','1970-02-02 02:00:03','1970-02-03 02:00:03','1970-02-04 02:00:03','1970-02-05 02:00:03'))""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_010
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_010", Include) {
  sql(s"""select BIGINT_COLUMN1,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,BIGINT_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where (BIGINT_COLUMN1 in (123372036869,123372036870,123372036871,123372036872) or BIGINT_COLUMN2 not in (-223372034862,-223372034889,-223372034902,-223372034860) )and (DECIMAL_COLUMN1 in(12345678916.1234,12345678917.1234,12345678918.1234,2345678919.1234,2345678920.1234) or DECIMAL_COLUMN2 not in (22345680900.1234,22345680895.1234,22345680892.1234)) and (Double_COLUMN1 in (11234567489.7976,11234567489.7976,11234567489.7976,11234567489.7976) or Double_COLUMN2 not in(1234567890,6789076,11234567489.7976))""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_011
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_011", Include) {
  checkAnswer(s"""select cust_id,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,BIGINT_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where INTEGER_COLUMN1>=50 and INTEGER_COLUMN1<=59 and BIGINT_COLUMN2>=-223372036805 and BIGINT_COLUMN2<=-223372036796 and ACTIVE_EMUI_VERSION rlike 'ACTIVE'
""",
    s"""select cust_id,BIGINT_COLUMN1,ACTIVE_EMUI_VERSION,BIGINT_COLUMN2 from uniqdata_EXCLUDEDICTIONARY_hive where INTEGER_COLUMN1>=50 and INTEGER_COLUMN1<=59 and BIGINT_COLUMN2>=-223372036805 and BIGINT_COLUMN2<=-223372036796 and ACTIVE_EMUI_VERSION rlike 'ACTIVE'
""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_012
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_012", Include) {
  sql(s"""select dob,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and 
Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_013
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_013", Include) {
  sql(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where cust_id rlike 9 and 
BIGINT_COLUMN1 rlike 12 and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and CUST_NAME rlike 'CUST' and DOB rlike '19' and DECIMAL_COLUMN1 rlike 1234 and Double_COLUMN1 >=1111 and integer_column1 is not null and BIGINT_COLUMN2 is not null and CUST_NAME is not null and DOJ is not null and  DECIMAL_COLUMN2 is not null and Double_COLUMN2 is not null""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_014
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_014", Include) {
  checkAnswer(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where integer_column1 like '5%' and BIGINT_COLUMN2 like '-22%' and CUST_NAME like 'CUST%' and ACTIVE_EMUI_VERSION like 'ACTIVE%' and dob like '19%' and decimal_column2 like '22%' and  Double_COLUMN2 >=-111""",
    s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY_hive where integer_column1 like '5%' and BIGINT_COLUMN2 like '-22%' and CUST_NAME like 'CUST%' and ACTIVE_EMUI_VERSION like 'ACTIVE%' and dob like '19%' and decimal_column2 like '22%' and  Double_COLUMN2 >=-111""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_015
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_015", Include) {
  checkAnswer(s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where integer_column1 is null and BIGINT_COLUMN2 is null and cust_id is null and dob  is null and decimal_column2 is null and  Double_COLUMN2 is null""",
    s"""select cust_id,integer_column1,BIGINT_COLUMN1,BIGINT_COLUMN2,ACTIVE_EMUI_VERSION,CUST_NAME,DOB,DOJ,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY_hive where integer_column1 is null and BIGINT_COLUMN2 is null and cust_id is null and dob  is null and decimal_column2 is null and  Double_COLUMN2 is null""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_017
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_017", Include) {
  sql(s"""select avg(cust_id),avg(integer_column1),avg(BIGINT_COLUMN1),avg(BIGINT_COLUMN2),avg(DECIMAL_COLUMN1),avg(Double_COLUMN1),avg(Double_COLUMN2),count(cust_id),count(integer_column1),count(ACTIVE_EMUI_VERSION),count(CUST_NAME),count(CUST_NAME),count(DOB),count(doj),count(BIGINT_COLUMN2),count(DECIMAL_COLUMN1),count(Double_COLUMN1),count(Double_COLUMN2),avg(DECIMAL_COLUMN2),sum(cust_id),sum(integer_column1),sum(BIGINT_COLUMN1),sum(BIGINT_COLUMN2),sum(DECIMAL_COLUMN1),sum(Double_COLUMN1),sum(DECIMAL_COLUMN2),min(cust_id),min(integer_column1),min(ACTIVE_EMUI_VERSION),min(CUST_NAME),min(CUST_NAME),min(DOB),min(doj),min(BIGINT_COLUMN2),min(DECIMAL_COLUMN1),min(Double_COLUMN1),min(Double_COLUMN2),max(cust_id),max(integer_column1),max(ACTIVE_EMUI_VERSION),max(CUST_NAME),max(CUST_NAME),max(DOB),max(doj),max(BIGINT_COLUMN2),max(DECIMAL_COLUMN1),max(Double_COLUMN1),max(Double_COLUMN2)from uniqdata_EXCLUDEDICTIONARY where cust_id between 9000 and 9100 and dob between '1970-01-01 01:00:03' and '1972-09-27 02:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1972-09-27 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and 
Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_019
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_019", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where cust_name not like '%abc%' and cust_id in (9002,9003,9004,9005,9006,9007,9008,9009,9010,9011,9012,9013,9014,9015,9016,9017,9018,9019,9021,9022,9023,9024,9025,9027,9028,9029,9030) and ACTIVE_EMUI_VERSION like 'ACTIVE%' group by CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 having count(DECIMAL_COLUMN1) <=2  order by CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 
""",
    s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where cust_name not like '%abc%' and cust_id in (9002,9003,9004,9005,9006,9007,9008,9009,9010,9011,9012,9013,9014,9015,9016,9017,9018,9019,9021,9022,9023,9024,9025,9027,9028,9029,9030) and ACTIVE_EMUI_VERSION like 'ACTIVE%' group by CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 having count(DECIMAL_COLUMN1) <=2  order by CUST_ID,CUST_NAME,DOB,BIGINT_COLUMN1,DECIMAL_COLUMN1,Double_COLUMN1 
""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_020
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_020", Include) {
  sql(s"""select (avg(DECIMAL_COLUMN1)),(avg(CUST_ID)),(avg(BIGINT_COLUMN1)), (avg(Double_COLUMN1)) from uniqdata_EXCLUDEDICTIONARY  where cust_id between 9000 and  9100 and dob between '1970-01-01 01:00:03' and '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976 
""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_021
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_021", Include) {
  sql(s"""select (sum(DECIMAL_COLUMN2)),(sum(integer_column1)),(sum(BIGINT_COLUMN2)),(sum(Double_COLUMN2)) from uniqdata_EXCLUDEDICTIONARY  where cust_id between 9000 and  9100 and dob between '1970-01-01 01:00:03' and '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1968-02-26 02:00:03' and doj<='1973-02-26 02:00:03' and DECIMAL_COLUMN2>=22348957.1234 and Double_COLUMN1>=11267489.7976  and Double_COLUMN2>=-1123224567489.7976 and Double_COLUMN2<=-3457489.7976""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_022
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_022", Include) {
  checkAnswer(s"""select distinct(count(Double_COLUMN2)),(count(integer_column1)),(count(BIGINT_COLUMN2)),(count(DECIMAL_COLUMN2)),count(ACTIVE_EMUI_VERSION),count(dob) from uniqdata_EXCLUDEDICTIONARY where doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) """,
    s"""select distinct(count(Double_COLUMN2)),(count(integer_column1)),(count(BIGINT_COLUMN2)),(count(DECIMAL_COLUMN2)),count(ACTIVE_EMUI_VERSION),count(dob) from uniqdata_EXCLUDEDICTIONARY_hive where doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) """)
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_024
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_024", Include) {
  checkAnswer(s"""select distinct(max(BIGINT_COLUMN2)),(max(integer_column1)),(max(DECIMAL_COLUMN2)),(max(Double_COLUMN2)),max(ACTIVE_EMUI_VERSION),max(dob) from uniqdata_EXCLUDEDICTIONARY where cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03' and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 )
""",
    s"""select distinct(max(BIGINT_COLUMN2)),(max(integer_column1)),(max(DECIMAL_COLUMN2)),(max(Double_COLUMN2)),max(ACTIVE_EMUI_VERSION),max(dob) from uniqdata_EXCLUDEDICTIONARY_hive where cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03' and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 )
""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_025
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_025", Include) {
  checkAnswer(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where cust_id between 9006 and 9080 and cust_name not rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) group by CUST_ID,dob,ACTIVE_EMUI_VERSION order by CUST_ID,dob,ACTIVE_EMUI_VERSION""",
    s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where cust_id between 9006 and 9080 and cust_name not rlike 'ACTIVE' and cust_name like 'CUST%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between -223372036853 and -223372036773   and Double_COLUMN2 in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567489.7976 ) group by CUST_ID,dob,ACTIVE_EMUI_VERSION order by CUST_ID,dob,ACTIVE_EMUI_VERSION""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_026
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_026", Include) {
  checkAnswer(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""",
    s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_027
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_027", Include) {
  checkAnswer(s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490 ) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""",
    s"""select avg(CUST_ID),sum(integer_column1),count(integer_column1),max(CUST_NAME),min(dob),avg(BIGINT_COLUMN2),sum(DECIMAL_COLUMN2),count(Double_COLUMN2),dob,ACTIVE_EMUI_VERSION from uniqdata_EXCLUDEDICTIONARY_hive where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'cust%' and doj>='1970-01-07 02:00:03'
and doj<='1970-03-22 02:00:03' and  BIGINT_COLUMN2 between 23372036853 and -223372034863   and Double_COLUMN2 not in (-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490,-11234567490 ) group by CUST_ID,dob,ACTIVE_EMUI_VERSION sort by CUST_ID,dob,ACTIVE_EMUI_VERSION""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_028
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_028", Include) {
  sql(s"""select distinct(instr(ACTIVE_EMUI_VERSION,'Active')),length(cust_name),locate(ACTIVE_EMUI_VERSION,'Active'),lower(cust_name),ltrim(cust_name),repeat(ACTIVE_EMUI_VERSION,1),reverse(cust_name),rpad(ACTIVE_EMUI_VERSION,5,'Cust'),rtrim(cust_name),space(ACTIVE_EMUI_VERSION),split(cust_name,2),substr(ACTIVE_EMUI_VERSION,5,length(ACTIVE_EMUI_VERSION)),trim(cust_name),unbase64(ACTIVE_EMUI_VERSION),Upper(cust_name),initcap(ACTIVE_EMUI_VERSION),soundex(cust_name) from uniqdata_EXCLUDEDICTIONARY where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-04-06 02:00:03'
and doj<='1971-02-05 02:00:03' and  BIGINT_COLUMN2 between -223372036759  and -223372036454   and cust_id not in(9500,9501,9506,9600,9700,9800,10000,9788)""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_029
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_029", Include) {
  sql(s"""select(instr(ACTIVE_EMUI_VERSION,'Active')),length(cust_name),locate(ACTIVE_EMUI_VERSION,'Active'),lower(cust_name),ltrim(cust_name),repeat(ACTIVE_EMUI_VERSION,1),reverse(cust_name),rpad(ACTIVE_EMUI_VERSION,5,'Cust'),rtrim(cust_name),space(ACTIVE_EMUI_VERSION),split(cust_name,2),substr(ACTIVE_EMUI_VERSION,5,length(ACTIVE_EMUI_VERSION)),trim(cust_name),unbase64(ACTIVE_EMUI_VERSION),Upper(cust_name),initcap(ACTIVE_EMUI_VERSION),soundex(cust_name) from uniqdata_EXCLUDEDICTIONARY where cust_id between 9095 and 9400 and cust_name not rlike 'abc' and cust_name like 'CUST%' and doj>='1970-04-06 02:00:03'
and doj<='1971-02-05 02:00:03' and  BIGINT_COLUMN2 between -223372036759  and -223372036454   and cust_id not in(9500,9501,9506,9600,9700,9800,10000,9788) group by ACTIVE_EMUI_VERSION,cust_name 
having count(length(cust_name))>=1 and min(lower(cust_name))not like '%abc%'""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_030
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_030", Include) {
  sql(s"""select cust_id,cust_name,to_date(doj),quarter(dob),month(doj),day(dob),hour(doj),minute(dob),second(doj),weekofyear(dob),datediff(doj,current_date),date_add(dob,4),date_sub(doj,1),to_utc_timestamp(doj,current_date),add_months(dob,5),last_day(doj),months_between(doj,current_date),date_format(dob,current_date) from uniqdata_EXCLUDEDICTIONARY where substr(cust_name,0,4)='CUST' and length(cust_name)in(15,14,13,16) and ACTIVE_EMUI_VERSION rlike 'ACTIVE' and month(dob)=01  and minute(dob)=0  group by doj,cust_id,cust_name,ACTIVE_EMUI_VERSION,dob having  max(cust_id)=10830 and count(distinct(cust_id))<=2001 and max(cust_name) not like '%def%' order by cust_id,cust_name""").collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_032
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_032", Include) {
  sql(s"""select variance(Double_COLUMN2),var_pop(Double_COLUMN1),var_samp(BIGINT_COLUMN2),stddev_pop(Double_COLUMN2),stddev_samp(DECIMAL_COLUMN2),covar_pop(DECIMAL_COLUMN2,DECIMAL_COLUMN1),covar_samp(DECIMAL_COLUMN2,DECIMAL_COLUMN1),corr(Double_COLUMN2,Double_COLUMN1),corr(DECIMAL_COLUMN2,DECIMAL_COLUMN1),percentile(cust_id,0.25),percentile_approx(BIGINT_COLUMN1,0.25,5) from uniqdata_EXCLUDEDICTIONARY where cust_id not in (9002,9003,9004,9005,9006,9007,9008,9009,9010,9011,9012,9013,9014,9015,9016,9017,9018,9019,9021,9022,9023,9024,9025,9027,9028,9029,9030) and cust_name like 'CUST%' and dob> '1970-01-01 01:00:03' and dob< '1975-06-23 01:00:03' and ACTIVE_EMUI_VERSION not like '%abc%' """).collect
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_036
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_036", Include) {
  checkAnswer(s"""select covar_pop(Double_COLUMN2,Double_COLUMN2) as a  from (select Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY where cust_id between 9000 and 10000 and cust_name like 'Cust%' and dob>='1970-01-01 00:00:00'  order by Double_COLUMN2 ) t""",
    s"""select covar_pop(Double_COLUMN2,Double_COLUMN2) as a  from (select Double_COLUMN2 from uniqdata_EXCLUDEDICTIONARY_hive where cust_id between 9000 and 10000 and cust_name like 'Cust%' and dob>='1970-01-01 00:00:00'  order by Double_COLUMN2 ) t""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_037
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_037", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 as a  from (select * from uniqdata_EXCLUDEDICTIONARY order by Double_COLUMN2)t where cust_name in ('CUST_NAME_01987','CUST_NAME_01988','CUST_NAME_01989','CUST_NAME_01990','CUST_NAME_01991' ,'CUST_NAME_01992')
""",
    s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 as a  from (select * from uniqdata_EXCLUDEDICTIONARY_hive order by Double_COLUMN2)t where cust_name in ('CUST_NAME_01987','CUST_NAME_01988','CUST_NAME_01989','CUST_NAME_01990','CUST_NAME_01991' ,'CUST_NAME_01992')
""")
}
       

//uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_038
test("uniqdata_EXCLUDEDICTIONARY_MultiBitSet_TC_038", Include) {
  checkAnswer(s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from uniqdata_EXCLUDEDICTIONARY where  ACTIVE_EMUI_VERSION rlike 'ACTIVE' and dob between '1970-10-15 01:00:03' and '1971-04-29 01:00:03' and doj between '1970-10-15 02:00:03' and '1971-04-29 02:00:03' and cust_name like 'CUST%'
""",
    s"""select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from uniqdata_EXCLUDEDICTIONARY_hive where  ACTIVE_EMUI_VERSION rlike 'ACTIVE' and dob between '1970-10-15 01:00:03' and '1971-04-29 01:00:03' and doj between '1970-10-15 02:00:03' and '1971-04-29 02:00:03' and cust_name like 'CUST%'
""")
}
       
override def afterAll {
sql("drop table if exists uniqdata_excludedictionary")
sql("drop table if exists uniqdata_excludedictionary_hive")
sql("drop table if exists uniqdata_EXCLUDEDICTIONARY")
sql("drop table if exists uniqdata_EXCLUDEDICTIONARY_hive")
}
}
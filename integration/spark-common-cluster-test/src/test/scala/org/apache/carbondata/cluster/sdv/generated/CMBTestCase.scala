
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
 * Test Class for cmb to verify all scenerios
 */

class CMBTestCase extends QueryTest with BeforeAndAfterAll {
         

//CMBC_CreateCube_1_Drop
test("CMBC_CreateCube_1_Drop", Include) {
  sql(s"""drop table if exists  cmb""").collect

  sql(s"""drop table if exists  cmb_hive""").collect

}
       

//CMBC_CreateCube_1
test("CMBC_CreateCube_1", Include) {
  sql(s"""CREATE table cmb (Cust_UID String,year String, month String, companyNumber String, familyadNumber String,  companyAddress String, company String, occupation String, certicardValidTime String, race String, CerticardCity String, birthday String, VIPLevel String, ageRange String, familyaddress String, dimension16 String, SubsidaryBank String, AccountCreationTime String, dimension19 String, dimension20 String, DemandDeposits double, TimeDeposits double, financial double, TreasuryBonds double, fund double, incomeOneyear double, outcomeOneyear double, insurance double, Goldaccount double, dollarDeposits int, euroDeposits int, euroDeposits1 double, euroDeposits2 double, yenDeposits int, wonDeposits int, rupeeDeposits double, HongKongDeposits double, numberoftransactions int, measure19 double, measure20 double, measure21 int, measure22 double, measure23 double, measure24 int, measure25 double, measure26 double, measure27 int, measure28 double, measure29 int, measure30 double, measure31 double, measure32 double, measure33 double, measure34 int, measure35 double, measure36 double, measure37 int, measure38 double, measure39 double, measure40 int, measure41 double, measure42 double, measure43 int, measure44 double, measure45 int, measure46 double, measure47 int, measure48 double, measure49 int, measure50 double, measure51 int, measure52 double, measure53 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_INCLUDE'='dollarDeposits,euroDeposits,yenDeposits,wonDeposits,numberoftransactions,measure21,measure24,measure27,measure29,measure34,measure37,measure40,measure43,measure45,measure47,measure49,measure51,measure53')""").collect

  sql(s"""CREATE table cmb_hive (Cust_UID String,year String, month String, companyAddress String,companyNumber String,company String,occupation String, certicardValidTime String,race String, CerticardCity String,birthday String, VIPLevel String, ageRange String, familyaddress String,familyadNumber String, dimension16 String, SubsidaryBank String, AccountCreationTime String, dimension19 String, dimension20 String, DemandDeposits double, TimeDeposits double, financial double, TreasuryBonds double, fund double, incomeOneyear double, outcomeOneyear double, insurance double, Goldaccount double, dollarDeposits int, euroDeposits int, euroDeposits1 double, euroDeposits2 double, yenDeposits int, wonDeposits int, rupeeDeposits double, HongKongDeposits double, numberoftransactions int, measure19 double, measure20 double, measure21 int, measure22 double, measure23 double, measure24 int, measure25 double, measure26 double, measure27 int, measure28 double, measure29 int, measure30 double, measure31 double, measure32 double, measure33 double, measure34 int, measure35 double, measure36 double, measure37 int, measure38 double, measure39 double, measure40 int, measure41 double, measure42 double, measure43 int, measure44 double, measure45 int, measure46 double, measure47 int, measure48 double, measure49 int, measure50 double, measure51 int, measure52 double, measure53 int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//CMBC_Query_1
test("CMBC_Query_1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/cmb/data.csv'  INTO table cmb OPTIONS ('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='Cust_UID,year,month,companyAddress,companyNumber,company,occupation,certicardValidTime,race,CerticardCity,birthday,VIPLevel,ageRange,familyaddress,familyadNumber,dimension16,SubsidaryBank,AccountCreationTime,dimension19,dimension20,DemandDeposits,TimeDeposits,financial,TreasuryBonds,fund,incomeOneyear,outcomeOneyear,insurance,Goldaccount,dollarDeposits,euroDeposits,euroDeposits1,euroDeposits2,yenDeposits,wonDeposits,rupeeDeposits,HongKongDeposits,numberoftransactions,measure19,measure20,measure21,measure22,measure23,measure24,measure25,measure26,measure27,measure28,measure29,measure30,measure31,measure32,measure33,measure34,measure35,measure36,measure37,measure38,measure39,measure40,measure41,measure42,measure43,measure44,measure45,measure46,measure47,measure48,measure49,measure50,measure51,measure52,measure53')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/cmb/data.csv'  INTO table cmb_hive """).collect

}
       

//CMBC_Query_2
test("CMBC_Query_2", Include) {
  checkAnswer(s"""select count(*) from cmb""",
    s"""select count(*) from cmb_hive""")
}
       

//CMBC_Query_3
test("CMBC_Query_3", Include) {
  checkAnswer(s"""select COUNT(DISTINCT Cust_UID) from cmb""",
    s"""select COUNT(DISTINCT Cust_UID) from cmb_hive""")
}
       

//CMBC_Query_4
test("CMBC_Query_4", Include) {
  checkAnswer(s"""SELECT `year`, `month`, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE `year` = "2015" GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""",
    s"""SELECT `year`, `month`, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE `year` = "2015" GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""")
}
       

//CMBC_Query_5
test("CMBC_Query_5", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, occupation, VIPLevel, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE ( ( occupation = "Administrative Support") AND ( SubsidaryBank = "ABN AMRO")) AND ( SubsidaryBank = "ABN AMRO") GROUP BY SubsidaryBank, occupation, VIPLevel ORDER BY SubsidaryBank ASC, occupation ASC, VIPLevel ASC""",
    s"""SELECT SubsidaryBank, occupation, VIPLevel, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE ( ( occupation = "Administrative Support") AND ( SubsidaryBank = "ABN AMRO")) AND ( SubsidaryBank = "ABN AMRO") GROUP BY SubsidaryBank, occupation, VIPLevel ORDER BY SubsidaryBank ASC, occupation ASC, VIPLevel ASC""")
}
       

//CMBC_Query_6
test("CMBC_Query_6", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""",
    s"""SELECT SubsidaryBank, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""")
}
       

//CMBC_Query_7
test("CMBC_Query_7", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE SubsidaryBank IN ("ABN AMRO","Bank Sepah") GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""",
    s"""SELECT SubsidaryBank, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE SubsidaryBank IN ("ABN AMRO","Bank Sepah") GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""")
}
       

//CMBC_Query_8
test("CMBC_Query_8", Include) {
  checkAnswer(s"""SELECT company, CerticardCity, VIPLevel, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE ( company IN ("Agricultural Bank of China","COSCO1")) AND ( CerticardCity IN ("Beijing1","Huangyan1","Yakeshi1","Korla1")) GROUP BY company, CerticardCity, VIPLevel ORDER BY company ASC, CerticardCity ASC, VIPLevel ASC""",
    s"""SELECT company, CerticardCity, VIPLevel, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE ( company IN ("Agricultural Bank of China","COSCO1")) AND ( CerticardCity IN ("Beijing1","Huangyan1","Yakeshi1","Korla1")) GROUP BY company, CerticardCity, VIPLevel ORDER BY company ASC, CerticardCity ASC, VIPLevel ASC""")
}
       

//CMBC_Query_9
test("CMBC_Query_9", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, ageRange, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE ( ageRange IN ("(1-3)","(100-105)")) AND ( SubsidaryBank IN ("ABN AMRO","Busan Bank","Huaxia Bank")) GROUP BY SubsidaryBank, ageRange ORDER BY SubsidaryBank ASC, ageRange ASC""",
    s"""SELECT SubsidaryBank, ageRange, COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE ( ageRange IN ("(1-3)","(100-105)")) AND ( SubsidaryBank IN ("ABN AMRO","Busan Bank","Huaxia Bank")) GROUP BY SubsidaryBank, ageRange ORDER BY SubsidaryBank ASC, ageRange ASC""")
}
       

//CMBC_Query_10
  ignore("CMBC_Query_10", Include) {
  sql(s"""SELECT SubsidaryBank, SUM(incomeOneyear) AS Sum_incomeOneyear, SUM(numberoftransactions) AS Sum_numberoftransactions FROM (select * from cmb) SUB_QRY WHERE SubsidaryBank IN ("Bank Bumiputera Indonesia","Daegu Bank","Real-Estate Bank") GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""").collect
}
       

//CMBC_Query_11
  ignore("CMBC_Query_11", Include) {
  sql(s"""SELECT `year`, `month`, SUM(DemandDeposits) AS Sum_DemandDeposits, SUM(numberoftransactions) AS Sum_numberoftransactions, SUM(yenDeposits) AS Sum_yenDeposits FROM (select * from cmb) SUB_QRY WHERE ( SubsidaryBank = "CMB Financial Leasing Ltd") AND ( Cust_UID = "CMB0000000000000000000000") GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""").collect
}
       

//CMBC_Query_12
  ignore("CMBC_Query_12", Include) {
  sql(s"""SELECT `year`, `month`, SUM(yenDeposits) AS Sum_yenDeposits, SUM(HongKongDeposits) AS Sum_HongKongDeposits, SUM(dollarDeposits) AS Sum_dollarDeposits, SUM(euroDeposits) AS Sum_euroDeposits FROM (select * from cmb) SUB_QRY WHERE ( SubsidaryBank = "Credit Suisse") AND ( `month` IN ("1","2","3")) GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""").collect
}
       

//CMBC_Query_13
test("CMBC_Query_13", Include) {
  checkAnswer(s"""SELECT Cust_UID, `month`, `year`, SUM(yenDeposits) AS Sum_yenDeposits FROM (select * from cmb) SUB_QRY WHERE Cust_UID IN ("CMB0000000000000000000119","CMB0000000000000000000308") and month="1" GROUP BY Cust_UID, `month`, `year` ORDER BY Cust_UID ASC, `month` ASC, `year` ASC""",
    s"""SELECT Cust_UID, `month`, `year`, SUM(yenDeposits) AS Sum_yenDeposits FROM (select * from cmb_hive) SUB_QRY WHERE Cust_UID IN ("CMB0000000000000000000119","CMB0000000000000000000308") and month="1" GROUP BY Cust_UID, `month`, `year` ORDER BY Cust_UID ASC, `month` ASC, `year` ASC""")
}
       

//CMBC_Query_14
test("CMBC_Query_14", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb) SUB_QRY WHERE SubsidaryBank = "Daegu Bank" GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""",
    s"""SELECT SubsidaryBank, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE SubsidaryBank = "Daegu Bank" GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""")
}
       

//CMBC_Query_15
test("CMBC_Query_15", Include) {
  checkAnswer(s"""SELECT COUNT(Cust_UID) AS Count_Cust_UID, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb) SUB_QRY WHERE ( SubsidaryBank IN ("Bank Bumiputera Indonesia","Daegu Bank","Minsheng Bank - First private bank in China")) AND ( dollarDeposits > 0)""",
    s"""SELECT COUNT(Cust_UID) AS Count_Cust_UID, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb_hive) SUB_QRY WHERE ( SubsidaryBank IN ("Bank Bumiputera Indonesia","Daegu Bank","Minsheng Bank - First private bank in China")) AND ( dollarDeposits > 0)""")
}
       

//CMBC_Query_16
test("CMBC_Query_16", Include) {
  checkAnswer(s"""SELECT SubsidaryBank, SUM(numberoftransactions) AS Sum_numberoftransactions FROM (select * from cmb) SUB_QRY WHERE SubsidaryBank IN ("Bank Bumiputera Indonesia","Daegu Bank") and month="1" GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""",
    s"""SELECT SubsidaryBank, SUM(numberoftransactions) AS Sum_numberoftransactions FROM (select * from cmb_hive) SUB_QRY WHERE SubsidaryBank IN ("Bank Bumiputera Indonesia","Daegu Bank") and month="1" GROUP BY SubsidaryBank ORDER BY SubsidaryBank ASC""")
}
       

//CMBC_Query_17
test("CMBC_Query_17", Include) {
  checkAnswer(s"""SELECT COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb) SUB_QRY WHERE ( SubsidaryBank = "ABC") AND ( numberoftransactions > 90.0)""",
    s"""SELECT COUNT(Cust_UID) AS Count_Cust_UID FROM (select * from cmb_hive) SUB_QRY WHERE ( SubsidaryBank = "ABC") AND ( numberoftransactions > 90.0)""")
}
       

//CMBC_Query_18
test("CMBC_Query_18", Include) {
  checkAnswer(s"""SELECT VIPLevel, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb) SUB_QRY GROUP BY VIPLevel ORDER BY VIPLevel ASC""",
    s"""SELECT VIPLevel, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb_hive) SUB_QRY GROUP BY VIPLevel ORDER BY VIPLevel ASC""")
}
       

//CMBC_Query_19
test("CMBC_Query_19", Include) {
  checkAnswer(s"""SELECT CerticardCity, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb) SUB_QRY GROUP BY CerticardCity ORDER BY CerticardCity ASC""",
    s"""SELECT CerticardCity, COUNT(DISTINCT Cust_UID) AS DistinctCount_Cust_UID FROM (select * from cmb_hive) SUB_QRY GROUP BY CerticardCity ORDER BY CerticardCity ASC""")
}
       

//CMBC_Query_20
test("CMBC_Query_20", Include) {
  checkAnswer(s"""SELECT VIPLevel, SUM(yenDeposits) AS Sum_yenDeposits, SUM(numberoftransactions) AS Sum_numberoftransactions, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb) SUB_QRY GROUP BY VIPLevel ORDER BY VIPLevel ASC""",
    s"""SELECT VIPLevel, SUM(yenDeposits) AS Sum_yenDeposits, SUM(numberoftransactions) AS Sum_numberoftransactions, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb_hive) SUB_QRY GROUP BY VIPLevel ORDER BY VIPLevel ASC""")
}
       

//CMBC_Query_21
test("CMBC_Query_21", Include) {
  checkAnswer(s"""SELECT CerticardCity, SUM(yenDeposits) AS Sum_yenDeposits, SUM(numberoftransactions) AS Sum_numberoftransactions, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb) SUB_QRY GROUP BY CerticardCity ORDER BY CerticardCity ASC""",
    s"""SELECT CerticardCity, SUM(yenDeposits) AS Sum_yenDeposits, SUM(numberoftransactions) AS Sum_numberoftransactions, SUM(dollarDeposits) AS Sum_dollarDeposits FROM (select * from cmb_hive) SUB_QRY GROUP BY CerticardCity ORDER BY CerticardCity ASC""")
}
       

//CMBC_Query_22
test("CMBC_Query_22", Include) {
  checkAnswer(s"""SELECT `year`, `month`, COUNT(Cust_UID) AS Count_Cust_UID, SUM(yenDeposits) AS Sum_yenDeposits FROM (select * from cmb) SUB_QRY WHERE ( `month` = "1") AND ( numberoftransactions > 90.0) GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""",
    s"""SELECT `year`, `month`, COUNT(Cust_UID) AS Count_Cust_UID, SUM(yenDeposits) AS Sum_yenDeposits FROM (select * from cmb_hive) SUB_QRY WHERE ( `month` = "1") AND ( numberoftransactions > 90.0) GROUP BY `year`, `month` ORDER BY `year` ASC, `month` ASC""")
}
       
override def afterAll {
sql("drop table if exists cmb")
sql("drop table if exists cmb_hive")
}
}
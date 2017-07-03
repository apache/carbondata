
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
 * Test Class for Traffic2G3G4Gvmall to verify all scenerios
 */

class TRAFFIC2G3G4GVMALLTestCase extends QueryTest with BeforeAndAfterAll {
         

//drop_smart
test("drop_smart", Include) {
  sql(s"""drop table if exists Traffic_2G_3G_4G_vmall""").collect

  sql(s"""drop table if exists Traffic_2G_3G_4G_vmall_hive""").collect

}
       

//SmartPCC_CreateCube_TC_001
test("SmartPCC_CreateCube_TC_001", Include) {
  sql(s"""create table IF NOT EXISTS traffic_2g_3g_4g_vmall (SOURCE_INFO String ,APP_CATEGORY_ID String ,APP_CATEGORY_NAME String ,APP_SUB_CATEGORY_ID String ,APP_SUB_CATEGORY_NAME String ,RAT_NAME String ,IMSI String ,OFFER_MSISDN String ,OFFER_ID String ,OFFER_OPTION_1 String ,OFFER_OPTION_2 String ,OFFER_OPTION_3 String ,MSISDN String ,PACKAGE_TYPE String ,PACKAGE_PRICE String ,TAG_IMSI String ,TAG_MSISDN String ,PROVINCE String ,CITY String ,AREA_CODE String ,TAC String ,IMEI String ,TERMINAL_TYPE String ,TERMINAL_BRAND String ,TERMINAL_MODEL String ,PRICE_LEVEL String ,NETWORK String ,SHIPPED_OS String ,WIFI String ,WIFI_HOTSPOT String ,GSM String ,WCDMA String ,TD_SCDMA String ,LTE_FDD String ,LTE_TDD String ,CDMA String ,SCREEN_SIZE String ,SCREEN_RESOLUTION String ,HOST_NAME String ,WEBSITE_NAME String ,OPERATOR String ,SRV_TYPE_NAME String ,TAG_HOST String ,CGI String ,CELL_NAME String ,COVERITY_TYPE1 String ,COVERITY_TYPE2 String ,COVERITY_TYPE3 String ,COVERITY_TYPE4 String ,COVERITY_TYPE5 String ,LATITUDE String ,LONGITUDE String ,AZIMUTH String ,TAG_CGI String ,APN String ,USER_AGENT String ,DAY String ,HOUR String ,MIN String ,IS_DEFAULT_BEAR int ,EPS_BEARER_ID String ,QCI int ,USER_FILTER String ,ANALYSIS_PERIOD String, UP_THROUGHPUT int,DOWN_THROUGHPUT int,UP_PKT_NUM int,DOWN_PKT_NUM int,APP_REQUEST_NUM int,PKT_NUM_LEN_1_64 int,PKT_NUM_LEN_64_128 int,PKT_NUM_LEN_128_256 int,PKT_NUM_LEN_256_512 int,PKT_NUM_LEN_512_768 int,PKT_NUM_LEN_768_1024 int,PKT_NUM_LEN_1024_ALL int,IP_FLOW_MARK int) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table IF NOT EXISTS traffic_2g_3g_4g_vmall_hive (SOURCE_INFO String ,APP_CATEGORY_ID String ,APP_CATEGORY_NAME String ,APP_SUB_CATEGORY_ID String ,APP_SUB_CATEGORY_NAME String ,RAT_NAME String ,IMSI String ,OFFER_MSISDN String ,OFFER_ID String ,OFFER_OPTION_1 String ,OFFER_OPTION_2 String ,OFFER_OPTION_3 String ,MSISDN String ,PACKAGE_TYPE String ,PACKAGE_PRICE String ,TAG_IMSI String ,TAG_MSISDN String ,PROVINCE String ,CITY String ,AREA_CODE String ,TAC String ,IMEI String ,TERMINAL_TYPE String ,TERMINAL_BRAND String ,TERMINAL_MODEL String ,PRICE_LEVEL String ,NETWORK String ,SHIPPED_OS String ,WIFI String ,WIFI_HOTSPOT String ,GSM String ,WCDMA String ,TD_SCDMA String ,LTE_FDD String ,LTE_TDD String ,CDMA String ,SCREEN_SIZE String ,SCREEN_RESOLUTION String ,HOST_NAME String ,WEBSITE_NAME String ,OPERATOR String ,SRV_TYPE_NAME String ,TAG_HOST String ,CGI String ,CELL_NAME String ,COVERITY_TYPE1 String ,COVERITY_TYPE2 String ,COVERITY_TYPE3 String ,COVERITY_TYPE4 String ,COVERITY_TYPE5 String ,LATITUDE String ,LONGITUDE String ,AZIMUTH String ,TAG_CGI String ,APN String ,USER_AGENT String ,DAY String ,HOUR String ,MIN String ,IS_DEFAULT_BEAR int ,EPS_BEARER_ID String ,QCI int ,USER_FILTER String ,ANALYSIS_PERIOD String, UP_THROUGHPUT int,DOWN_THROUGHPUT int,UP_PKT_NUM int,DOWN_PKT_NUM int,APP_REQUEST_NUM int,PKT_NUM_LEN_1_64 int,PKT_NUM_LEN_64_128 int,PKT_NUM_LEN_128_256 int,PKT_NUM_LEN_256_512 int,PKT_NUM_LEN_512_768 int,PKT_NUM_LEN_768_1024 int,PKT_NUM_LEN_1024_ALL int,IP_FLOW_MARK int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//SmartPCC_DataLoad_TC_001
test("SmartPCC_DataLoad_TC_001", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table traffic_2g_3g_4g_vmall OPTIONS ('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,APP_SUB_CATEGORY_ID,APP_SUB_CATEGORY_NAME,RAT_NAME,IMSI,OFFER_MSISDN,OFFER_ID,OFFER_OPTION_1,OFFER_OPTION_2,OFFER_OPTION_3,MSISDN,PACKAGE_TYPE,PACKAGE_PRICE,TAG_IMSI,TAG_MSISDN,PROVINCE,CITY,AREA_CODE,TAC,IMEI,TERMINAL_TYPE,TERMINAL_BRAND,TERMINAL_MODEL,PRICE_LEVEL,NETWORK,SHIPPED_OS,WIFI,WIFI_HOTSPOT,GSM,WCDMA,TD_SCDMA,LTE_FDD,LTE_TDD,CDMA,SCREEN_SIZE,SCREEN_RESOLUTION,HOST_NAME,WEBSITE_NAME,OPERATOR,SRV_TYPE_NAME,TAG_HOST,CGI,CELL_NAME,COVERITY_TYPE1,COVERITY_TYPE2,COVERITY_TYPE3,COVERITY_TYPE4,COVERITY_TYPE5,LATITUDE,LONGITUDE,AZIMUTH,TAG_CGI,APN,USER_AGENT,DAY,HOUR,MIN,IS_DEFAULT_BEAR,EPS_BEARER_ID,QCI,USER_FILTER,ANALYSIS_PERIOD,UP_THROUGHPUT,DOWN_THROUGHPUT,UP_PKT_NUM,DOWN_PKT_NUM,APP_REQUEST_NUM,PKT_NUM_LEN_1_64,PKT_NUM_LEN_64_128,PKT_NUM_LEN_128_256,PKT_NUM_LEN_256_512,PKT_NUM_LEN_512_768,PKT_NUM_LEN_768_1024,PKT_NUM_LEN_1024_ALL,IP_FLOW_MARK')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table traffic_2g_3g_4g_vmall_hive """).collect

}
       

//SmartPCC_Perf_TC_001
test("SmartPCC_Perf_TC_001", Include) {
  sql(s"""select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN""").collect
}
       

//SmartPCC_Perf_TC_002
test("SmartPCC_Perf_TC_002", Include) {
  checkAnswer(s"""select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc""",
    s"""select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc""")
}
       

//SmartPCC_Perf_TC_003
test("SmartPCC_Perf_TC_003", Include) {
  checkAnswer(s"""select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc""",
    s"""select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc""")
}
       

//SmartPCC_Perf_TC_004
test("SmartPCC_Perf_TC_004", Include) {
  sql(s"""select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number, sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by APP_CATEGORY_NAME order by APP_CATEGORY_NAME""").collect
}
       

//SmartPCC_Perf_TC_005
test("SmartPCC_Perf_TC_005", Include) {
  checkAnswer(s"""select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc""",
    s"""select APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where APP_CATEGORY_NAME='Web_Browsing' group by APP_CATEGORY_NAME order by msidn_number desc""")
}
       

//SmartPCC_Perf_TC_006
test("SmartPCC_Perf_TC_006", Include) {
  checkAnswer(s"""select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by APP_SUB_CATEGORY_NAME""",
    s"""select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by APP_SUB_CATEGORY_NAME""")
}
       

//SmartPCC_Perf_TC_007
test("SmartPCC_Perf_TC_007", Include) {
  checkAnswer(s"""select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc""",
    s"""select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc""")
}
       

//SmartPCC_Perf_TC_008
test("SmartPCC_Perf_TC_008", Include) {
  checkAnswer(s"""select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by TERMINAL_BRAND""",
    s"""select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by TERMINAL_BRAND""")
}
       

//SmartPCC_Perf_TC_009
test("SmartPCC_Perf_TC_009", Include) {
  checkAnswer(s"""select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc""",
    s"""select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where TERMINAL_BRAND='MARCONI' group by TERMINAL_BRAND order by msidn_number desc""")
}
       

//SmartPCC_Perf_TC_010
test("SmartPCC_Perf_TC_010", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by TERMINAL_TYPE""",
    s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by TERMINAL_TYPE""")
}
       

//SmartPCC_Perf_TC_011
test("SmartPCC_Perf_TC_011", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc""",
    s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall_hive  where RAT_NAME='GERAN' group by TERMINAL_TYPE order by total desc""")
}
       

//SmartPCC_Perf_TC_012
test("SmartPCC_Perf_TC_012", Include) {
  checkAnswer(s"""select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by CGI""",
    s"""select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by CGI""")
}
       

//SmartPCC_Perf_TC_013
test("SmartPCC_Perf_TC_013", Include) {
  checkAnswer(s"""select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where CGI='460003772902063' group by CGI order by total desc""",
    s"""select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where CGI='460003772902063' group by CGI order by total desc""")
}
       

//SmartPCC_Perf_TC_014
test("SmartPCC_Perf_TC_014", Include) {
  checkAnswer(s"""select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by RAT_NAME""",
    s"""select RAT_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by RAT_NAME""")
}
       

//SmartPCC_Perf_TC_015
test("SmartPCC_Perf_TC_015", Include) {
  checkAnswer(s"""select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by DAY,HOUR""",
    s"""select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by DAY,HOUR""")
}
       

//SmartPCC_Perf_TC_016
test("SmartPCC_Perf_TC_016", Include) {
  checkAnswer(s"""select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where hour between 20 and 24 group by DAY,HOUR order by total desc""",
    s"""select DAY,HOUR,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where hour between 20 and 24 group by DAY,HOUR order by total desc""")
}
       

//SmartPCC_Perf_TC_017
test("SmartPCC_Perf_TC_017", Include) {
  checkAnswer(s"""select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME""",
    s"""select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME""")
}
       

//SmartPCC_Perf_TC_018
test("SmartPCC_Perf_TC_018", Include) {
  checkAnswer(s"""select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc""",
    s"""select APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where  APP_CATEGORY_NAME='Web_Browsing' group by APP_SUB_CATEGORY_NAME,APP_CATEGORY_NAME order by total desc""")
}
       

//SmartPCC_Perf_TC_019
test("SmartPCC_Perf_TC_019", Include) {
  checkAnswer(s"""select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND""",
    s"""select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall_hive  group by APP_SUB_CATEGORY_NAME,TERMINAL_BRAND""")
}
       

//SmartPCC_Perf_TC_021
test("SmartPCC_Perf_TC_021", Include) {
  checkAnswer(s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall   group by APP_SUB_CATEGORY_NAME,RAT_NAME""",
    s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive   group by APP_SUB_CATEGORY_NAME,RAT_NAME""")
}
       

//SmartPCC_Perf_TC_022
test("SmartPCC_Perf_TC_022", Include) {
  checkAnswer(s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from traffic_2g_3g_4g_vmall where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc""",
    s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from traffic_2g_3g_4g_vmall_hive where  RAT_NAME='GERAN' group by APP_SUB_CATEGORY_NAME,RAT_NAME order by total desc""")
}
       

//SmartPCC_Perf_TC_023
test("SmartPCC_Perf_TC_023", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE""",
    s"""select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE""")
}
       

//SmartPCC_Perf_TC_024
test("SmartPCC_Perf_TC_024", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc""",
    s"""select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  where  CGI in('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order by total desc""")
}
       

//SmartPCC_Perf_TC_025
test("SmartPCC_Perf_TC_025", Include) {
  checkAnswer(s"""select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  group by HOUR,cgi,APP_SUB_CATEGORY_NAME""",
    s"""select HOUR,cgi,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT+down_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  group by HOUR,cgi,APP_SUB_CATEGORY_NAME""")
}
       

//SmartPCC_Perf_TC_026
test("SmartPCC_Perf_TC_026", Include) {
  checkAnswer(s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from traffic_2g_3g_4g_vmall  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE""",
    s"""select RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from traffic_2g_3g_4g_vmall_hive  group by RAT_NAME,APP_SUB_CATEGORY_NAME,TERMINAL_TYPE""")
}
       

//SmartPCC_CreateVIPTable_TC_001_Drop
test("SmartPCC_CreateVIPTable_TC_001_Drop", Include) {
  sql(s"""drop table if exists  viptable""").collect

  sql(s"""drop table if exists  viptable_hive""").collect

}
       

//SmartPCC_CreateVIPTable_TC_001
test("SmartPCC_CreateVIPTable_TC_001", Include) {
  sql(s"""create table viptable (SOURCE_INFO String,APP_CATEGORY_ID String,APP_CATEGORY_NAME String,APP_SUB_CATEGORY_ID String,APP_SUB_CATEGORY_NAME String,RAT_NAME String,IMSI String,OFFER_MSISDN String,OFFER_ID String,OFFER_OPTION_1 String,OFFER_OPTION_2 String,OFFER_OPTION_3 String,MSISDN String,PACKAGE_TYPE String,PACKAGE_PRICE String,TAG_IMSI String,TAG_MSISDN String,PROVINCE String,CITY String,AREA_CODE String,TAC String,IMEI String,TERMINAL_TYPE String,TERMINAL_BRAND String,TERMINAL_MODEL String,PRICE_LEVEL String,NETWORK String,SHIPPED_OS String,WIFI String,WIFI_HOTSPOT String,GSM String,WCDMA String,TD_SCDMA String,LTE_FDD String,LTE_TDD String,CDMA String,SCREEN_SIZE String,SCREEN_RESOLUTION String,HOST_NAME String,WEBSITE_NAME String,OPERATOR String,SRV_TYPE_NAME String,TAG_HOST String,CGI String,CELL_NAME String,COVERITY_TYPE1 String,COVERITY_TYPE2 String,COVERITY_TYPE3 String,COVERITY_TYPE4 String,COVERITY_TYPE5 String,LATITUDE String,LONGITUDE String,AZIMUTH String,TAG_CGI String,APN String,USER_AGENT String,DAY String,HOUR String,`MIN` String,IS_DEFAULT_BEAR int,EPS_BEARER_ID String,QCI int,USER_FILTER String,ANALYSIS_PERIOD String,UP_THROUGHPUT double,DOWN_THROUGHPUT double,UP_PKT_NUM double,DOWN_PKT_NUM double,APP_REQUEST_NUM double,PKT_NUM_LEN_1_64 double,PKT_NUM_LEN_64_128 double,PKT_NUM_LEN_128_256 double,PKT_NUM_LEN_256_512 double,PKT_NUM_LEN_512_768 double,PKT_NUM_LEN_768_1024 double,PKT_NUM_LEN_1024_ALL double,IP_FLOW_MARK double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table viptable_hive (SOURCE_INFO String,APP_CATEGORY_ID String,APP_CATEGORY_NAME String,APP_SUB_CATEGORY_ID String,APP_SUB_CATEGORY_NAME String,RAT_NAME String,IMSI String,OFFER_MSISDN String,OFFER_ID String,OFFER_OPTION_1 String,OFFER_OPTION_2 String,OFFER_OPTION_3 String,MSISDN String,PACKAGE_TYPE String,PACKAGE_PRICE String,TAG_IMSI String,TAG_MSISDN String,PROVINCE String,CITY String,AREA_CODE String,TAC String,IMEI String,TERMINAL_TYPE String,TERMINAL_BRAND String,TERMINAL_MODEL String,PRICE_LEVEL String,NETWORK String,SHIPPED_OS String,WIFI String,WIFI_HOTSPOT String,GSM String,WCDMA String,TD_SCDMA String,LTE_FDD String,LTE_TDD String,CDMA String,SCREEN_SIZE String,SCREEN_RESOLUTION String,HOST_NAME String,WEBSITE_NAME String,OPERATOR String,SRV_TYPE_NAME String,TAG_HOST String,CGI String,CELL_NAME String,COVERITY_TYPE1 String,COVERITY_TYPE2 String,COVERITY_TYPE3 String,COVERITY_TYPE4 String,COVERITY_TYPE5 String,LATITUDE String,LONGITUDE String,AZIMUTH String,TAG_CGI String,APN String,USER_AGENT String,DAY String,HOUR String,`MIN` String,IS_DEFAULT_BEAR int,EPS_BEARER_ID String,QCI int,USER_FILTER String,ANALYSIS_PERIOD String,UP_THROUGHPUT double,DOWN_THROUGHPUT double,UP_PKT_NUM double,DOWN_PKT_NUM double,APP_REQUEST_NUM double,PKT_NUM_LEN_1_64 double,PKT_NUM_LEN_64_128 double,PKT_NUM_LEN_128_256 double,PKT_NUM_LEN_256_512 double,PKT_NUM_LEN_512_768 double,PKT_NUM_LEN_768_1024 double,PKT_NUM_LEN_1024_ALL double,IP_FLOW_MARK double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//SmartPCC_LoadVIPTable_TC_002
test("SmartPCC_LoadVIPTable_TC_002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table viptable OPTIONS ('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,APP_SUB_CATEGORY_ID,APP_SUB_CATEGORY_NAME,RAT_NAME,IMSI,OFFER_MSISDN,OFFER_ID,OFFER_OPTION_1,OFFER_OPTION_2,OFFER_OPTION_3,MSISDN,PACKAGE_TYPE,PACKAGE_PRICE,TAG_IMSI,TAG_MSISDN,PROVINCE,CITY,AREA_CODE,TAC,IMEI,TERMINAL_TYPE,TERMINAL_BRAND,TERMINAL_MODEL,PRICE_LEVEL,NETWORK,SHIPPED_OS,WIFI,WIFI_HOTSPOT,GSM,WCDMA,TD_SCDMA,LTE_FDD,LTE_TDD,CDMA,SCREEN_SIZE,SCREEN_RESOLUTION,HOST_NAME,WEBSITE_NAME,OPERATOR,SRV_TYPE_NAME,TAG_HOST,CGI,CELL_NAME,COVERITY_TYPE1,COVERITY_TYPE2,COVERITY_TYPE3,COVERITY_TYPE4,COVERITY_TYPE5,LATITUDE,LONGITUDE,AZIMUTH,TAG_CGI,APN,USER_AGENT,DAY,HOUR,MIN,IS_DEFAULT_BEAR,EPS_BEARER_ID,QCI,USER_FILTER,ANALYSIS_PERIOD,UP_THROUGHPUT,DOWN_THROUGHPUT,UP_PKT_NUM,DOWN_PKT_NUM,APP_REQUEST_NUM,PKT_NUM_LEN_1_64,PKT_NUM_LEN_64_128,PKT_NUM_LEN_128_256,PKT_NUM_LEN_256_512,PKT_NUM_LEN_512_768,PKT_NUM_LEN_768_1024,PKT_NUM_LEN_1024_ALL,IP_FLOW_MARK')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table viptable_hive """).collect

}
       

//SmartPCC_Perf_TC_027
test("SmartPCC_Perf_TC_027", Include) {
  checkAnswer(s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN""",
    s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  t1, viptable_hive t2 where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN""")
}
       

//SmartPCC_Perf_TC_028
test("SmartPCC_Perf_TC_028", Include) {
  checkAnswer(s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN""",
    s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  t1, viptable_hive t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN""")
}
       

//SmartPCC_Perf_TC_029
test("SmartPCC_Perf_TC_029", Include) {
  checkAnswer(s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  t1, viptable t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc""",
    s"""select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  t1, viptable_hive t2 where t1.TERMINAL_BRAND='HTC' and t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN order by total desc""")
}
       

//SmartPCC_Perf_TC_030
test("SmartPCC_Perf_TC_030", Include) {
  checkAnswer(s"""select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall  t1, viptable t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN""",
    s"""select t2.MSISDN,t1.RAT_NAME,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum(t1.DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g_vmall_hive  t1, viptable_hive t2 where t1.MSISDN=t2.MSISDN group by t1.RAT_NAME,t2.MSISDN""")
}
       

//SmartPCC_Perf_TC_031
test("SmartPCC_Perf_TC_031", Include) {
  checkAnswer(s"""select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g_vmall  t1) t2 group by level
""",
    s"""select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760,'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level from  traffic_2g_3g_4g_vmall_hive  t1) t2 group by level
""")
}
       

//SmartPCC_Perf_TC_032
test("SmartPCC_Perf_TC_032", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G_vmall where MSISDN='8613993800024' group by TERMINAL_TYPE""",
    s"""select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G_vmall_hive where MSISDN='8613993800024' group by TERMINAL_TYPE""")
}
       

//SmartPCC_Perf_TC_033
test("SmartPCC_Perf_TC_033", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall where MSISDN='8613519003078' group by TERMINAL_TYPE""",
    s"""select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from   traffic_2g_3g_4g_vmall_hive where MSISDN='8613519003078' group by TERMINAL_TYPE""")
}
       

//SmartPCC_Perf_TC_034
test("SmartPCC_Perf_TC_034", Include) {
  checkAnswer(s"""select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G_vmall where MSISDN='8613993104233'""",
    s"""select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G_vmall_hive where MSISDN='8613993104233'""")
}
       

//SmartPCC_Perf_TC_035
test("SmartPCC_Perf_TC_035", Include) {
  checkAnswer(s"""select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G_vmall where MSISDN='8618394185970' and APP_CATEGORY_ID='2'""",
    s"""select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G_vmall_hive where MSISDN='8618394185970' and APP_CATEGORY_ID='2'""")
}
       

//SmartPCC_Perf_TC_036
test("SmartPCC_Perf_TC_036", Include) {
  checkAnswer(s"""select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'""",
    s"""select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall_hive where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'""")
}
       

//SmartPCC_Perf_TC_037
test("SmartPCC_Perf_TC_037", Include) {
  checkAnswer(s"""select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G_vmall where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'""",
    s"""select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G_vmall_hive where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'""")
}
       

//SmartPCC_Perf_TC_038
test("SmartPCC_Perf_TC_038", Include) {
  checkAnswer(s"""select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'""",
    s"""select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall_hive where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'""")
}
       

//SmartPCC_Perf_TC_039
test("SmartPCC_Perf_TC_039", Include) {
  checkAnswer(s"""select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'""",
    s"""select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G_vmall_hive where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'""")
}
       
override def afterAll {
sql("drop table if exists Traffic_2G_3G_4G_vmall")
sql("drop table if exists Traffic_2G_3G_4G_vmall_hive")
sql("drop table if exists traffic_2g_3g_4g_vmall")
sql("drop table if exists traffic_2g_3g_4g_vmall_hive")
sql("drop table if exists viptable")
sql("drop table if exists viptable_hive")
}
}

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
 * Test Class for smart500de to verify all scenerios
 */

class SMART500DETestCase extends QueryTest with BeforeAndAfterAll {
         

//C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01_Drop
test("C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01_Drop", Include) {
  sql(s"""drop table if exists  smart_500_de""").collect

  sql(s"""drop table if exists  smart_500_de_hive""").collect

}
       

//C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01
test("C20_SEQ_CreateTable-DICTIONARY_EXCLUDE-01", Include) {
  sql(s"""create table smart_500_DE (MSISDN string,IMSI string,IMEI string,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,SID double,PROBEID double,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,SERVER_DECIMAL Decimal,APN string,SGSN_SIG_IP string,GGSN_SIG_IP_BigInt_NEGATIVE bigint,SGSN_USER_IP string,GGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,TCP_STATES_BIGINTPOSITIVE int,TCP_WIN_SIZE int,TCP_MSS int,TCP_CONN_TIMES int,TCP_CONN_2_FAILED_TIMES int,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,GET_STREAMING_FAILED_CODE int,GET_STREAMING_FLAG int,GET_NUM int,GET_SUCCEED_NUM int,GET_RETRANS_NUM int,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,VIDEO_FRAME_RATE int,VIDEO_CODEC_ID string,VIDEO_WIDTH int,VIDEO_HEIGHT int,AUDIO_CODEC_ID string,MEDIA_FILE_TYPE int,PLAY_STATE int,STREAMING_FLAG int,TCP_STATUS_INDICATOR int,DISCONNECTION_FLAG int,FAILURE_CODE int,FLAG int,TAC string,ECI string,TCP_SYN_TIME_MSEL int,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,CHARGE_FLAG int,PREPAID_FLAG int,USER_AGENT string,MS_WIN_STAT_TOTAL_NUM int,MS_WIN_STAT_SMALL_NUM int,MS_ACK_TO_1STGET_DELAY int,SERVER_ACK_TO_1STDATA_DELAY int,STREAMING_TYPE int,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,LAYER3ID int,LAYER4ID int,LAYER5ID int,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,DNS_RETRANS_NUM int,DNS_FAIL_CODE int,FIRST_RAT int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,FIRST_LONGITUDE double,FIRST_LATITUDE double,FIRST_ALTITUDE int,FIRST_RASTERLONGITUDE double,FIRST_RASTERLATITUDE double,FIRST_RASTERALTITUDE int,FIRST_FREQUENCYSPOT int,FIRST_CLUTTER int,FIRST_USERBEHAVIOR int,FIRST_SPEED int,FIRST_CREDIBILITY int,LAST_LONGITUDE double,LAST_LATITUDE double,LAST_ALTITUDE int,LAST_RASTERLONGITUDE double,LAST_RASTERLATITUDE double,LAST_RASTERALTITUDE int,LAST_FREQUENCYSPOT int,LAST_CLUTTER int,LAST_USERBEHAVIOR int,LAST_SPEED int,LAST_CREDIBILITY int,IMEI_CIPHERTEXT string,APP_ID int,DOMAIN_NAME string,STREAMING_CACHE_IP string,STOP_LONGER_THAN_MIN_THRESHOLD int,STOP_LONGER_THAN_MAX_THRESHOLD int,PLAY_END_STAT int,STOP_START_TIME1 double,STOP_END_TIME1 double,STOP_START_TIME2 double,STOP_END_TIME2 double,STOP_START_TIME3 double,STOP_END_TIME3 double,STOP_START_TIME4 double,STOP_END_TIME4 double,STOP_START_TIME5 double,STOP_END_TIME5 double,STOP_START_TIME6 double,STOP_END_TIME6 double,STOP_START_TIME7 double,STOP_END_TIME7 double,STOP_START_TIME8 double,STOP_END_TIME8 double,STOP_START_TIME9 double,STOP_END_TIME9 double,STOP_START_TIME10 double,STOP_END_TIME10 double,FAIL_CLASS double,RECORD_TYPE double,NODATA_COUNT double,VIDEO_NODATA_DURATION double,VIDEO_SMOOTH_DURATION double,VIDEO_SD_DURATION double,VIDEO_HD_DURATION double,VIDEO_UHD_DURATION double,VIDEO_FHD_DURATION double,FLUCTUATION double,START_DOWNLOAD_THROUGHPUT double,L7_UL_GOODPUT_FULL_MSS double,SESSIONKEY string,FIRST_UCELLID double,LAST_UCELLID double,UCELLID1 double,LONGITUDE1 double,LATITUDE1 double,UCELLID2 double,LONGITUDE2 double,LATITUDE2 double,UCELLID3 double,LONGITUDE3 double,LATITUDE3 double,UCELLID4 double,LONGITUDE4 double,LATITUDE4 double,UCELLID5 double,LONGITUDE5 double,LATITUDE5 double,UCELLID6 double,LONGITUDE6 double,LATITUDE6 double,UCELLID7 double,LONGITUDE7 double,LATITUDE7 double,UCELLID8 double,LONGITUDE8 double,LATITUDE8 double,UCELLID9 double,LONGITUDE9 double,LATITUDE9 double,UCELLID10 double,LONGITUDE10 double,LATITUDE10 double,INTBUFFER_FULL_DELAY double,STALL_DURATION double,STREAMING_DW_PACKETS double,STREAMING_DOWNLOAD_DELAY double,PLAY_DURATION double,STREAMING_QUALITY int,VIDEO_DATA_RATE double,AUDIO_DATA_RATE double,STREAMING_FILESIZE double,STREAMING_DURATIOIN double,TCP_SYN_TIME double,TCP_RTT_STEP1 double,CHARGE_ID double,UL_REVERSE_TO_DL_DELAY double,DL_REVERSE_TO_UL_DELAY double,DATATRANS_DW_GOODPUT double,DATATRANS_DW_TOTAL_DURATION double,SUM_FRAGMENT_INTERVAL double,TCP_FIN_TIMES double,TCP_RESET_TIMES double,URL_CLASSIFICATION double,STREAMING_LQ_DURATIOIN double,MAX_DNS_DELAY double,MAX_DNS2SYN double,MAX_LATANCY_OF_LINK_SETUP double,MAX_SYNACK2FIRSTACK double,MAX_SYNACK2LASTACK double,MAX_ACK2GET_DELAY double,MAX_FRAG_INTERVAL_PREDELAY double,SUM_FRAG_INTERVAL_PREDELAY double,SERVICE_DELAY_MSEC double,HOMEPROVINCE double,HOMECITY double,SERVICE_ID double,CHARGING_CLASS double,DATATRANS_UL_DURATION double,ASSOCIATED_ID double,PACKET_LOSS_NUM double,JITTER double,MS_DNS_DELAY_MSEL double,GET_STREAMING_DELAY double,TCP_UL_RETRANS_WITHOUTPL double,TCP_DW_RETRANS_WITHOUTPL double,GET_MAX_UL_SIZE double,GET_MIN_UL_SIZE double,GET_MAX_DL_SIZE double,GET_MIN_DL_SIZE double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double,TCP_UL_RETRANS_WITHPL double,TCP_DW_RETRANS_WITHPL double,TCP_UL_PACKAGES_WITHPL double,TCP_DW_PACKAGES_WITHPL double,TCP_UL_PACKAGES_WITHOUTPL double,TCP_DW_PACKAGES_WITHOUTPL double,UPPERLAYER_IP_UL_PACKETS double,UPPERLAYER_IP_DL_PACKETS double,DOWNLAYER_IP_UL_PACKETS double,DOWNLAYER_IP_DL_PACKETS double,UPPERLAYER_IP_UL_FRAGMENTS double,UPPERLAYER_IP_DL_FRAGMENTS double,DOWNLAYER_IP_UL_FRAGMENTS double,DOWNLAYER_IP_DL_FRAGMENTS double,VALID_TRANS_DURATION double,AIR_PORT_DURATION double,RADIO_CONN_TIMES double,RAN_NE_ID double,AVG_UL_RTT double,AVG_DW_RTT double,UL_RTT_LONG_NUM int,DW_RTT_LONG_NUM int,UL_RTT_STAT_NUM int,DW_RTT_STAT_NUM int,USER_PROBE_UL_LOST_PKT int,SERVER_PROBE_UL_LOST_PKT int,SERVER_PROBE_DW_LOST_PKT int,USER_PROBE_DW_LOST_PKT int,CHARGING_CHARACTERISTICS double,DL_SERIOUS_OUT_OF_ORDER_NUM double,DL_SLIGHT_OUT_OF_ORDER_NUM double,DL_FLIGHT_TOTAL_SIZE double,DL_FLIGHT_TOTAL_NUM double,DL_MAX_FLIGHT_SIZE double,UL_SERIOUS_OUT_OF_ORDER_NUM double,UL_SLIGHT_OUT_OF_ORDER_NUM double,UL_FLIGHT_TOTAL_SIZE double,UL_FLIGHT_TOTAL_NUM double,UL_MAX_FLIGHT_SIZE double,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,DL_CONTINUOUS_RETRANSMISSION_DELAY double,USER_HUNGRY_DELAY double,SERVER_HUNGRY_DELAY double,AVG_DW_RTT_MICRO_SEC int,AVG_UL_RTT_MICRO_SEC int,FLOW_SAMPLE_RATIO int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ( 'DICTIONARY_EXCLUDE'='MSISDN,IMSI,IMEI,MS_IP,SERVER_IP,HOST,SP,MS_INDICATOR,streaming_url','DICTIONARY_INCLUDE'='SESSION_INDICATOR,SERVER_DECIMAL,TCP_STATES_BIGINTPOSITIVE')""").collect

  sql(s"""create table smart_500_DE_hive (MSISDN string,IMSI string,IMEI string,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,SID double,PROBEID double,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,SERVER_DECIMAL Decimal,APN string,SGSN_SIG_IP string,GGSN_SIG_IP_BigInt_NEGATIVE bigint,SGSN_USER_IP string,GGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,TCP_STATES_BIGINTPOSITIVE int,TCP_WIN_SIZE int,TCP_MSS int,TCP_CONN_TIMES int,TCP_CONN_2_FAILED_TIMES int,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,GET_STREAMING_FAILED_CODE int,GET_STREAMING_FLAG int,GET_NUM int,GET_SUCCEED_NUM int,GET_RETRANS_NUM int,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,VIDEO_FRAME_RATE int,VIDEO_CODEC_ID string,VIDEO_WIDTH int,VIDEO_HEIGHT int,AUDIO_CODEC_ID string,MEDIA_FILE_TYPE int,PLAY_STATE int,STREAMING_FLAG int,TCP_STATUS_INDICATOR int,DISCONNECTION_FLAG int,FAILURE_CODE int,FLAG int,TAC string,ECI string,TCP_SYN_TIME_MSEL int,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,CHARGE_FLAG int,PREPAID_FLAG int,USER_AGENT string,MS_WIN_STAT_TOTAL_NUM int,MS_WIN_STAT_SMALL_NUM int,MS_ACK_TO_1STGET_DELAY int,SERVER_ACK_TO_1STDATA_DELAY int,STREAMING_TYPE int,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,LAYER3ID int,LAYER4ID int,LAYER5ID int,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,DNS_RETRANS_NUM int,DNS_FAIL_CODE int,FIRST_RAT int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,FIRST_LONGITUDE double,FIRST_LATITUDE double,FIRST_ALTITUDE int,FIRST_RASTERLONGITUDE double,FIRST_RASTERLATITUDE double,FIRST_RASTERALTITUDE int,FIRST_FREQUENCYSPOT int,FIRST_CLUTTER int,FIRST_USERBEHAVIOR int,FIRST_SPEED int,FIRST_CREDIBILITY int,LAST_LONGITUDE double,LAST_LATITUDE double,LAST_ALTITUDE int,LAST_RASTERLONGITUDE double,LAST_RASTERLATITUDE double,LAST_RASTERALTITUDE int,LAST_FREQUENCYSPOT int,LAST_CLUTTER int,LAST_USERBEHAVIOR int,LAST_SPEED int,LAST_CREDIBILITY int,IMEI_CIPHERTEXT string,APP_ID int,DOMAIN_NAME string,STREAMING_CACHE_IP string,STOP_LONGER_THAN_MIN_THRESHOLD int,STOP_LONGER_THAN_MAX_THRESHOLD int,PLAY_END_STAT int,STOP_START_TIME1 double,STOP_END_TIME1 double,STOP_START_TIME2 double,STOP_END_TIME2 double,STOP_START_TIME3 double,STOP_END_TIME3 double,STOP_START_TIME4 double,STOP_END_TIME4 double,STOP_START_TIME5 double,STOP_END_TIME5 double,STOP_START_TIME6 double,STOP_END_TIME6 double,STOP_START_TIME7 double,STOP_END_TIME7 double,STOP_START_TIME8 double,STOP_END_TIME8 double,STOP_START_TIME9 double,STOP_END_TIME9 double,STOP_START_TIME10 double,STOP_END_TIME10 double,FAIL_CLASS double,RECORD_TYPE double,NODATA_COUNT double,VIDEO_NODATA_DURATION double,VIDEO_SMOOTH_DURATION double,VIDEO_SD_DURATION double,VIDEO_HD_DURATION double,VIDEO_UHD_DURATION double,VIDEO_FHD_DURATION double,FLUCTUATION double,START_DOWNLOAD_THROUGHPUT double,L7_UL_GOODPUT_FULL_MSS double,SESSIONKEY string,FIRST_UCELLID double,LAST_UCELLID double,UCELLID1 double,LONGITUDE1 double,LATITUDE1 double,UCELLID2 double,LONGITUDE2 double,LATITUDE2 double,UCELLID3 double,LONGITUDE3 double,LATITUDE3 double,UCELLID4 double,LONGITUDE4 double,LATITUDE4 double,UCELLID5 double,LONGITUDE5 double,LATITUDE5 double,UCELLID6 double,LONGITUDE6 double,LATITUDE6 double,UCELLID7 double,LONGITUDE7 double,LATITUDE7 double,UCELLID8 double,LONGITUDE8 double,LATITUDE8 double,UCELLID9 double,LONGITUDE9 double,LATITUDE9 double,UCELLID10 double,LONGITUDE10 double,LATITUDE10 double,INTBUFFER_FULL_DELAY double,STALL_DURATION double,STREAMING_DW_PACKETS double,STREAMING_DOWNLOAD_DELAY double,PLAY_DURATION double,STREAMING_QUALITY int,VIDEO_DATA_RATE double,AUDIO_DATA_RATE double,STREAMING_FILESIZE double,STREAMING_DURATIOIN double,TCP_SYN_TIME double,TCP_RTT_STEP1 double,CHARGE_ID double,UL_REVERSE_TO_DL_DELAY double,DL_REVERSE_TO_UL_DELAY double,DATATRANS_DW_GOODPUT double,DATATRANS_DW_TOTAL_DURATION double,SUM_FRAGMENT_INTERVAL double,TCP_FIN_TIMES double,TCP_RESET_TIMES double,URL_CLASSIFICATION double,STREAMING_LQ_DURATIOIN double,MAX_DNS_DELAY double,MAX_DNS2SYN double,MAX_LATANCY_OF_LINK_SETUP double,MAX_SYNACK2FIRSTACK double,MAX_SYNACK2LASTACK double,MAX_ACK2GET_DELAY double,MAX_FRAG_INTERVAL_PREDELAY double,SUM_FRAG_INTERVAL_PREDELAY double,SERVICE_DELAY_MSEC double,HOMEPROVINCE double,HOMECITY double,SERVICE_ID double,CHARGING_CLASS double,DATATRANS_UL_DURATION double,ASSOCIATED_ID double,PACKET_LOSS_NUM double,JITTER double,MS_DNS_DELAY_MSEL double,GET_STREAMING_DELAY double,TCP_UL_RETRANS_WITHOUTPL double,TCP_DW_RETRANS_WITHOUTPL double,GET_MAX_UL_SIZE double,GET_MIN_UL_SIZE double,GET_MAX_DL_SIZE double,GET_MIN_DL_SIZE double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double,TCP_UL_RETRANS_WITHPL double,TCP_DW_RETRANS_WITHPL double,TCP_UL_PACKAGES_WITHPL double,TCP_DW_PACKAGES_WITHPL double,TCP_UL_PACKAGES_WITHOUTPL double,TCP_DW_PACKAGES_WITHOUTPL double,UPPERLAYER_IP_UL_PACKETS double,UPPERLAYER_IP_DL_PACKETS double,DOWNLAYER_IP_UL_PACKETS double,DOWNLAYER_IP_DL_PACKETS double,UPPERLAYER_IP_UL_FRAGMENTS double,UPPERLAYER_IP_DL_FRAGMENTS double,DOWNLAYER_IP_UL_FRAGMENTS double,DOWNLAYER_IP_DL_FRAGMENTS double,VALID_TRANS_DURATION double,AIR_PORT_DURATION double,RADIO_CONN_TIMES double,RAN_NE_ID double,AVG_UL_RTT double,AVG_DW_RTT double,UL_RTT_LONG_NUM int,DW_RTT_LONG_NUM int,UL_RTT_STAT_NUM int,DW_RTT_STAT_NUM int,USER_PROBE_UL_LOST_PKT int,SERVER_PROBE_UL_LOST_PKT int,SERVER_PROBE_DW_LOST_PKT int,USER_PROBE_DW_LOST_PKT int,CHARGING_CHARACTERISTICS double,DL_SERIOUS_OUT_OF_ORDER_NUM double,DL_SLIGHT_OUT_OF_ORDER_NUM double,DL_FLIGHT_TOTAL_SIZE double,DL_FLIGHT_TOTAL_NUM double,DL_MAX_FLIGHT_SIZE double,UL_SERIOUS_OUT_OF_ORDER_NUM double,UL_SLIGHT_OUT_OF_ORDER_NUM double,UL_FLIGHT_TOTAL_SIZE double,UL_FLIGHT_TOTAL_NUM double,UL_MAX_FLIGHT_SIZE double,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,DL_CONTINUOUS_RETRANSMISSION_DELAY double,USER_HUNGRY_DELAY double,SERVER_HUNGRY_DELAY double,AVG_DW_RTT_MICRO_SEC int,AVG_UL_RTT_MICRO_SEC int,FLOW_SAMPLE_RATIO int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//C20_SEQ_Dataload-01
test("C20_SEQ_Dataload-01", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DE options('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SID,PROBEID,INTERFACEID,GROUPID,GGSN_ID,SGSN_ID,dummy,SESSION_INDICATOR,BEGIN_TIME,BEGIN_TIME_MSEL,END_TIME,END_TIME_MSEL,PROT_CATEGORY,PROT_TYPE,L7_CARRIER_PROT,SUB_PROT_TYPE,MSISDN,IMSI,IMEI,ENCRYPT_VERSION,ROAMING_TYPE,ROAM_DIRECTION,MS_IP,SERVER_IP,MS_PORT,APN,SGSN_SIG_IP,GGSN_USER_IP,SGSN_USER_IP,MCC,MNC,RAT,LAC,RAC,SAC,CI,SERVER_DECIMAL,BROWSER_TIMESTAMP,TCP_CONN_STATES,GGSN_SIG_IP_BigInt_NEGATIVE,TCP_STATES_BIGINTPOSITIVE,dummy,TCP_WIN_SIZE,dummy,TCP_MSS,dummy,TCP_CONN_TIMES,dummy,TCP_CONN_2_FAILED_TIMES,dummy,TCP_CONN_3_FAILED_TIMES,HOST,STREAMING_URL,dummy,GET_STREAMING_FAILED_CODE,dummy,GET_STREAMING_FLAG,dummy,GET_NUM,dummy,GET_SUCCEED_NUM,dummy,GET_RETRANS_NUM,dummy,GET_TIMEOUT_NUM,INTBUFFER_FST_FLAG,INTBUFFER_FULL_FLAG,STALL_NUM,dummy,VIDEO_FRAME_RATE,dummy,VIDEO_CODEC_ID,dummy,VIDEO_WIDTH,dummy,VIDEO_HEIGHT,dummy,AUDIO_CODEC_ID,dummy,MEDIA_FILE_TYPE,dummy,PLAY_STATE,dummy,PLAY_STATE,dummy,STREAMING_FLAG,dummy,TCP_STATUS_INDICATOR,dummy,DISCONNECTION_FLAG,dummy,FAILURE_CODE,FLAG,TAC,ECI,dummy,TCP_SYN_TIME_MSEL,dummy,TCP_FST_SYN_DIRECTION,RAN_NE_USER_IP,HOMEMCC,HOMEMNC,dummy,CHARGE_FLAG,dummy,PREPAID_FLAG,dummy,USER_AGENT,dummy,MS_WIN_STAT_TOTAL_NUM,dummy,MS_WIN_STAT_SMALL_NUM,dummy,MS_ACK_TO_1STGET_DELAY,dummy,SERVER_ACK_TO_1STDATA_DELAY,dummy,STREAMING_TYPE,dummy,SOURCE_VIDEO_QUALITY,TETHERING_FLAG,CARRIER_ID,LAYER1ID,LAYER2ID,dummy,LAYER3ID,dummy,LAYER4ID,dummy,LAYER5ID,dummy,LAYER6ID,CHARGING_RULE_BASE_NAME,SP,dummy,EXTENDED_URL,SV,FIRST_SAI_CGI_ECGI,dummy,EXTENDED_URL_OTHER,SIGNALING_USE_FLAG,dummy,DNS_RETRANS_NUM,dummy,DNS_FAIL_CODE,FIRST_RAT,FIRST_RAT,MS_INDICATOR,LAST_SAI_CGI_ECGI,LAST_RAT,dummy,FIRST_LONGITUDE,dummy,FIRST_LATITUDE,dummy,FIRST_ALTITUDE,dummy,FIRST_RASTERLONGITUDE,dummy,FIRST_RASTERLATITUDE,dummy,FIRST_RASTERALTITUDE,dummy,FIRST_FREQUENCYSPOT,dummy,FIRST_CLUTTER,dummy,FIRST_USERBEHAVIOR,dummy,FIRST_SPEED,dummy,FIRST_CREDIBILITY,dummy,LAST_LONGITUDE,dummy,LAST_LATITUDE,dummy,LAST_ALTITUDE,dummy,LAST_RASTERLONGITUDE,dummy,LAST_RASTERLATITUDE,dummy,LAST_RASTERALTITUDE,dummy,LAST_FREQUENCYSPOT,dummy,LAST_CLUTTER,dummy,LAST_USERBEHAVIOR,dummy,LAST_SPEED,dummy,LAST_CREDIBILITY,dummy,IMEI_CIPHERTEXT,APP_ID,dummy,DOMAIN_NAME,dummy,STREAMING_CACHE_IP,dummy,STOP_LONGER_THAN_MIN_THRESHOLD,dummy,STOP_LONGER_THAN_MAX_THRESHOLD,dummy,PLAY_END_STAT,dummy,STOP_START_TIME1,dummy,STOP_END_TIME1,dummy,STOP_START_TIME2,dummy,STOP_END_TIME2,dummy,STOP_START_TIME3,dummy,STOP_END_TIME3,dummy,STOP_START_TIME4,dummy,STOP_END_TIME4,dummy,STOP_START_TIME5,dummy,STOP_END_TIME5,dummy,STOP_START_TIME6,dummy,STOP_END_TIME6,dummy,STOP_START_TIME7,dummy,STOP_END_TIME7,dummy,STOP_START_TIME8,dummy,STOP_END_TIME8,dummy,STOP_START_TIME9,dummy,STOP_END_TIME9,dummy,STOP_START_TIME10,dummy,STOP_END_TIME10,dummy,FAIL_CLASS,RECORD_TYPE,dummy,NODATA_COUNT,dummy,VIDEO_NODATA_DURATION,dummy,VIDEO_SMOOTH_DURATION,dummy,VIDEO_SD_DURATION,dummy,VIDEO_HD_DURATION,dummy,VIDEO_UHD_DURATION,dummy,VIDEO_FHD_DURATION,dummy,FLUCTUATION,dummy,START_DOWNLOAD_THROUGHPUT,dummy,L7_UL_GOODPUT_FULL_MSS,dummy,SESSIONKEY,dummy,FIRST_UCELLID,dummy,LAST_UCELLID,dummy,UCELLID1,dummy,LONGITUDE1,dummy,LATITUDE1,dummy,UCELLID2,dummy,LONGITUDE2,dummy,LATITUDE2,dummy,UCELLID3,dummy,LONGITUDE3,dummy,LATITUDE3,dummy,UCELLID4,dummy,LONGITUDE4,dummy,LATITUDE4,dummy,UCELLID5,dummy,LONGITUDE5,dummy,LATITUDE5,dummy,UCELLID6,dummy,LONGITUDE6,dummy,LATITUDE6,dummy,UCELLID7,dummy,LONGITUDE7,dummy,LATITUDE7,dummy,UCELLID8,dummy,LONGITUDE8,dummy,LATITUDE8,dummy,UCELLID9,dummy,LONGITUDE9,dummy,LATITUDE9,dummy,UCELLID10,dummy,LONGITUDE10,dummy,LATITUDE10,dummy,INTBUFFER_FULL_DELAY,dummy,STALL_DURATION,dummy,STREAMING_DW_PACKETS,dummy,STREAMING_DOWNLOAD_DELAY,dummy,PLAY_DURATION,dummy,STREAMING_QUALITY,dummy,VIDEO_DATA_RATE,dummy,AUDIO_DATA_RATE,dummy,STREAMING_FILESIZE,dummy,STREAMING_DURATIOIN,dummy,TCP_SYN_TIME,dummy,TCP_RTT_STEP1,CHARGE_ID,dummy,UL_REVERSE_TO_DL_DELAY,dummy,DL_REVERSE_TO_UL_DELAY,dummy,DATATRANS_DW_GOODPUT,dummy,DATATRANS_DW_TOTAL_DURATION,dummy,SUM_FRAGMENT_INTERVAL,dummy,TCP_FIN_TIMES,dummy,TCP_RESET_TIMES,dummy,URL_CLASSIFICATION,dummy,STREAMING_LQ_DURATIOIN,dummy,MAX_DNS_DELAY,dummy,MAX_DNS2SYN,dummy,MAX_LATANCY_OF_LINK_SETUP,dummy,MAX_SYNACK2FIRSTACK,dummy,MAX_SYNACK2LASTACK,dummy,MAX_ACK2GET_DELAY,dummy,MAX_FRAG_INTERVAL_PREDELAY,dummy,SUM_FRAG_INTERVAL_PREDELAY,dummy,SERVICE_DELAY_MSEC,dummy,HOMEPROVINCE,dummy,HOMECITY,dummy,SERVICE_ID,dummy,CHARGING_CLASS,dummy,DATATRANS_UL_DURATION,dummy,ASSOCIATED_ID,dummy,PACKET_LOSS_NUM,dummy,JITTER,dummy,MS_DNS_DELAY_MSEL,dummy,GET_STREAMING_DELAY,dummy,TCP_UL_RETRANS_WITHOUTPL,dummy,TCP_DW_RETRANS_WITHOUTPL,dummy,GET_MAX_UL_SIZE,dummy,GET_MIN_UL_SIZE,dummy,GET_MAX_DL_SIZE,dummy,GET_MIN_DL_SIZE,dummy,FLOW_SAMPLE_RATIO,dummy,UL_RTT_LONG_NUM,dummy,DW_RTT_LONG_NUM,dummy,UL_RTT_STAT_NUM,dummy,DW_RTT_STAT_NUM,dummy,USER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_DW_LOST_PKT,dummy,USER_PROBE_DW_LOST_PKT,dummy,AVG_DW_RTT_MICRO_SEC,dummy,AVG_UL_RTT_MICRO_SEC,dummy,RAN_NE_ID,dummy,AVG_UL_RTT,dummy,AVG_DW_RTT,dummy,CHARGING_CHARACTERISTICS,dummy,DL_SERIOUS_OUT_OF_ORDER_NUM,dummy,DL_SLIGHT_OUT_OF_ORDER_NUM,dummy,DL_FLIGHT_TOTAL_SIZE,dummy,DL_FLIGHT_TOTAL_NUM,dummy,DL_MAX_FLIGHT_SIZE,dummy,VALID_TRANS_DURATION,dummy,AIR_PORT_DURATION,dummy,RADIO_CONN_TIMES,dummy,UL_SERIOUS_OUT_OF_ORDER_NUM,dummy,UL_SLIGHT_OUT_OF_ORDER_NUM,dummy,UL_FLIGHT_TOTAL_SIZE,dummy,UL_FLIGHT_TOTAL_NUM,dummy,UL_MAX_FLIGHT_SIZE,dummy,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,DL_CONTINUOUS_RETRANSMISSION_DELAY,dummy,USER_HUNGRY_DELAY,dummy,SERVER_HUNGRY_DELAY,dummy,UPPERLAYER_IP_UL_FRAGMENTS,dummy,UPPERLAYER_IP_DL_FRAGMENTS,dummy,DOWNLAYER_IP_UL_FRAGMENTS,dummy,DOWNLAYER_IP_DL_FRAGMENTS,dummy,UPPERLAYER_IP_UL_PACKETS,dummy,UPPERLAYER_IP_DL_PACKETS,dummy,DOWNLAYER_IP_UL_PACKETS,dummy,DOWNLAYER_IP_DL_PACKETS,dummy,TCP_UL_PACKAGES_WITHPL,dummy,TCP_DW_PACKAGES_WITHPL,dummy,TCP_UL_PACKAGES_WITHOUTPL,dummy,TCP_DW_PACKAGES_WITHOUTPL,dummy,TCP_UL_RETRANS_WITHPL,dummy,TCP_DW_RETRANS_WITHPL,L4_UL_THROUGHPUT,L4_DW_THROUGHPUT,L4_UL_GOODPUT,L4_DW_GOODPUT,NETWORK_UL_TRAFFIC,NETWORK_DL_TRAFFIC,L4_UL_PACKETS,L4_DW_PACKETS,TCP_RTT,TCP_UL_OUTOFSEQU,TCP_DW_OUTOFSEQU,TCP_UL_RETRANS,TCP_DW_RETRANS')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/SEQ500/seq_500Records.csv' into table smart_500_DE_hive """).collect

}
       

//C20_DICTIONARY_EXCLUDE_TC001
test("C20_DICTIONARY_EXCLUDE_TC001", Include) {
  sql(s"""select SID, IMEI from smart_500_DE where HOST not in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC002
test("C20_DICTIONARY_EXCLUDE_TC002", Include) {
  sql(s"""select SID, IMEI from smart_500_DE where HOST in  ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC003
test("C20_DICTIONARY_EXCLUDE_TC003", Include) {
  sql(s"""select SID, IMEI from smart_500_DE where HOST LIKE  'www.hua735435.com'""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC004
test("C20_DICTIONARY_EXCLUDE_TC004", Include) {
  sql(s"""select SID, IMEI from smart_500_DE where HOST Not LIKE  'www.hua735435.com'""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC005
test("C20_DICTIONARY_EXCLUDE_TC005", Include) {
  sql(s"""select length(HOST) from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC006
test("C20_DICTIONARY_EXCLUDE_TC006", Include) {
  checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE where HOST in ('www.hua735435.com')""",
    s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE_hive where HOST in ('www.hua735435.com')""")
}
       

//C20_DICTIONARY_EXCLUDE_TC007
test("C20_DICTIONARY_EXCLUDE_TC007", Include) {
  checkAnswer(s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE where HOST not in ('www.hua735435.com')""",
    s"""select avg(HOST),avg(LAYER1ID) from smart_500_DE_hive where HOST not in ('www.hua735435.com')""")
}
       

//C20_DICTIONARY_EXCLUDE_TC008
test("C20_DICTIONARY_EXCLUDE_TC008", Include) {
  sql(s"""select substring(IMEI,1,4) from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC009
test("C20_DICTIONARY_EXCLUDE_TC009", Include) {
  sql(s"""select length(HOST)+10 from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC010
test("C20_DICTIONARY_EXCLUDE_TC010", Include) {
  sql(s"""select length(HOST)-10 from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC011
test("C20_DICTIONARY_EXCLUDE_TC011", Include) {
  sql(s"""select length(HOST)/10 from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC012
test("C20_DICTIONARY_EXCLUDE_TC012", Include) {
  sql(s"""select length(HOST)*10 from smart_500_DE where HOST in ('www.hua735435.com')""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC013
test("C20_DICTIONARY_EXCLUDE_TC013", Include) {
  sql(s"""select lower(MS_IP),sum(LAYER1ID) from smart_500_DE  group by lower(MS_IP)""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC014
test("C20_DICTIONARY_EXCLUDE_TC014", Include) {
  checkAnswer(s"""select * from smart_500_DE  where unix_timestamp(MS_IP)=1420268400""",
    s"""select * from smart_500_DE_hive  where unix_timestamp(MS_IP)=1420268400""")
}
       

//C20_DICTIONARY_EXCLUDE_TC015
test("C20_DICTIONARY_EXCLUDE_TC015", Include) {
  checkAnswer(s"""select * from smart_500_DE  where to_date(MS_IP)='2015-01-07'""",
    s"""select * from smart_500_DE_hive  where to_date(MS_IP)='2015-01-07'""")
}
       

//C20_DICTIONARY_EXCLUDE_TC016
test("C20_DICTIONARY_EXCLUDE_TC016", Include) {
  checkAnswer(s"""select * from smart_500_DE  where datediff(MS_IP,'2014-12-01')>=35""",
    s"""select * from smart_500_DE_hive  where datediff(MS_IP,'2014-12-01')>=35""")
}
       

//C20_DICTIONARY_EXCLUDE_TC017
test("C20_DICTIONARY_EXCLUDE_TC017", Include) {
  sql(s"""select MS_IP,count(*) from smart_500_DE  group by MS_IP""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC018
test("C20_DICTIONARY_EXCLUDE_TC018", Include) {
  sql(s"""select MS_IP,SID,count(*) from smart_500_DE  group by MS_IP,SID order by MS_IP limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC019
test("C20_DICTIONARY_EXCLUDE_TC019", Include) {
  checkAnswer(s"""select SID,length( MSISDN),avg(LAYER1ID),avg(TCP_DW_RETRANS) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""",
    s"""select SID,length( MSISDN),avg(LAYER1ID),avg(TCP_DW_RETRANS) from smart_500_DE_hive  group by SID,length( MSISDN) order by SID limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC020
test("C20_DICTIONARY_EXCLUDE_TC020", Include) {
  checkAnswer(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""",
    s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID) from smart_500_DE_hive  group by SID,length( MSISDN) order by SID limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC021
test("C20_DICTIONARY_EXCLUDE_TC021", Include) {
  checkAnswer(s"""select SID,length( MSISDN),max(LAYER1ID),max(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""",
    s"""select SID,length( MSISDN),max(LAYER1ID),max(LAYER1ID) from smart_500_DE_hive  group by SID,length( MSISDN) order by SID limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC022
test("C20_DICTIONARY_EXCLUDE_TC022", Include) {
  checkAnswer(s"""select SID,length( MSISDN),min(LAYER1ID),min(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""",
    s"""select SID,length( MSISDN),min(LAYER1ID),min(LAYER1ID) from smart_500_DE_hive  group by SID,length( MSISDN) order by SID limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC023
test("C20_DICTIONARY_EXCLUDE_TC023", Include) {
  checkAnswer(s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID),avg(LAYER1ID) from smart_500_DE  group by SID,length( MSISDN) order by SID limit 10""",
    s"""select SID,length( MSISDN),max(LAYER1ID),min(LAYER1ID),avg(LAYER1ID) from smart_500_DE_hive  group by SID,length( MSISDN) order by SID limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC024
test("C20_DICTIONARY_EXCLUDE_TC024", Include) {
  sql(s"""select concat(upper(MSISDN),1),sum(LAYER1ID) from smart_500_DE  group by concat(upper(MSISDN),1)""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC025
test("C20_DICTIONARY_EXCLUDE_TC025", Include) {
  sql(s"""select upper(substring(MSISDN,1,4)),sum(LAYER1ID) from smart_500_DE group by upper(substring(MSISDN,1,4)) """).collect
}
       

//C20_DICTIONARY_EXCLUDE_TC026
test("C20_DICTIONARY_EXCLUDE_TC026", Include) {
  checkAnswer(s"""select max(SERVER_IP) from smart_500_DE""",
    s"""select max(SERVER_IP) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC027
test("C20_DICTIONARY_EXCLUDE_TC027", Include) {
  checkAnswer(s"""select max(SERVER_IP+10) from smart_500_DE""",
    s"""select max(SERVER_IP+10) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC028
test("C20_DICTIONARY_EXCLUDE_TC028", Include) {
  checkAnswer(s"""select max(MSISDN) from smart_500_DE""",
    s"""select max(MSISDN) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC029
test("C20_DICTIONARY_EXCLUDE_TC029", Include) {
  checkAnswer(s"""select max(MSISDN+10) from smart_500_DE""",
    s"""select max(MSISDN+10) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC030
test("C20_DICTIONARY_EXCLUDE_TC030", Include) {
  checkAnswer(s"""select avg(TCP_DW_RETRANS) from smart_500_DE""",
    s"""select avg(TCP_DW_RETRANS) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC031
test("C20_DICTIONARY_EXCLUDE_TC031", Include) {
  checkAnswer(s"""select avg(TCP_DW_RETRANS+10) from smart_500_DE""",
    s"""select avg(TCP_DW_RETRANS+10) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC032
test("C20_DICTIONARY_EXCLUDE_TC032", Include) {
  checkAnswer(s"""select avg(TCP_DW_RETRANS-10) from smart_500_DE""",
    s"""select avg(TCP_DW_RETRANS-10) from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC033
test("C20_DICTIONARY_EXCLUDE_TC033", Include) {
  sql(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC034
test("C20_DICTIONARY_EXCLUDE_TC034", Include) {
  sql(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC035
test("C20_DICTIONARY_EXCLUDE_TC035", Include) {
  sql(s"""select count(TCP_DW_RETRANS)-10 from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC036
test("C20_DICTIONARY_EXCLUDE_TC036", Include) {
  sql(s"""select sum(MSISDN), sum(DISTINCT MSISDN) from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC037
test("C20_DICTIONARY_EXCLUDE_TC037", Include) {
  sql(s"""select count (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC038
test("C20_DICTIONARY_EXCLUDE_TC038", Include) {
  sql(s"""select count (if(TCP_DW_RETRANS<100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC039
test("C20_DICTIONARY_EXCLUDE_TC039", Include) {
  sql(s"""select count (if(TCP_DW_RETRANS=100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC040
test("C20_DICTIONARY_EXCLUDE_TC040", Include) {
  sql(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS=100""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC041
test("C20_DICTIONARY_EXCLUDE_TC041", Include) {
  sql(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS<100""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC042
test("C20_DICTIONARY_EXCLUDE_TC042", Include) {
  sql(s"""select count(TCP_DW_RETRANS) from smart_500_DE where TCP_DW_RETRANS>100""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC043
test("C20_DICTIONARY_EXCLUDE_TC043", Include) {
  sql(s"""select MSISDN, TCP_DW_RETRANS + LAYER1ID as a  from smart_500_DE order by MSISDN limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC044
test("C20_DICTIONARY_EXCLUDE_TC044", Include) {
  sql(s"""select MSISDN, sum(TCP_DW_RETRANS + 10) Total from smart_500_DE group by  MSISDN order by Total limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC045
test("C20_DICTIONARY_EXCLUDE_TC045", Include) {
  checkAnswer(s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DE group by  MSISDN order by Total""",
    s"""select MSISDN, min(LAYER1ID + 10)  Total from smart_500_DE_hive group by  MSISDN order by Total""")
}
       

//C20_DICTIONARY_EXCLUDE_TC046
test("C20_DICTIONARY_EXCLUDE_TC046", Include) {
  checkAnswer(s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DE""",
    s"""select avg (if(LAYER1ID>100,NULL,LAYER1ID))  a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC047
test("C20_DICTIONARY_EXCLUDE_TC047", Include) {
  checkAnswer(s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE""",
    s"""select avg (if(TCP_DW_RETRANS>100,NULL,TCP_DW_RETRANS))  a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC048
test("C20_DICTIONARY_EXCLUDE_TC048", Include) {
  sql(s"""select variance(LAYER1ID) as a   from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC049
test("C20_DICTIONARY_EXCLUDE_TC049", Include) {
  sql(s"""select var_pop(LAYER1ID)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC050
test("C20_DICTIONARY_EXCLUDE_TC050", Include) {
  sql(s"""select var_samp(LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC051
test("C20_DICTIONARY_EXCLUDE_TC051", Include) {
  sql(s"""select stddev_pop(LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC052
test("C20_DICTIONARY_EXCLUDE_TC052", Include) {
  sql(s"""select stddev_samp(LAYER1ID)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC053
test("C20_DICTIONARY_EXCLUDE_TC053", Include) {
  sql(s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC054
test("C20_DICTIONARY_EXCLUDE_TC054", Include) {
  sql(s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC055
test("C20_DICTIONARY_EXCLUDE_TC055", Include) {
  checkAnswer(s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE""",
    s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC056
test("C20_DICTIONARY_EXCLUDE_TC056", Include) {
  checkAnswer(s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DE""",
    s"""select percentile(LAYER1ID,0.2) as  a  from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC057
test("C20_DICTIONARY_EXCLUDE_TC057", Include) {
  checkAnswer(s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DE""",
    s"""select percentile(LAYER1ID,array(0,0.2,0.3,1))  as  a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC058
test("C20_DICTIONARY_EXCLUDE_TC058", Include) {
  sql(s"""select percentile_approx(LAYER1ID,0.2) as a  from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC059
test("C20_DICTIONARY_EXCLUDE_TC059", Include) {
  sql(s"""select percentile_approx(LAYER1ID,0.2,5) as a  from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC060
test("C20_DICTIONARY_EXCLUDE_TC060", Include) {
  sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99))  as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC061
test("C20_DICTIONARY_EXCLUDE_TC061", Include) {
  sql(s"""select percentile_approx(LAYER1ID,array(0.2,0.3,0.99),5) as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC062
test("C20_DICTIONARY_EXCLUDE_TC062", Include) {
  sql(s"""select histogram_numeric(LAYER1ID,2)  as a from (select LAYER1ID from smart_500_DE order by LAYER1ID) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC063
test("C20_DICTIONARY_EXCLUDE_TC063", Include) {
  sql(s"""select variance(TCP_DW_RETRANS) as a   from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC064
test("C20_DICTIONARY_EXCLUDE_TC064", Include) {
  sql(s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC065
test("C20_DICTIONARY_EXCLUDE_TC065", Include) {
  sql(s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC066
test("C20_DICTIONARY_EXCLUDE_TC066", Include) {
  sql(s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC067
test("C20_DICTIONARY_EXCLUDE_TC067", Include) {
  sql(s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC068
test("C20_DICTIONARY_EXCLUDE_TC068", Include) {
  sql(s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC069
test("C20_DICTIONARY_EXCLUDE_TC069", Include) {
  sql(s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC070
test("C20_DICTIONARY_EXCLUDE_TC070", Include) {
  checkAnswer(s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE""",
    s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC073
test("C20_DICTIONARY_EXCLUDE_TC073", Include) {
  sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2) as a  from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC074
test("C20_DICTIONARY_EXCLUDE_TC074", Include) {
  sql(s"""select percentile_approx(TCP_DW_RETRANS,0.2,5) as a  from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC075
test("C20_DICTIONARY_EXCLUDE_TC075", Include) {
  sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99))  as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC076
test("C20_DICTIONARY_EXCLUDE_TC076", Include) {
  sql(s"""select percentile_approx(TCP_DW_RETRANS,array(0.2,0.3,0.99),5) as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC077
test("C20_DICTIONARY_EXCLUDE_TC077", Include) {
  sql(s"""select histogram_numeric(TCP_DW_RETRANS,2)  as a from (select TCP_DW_RETRANS from smart_500_DE order by TCP_DW_RETRANS) t""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC078
test("C20_DICTIONARY_EXCLUDE_TC078", Include) {
  sql(s"""select variance(LAYER1ID) as a   from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC079
test("C20_DICTIONARY_EXCLUDE_TC079", Include) {
  sql(s"""select var_pop(LAYER1ID)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC080
test("C20_DICTIONARY_EXCLUDE_TC080", Include) {
  sql(s"""select var_samp(LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC081
test("C20_DICTIONARY_EXCLUDE_TC081", Include) {
  sql(s"""select stddev_pop(LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC082
test("C20_DICTIONARY_EXCLUDE_TC082", Include) {
  sql(s"""select stddev_samp(LAYER1ID)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC083
test("C20_DICTIONARY_EXCLUDE_TC083", Include) {
  sql(s"""select covar_pop(LAYER1ID,LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC084
test("C20_DICTIONARY_EXCLUDE_TC084", Include) {
  sql(s"""select covar_samp(LAYER1ID,LAYER1ID) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC085
test("C20_DICTIONARY_EXCLUDE_TC085", Include) {
  checkAnswer(s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE""",
    s"""select corr(LAYER1ID,LAYER1ID)  as a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC086
test("C20_DICTIONARY_EXCLUDE_TC086", Include) {
  sql(s"""select variance(TCP_DW_RETRANS) as a   from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC087
test("C20_DICTIONARY_EXCLUDE_TC087", Include) {
  sql(s"""select var_pop(TCP_DW_RETRANS)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC088
test("C20_DICTIONARY_EXCLUDE_TC088", Include) {
  sql(s"""select var_samp(TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC089
test("C20_DICTIONARY_EXCLUDE_TC089", Include) {
  sql(s"""select stddev_pop(TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC090
test("C20_DICTIONARY_EXCLUDE_TC090", Include) {
  sql(s"""select stddev_samp(TCP_DW_RETRANS)  as a from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC091
test("C20_DICTIONARY_EXCLUDE_TC091", Include) {
  sql(s"""select covar_pop(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC092
test("C20_DICTIONARY_EXCLUDE_TC092", Include) {
  sql(s"""select covar_samp(TCP_DW_RETRANS,TCP_DW_RETRANS) as a  from smart_500_DE""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC093
test("C20_DICTIONARY_EXCLUDE_TC093", Include) {
  checkAnswer(s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE""",
    s"""select corr(TCP_DW_RETRANS,TCP_DW_RETRANS)  as a from smart_500_DE_hive""")
}
       

//C20_DICTIONARY_EXCLUDE_TC094
test("C20_DICTIONARY_EXCLUDE_TC094", Include) {
  sql(s"""select Upper(streaming_url) a ,host from smart_500_DE order by host limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC095
test("C20_DICTIONARY_EXCLUDE_TC095", Include) {
  sql(s"""select Lower(streaming_url) a  from smart_500_DE order by host limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC096
test("C20_DICTIONARY_EXCLUDE_TC096", Include) {
  sql(s"""select streaming_url as b,LAYER1ID as a from smart_500_DE  order by a,b asc limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC097
test("C20_DICTIONARY_EXCLUDE_TC097", Include) {
  sql(s"""select streaming_url as b,TCP_DW_RETRANS as a from smart_500_DE  order by a,b desc limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC098
test("C20_DICTIONARY_EXCLUDE_TC098", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua1/xyz'""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC099
test("C20_DICTIONARY_EXCLUDE_TC099", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua90/xyz' and TCP_DW_RETRANS ='82.0' limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC100
test("C20_DICTIONARY_EXCLUDE_TC100", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url ='www.hua1/xyz' or  TCP_DW_RETRANS ='82.0'""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC101
test("C20_DICTIONARY_EXCLUDE_TC101", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url !='www.hua1/xyz'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC102
test("C20_DICTIONARY_EXCLUDE_TC102", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where streaming_url !='www.hua1/xyz' and TCP_DW_RETRANS !='152.0'  order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC103
test("C20_DICTIONARY_EXCLUDE_TC103", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where TCP_DW_RETRANS >2.0 order by LAYER1ID,TCP_DW_RETRANS,streaming_url limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC104
test("C20_DICTIONARY_EXCLUDE_TC104", Include) {
  checkAnswer(s"""select LAYER1ID as a from smart_500_DE where LAYER1ID<=>LAYER1ID order by LAYER1ID desc limit 10""",
    s"""select LAYER1ID as a from smart_500_DE_hive where LAYER1ID<=>LAYER1ID order by LAYER1ID desc limit 10""")
}
       

//C20_DICTIONARY_EXCLUDE_TC105
test("C20_DICTIONARY_EXCLUDE_TC105", Include) {
  sql(s"""SELECT LAYER1ID,TCP_DW_RETRANS,streaming_url FROM (select * from smart_500_DE) SUB_QRY ORDER BY LAYER1ID,TCP_DW_RETRANS,streaming_url ASC limit 10""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC106
test("C20_DICTIONARY_EXCLUDE_TC106", Include) {
  sql(s"""select LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where ( LAYER1ID+1) == 101 order by TCP_DW_RETRANS,LAYER1ID limit 5""").collect
}
       

//C20_DICTIONARY_EXCLUDE_TC107
test("C20_DICTIONARY_EXCLUDE_TC107", Include) {
  checkAnswer(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """,
    s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE_hive where  streaming_url is  null order by LAYER1ID,TCP_DW_RETRANS,streaming_url                                """)
}
       

//C20_DICTIONARY_EXCLUDE_TC108
test("C20_DICTIONARY_EXCLUDE_TC108", Include) {
  sql(s"""select  LAYER1ID,TCP_DW_RETRANS,streaming_url from smart_500_DE where  streaming_url is  not null order by LAYER1ID,TCP_DW_RETRANS,streaming_url""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC001
test("PushUP_FILTER_smart_500_DE_TC001", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN=17846415579 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC002
test("PushUP_FILTER_smart_500_DE_TC002", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN=17846602163 and  BEGIN_TIME=1.463483694712E12 and SERVER_IP='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC003
test("PushUP_FILTER_smart_500_DE_TC003", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846602163) and  (BEGIN_TIME=1.463483694712E12) and (SERVER_IP=='192.26.210.204')""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC004
test("PushUP_FILTER_smart_500_DE_TC004", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846415579) or  (IMSI==460075195040377) or (BEGIN_TIME==1.463483694712E12)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC005
test("PushUP_FILTER_smart_500_DE_TC005", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where (MSISDN==17846602163) and  (BEGIN_TIME==1.463483694712E12) or (SERVER_IP=='192.26.210.204')""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC006
test("PushUP_FILTER_smart_500_DE_TC006", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC007
test("PushUP_FILTER_smart_500_DE_TC007", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 or  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC008
test("PushUP_FILTER_smart_500_DE_TC008", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN!=17846602163 and  BEGIN_TIME!=1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC009
test("PushUP_FILTER_smart_500_DE_TC009", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IS NOT NULL and  BEGIN_TIME IS NOT NULL and SERVER_IP IS NOT NULL""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC010
test("PushUP_FILTER_smart_500_DE_TC010", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IS NOT NULL or  BEGIN_TIME IS NOT NULL or SERVER_IP IS NOT NULL""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC011
test("PushUP_FILTER_smart_500_DE_TC011", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN NOT IN (17846415579,17846415580) and  IMSI NOT IN (460075195040377,460075195040378) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E13)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC012
test("PushUP_FILTER_smart_500_DE_TC012", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN IN (17846415579,17846415580) and  IMSI IN (460075195040377,460075195040378) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E13)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC013
test("PushUP_FILTER_smart_500_DE_TC013", Include) {
  sql(s"""select MSISDN+0.1000001,IMSI+9999999,BEGIN_TIME+9.999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC014
test("PushUP_FILTER_smart_500_DE_TC014", Include) {
  sql(s"""select MSISDN-0.1000001,IMSI-9999999,BEGIN_TIME-9.999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC015
test("PushUP_FILTER_smart_500_DE_TC015", Include) {
  sql(s"""select MSISDN*0.1000001,IMSI*9999999,BEGIN_TIME*9.999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC016
test("PushUP_FILTER_smart_500_DE_TC016", Include) {
  sql(s"""select MSISDN/0.1000001,IMSI/9999999,BEGIN_TIME/9.999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC017
test("PushUP_FILTER_smart_500_DE_TC017", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN>17846602163 and  BEGIN_TIME>1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC018
test("PushUP_FILTER_smart_500_DE_TC018", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN<17846602163 and  BEGIN_TIME<1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC019
test("PushUP_FILTER_smart_500_DE_TC019", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN>=17846602163 and  BEGIN_TIME>=1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC020
test("PushUP_FILTER_smart_500_DE_TC020", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN<=17846602163 and  BEGIN_TIME<=1.463483694712E12 or SERVER_IP!='192.26.210.204'""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC021
test("PushUP_FILTER_smart_500_DE_TC021", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN like 17846415579 and  IMSI like 460075195040377 or BEGIN_TIME like 1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC022
test("PushUP_FILTER_smart_500_DE_TC022", Include) {
  sql(s"""select BEGIN_TIME,END_TIME from smart_500_DE where MSISDN not like 17846415579 and  IMSI not like 460075195040377 or BEGIN_TIME not like 1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC023
test("PushUP_FILTER_smart_500_DE_TC023", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID=4.61168620184322E14 or  IMSI=460075195040377 or BEGIN_TIME=1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC024
test("PushUP_FILTER_smart_500_DE_TC024", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID=4.61168620184322E14 and  IMSI=460075195040377 and BEGIN_TIME=1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC025
test("PushUP_FILTER_smart_500_DE_TC025", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) and  (IMSI==460075171072129) and (BEGIN_TIME==1.463483694712E12)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC026
test("PushUP_FILTER_smart_500_DE_TC026", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) or  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC027
test("PushUP_FILTER_smart_500_DE_TC027", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where (SID==4.61168620184322E14) and  (IMSI==460075171072129) or (BEGIN_TIME==1.463483694712E12)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC028
test("PushUP_FILTER_smart_500_DE_TC028", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID!=4.61168620184322E14 or  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC029
test("PushUP_FILTER_smart_500_DE_TC029", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID!=4.61168620184322E14 and  IMSI!=460075171072129 or BEGIN_TIME!=1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC030
test("PushUP_FILTER_smart_500_DE_TC030", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID is NOT NULL and  IMSI is not null and BEGIN_TIME is not null""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC031
test("PushUP_FILTER_smart_500_DE_TC031", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID is NOT NULL or  IMSI is not null or BEGIN_TIME is not null""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC032
test("PushUP_FILTER_smart_500_DE_TC032", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID NOT IN (4.61168620184322E14,4.61168620184322E16) or  IMSI NOT IN (460075171072129,460075171072130) or BEGIN_TIME NOT IN (1.463483694712E12,1.463483694712E14)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC033
test("PushUP_FILTER_smart_500_DE_TC033", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID IN (4.61168620184322E14,4.61168620184322E16) or  IMSI IN (460075171072129,460075171072130) or BEGIN_TIME IN (1.463483694712E12,1.463483694712E14)""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC034
test("PushUP_FILTER_smart_500_DE_TC034", Include) {
  sql(s"""select BEGIN_TIME+2,SID+0.234,IMSI+99.99999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC035
test("PushUP_FILTER_smart_500_DE_TC035", Include) {
  sql(s"""select BEGIN_TIME-2,SID-0.234,IMSI-99.99999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC036
test("PushUP_FILTER_smart_500_DE_TC036", Include) {
  sql(s"""select BEGIN_TIME*2,SID*0.234,IMSI*99.99999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC037
test("PushUP_FILTER_smart_500_DE_TC037", Include) {
  sql(s"""select BEGIN_TIME/2,SID/0.234,IMSI/99.99999999 from smart_500_DE""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC038
test("PushUP_FILTER_smart_500_DE_TC038", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID like 4.61168620184322E14 and  IMSI like 460075171072129  or BEGIN_TIME like 1.463483694712E12""").collect
}
       

//PushUP_FILTER_smart_500_DE_TC039
test("PushUP_FILTER_smart_500_DE_TC039", Include) {
  sql(s"""select BEGIN_TIME,SID from smart_500_DE where SID not like 4.61168620184322E14 and  IMSI not like 460075171072129  or BEGIN_TIME not like 1.463483694712E12""").collect
}
       
override def afterAll {
sql("drop table if exists smart_500_de")
sql("drop table if exists smart_500_de_hive")
sql("drop table if exists smart_500_DE")
sql("drop table if exists smart_500_DE_hive")
}
}
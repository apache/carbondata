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

package org.apache.carbondata.spark.testsuite.dataload

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.test.util.QueryTest

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
 * This class will test data load in which number of columns in data are more than
 * the number of columns in schema
 */
class TestDataLoadWithColumnsMoreThanSchema extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS char_test")
    sql("DROP TABLE IF EXISTS hive_char_test")
    sql("DROP TABLE IF EXISTS max_columns_value_test")
    sql("DROP TABLE IF EXISTS boundary_max_columns_test")
    sql("DROP TABLE IF EXISTS valid_max_columns_test")
    sql("DROP TABLE IF EXISTS max_columns_test")
    sql("DROP TABLE IF EXISTS smart_500_DE")
    sql("CREATE TABLE char_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED AS carbondata")
    sql("CREATE TABLE hive_char_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)row format delimited fields terminated by ','")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/character_carbon.csv' into table char_test")
    sql(s"LOAD DATA local inpath '$resourcesPath/character_hive.csv' INTO table hive_char_test")
  }

  test("test count(*) to check for data loss") {
    checkAnswer(sql("select count(*) from char_test"),
      sql("select count(*) from hive_char_test"))
  }

  test("test for invalid value of maxColumns") {
    sql("DROP TABLE IF EXISTS max_columns_test")
    sql(
      s"""
         |  CREATE TABLE max_columns_test (
         |    imei string,
         |    age int,
         |    task bigint,
         |    num double,
         |    level decimal(10,3),
         |    productdate timestamp,
         |    mark int,
         |    name string)
         |  STORED AS carbondata
       """.stripMargin)
    intercept[Throwable] {
      sql(
        s"""
           | LOAD DATA LOCAL INPATH '$resourcesPath/character_carbon.csv'
           | into table max_columns_test
           | options('MAXCOLUMNS'='avfgd')
         """.stripMargin)
    }
  }

  test("test for valid value of maxColumns") {
    sql("DROP TABLE IF EXISTS valid_max_columns_test")
    sql("CREATE TABLE valid_max_columns_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED AS carbondata")
    try {
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/character_carbon.csv' into table valid_max_columns_test options('MAXCOLUMNS'='400')")
      checkAnswer(sql("select count(*) from valid_max_columns_test"),
        sql("select count(*) from hive_char_test"))
    } catch {
      case _: Throwable => assert(false)
    }
  }

  test("test with invalid maxColumns value") {
    sql(
      "CREATE TABLE max_columns_value_test (imei string,age int,task bigint,num double,level " +
      "decimal(10,3),productdate timestamp,mark int,name string) STORED AS carbondata")


    intercept[Throwable] {
      sql(
        s"LOAD DATA LOCAL INPATH '$resourcesPath/character_carbon.csv' into table " +
          "max_columns_value_test options('FILEHEADER='imei,age','MAXCOLUMNS'='2')")
    }
  }

  test("test for maxcolumns option value greater than threshold value for maxcolumns") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS valid_max_columns_test")
      sql(
        "CREATE TABLE valid_max_columns_test (imei string,age int,task bigint,num double,level decimal(10,3),productdate timestamp,mark int,name string)STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/character_carbon.csv' into table valid_max_columns_test options('MAXCOLUMNS'='22000')")
    }
  }

  test("test for boundary value for maxcolumns") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS boundary_max_columns_test")
      sql(
        "CREATE TABLE boundary_max_columns_test (empno string, empname String, designation " +
        "String, doj String, " +
        "workgroupcategory string, workgroupcategoryname String, deptno string, deptname String, " +
        "projectcode string, projectjoindate String, projectenddate String,attendance double," +
        "utilization double,salary double) STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' into table boundary_max_columns_test" +
          s" options('MAXCOLUMNS'='14')")

    }
  }

  test("test for maxcolumns value less than columns in 1st line of csv file") {
    intercept[Exception] {
      sql("DROP TABLE IF EXISTS boundary_max_columns_test")
      sql(
        "CREATE TABLE boundary_max_columns_test (empno string, empname String, designation String, doj String, " +
        "workgroupcategory string, workgroupcategoryname String, deptno string, deptname String, " +
        "projectcode string, projectjoindate String, projectenddate String,attendance double," +
        "utilization double,salary double) STORED AS carbondata ")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' into table boundary_max_columns_test options('MAXCOLUMNS'='13')")
    }
  }

  test("test for duplicate column name in the Fileheader options in load command") {
    sql("create table smart_500_DE (MSISDN string,IMSI string,IMEI string,INTERFACEID int,GROUPID int,GGSN_ID double,SGSN_ID double,SESSION_INDICATOR int,BEGIN_TIME double,BEGIN_TIME_MSEL int,END_TIME double,END_TIME_MSEL int,PROT_CATEGORY int,PROT_TYPE int,L7_CARRIER_PROT int,SUB_PROT_TYPE int,SID double,PROBEID double,ENCRYPT_VERSION int,ROAMING_TYPE int,ROAM_DIRECTION int,MS_IP string,SERVER_IP string,MS_PORT int,SERVER_DECIMAL Decimal,APN string,SGSN_SIG_IP string,GGSN_SIG_IP_BigInt_NEGATIVE bigint,SGSN_USER_IP string,GGSN_USER_IP string,MCC string,MNC string,RAT int,LAC string,RAC string,SAC string,CI string,BROWSER_TIMESTAMP timestamp,TCP_CONN_STATES int,TCP_STATES_BIGINTPOSITIVE int,TCP_WIN_SIZE int,TCP_MSS int,TCP_CONN_TIMES int,TCP_CONN_2_FAILED_TIMES int,TCP_CONN_3_FAILED_TIMES int,HOST string,STREAMING_URL string,GET_STREAMING_FAILED_CODE int,GET_STREAMING_FLAG int,GET_NUM int,GET_SUCCEED_NUM int,GET_RETRANS_NUM int,GET_TIMEOUT_NUM int,INTBUFFER_FST_FLAG int,INTBUFFER_FULL_FLAG int,STALL_NUM int,VIDEO_FRAME_RATE int,VIDEO_CODEC_ID string,VIDEO_WIDTH int,VIDEO_HEIGHT int,AUDIO_CODEC_ID string,MEDIA_FILE_TYPE int,PLAY_STATE int,STREAMING_FLAG int,TCP_STATUS_INDICATOR int,DISCONNECTION_FLAG int,FAILURE_CODE int,FLAG int,TAC string,ECI string,TCP_SYN_TIME_MSEL int,TCP_FST_SYN_DIRECTION int,RAN_NE_USER_IP string,HOMEMCC string,HOMEMNC string,CHARGE_FLAG int,PREPAID_FLAG int,USER_AGENT string,MS_WIN_STAT_TOTAL_NUM int,MS_WIN_STAT_SMALL_NUM int,MS_ACK_TO_1STGET_DELAY int,SERVER_ACK_TO_1STDATA_DELAY int,STREAMING_TYPE int,SOURCE_VIDEO_QUALITY int,TETHERING_FLAG int,CARRIER_ID double,LAYER1ID int,LAYER2ID int,LAYER3ID int,LAYER4ID int,LAYER5ID int,LAYER6ID int,CHARGING_RULE_BASE_NAME string,SP string,EXTENDED_URL string,SV string,FIRST_SAI_CGI_ECGI string,EXTENDED_URL_OTHER string,SIGNALING_USE_FLAG int,DNS_RETRANS_NUM int,DNS_FAIL_CODE int,FIRST_RAT int,MS_INDICATOR string,LAST_SAI_CGI_ECGI string,LAST_RAT int,FIRST_LONGITUDE double,FIRST_LATITUDE double,FIRST_ALTITUDE int,FIRST_RASTERLONGITUDE double,FIRST_RASTERLATITUDE double,FIRST_RASTERALTITUDE int,FIRST_FREQUENCYSPOT int,FIRST_CLUTTER int,FIRST_USERBEHAVIOR int,FIRST_SPEED int,FIRST_CREDIBILITY int,LAST_LONGITUDE double,LAST_LATITUDE double,LAST_ALTITUDE int,LAST_RASTERLONGITUDE double,LAST_RASTERLATITUDE double,LAST_RASTERALTITUDE int,LAST_FREQUENCYSPOT int,LAST_CLUTTER int,LAST_USERBEHAVIOR int,LAST_SPEED int,LAST_CREDIBILITY int,IMEI_CIPHERTEXT string,APP_ID int,DOMAIN_NAME string,STREAMING_CACHE_IP string,STOP_LONGER_THAN_MIN_THRESHOLD int,STOP_LONGER_THAN_MAX_THRESHOLD int,PLAY_END_STAT int,STOP_START_TIME1 double,STOP_END_TIME1 double,STOP_START_TIME2 double,STOP_END_TIME2 double,STOP_START_TIME3 double,STOP_END_TIME3 double,STOP_START_TIME4 double,STOP_END_TIME4 double,STOP_START_TIME5 double,STOP_END_TIME5 double,STOP_START_TIME6 double,STOP_END_TIME6 double,STOP_START_TIME7 double,STOP_END_TIME7 double,STOP_START_TIME8 double,STOP_END_TIME8 double,STOP_START_TIME9 double,STOP_END_TIME9 double,STOP_START_TIME10 double,STOP_END_TIME10 double,FAIL_CLASS double,RECORD_TYPE double,NODATA_COUNT double,VIDEO_NODATA_DURATION double,VIDEO_SMOOTH_DURATION double,VIDEO_SD_DURATION double,VIDEO_HD_DURATION double,VIDEO_UHD_DURATION double,VIDEO_FHD_DURATION double,FLUCTUATION double,START_DOWNLOAD_THROUGHPUT double,L7_UL_GOODPUT_FULL_MSS double,SESSIONKEY string,FIRST_UCELLID double,LAST_UCELLID double,UCELLID1 double,LONGITUDE1 double,LATITUDE1 double,UCELLID2 double,LONGITUDE2 double,LATITUDE2 double,UCELLID3 double,LONGITUDE3 double,LATITUDE3 double,UCELLID4 double,LONGITUDE4 double,LATITUDE4 double,UCELLID5 double,LONGITUDE5 double,LATITUDE5 double,UCELLID6 double,LONGITUDE6 double,LATITUDE6 double,UCELLID7 double,LONGITUDE7 double,LATITUDE7 double,UCELLID8 double,LONGITUDE8 double,LATITUDE8 double,UCELLID9 double,LONGITUDE9 double,LATITUDE9 double,UCELLID10 double,LONGITUDE10 double,LATITUDE10 double,INTBUFFER_FULL_DELAY double,STALL_DURATION double,STREAMING_DW_PACKETS double,STREAMING_DOWNLOAD_DELAY double,PLAY_DURATION double,STREAMING_QUALITY int,VIDEO_DATA_RATE double,AUDIO_DATA_RATE double,STREAMING_FILESIZE double,STREAMING_DURATIOIN double,TCP_SYN_TIME double,TCP_RTT_STEP1 double,CHARGE_ID double,UL_REVERSE_TO_DL_DELAY double,DL_REVERSE_TO_UL_DELAY double,DATATRANS_DW_GOODPUT double,DATATRANS_DW_TOTAL_DURATION double,SUM_FRAGMENT_INTERVAL double,TCP_FIN_TIMES double,TCP_RESET_TIMES double,URL_CLASSIFICATION double,STREAMING_LQ_DURATIOIN double,MAX_DNS_DELAY double,MAX_DNS2SYN double,MAX_LATANCY_OF_LINK_SETUP double,MAX_SYNACK2FIRSTACK double,MAX_SYNACK2LASTACK double,MAX_ACK2GET_DELAY double,MAX_FRAG_INTERVAL_PREDELAY double,SUM_FRAG_INTERVAL_PREDELAY double,SERVICE_DELAY_MSEC double,HOMEPROVINCE double,HOMECITY double,SERVICE_ID double,CHARGING_CLASS double,DATATRANS_UL_DURATION double,ASSOCIATED_ID double,PACKET_LOSS_NUM double,JITTER double,MS_DNS_DELAY_MSEL double,GET_STREAMING_DELAY double,TCP_UL_RETRANS_WITHOUTPL double,TCP_DW_RETRANS_WITHOUTPL double,GET_MAX_UL_SIZE double,GET_MIN_UL_SIZE double,GET_MAX_DL_SIZE double,GET_MIN_DL_SIZE double,L4_UL_THROUGHPUT double,L4_DW_THROUGHPUT double,L4_UL_GOODPUT double,L4_DW_GOODPUT double,NETWORK_UL_TRAFFIC double,NETWORK_DL_TRAFFIC double,L4_UL_PACKETS double,L4_DW_PACKETS double,TCP_RTT double,TCP_UL_OUTOFSEQU double,TCP_DW_OUTOFSEQU double,TCP_UL_RETRANS double,TCP_DW_RETRANS double,TCP_UL_RETRANS_WITHPL double,TCP_DW_RETRANS_WITHPL double,TCP_UL_PACKAGES_WITHPL double,TCP_DW_PACKAGES_WITHPL double,TCP_UL_PACKAGES_WITHOUTPL double,TCP_DW_PACKAGES_WITHOUTPL double,UPPERLAYER_IP_UL_PACKETS double,UPPERLAYER_IP_DL_PACKETS double,DOWNLAYER_IP_UL_PACKETS double,DOWNLAYER_IP_DL_PACKETS double,UPPERLAYER_IP_UL_FRAGMENTS double,UPPERLAYER_IP_DL_FRAGMENTS double,DOWNLAYER_IP_UL_FRAGMENTS double,DOWNLAYER_IP_DL_FRAGMENTS double,VALID_TRANS_DURATION double,AIR_PORT_DURATION double,RADIO_CONN_TIMES double,RAN_NE_ID double,AVG_UL_RTT double,AVG_DW_RTT double,UL_RTT_LONG_NUM int,DW_RTT_LONG_NUM int,UL_RTT_STAT_NUM int,DW_RTT_STAT_NUM int,USER_PROBE_UL_LOST_PKT int,SERVER_PROBE_UL_LOST_PKT int,SERVER_PROBE_DW_LOST_PKT int,USER_PROBE_DW_LOST_PKT int,CHARGING_CHARACTERISTICS double,DL_SERIOUS_OUT_OF_ORDER_NUM double,DL_SLIGHT_OUT_OF_ORDER_NUM double,DL_FLIGHT_TOTAL_SIZE double,DL_FLIGHT_TOTAL_NUM double,DL_MAX_FLIGHT_SIZE double,UL_SERIOUS_OUT_OF_ORDER_NUM double,UL_SLIGHT_OUT_OF_ORDER_NUM double,UL_FLIGHT_TOTAL_SIZE double,UL_FLIGHT_TOTAL_NUM double,UL_MAX_FLIGHT_SIZE double,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS double,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS double,DL_CONTINUOUS_RETRANSMISSION_DELAY double,USER_HUNGRY_DELAY double,SERVER_HUNGRY_DELAY double,AVG_DW_RTT_MICRO_SEC int,AVG_UL_RTT_MICRO_SEC int,FLOW_SAMPLE_RATIO int) STORED AS carbondata ")
    try {
      sql(s"LOAD DATA INPATH '$resourcesPath/seq_20Records.csv' into table smart_500_DE options('DELIMITER'=',','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='SID,PROBEID,INTERFACEID,GROUPID,GGSN_ID,SGSN_ID,dummy,SESSION_INDICATOR,BEGIN_TIME,BEGIN_TIME_MSEL,END_TIME,END_TIME_MSEL,PROT_CATEGORY,PROT_TYPE,L7_CARRIER_PROT,SUB_PROT_TYPE,MSISDN,IMSI,IMEI,ENCRYPT_VERSION,ROAMING_TYPE,ROAM_DIRECTION,MS_IP,SERVER_IP,MS_PORT,APN,SGSN_SIG_IP,GGSN_USER_IP,SGSN_USER_IP,MCC,MNC,RAT,LAC,RAC,SAC,CI,SERVER_DECIMAL,BROWSER_TIMESTAMP,TCP_CONN_STATES,GGSN_SIG_IP_BigInt_NEGATIVE,TCP_STATES_BIGINTPOSITIVE,dummy,TCP_WIN_SIZE,dummy,TCP_MSS,dummy,TCP_CONN_TIMES,dummy,TCP_CONN_2_FAILED_TIMES,dummy,TCP_CONN_3_FAILED_TIMES,HOST,STREAMING_URL,dummy,GET_STREAMING_FAILED_CODE,dummy,GET_STREAMING_FLAG,dummy,GET_NUM,dummy,GET_SUCCEED_NUM,dummy,GET_RETRANS_NUM,dummy,GET_TIMEOUT_NUM,INTBUFFER_FST_FLAG,INTBUFFER_FULL_FLAG,STALL_NUM,dummy,VIDEO_FRAME_RATE,dummy,VIDEO_CODEC_ID,dummy,VIDEO_WIDTH,dummy,VIDEO_HEIGHT,dummy,AUDIO_CODEC_ID,dummy,MEDIA_FILE_TYPE,dummy,PLAY_STATE,dummy,PLAY_STATE,dummy,STREAMING_FLAG,dummy,TCP_STATUS_INDICATOR,dummy,DISCONNECTION_FLAG,dummy,FAILURE_CODE,FLAG,TAC,ECI,dummy,TCP_SYN_TIME_MSEL,dummy,TCP_FST_SYN_DIRECTION,RAN_NE_USER_IP,HOMEMCC,HOMEMNC,dummy,CHARGE_FLAG,dummy,PREPAID_FLAG,dummy,USER_AGENT,dummy,MS_WIN_STAT_TOTAL_NUM,dummy,MS_WIN_STAT_SMALL_NUM,dummy,MS_ACK_TO_1STGET_DELAY,dummy,SERVER_ACK_TO_1STDATA_DELAY,dummy,STREAMING_TYPE,dummy,SOURCE_VIDEO_QUALITY,TETHERING_FLAG,CARRIER_ID,LAYER1ID,LAYER2ID,dummy,LAYER3ID,dummy,LAYER4ID,dummy,LAYER5ID,dummy,LAYER6ID,CHARGING_RULE_BASE_NAME,SP,dummy,EXTENDED_URL,SV,FIRST_SAI_CGI_ECGI,dummy,EXTENDED_URL_OTHER,SIGNALING_USE_FLAG,dummy,DNS_RETRANS_NUM,dummy,DNS_FAIL_CODE,FIRST_RAT,FIRST_RAT,MS_INDICATOR,LAST_SAI_CGI_ECGI,LAST_RAT,dummy,FIRST_LONGITUDE,dummy,FIRST_LATITUDE,dummy,FIRST_ALTITUDE,dummy,FIRST_RASTERLONGITUDE,dummy,FIRST_RASTERLATITUDE,dummy,FIRST_RASTERALTITUDE,dummy,FIRST_FREQUENCYSPOT,dummy,FIRST_CLUTTER,dummy,FIRST_USERBEHAVIOR,dummy,FIRST_SPEED,dummy,FIRST_CREDIBILITY,dummy,LAST_LONGITUDE,dummy,LAST_LATITUDE,dummy,LAST_ALTITUDE,dummy,LAST_RASTERLONGITUDE,dummy,LAST_RASTERLATITUDE,dummy,LAST_RASTERALTITUDE,dummy,LAST_FREQUENCYSPOT,dummy,LAST_CLUTTER,dummy,LAST_USERBEHAVIOR,dummy,LAST_SPEED,dummy,LAST_CREDIBILITY,dummy,IMEI_CIPHERTEXT,APP_ID,dummy,DOMAIN_NAME,dummy,STREAMING_CACHE_IP,dummy,STOP_LONGER_THAN_MIN_THRESHOLD,dummy,STOP_LONGER_THAN_MAX_THRESHOLD,dummy,PLAY_END_STAT,dummy,STOP_START_TIME1,dummy,STOP_END_TIME1,dummy,STOP_START_TIME2,dummy,STOP_END_TIME2,dummy,STOP_START_TIME3,dummy,STOP_END_TIME3,dummy,STOP_START_TIME4,dummy,STOP_END_TIME4,dummy,STOP_START_TIME5,dummy,STOP_END_TIME5,dummy,STOP_START_TIME6,dummy,STOP_END_TIME6,dummy,STOP_START_TIME7,dummy,STOP_END_TIME7,dummy,STOP_START_TIME8,dummy,STOP_END_TIME8,dummy,STOP_START_TIME9,dummy,STOP_END_TIME9,dummy,STOP_START_TIME10,dummy,STOP_END_TIME10,dummy,FAIL_CLASS,RECORD_TYPE,dummy,NODATA_COUNT,dummy,VIDEO_NODATA_DURATION,dummy,VIDEO_SMOOTH_DURATION,dummy,VIDEO_SD_DURATION,dummy,VIDEO_HD_DURATION,dummy,VIDEO_UHD_DURATION,dummy,VIDEO_FHD_DURATION,dummy,FLUCTUATION,dummy,START_DOWNLOAD_THROUGHPUT,dummy,L7_UL_GOODPUT_FULL_MSS,dummy,SESSIONKEY,dummy,FIRST_UCELLID,dummy,LAST_UCELLID,dummy,UCELLID1,dummy,LONGITUDE1,dummy,LATITUDE1,dummy,UCELLID2,dummy,LONGITUDE2,dummy,LATITUDE2,dummy,UCELLID3,dummy,LONGITUDE3,dummy,LATITUDE3,dummy,UCELLID4,dummy,LONGITUDE4,dummy,LATITUDE4,dummy,UCELLID5,dummy,LONGITUDE5,dummy,LATITUDE5,dummy,UCELLID6,dummy,LONGITUDE6,dummy,LATITUDE6,dummy,UCELLID7,dummy,LONGITUDE7,dummy,LATITUDE7,dummy,UCELLID8,dummy,LONGITUDE8,dummy,LATITUDE8,dummy,UCELLID9,dummy,LONGITUDE9,dummy,LATITUDE9,dummy,UCELLID10,dummy,LONGITUDE10,dummy,LATITUDE10,dummy,INTBUFFER_FULL_DELAY,dummy,STALL_DURATION,dummy,STREAMING_DW_PACKETS,dummy,STREAMING_DOWNLOAD_DELAY,dummy,PLAY_DURATION,dummy,STREAMING_QUALITY,dummy,VIDEO_DATA_RATE,dummy,AUDIO_DATA_RATE,dummy,STREAMING_FILESIZE,dummy,STREAMING_DURATIOIN,dummy,TCP_SYN_TIME,dummy,TCP_RTT_STEP1,CHARGE_ID,dummy,UL_REVERSE_TO_DL_DELAY,dummy,DL_REVERSE_TO_UL_DELAY,dummy,DATATRANS_DW_GOODPUT,dummy,DATATRANS_DW_TOTAL_DURATION,dummy,SUM_FRAGMENT_INTERVAL,dummy,TCP_FIN_TIMES,dummy,TCP_RESET_TIMES,dummy,URL_CLASSIFICATION,dummy,STREAMING_LQ_DURATIOIN,dummy,MAX_DNS_DELAY,dummy,MAX_DNS2SYN,dummy,MAX_LATANCY_OF_LINK_SETUP,dummy,MAX_SYNACK2FIRSTACK,dummy,MAX_SYNACK2LASTACK,dummy,MAX_ACK2GET_DELAY,dummy,MAX_FRAG_INTERVAL_PREDELAY,dummy,SUM_FRAG_INTERVAL_PREDELAY,dummy,SERVICE_DELAY_MSEC,dummy,HOMEPROVINCE,dummy,HOMECITY,dummy,SERVICE_ID,dummy,CHARGING_CLASS,dummy,DATATRANS_UL_DURATION,dummy,ASSOCIATED_ID,dummy,PACKET_LOSS_NUM,dummy,JITTER,dummy,MS_DNS_DELAY_MSEL,dummy,GET_STREAMING_DELAY,dummy,TCP_UL_RETRANS_WITHOUTPL,dummy,TCP_DW_RETRANS_WITHOUTPL,dummy,GET_MAX_UL_SIZE,dummy,GET_MIN_UL_SIZE,dummy,GET_MAX_DL_SIZE,dummy,GET_MIN_DL_SIZE,dummy,FLOW_SAMPLE_RATIO,dummy,UL_RTT_LONG_NUM,dummy,DW_RTT_LONG_NUM,dummy,UL_RTT_STAT_NUM,dummy,DW_RTT_STAT_NUM,dummy,USER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_UL_LOST_PKT,dummy,SERVER_PROBE_DW_LOST_PKT,dummy,USER_PROBE_DW_LOST_PKT,dummy,AVG_DW_RTT_MICRO_SEC,dummy,AVG_UL_RTT_MICRO_SEC,dummy,RAN_NE_ID,dummy,AVG_UL_RTT,dummy,AVG_DW_RTT,dummy,CHARGING_CHARACTERISTICS,dummy,DL_SERIOUS_OUT_OF_ORDER_NUM,dummy,DL_SLIGHT_OUT_OF_ORDER_NUM,dummy,DL_FLIGHT_TOTAL_SIZE,dummy,DL_FLIGHT_TOTAL_NUM,dummy,DL_MAX_FLIGHT_SIZE,dummy,VALID_TRANS_DURATION,dummy,AIR_PORT_DURATION,dummy,RADIO_CONN_TIMES,dummy,UL_SERIOUS_OUT_OF_ORDER_NUM,dummy,UL_SLIGHT_OUT_OF_ORDER_NUM,dummy,UL_FLIGHT_TOTAL_SIZE,dummy,UL_FLIGHT_TOTAL_NUM,dummy,UL_MAX_FLIGHT_SIZE,dummy,USER_DL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,SERVER_UL_SLIGHT_OUT_OF_ORDER_PACKETS,dummy,DL_CONTINUOUS_RETRANSMISSION_DELAY,dummy,USER_HUNGRY_DELAY,dummy,SERVER_HUNGRY_DELAY,dummy,UPPERLAYER_IP_UL_FRAGMENTS,dummy,UPPERLAYER_IP_DL_FRAGMENTS,dummy,DOWNLAYER_IP_UL_FRAGMENTS,dummy,DOWNLAYER_IP_DL_FRAGMENTS,dummy,UPPERLAYER_IP_UL_PACKETS,dummy,UPPERLAYER_IP_DL_PACKETS,dummy,DOWNLAYER_IP_UL_PACKETS,dummy,DOWNLAYER_IP_DL_PACKETS,dummy,TCP_UL_PACKAGES_WITHPL,dummy,TCP_DW_PACKAGES_WITHPL,dummy,TCP_UL_PACKAGES_WITHOUTPL,dummy,TCP_DW_PACKAGES_WITHOUTPL,dummy,TCP_UL_RETRANS_WITHPL,dummy,TCP_DW_RETRANS_WITHPL,L4_UL_THROUGHPUT,L4_DW_THROUGHPUT,L4_UL_GOODPUT,L4_DW_GOODPUT,NETWORK_UL_TRAFFIC,NETWORK_DL_TRAFFIC,L4_UL_PACKETS,L4_DW_PACKETS,TCP_RTT,TCP_UL_OUTOFSEQU,TCP_DW_OUTOFSEQU,TCP_UL_RETRANS,TCP_DW_RETRANS')")
      assert(true)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        assert(false)
    }
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS char_test")
    sql("DROP TABLE IF EXISTS hive_char_test")
    sql("DROP TABLE IF EXISTS max_columns_value_test")
    sql("DROP TABLE IF EXISTS boundary_max_columns_test")
    sql("DROP TABLE IF EXISTS valid_max_columns_test")
    sql("DROP TABLE IF EXISTS max_columns_test")
    sql("DROP TABLE IF EXISTS smart_500_DE")
  }
}

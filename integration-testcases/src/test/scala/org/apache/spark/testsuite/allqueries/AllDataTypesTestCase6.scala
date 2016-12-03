/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.allqueries

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import junit.framework.TestCase._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{NonRunningTests, QueryTest}
import org.codehaus.groovy.util.AbstractConcurrentMapBase.Segment
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for all queries on multiple datatypes
 * Manohar
 */
class AllDataTypesTestCase6 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    try {
      sql(
        "create table Carbon_automation_test6 (imei string,deviceInformationId int,MAC string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        " gamePointId int,contractNumber int,gamePointDescription string) stored by 'org.apache" +
        ".carbondata.format'")
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test6 OPTIONS" +
          "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
          "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked," +
          "series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
          "deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict," +
          "deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId," +
          "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
          "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
          "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
          "Active_webTypeDataVerNumber,Active_operatorsVersion," +
          "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
          "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
          "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
          "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
          "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
          "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId," +
          "contractNumber,gamePointDescription')")


      sql(
        "create table Carbon_automation_test6_hive (imei string,deviceInformationId int,MAC " +
        "string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        " gamePointId int,contractNumber int,gamePointDescription string) row format delimited " +
        "fields terminated by ','")

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test6_hive")

      sql(
        "create table hivetable(imei string,deviceInformationId int,MAC string,deviceColor " +
        "string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize " +
        "string,CUPAudit string,CPIClocked string,series string,productionDate timestamp," +
        "bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string,contractNumber int, ActiveCheckTime string, ActiveAreaId string, " +
        "ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber" +
        " string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, " +
        "Latest_areaId string, Latest_country string, Latest_province string, Latest_city " +
        "string, Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string," +
        " Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string,gamePointId int," +
        "gamePointDescription string) row format " +
        "delimited fields terminated by ','"
      )


      sql(
        "LOAD DATA local inpath'" + currentDirectory + "/src/test/resources/100_olap.csv'" +
        " overwrite INTO table hivetable"
      )

      sql(
        "create table myvmallTest (imei String,uuid String,MAC String,device_color String," +
        "device_shell_color String,device_name String,product_name String,ram String,rom  String," +
        "cpu_clock String,series String,check_date String,check_month int , check_day int," +
        "check_hour int,bom String,inside_name String,packing_date  String,packing_year String," +
        "packing_month String,packing_day String,packing_hour String,customer_name String," +
        "deliveryAreaId String,deliveryCountry String, deliveryProvince String,deliveryCity " +
        "String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time " +
        "String,Active_check_year int, Active_check_month int,Active_check_day int," +
        "Active_check_hour int, ActiveAreaId String,ActiveCountry String,ActiveProvince String," +
        "Activecity String, ActiveDistrict String,Active_network String,Active_firmware_version " +
        "String, Active_emui_version String,Active_os_version String,Latest_check_time String, " +
        "Latest_check_year int,Latest_check_month int,Latest_check_day int, Latest_check_hour " +
        "int,Latest_areaId String,Latest_country String,Latest_province  String,Latest_city " +
        "String,Latest_district String,Latest_firmware_version String, Latest_emui_version " +
        "String,Latest_os_version String,Latest_network String,site String, site_desc String," +
        "product String,product_desc String,check_year int) " +
        "stored by 'org.apache.carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO table myvmallTest " +
          "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,uuid,MAC," +
          "device_color,device_shell_color,device_name,product_name,ram,rom,cpu_clock,series," +
          "check_date,check_year,check_month,check_day,check_hour,bom,inside_name,packing_date," +
          "packing_year,packing_month,packing_day,packing_hour,customer_name,deliveryAreaId," +
          "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,packing_list_no," +
          "order_no,Active_check_time,Active_check_year,Active_check_month,Active_check_day," +
          "Active_check_hour,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict," +
          "Active_network,Active_firmware_version,Active_emui_version,Active_os_version," +
          "Latest_check_time,Latest_check_year,Latest_check_month,Latest_check_day," +
          "Latest_check_hour,Latest_areaId,Latest_country,Latest_province,Latest_city," +
          "Latest_district,Latest_firmware_version,Latest_emui_version,Latest_os_version," +
          "Latest_network,site,site_desc,product,product_desc')")

      sql(
        "create table myvmallTest_hive (imei String,uuid String,MAC String,device_color String," +
        "device_shell_color String,device_name String,product_name String,ram String,rom  String," +
        "cpu_clock String,series String,check_date String,check_month int , check_day int," +
        "check_hour int,bom String,inside_name String,packing_date  String,packing_year String," +
        "packing_month String,packing_day String,packing_hour String,customer_name String," +
        "deliveryAreaId String,deliveryCountry String, deliveryProvince String,deliveryCity " +
        "String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time " +
        "String,Active_check_year int, Active_check_month int,Active_check_day int," +
        "Active_check_hour int, ActiveAreaId String,ActiveCountry String,ActiveProvince String," +
        "Activecity String, ActiveDistrict String,Active_network String,Active_firmware_version " +
        "String, Active_emui_version String,Active_os_version String,Latest_check_time String, " +
        "Latest_check_year int,Latest_check_month int,Latest_check_day int, Latest_check_hour " +
        "int,Latest_areaId String,Latest_country String,Latest_province  String,Latest_city " +
        "String,Latest_district String,Latest_firmware_version String, Latest_emui_version " +
        "String,Latest_os_version String,Latest_network String,site String, site_desc String," +
        "product String,product_desc String,check_year int) " +
        "row format delimited fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO table myvmallTest_hive")


      sql(
        "create table if not exists traffic_2g_3g_4g (source_info string , app_category_id string" +
        " ,app_category_name string ,app_sub_category_id string , app_sub_category_name string ," +
        "rat_name string ,imsi string ,offer_msisdn string , offer_id string ,offer_option_1 " +
        "string ,offer_option_2 string ,offer_option_3 string , msisdn string ,package_type " +
        "string ,package_price string ,tag_imsi string ,tag_msisdn string ,province string ,city " +
        "string ,area_code string ,tac string ,imei string , terminal_type string ,terminal_brand" +
        " string ,terminal_model string ,price_level string  ,network string ,shipped_os string ," +
        "wifi string ,wifi_hotspot string ,gsm string , wcdma string ,td_scdma string ,lte_fdd " +
        "string ,lte_tdd string ,cdma string , screen_size string ,screen_resolution string ," +
        "host_name string ,website_name string , operator string ,srv_type_name string ,tag_host " +
        "string ,cgi string ,cell_name string , coverity_type1 string ,coverity_type2 string ," +
        "coverity_type3 string ,coverity_type4  string ,coverity_type5 string ,latitude string ," +
        "longitude string ,azimuth string , tag_cgi string ,apn string ,user_agent string ,day " +
        "string ,hour string ,`min` string , is_default_bear int ,eps_bearer_id string ,qci int ," +
        "user_filter string , analysis_period string, up_throughput decimal,down_throughput " +
        "decimal, up_pkt_num decimal,down_pkt_num decimal,app_request_num decimal," +
        "pkt_num_len_1_64  decimal,pkt_num_len_64_128 decimal,pkt_num_len_128_256 decimal," +
        "pkt_num_len_256_512  decimal,pkt_num_len_512_768 decimal,pkt_num_len_768_1024 decimal," +
        "pkt_num_len_1024_all  decimal,ip_flow_mark decimal)" +
        " stored by 'org.apache.carbondata.format'")

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table traffic_2g_3g_4g" +
          "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'source_info   , " +
          "app_category_id    ,app_category_name   ,app_sub_category_id   , app_sub_category_name" +
          "   , rat_name   ,imsi   ,offer_msisdn   , offer_id   ,offer_option_1    ," +
          "offer_option_2   ,offer_option_3   , msisdn   ,package_type    ,package_price   ," +
          "tag_imsi   ,tag_msisdn   ,province   ,city    ,area_code   ,tac   ,imei   , " +
          "terminal_type   ,terminal_brand    ,terminal_model   ,price_level   ,network   ," +
          "shipped_os   , wifi   ,wifi_hotspot   ,gsm   , wcdma   ,td_scdma   ,lte_fdd    ," +
          "lte_tdd   ,cdma   , screen_size   ,screen_resolution   , host_name   ,website_name   ," +
          " operator   ,srv_type_name   ,tag_host    ,cgi   ,cell_name   , coverity_type1   ," +
          "coverity_type2   , coverity_type3   ,coverity_type4   ,coverity_type5   ,latitude   , " +
          "longitude   ,azimuth   , tag_cgi   ,apn   ,user_agent   ,day    ,hour   ,`min`   , " +
          "is_default_bear int ,eps_bearer_id   ,qci int , user_filter   , analysis_period  , " +
          "up_throughput  ,down_throughput   , up_pkt_num  ,down_pkt_num  ,app_request_num  , " +
          "pkt_num_len_1_64  ,pkt_num_len_64_128  ,pkt_num_len_128_256  , pkt_num_len_256_512  ," +
          "pkt_num_len_512_768  ,pkt_num_len_768_1024  , pkt_num_len_1024_all  ,ip_flow_mark  ')")

      sql(
        "create table if not exists traffic_2g_3g_4g_hive (source_info string , app_category_id " +
        "string" +
        " ,app_category_name string ,app_sub_category_id string , app_sub_category_name string ," +
        "rat_name string ,imsi string ,offer_msisdn string , offer_id string ,offer_option_1 " +
        "string ,offer_option_2 string ,offer_option_3 string , msisdn string ,package_type " +
        "string ,package_price string ,tag_imsi string ,tag_msisdn string ,province string ,city " +
        "string ,area_code string ,tac string ,imei string , terminal_type string ,terminal_brand" +
        " string ,terminal_model string ,price_level string  ,network string ,shipped_os string ," +
        "wifi string ,wifi_hotspot string ,gsm string , wcdma string ,td_scdma string ,lte_fdd " +
        "string ,lte_tdd string ,cdma string , screen_size string ,screen_resolution string ," +
        "host_name string ,website_name string , operator string ,srv_type_name string ,tag_host " +
        "string ,cgi string ,cell_name string , coverity_type1 string ,coverity_type2 string ," +
        "coverity_type3 string ,coverity_type4  string ,coverity_type5 string ,latitude string ," +
        "longitude string ,azimuth string , tag_cgi string ,apn string ,user_agent string ,day " +
        "string ,hour string ,`min` string , is_default_bear int ,eps_bearer_id string ,qci int ," +
        "user_filter string , analysis_period string, up_throughput decimal,down_throughput " +
        "decimal, up_pkt_num decimal,down_pkt_num decimal,app_request_num decimal," +
        "pkt_num_len_1_64  decimal,pkt_num_len_64_128 decimal,pkt_num_len_128_256 decimal," +
        "pkt_num_len_256_512  decimal,pkt_num_len_512_768 decimal,pkt_num_len_768_1024 decimal," +
        "pkt_num_len_1024_all  decimal,ip_flow_mark decimal)" +
        "row format delimited fields terminated by ','")

      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/FACT_UNITED_DATA_INFO_sample_cube.csv' INTO table " +
          "traffic_2g_3g_4g_hive")



      sql(
        "Create table cube_restructure444 (a0 STRING,a STRING,b0 INT)  stored by 'org.apache" +
        ".carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/restructure_cube.csv'" +
          " INTO TABLE cube_restructure444 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '\"')")

      sql(
        "Create table cube_restructure444_hive (a0 STRING,a STRING,b0 INT)  row format delimited " +
        "fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/restructure_cube.csv'" +
          " INTO TABLE cube_restructure444_hive")

      sql(
        "create table  Carbon_automation_vmall_test1 (imei string,deviceInformationId int,MAC " +
        "string,deviceColor string,device_backColor string,modelId string, marketName string," +
        "AMSize string,ROMSize string,CUPAudit string,CPIClocked string, series string," +
        "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
        "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
        "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
        "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, " +
        "ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
        "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
        "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
        "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY" +
        " int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province " +
        "string, Latest_city string, Latest_district string, Latest_street string, " +
        "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
        "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
        "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string,gamePointId decimal,contractNumber" +
        " decimal) stored by 'org.apache.carbondata.format'")
      sql("LOAD DATA LOCAL INPATH 'hdfs://hacluster/mano/Vmall_100_olap.csv' INTO table " +
          "Carbon_automation_vmall_test1 OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', " +
          "'FILEHEADER'= 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId," +
          "marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode," +
          "internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry," +
          "deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber," +
          "contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
          "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
          "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
          "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
          "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
          "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
          "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
          "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
          "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
          "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")


      sql(
        "create table  Carbon_automation_vmall_test1_hive (imei string,deviceInformationId int," +
        "MAC " +
        "string,deviceColor string,device_backColor string,modelId string, marketName string," +
        "AMSize string,ROMSize string,CUPAudit string,CPIClocked string, series string," +
        "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
        "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, " +
        "deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet " +
        "string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, " +
        "ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, " +
        "ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
        "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
        "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
        "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
        "Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY" +
        " int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province " +
        "string, Latest_city string, Latest_district string, Latest_street string, " +
        "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
        "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
        "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
        "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
        "Latest_operatorId string, gamePointDescription string,gamePointId decimal,contractNumber" +
        " decimal) row format delimited fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH 'hdfs://hacluster/mano/Vmall_100_olap.csv' INTO table " +
          "Carbon_automation_vmall_test1_hive")


      sql(
        "create table Carbon_automation_test5 (imei string,deviceInformationId int,MAC string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        "gamePointDescription string, gamePointId int,contractNumber int) stored by 'org.apache" +
        ".carbondata.format'")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test5 OPTIONS" +
          "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
          "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked," +
          "series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
          "deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict," +
          "deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId," +
          "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
          "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
          "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
          "Active_webTypeDataVerNumber,Active_operatorsVersion," +
          "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
          "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district," +
          "Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion," +
          "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
          "Latest_webTypeDataVerNumber,Latest_operatorsVersion," +
          "Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')")

      sql(
        "create table Carbon_automation_test5_hive (imei string,deviceInformationId int,MAC " +
        "string," +
        "deviceColor string,device_backColor string,modelId string,marketName string,AMSize " +
        "string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate " +
        "string,bomCode string,internalModels string, deliveryTime string, channelsId string, " +
        "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
        "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
        "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
        "string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet " +
        "string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, " +
        "Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, " +
        "Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber " +
        "string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, " +
        "Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId " +
        "string, Latest_country string, Latest_province string, Latest_city string, " +
        "Latest_district string, Latest_street string, Latest_releaseId string, " +
        "Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, " +
        "Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, " +
        "Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, " +
        "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
        "gamePointDescription string, gamePointId int,contractNumber int) row format delimited " +
        "fields terminated by ','")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test5_hive")

      sql("create schema myschema")
      sql("create schema myschema1")
      sql("create schema drug")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
  }

  override def afterAll {
    try {
      sql("drop table Carbon_automation_test6")
      sql("drop table Carbon_automation_test6_hive")
      sql("drop table hivetable")
      sql("drop table myvmallTest")
      sql("drop table traffic_2g_3g_4g")
      sql("drop table traffic_2g_3g_4g_hive")
      sql("drop table cube_restructure444")
      sql("drop table Carbon_automation_vmall_test1")
      sql("drop table Carbon_automation_test5")
      sql("drop table Carbon_automation_test5_hive")
      sql("drop schema myschema")
      sql("drop schema myschema1")
      sql("drop schema drug")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
  }

  //AllDataTypesTestCases2
  //gamePointId int,contractNumber int,gamePointDescription string
  //Test-46
  test("select * Carbon_automation_test6 ")(
  {
    checkAnswer(
      sql(
        "select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName," +
        "AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels," +
        "deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince," +
        "deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime," +
        "ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet," +
        "ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion," +
        "Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId,contractNumber,gamePointDescription from " +
        "Carbon_automation_test6"),
      sql(
        "select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName," +
        "AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels," +
        "deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince," +
        "deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime," +
        "ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet," +
        "ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion," +
        "Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId,contractNumber,gamePointDescription from " +
        "Carbon_automation_test6_hive")
    )
  })



  //Test-46
  test(
    "select sum(deviceinformationid)+10 as a ,series  from Carbon_automation_test6 group by " +
    "series.")(
  {

    checkAnswer(
      sql(
        "select sum(deviceinformationid)+10 as a ,series  from Carbon_automation_test6 group by " +
        "series"),
      sql(
        "select sum(deviceinformationid)+10 as a ,series  from Carbon_automation_test6 group by " +
        "series")
    )
  })



  //Test-48
  test("select sum(latest_year)+10 as a ,series  from Carbon_automation_test6 group by series")({

    checkAnswer(
      sql("select sum(latest_year)+10 as a ,series  from Carbon_automation_test6 group by series"),
      sql(
        "select sum(latest_year)+10 as a ,series  from Carbon_automation_test6_hive group by " +
        "series"))

  })

  //Test-49
  test(
    "select sum(deviceinformationid)+10.32 as a ,series  from Carbon_automation_test6 group by " +
    "series")({

    checkAnswer(
      sql(
        "select sum(deviceinformationid)+10.32 as a ,series  from Carbon_automation_test6 group " +
        "by series"),
      sql(
        "select sum(deviceinformationid)+10.32 as a ,series  from Carbon_automation_test6_hive " +
        "group " +
        "by series"))
  })



  //TC_051
  test("select sum(latest_year)+10.364 as a,series  from Carbon_automation_test6 group by series")({
    checkAnswer(
      sql("select sum(latest_year)+10.364 as a,series  from Carbon_automation_test6 group by " +
          "series"),
      sql(
        "select sum(latest_year)+10.364 as a,series  from Carbon_automation_test6_hive group by " +
        "series")
    )
  })

  //TC_052
  test(
    "select avg(deviceinformationid)+10 as a ,series  from Carbon_automation_test6 group by " +
    "series")({
    checkAnswer(
      sql(
        "select avg(deviceinformationid)+10 as a ,series  from Carbon_automation_test6 group by " +
        "series"),
      sql(
        "select avg(deviceinformationid)+10 as a ,series  from Carbon_automation_test6_hive group" +
        " by " +
        "series")
    )
  })

  //TC_054
  test("select avg(latest_year)+10 as a ,series  from Carbon_automation_test6 group by " +
       "series")({
    checkAnswer(
      sql("select avg(latest_year)+10 as a ,series  from Carbon_automation_test6 group by series"),
      sql(
        "select avg(latest_year)+10 as a ,series  from Carbon_automation_test6_hive group by " +
        "series")
    )
  })


  //TC_072
  test("select sum(deviceInformationId) a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select sum(deviceInformationId) a  from Carbon_automation_test6"),
      sql("select sum(deviceInformationId) a  from Carbon_automation_test6_hive"))
  })

  //TC_073
  test("select sum(channelsId) a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select sum(channelsId) a  from Carbon_automation_test6"),
      sql("select sum(channelsId) a  from Carbon_automation_test6_hive")
    )
  })

  //TC_074
  test("select sum(bomCode)  a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select sum(bomCode)  a  from Carbon_automation_test6"),
      sql("select sum(bomCode)  a  from Carbon_automation_test6_hive")
    )
  })

  //TC_075
  test("select sum(Latest_MONTH)  a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select sum(Latest_MONTH)  a  from Carbon_automation_test6"),
      sql("select sum(Latest_MONTH)  a  from Carbon_automation_test6_hive")
    )
  })

  //TC_078
  test("select sum( DISTINCT channelsId) a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select sum( DISTINCT channelsId) a  from Carbon_automation_test6"),
      sql("select sum( DISTINCT channelsId) a  from hivetable"))
  })

  //TC_083
  test("select avg(deviceInformationId) a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select avg(deviceInformationId) a  from Carbon_automation_test6"),
      sql("select avg(deviceInformationId) a  from Carbon_automation_test6_hive")
    )
  })

  //TC_084
  test("select avg(channelsId) a  from Carbon_automation_test6", NonRunningTests)({
    checkAnswer(
      sql("select avg(channelsId) a  from Carbon_automation_test6"),
      sql("select avg(channelsId) a  from Carbon_automation_test6_hive")
    )
  })

  //TC_085
  test("select avg(bomCode)  a  from Carbon_automation_test6", NonRunningTests)({
    checkAnswer(
      sql("select avg(bomCode)  a  from Carbon_automation_test6"),
      sql("select avg(bomCode)  a  from Carbon_automation_test6_hive")
    )
  })

  //TC_086
  test("select avg(Latest_MONTH)  a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select avg(Latest_MONTH)  a  from Carbon_automation_test6"),
      sql("select avg(Latest_MONTH)  a  from Carbon_automation_test6_hive")
    )
  })


  //TC_104
  test("select var_pop(deviceInformationId)  as a from Carbon_automation_test6")({
    checkAnswer(
      sql("select var_pop(deviceInformationId)  as a from Carbon_automation_test6"),
      sql("select var_pop(deviceInformationId)  as a from Carbon_automation_test6_hive")

    )
  })


  //TC_114
  test("select percentile_approx(deviceInformationId,0.2,5) as a  from " +
       "Carbon_automation_test6")({
    checkAnswer(
      sql("select percentile_approx(deviceInformationId,0.2,5) as a  from Carbon_automation_test6"),
      sql(
        "select percentile_approx(deviceInformationId,0.2,5) as a  from " +
        "Carbon_automation_test6_hive")
    )
  })


  //TC_119
  test("select variance(gamePointId) as a   from Carbon_automation_test6")({
    checkAnswer(
      sql("select variance(gamePointId) as a from Carbon_automation_test6"),
      sql("select variance(gamePointId) as a from Carbon_automation_test6_hive"))
  })

  //TC_120
  test("select var_pop(gamePointId)  as a from Carbon_automation_test6")({
    checkAnswer(
      sql("select var_pop(gamePointId)  as a from Carbon_automation_test6"),
      sql("select var_pop(gamePointId)  as a from Carbon_automation_test6_hive"))
  })

  //TC_121
  test("select var_samp(gamePointId) as a  from Carbon_automation_test6")({
    checkAnswer(
      sql("select var_samp(gamePointId) as a  from Carbon_automation_test6"),
      sql("select var_samp(gamePointId) as a  from Carbon_automation_test6_hive"))
  })

  //TC_123
  test("select stddev_samp(gamePointId)  as a from Carbon_automation_test6")({
    checkAnswer(
      sql("select stddev_samp(gamePointId)  as a from Carbon_automation_test6"),
      sql("select stddev_samp(gamePointId)  as a from Carbon_automation_test6_hive"))
  })

  //TC_126
  test("select corr(gamePointId,gamePointId)  as a from Carbon_automation_test6")({
    checkAnswer(
      sql("select corr(gamePointId,contractNumber)  as a from Carbon_automation_test6"),
      sql("select corr(gamePointId,contractNumber)  as a from Carbon_automation_test6_hive")
    )
  })


  //TC_130
  test("select percentile_approx(gamePointId,0.2,5) as a  from Carbon_automation_test6",
    NonRunningTests)({
    checkAnswer(
      sql("select percentile_approx(gamePointId,0.2,5) as a  from Carbon_automation_test6"),
      sql("select percentile_approx(gamePointId,0.2,5) as a  from Carbon_automation_test6_hive"))
  })



  //TC_135
  test("select FIRST(imei) a from Carbon_automation_test6", NonRunningTests)({
    checkAnswer(
      sql("select FIRST(imei) a from Carbon_automation_test6"),
      sql("select FIRST(imei) a from Carbon_automation_test6_hive"))
  })


  //TC_137
  test("select series,count(imei) as  a from Carbon_automation_test6 group by series order " +
       "by a",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select series,count(imei) as a from Carbon_automation_test6 group by series order by " +
        "a"),
      Seq(Row("1Series", 3),
        Row("9Series", 8),
        Row("3Series", 8),
        Row("4Series", 8),
        Row("2Series", 9),
        Row("6Series", 9),
        Row("7Series", 11),
        Row("8Series", 11),
        Row("0Series", 15),
        Row("5Series", 17))
    )
  })

  //TC_143
  test(
    "select deliveryProvince,series,sum(deviceInformationId) a from Carbon_automation_test6 group" +
    " by deliveryProvince,series order by deliveryProvince,series")({
    checkAnswer(
      sql(
        "select deliveryProvince,series,sum(deviceInformationId) a from Carbon_automation_test6 " +
        "group by deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,sum(deviceInformationId) a from " +
        "Carbon_automation_test6_hive " +
        "group by deliveryProvince,series order by deliveryProvince,series"))
  })

  //TC_144
  test(
    "select deliveryProvince,series,sum(channelsId) a from Carbon_automation_test6 group by " +
    "deliveryProvince,series order by deliveryProvince,series")({
    checkAnswer(
      sql(
        "select deliveryProvince,series,sum(channelsId) a from Carbon_automation_test6 group by " +
        "deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,sum(channelsId) a from Carbon_automation_test6_hive group" +
        " by " +
        "deliveryProvince,series order by deliveryProvince,series"))
  })

  //TC_145
  test(
    "select deliveryProvince,series,sum(bomCode) a from Carbon_automation_test6 group by " +
    "deliveryProvince,series order by deliveryProvince,series")({
    checkAnswer(
      sql(
        "select deliveryProvince,series,sum(bomCode) a from Carbon_automation_test6 group by " +
        "deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,sum(bomCode) a from Carbon_automation_test6_hive group by" +
        " " +
        "deliveryProvince,series order by deliveryProvince,series"))
  })


  //TC_148
  test(
    "select deliveryProvince,series,avg(deviceInformationId) a from Carbon_automation_test6 group" +
    " by deliveryProvince,series order by deliveryProvince,series")({
    checkAnswer(
      sql(
        "select deliveryProvince,series,avg(deviceInformationId) a from Carbon_automation_test6 " +
        "group by deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,avg(deviceInformationId) a from " +
        "Carbon_automation_test6_hive " +
        "group by deliveryProvince,series order by deliveryProvince,series"))
  })

  //TC_149
  test(
    "select deliveryProvince,series,avg(channelsId) a from Carbon_automation_test6 group by " +
    "deliveryProvince,series order by deliveryProvince,series",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select deliveryProvince,series,avg(channelsId) a from Carbon_automation_test6 group by " +
        "deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,avg(channelsId) a from Carbon_automation_test6_hive group" +
        " by " +
        "deliveryProvince,series order by deliveryProvince,series"))
  })

  //TC_150
  test(
    "select deliveryProvince,series,avg(bomCode) a from Carbon_automation_test6 group by " +
    "deliveryProvince,series order by deliveryProvince,series",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select deliveryProvince,series,avg(bomCode) a from Carbon_automation_test6 group by " +
        "deliveryProvince,series order by deliveryProvince,series"),
      sql(
        "select deliveryProvince,series,avg(bomCode) a from Carbon_automation_test6_hive group by" +
        " " +
        "deliveryProvince,series order by deliveryProvince,series"))
  })

  //TC_161
  test(
    "select Latest_DAY from Carbon_automation_test6 where Latest_DAY in (select  Latest_DAY from " +
    "Carbon_automation_test6) order by Latest_DAY",
    NonRunningTests)({
    pending
    checkAnswer(
      sql(
        "select Latest_DAY from Carbon_automation_test6 where Latest_DAY in (select  Latest_DAY" +
        "from Carbon_automation_test6) order by Latest_DAY"),
      sql(
        "select Latest_DAY from Carbon_automation_test6_hive where Latest_DAY in (select  " +
        "Latest_DAY from Carbon_automation_test6_hive) order by Latest_DAY"))
  })


  //TC_164
  test(
    "select channelsId from Carbon_automation_test6 where channelsId in (select  channelsId from " +
    "Carbon_automation_test6) order by channelsId",
    NonRunningTests)({
    pending
    checkAnswer(
      sql(
        "select channelsId from Carbon_automation_test6 where channelsId in (select  channelsId " +
        "from Carbon_automation_test6) order by channelsId"),
      sql(
        "select channelsId from Carbon_automation_test6_hive where channelsId in (select  " +
        "channelsId from Carbon_automation_test6_hive) order by channelsId")
    )
  })

  //TC_165
  test(
    "select  imei, sum(deviceInformationId) as a  from Carbon_automation_test6 where " +
    "deviceInformationId in (select deviceInformationId  from Carbon_automation_test6) group by " +
    "deviceInformationId,imei",
    NonRunningTests)({
    pending
    checkAnswer(
      sql(
        "select  imei, sum(deviceInformationId) as a  from Carbon_automation_test6 where " +
        "deviceInformationId in (select deviceInformationId  from Carbon_automation_test6)      " +
        "group " +
        "by deviceInformationId,imei"),
      sql(
        "select  imei, sum(deviceInformationId) as a  from Carbon_automation_test6_hive where " +
        "deviceInformationId in (select deviceInformationId  from Carbon_automation_test6_hive)  " +
        "    " +
        "group by deviceInformationId,imei"))
  })

  //TC_170
  test(
    "select Upper(series) a ,channelsId from Carbon_automation_test6 group by series,channelsId  " +
    "    " +
    "order by channelsId",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select Upper(series) as a ,channelsId from Carbon_automation_test6 group by series," +
        "channelsId order by channelsId"),
      Seq(Row("8SERIES", "1"),
        Row("0SERIES", "1"),
        Row("5SERIES", "1"),
        Row("3SERIES", "1"),
        Row("1SERIES", "1"),
        Row("9SERIES", "1"),
        Row("4SERIES", "1"),
        Row("7SERIES", "1"),
        Row("2SERIES", "1"),
        Row("8SERIES", "2"),
        Row("0SERIES", "2"),
        Row("5SERIES", "2"),
        Row("6SERIES", "2"),
        Row("4SERIES", "2"),
        Row("7SERIES", "2"),
        Row("2SERIES", "3"),
        Row("0SERIES", "3"),
        Row("8SERIES", "3"),
        Row("5SERIES", "3"),
        Row("3SERIES", "3"),
        Row("6SERIES", "3"),
        Row("9SERIES", "3"),
        Row("7SERIES", "3"),
        Row("8SERIES", "4"),
        Row("0SERIES", "4"),
        Row("5SERIES", "4"),
        Row("3SERIES", "4"),
        Row("6SERIES", "4"),
        Row("7SERIES", "4"),
        Row("2SERIES", "5"),
        Row("0SERIES", "5"),
        Row("8SERIES", "5"),
        Row("5SERIES", "5"),
        Row("3SERIES", "5"),
        Row("6SERIES", "5"),
        Row("7SERIES", "6"),
        Row("2SERIES", "6"),
        Row("0SERIES", "6"),
        Row("8SERIES", "6"),
        Row("5SERIES", "6"),
        Row("3SERIES", "6"),
        Row("6SERIES", "6"),
        Row("9SERIES", "6"),
        Row("1SERIES", "6"),
        Row("4SERIES", "6"),
        Row("7SERIES", "7"),
        Row("2SERIES", "7"),
        Row("0SERIES", "7"),
        Row("8SERIES", "7"),
        Row("5SERIES", "7"),
        Row("6SERIES", "7"),
        Row("1SERIES", "7"),
        Row("4SERIES", "7"))
    )
  })


  //TC_190
  test("select series,gamePointId as a from Carbon_automation_test6  order by series asc limit  " +
       "10 ",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select series,gamePointId as a from Carbon_automation_test6  order by series asc limit  " +
        "    " +
        "10"),
      Seq(Row("0Series", 100001),
        Row("0Series", 100002),
        Row("0Series", 100009),
        Row("0Series", 100011),
        Row("0Series", 10002),
        Row("0Series", 100021),
        Row("0Series", 100025),
        Row("0Series", 100027),
        Row("0Series", 100049),
        Row("0Series", 100065))
    )
  })

  //TC_191
  test(
    "select series,gamePointId as a from Carbon_automation_test6  order by series desc limit     " +
    " 10 ",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select series,gamePointId as a from Carbon_automation_test6  order by series desc      " +
        "limit " +
        "10"),
      Seq(Row("9Series", 100000),
        Row("9Series", 100007),
        Row("9Series", 100017),
        Row("9Series", 100043),
        Row("9Series", 100047),
        Row("9Series", 100057),
        Row("9Series", 100062),
        Row("9Series", 100080),
        Row("8Series", 100008),
        Row("8Series", 100018)))
  })

  //TC_209
  test(
    "select * from (select if( Latest_areaId=3,NULL,Latest_areaId) as babu,NULL a from " +
    "Carbon_automation_test6) qq where babu<=>a",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select * from (select if( Latest_areaId=3,NULL,Latest_areaId) as babu,NULL a from " +
        "Carbon_automation_test6) qq where babu<=>a"),
      sql(
        "select * from (select if( Latest_areaId=3,NULL,Latest_areaId) as babu,NULL a from " +
        "Carbon_automation_test6_hive) qq where babu<=>a"))
  })


  //TC_233
  test(
    "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where channelsId " +
    ">=10 OR channelsId <=1 and series='7Series'",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where " +
        "channelsId >=10 OR channelsId <=1 and series='7Series'"),
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6_hive where " +
        "channelsId >=10 OR channelsId <=1 and series='7Series'"))
  })

  //TC_234
  test(
    "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where channelsId " +
    ">=10 OR channelsId <=1 or series='7Series'",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where " +
        "channelsId >=10 OR channelsId <=1 or series='7Series'"),
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6_hive where " +
        "channelsId >=10 OR channelsId <=1 or series='7Series'"))
  })

  //TC_235
  test(
    "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where channelsId " +
    ">=10 OR (channelsId <=1 and series='1Series')",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6 where " +
        "channelsId >=10 OR (channelsId <=1 and series='1Series')"),
      sql(
        "select imei,gamePointId, channelsId,series  from Carbon_automation_test6_hive where " +
        "channelsId >=10 OR (channelsId <=1 and series='1Series')"))
  })

  //TC_236
  test(
    "select sum(gamePointId) a from Carbon_automation_test6 where channelsId >=10 OR      " +
    "(channelsId " +
    "<=1 and series='1Series')",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select sum(gamePointId) a from Carbon_automation_test6 where channelsId >=10 OR " +
        "(channelsId <=1 and series='1Series')"),
      sql(
        "select sum(gamePointId) a from Carbon_automation_test6_hive where channelsId >=10 OR " +
        "(channelsId <=1 and series='1Series')"))
  })

  //TC_237
  test(
    "select * from (select imei,if(imei='1AA100060',NULL,imei) a from Carbon_automation_test6)  " +
    "aa" +
    "  where a IS NULL",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select * from (select imei,if(imei='1AA100060',NULL,imei) a from " +
        "Carbon_automation_test6) aa  where a IS NULL"),
      sql(
        "select * from (select imei,if(imei='1AA100060',NULL,imei) a from " +
        "Carbon_automation_test6_hive) aa  where a IS NULL"))
  })


  //TC_280
  test(
    "SELECT Activecity, imei, ActiveCountry, ActiveDistrict FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ORDER BY ActiveCountry ASC, ActiveDistrict ASC,      " +
    "Activecity " +
    "ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Activecity, imei, ActiveCountry, ActiveDistrict FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ORDER BY ActiveCountry ASC, ActiveDistrict ASC, " +
        "Activecity ASC"),
      sql(
        "SELECT Activecity, imei, ActiveCountry, ActiveDistrict FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ORDER BY ActiveCountry ASC, ActiveDistrict ASC, " +
        "Activecity ASC"))
  })

  //TC_281
  test(
    "SELECT Activecity, ActiveCountry, ActiveDistrict, MIN(imei) AS Min_imei, MAX(imei) AS " +
    "Max_imei FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY Activecity, " +
    "ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict    " +
    "ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, MIN(imei) AS Min_imei, MAX(imei) AS " +
        "Max_imei FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY Activecity, " +
        "ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC,    " +
        "ActiveDistrict" +
        " ASC"),
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, MIN(imei) AS Min_imei, MAX(imei) AS " +
        "Max_imei FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY    " +
        "Activecity, " +
        "ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC,    " +
        "ActiveDistrict" +
        " ASC"))
  })


  //TC_322
  test(
    "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR    " +
    "FROM" +
    " (select * from Carbon_automation_test6) SUB_QRY WHERE series = \"5Series\" GROUP BY " +
    "ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict    " +
    "ASC, " +
    "deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS    " +
        "Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series = \"5Series\" GROUP   " +
        " BY" +
        " ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC,    " +
        "ActiveDistrict " +
        "ASC, deliveryCity ASC"),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS    " +
        "Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series = \"5Series\" " +
        "GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, " +
        "ActiveDistrict ASC, deliveryCity ASC"))
  })

  //TC_323
  test(
    "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR    " +
    "FROM" +
    " (select * from Carbon_automation_test6) SUB_QRY WHERE deliveryCity = \"zhuzhou\" GROUP BY  " +
    "  " +
    "ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, ActiveDistrict    " +
    "ASC, " +
    "deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS    " +
        "Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE deliveryCity = \"zhuzhou\" " +
        "GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY ActiveCountry ASC, " +
        "ActiveDistrict ASC, deliveryCity ASC"),
      sql(
        "SELECT ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS    " +
        "Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE deliveryCity = " +
        "\"zhuzhou\" GROUP BY ActiveCountry, ActiveDistrict, deliveryCity ORDER BY    " +
        "ActiveCountry " +
        "ASC, ActiveDistrict ASC, deliveryCity ASC"))
  })

  //TC_324
  test(
    "SELECT modelId, ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS " +
    "Sum_Latest_YEAR FROM (select * from Carbon_automation_test6) SUB_QRY WHERE modelId > " +
    "\"2000\" GROUP BY modelId, ActiveCountry, ActiveDistrict, deliveryCity ORDER BY modelId    " +
    "ASC, " +
    " ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT modelId, ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS " +
        "Sum_Latest_YEAR FROM (select * from Carbon_automation_test6) SUB_QRY WHERE modelId > " +
        "\"2000\" GROUP BY modelId, ActiveCountry, ActiveDistrict, deliveryCity ORDER BY    " +
        "modelId " +
        "ASC, ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC"),
      sql(
        "SELECT modelId, ActiveCountry, ActiveDistrict, deliveryCity, SUM(Latest_YEAR) AS " +
        "Sum_Latest_YEAR FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE    " +
        "modelId " +
        "> \"2000\" GROUP BY modelId, ActiveCountry, ActiveDistrict, deliveryCity ORDER BY " +
        "modelId ASC, ActiveCountry ASC, ActiveDistrict ASC, deliveryCity ASC"))
  })

  //TC_325
  test(
    "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
    "Sum_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY WHERE modelId > " +
    "\"2000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
        "Sum_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY WHERE modelId > " +
        "\"2000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"),
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
        "Sum_gamePointId FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE    " +
        "modelId " +
        "> \"2000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"))
  })

  //TC_326
  test(
    "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
    "Sum_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY WHERE imei >= " +
    "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
        "Sum_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY WHERE imei >= " +
        "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"),
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(gamePointId) AS " +
        "Sum_gamePointId FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE imei    " +
        ">= " +
        "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"))
  })

  //TC_328
  test(
    "SELECT imei, deliveryCity, series, Latest_YEAR, gamePointId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY WHERE imei >= \"1AA1000000\" ORDER BY series ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, Latest_YEAR, gamePointId FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY WHERE imei >= \"1AA1000000\" ORDER BY series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, Latest_YEAR, gamePointId FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY WHERE imei >= \"1AA1000000\" ORDER BY series ASC"))
  })

  //TC_330
  test(
    "SELECT deliveryCity, channelsId, SUM(deviceInformationId) AS Sum_deviceInformationId FROM " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, channelsId ORDER BY  " +
    "  " +
    "deliveryCity ASC, channelsId ASC")({
    checkAnswer(
      sql(
        "SELECT deliveryCity, channelsId, SUM(deviceInformationId) AS Sum_deviceInformationId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, channelsId  " +
        "  " +
        "ORDER BY deliveryCity ASC, channelsId ASC"),
      sql(
        "SELECT deliveryCity, channelsId, SUM(deviceInformationId) AS Sum_deviceInformationId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY deliveryCity, " +
        "channelsId ORDER BY deliveryCity ASC, channelsId ASC"))
  })

  //TC_331
  test(
    "SELECT series, imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS  " +
    "  " +
    "Sum_Latest_MONTH, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(contractNumber) AS " +
    "Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId)   " +
    " AS" +
    " Avg_gamePointId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY GROUP BY series, imei, deliveryCity ORDER BY series ASC, " +
    "imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM    " +
        "(Latest_MONTH) " +
        "AS Sum_Latest_MONTH, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(contractNumber) AS " +
        "Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG" +
        "(gamePointId) AS Avg_gamePointId, SUM(gamepointid) AS Sum_gamepointid FROM (select * " +
        "from Carbon_automation_test6) SUB_QRY GROUP BY series, imei, deliveryCity ORDER BY " +
        "series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM    " +
        "(Latest_MONTH) " +
        "AS Sum_Latest_MONTH, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(contractNumber) AS " +
        "Sum_contractNumber, SUM(deviceInformationId) AS Sum_deviceInformationId, AVG" +
        "(gamePointId) AS Avg_gamePointId, SUM(gamepointid) AS Sum_gamepointid FROM (select * " +
        "from Carbon_automation_test6_hive) SUB_QRY GROUP BY series, imei, deliveryCity ORDER    " +
        "BY " +
        "series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_332
  test(
    "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
    "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS " +
    "Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
    "Sum_gamepointid, COUNT(series) AS Count_series FROM (select * from    " +
    "Carbon_automation_test6) " +
    "SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
        "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId)   " +
        " AS" +
        " Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
        "Sum_gamepointid, COUNT(series) AS Count_series FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, " +
        "deliveryCity ASC"),
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
        "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId)   " +
        " AS" +
        " Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
        "Sum_gamepointid, COUNT(series) AS Count_series FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, " +
        "deliveryCity ASC"))
  })

  //TC_333
  test(
    "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
    "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId) AS " +
    "Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
    "Sum_gamepointid, COUNT(DISTINCT series) AS DistinctCount_series FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC,    " +
    "deliveryCity" +
    " ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
        "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId)   " +
        " AS" +
        " Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
        "Sum_gamepointid, COUNT(DISTINCT series) AS DistinctCount_series FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, " +
        "deliveryCity ASC"),
      sql(
        "SELECT imei, deliveryCity, SUM(Latest_DAY) AS Sum_Latest_DAY, SUM(Latest_MONTH) AS " +
        "Sum_Latest_MONTH, SUM(contractNumber) AS Sum_contractNumber, SUM(deviceInformationId)   " +
        " AS" +
        " Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId, SUM(gamepointid) AS " +
        "Sum_gamepointid, COUNT(DISTINCT series) AS DistinctCount_series FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity ORDER BY imei ASC, " +
        "deliveryCity ASC"))
  })

  //TC_334
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE series = \"7Series\" GROUP BY    " +
    "series, " +
    "imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series = \"7Series\" GROUP   " +
        " BY" +
        " series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series = \"7Series\" " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_335
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE series > \"5Series\" GROUP BY    " +
    "series, " +
    "imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series > \"5Series\" GROUP   " +
        " BY" +
        " series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series > \"5Series\" " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_336
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE series >= \"4Series\" GROUP BY    " +
    "series, " +
    " imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series >= \"4Series\" GROUP  " +
        "  " +
        "BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series >= \"4Series\" " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_337
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE series < \"3Series\" GROUP BY    " +
    "series, " +
    "imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series < \"3Series\" GROUP   " +
        " BY" +
        " series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series < \"3Series\" " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_338
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE series <= \"5Series\" GROUP BY    " +
    "series, " +
    " imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE series <= \"5Series\" GROUP  " +
        "  " +
        "BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE series <= \"5Series\" " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"))
  })

  //TC_339
  test(
    "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY WHERE deliveryCity LIKE '%wuhan%' GROUP BY  " +
    "  " +
    "series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY WHERE deliveryCity LIKE '%wuhan%'  " +
        "  " +
        "GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, deliveryCity ASC"),
      sql(
        "SELECT series, imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(Latest_YEAR) AS Sum_Latest_YEAR " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY WHERE deliveryCity LIKE " +
        "'%wuhan%' GROUP BY series, imei, deliveryCity ORDER BY series ASC, imei ASC, " +
        "deliveryCity ASC"))
  })

  //TC_350
  test(
    "SELECT imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(Latest_YEAR) AS " +
    "Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY WHERE NOT(deliveryCity = \"wuhan\") GROUP BY imei, " +
    "deliveryCity ORDER BY imei ASC, deliveryCity ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(Latest_YEAR) AS " +
        "Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select *    " +
        "from" +
        " Carbon_automation_test6) SUB_QRY WHERE NOT(deliveryCity = \"wuhan\") GROUP BY imei, " +
        "deliveryCity ORDER BY imei ASC, deliveryCity ASC"),
      sql(
        "SELECT imei, deliveryCity, SUM(gamePointId) AS Sum_gamePointId, SUM(Latest_YEAR) AS " +
        "Sum_Latest_YEAR, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select *    " +
        "from" +
        " Carbon_automation_test6_hive) SUB_QRY WHERE NOT(deliveryCity = \"wuhan\") GROUP BY " +
        "imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"))
  })

  //TC_365
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_366
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, AVG" +
    "(deviceInformationId) AS Avg_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, AVG" +
        "(deviceInformationId) AS Avg_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, AVG" +
        "(deviceInformationId) AS Avg_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_367
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT" +
    "(deviceInformationId) AS Count_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId    " +
    "FROM" +
    " (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER  " +
    "  " +
    "BY imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT" +
        "(deviceInformationId) AS Count_deviceInformationId, SUM(gamePointId) AS    " +
        "Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT" +
        "(deviceInformationId) AS Count_deviceInformationId, SUM(gamePointId) AS    " +
        "Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_368
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(DISTINCT " +
    "deviceInformationId) AS LONG_COL_0, SUM(gamePointId) AS Sum_gamePointId FROM (select *    " +
    "from " +
    "Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY imei ASC, " +
    "deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(DISTINCT  " +
        "  " +
        "deviceInformationId) AS LONG_COL_0, SUM(gamePointId) AS Sum_gamePointId FROM (select *  " +
        "  " +
        "from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER BY    " +
        "imei " +
        "ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, COUNT(DISTINCT  " +
        "  " +
        "deviceInformationId) AS LONG_COL_0, SUM(gamePointId) AS Sum_gamePointId FROM (select *  " +
        "  " +
        "from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity, series ORDER    " +
        "BY " +
        "imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_370
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MAX" +
    "(deviceInformationId) AS Max_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MAX" +
        "(deviceInformationId) AS Max_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MAX" +
        "(deviceInformationId) AS Max_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_371
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MIN" +
    "(deviceInformationId) AS Min_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MIN" +
        "(deviceInformationId) AS Min_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, MIN" +
        "(deviceInformationId) AS Min_deviceInformationId, SUM(gamePointId) AS Sum_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_374
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, AVG(gamePointId) AS Avg_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_375
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, COUNT(gamePointId) AS Count_gamePointId " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series " +
    "ORDER BY imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(gamePointId) AS " +
        "Count_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, " +
        "deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(gamePointId) AS " +
        "Count_gamePointId FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY " +
        "imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_376
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT gamePointId) AS " +
    "DistinctCount_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY " +
    "imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT gamePointId) AS " +
        "DistinctCount_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY GROUP    " +
        "BY " +
        "imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT gamePointId) AS " +
        "DistinctCount_gamePointId FROM (select * from Carbon_automation_test6_hive) SUB_QRY " +
        "GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_377
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, MAX(gamePointId) AS Max_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MAX(gamePointId) AS Max_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MAX(gamePointId) AS Max_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_378
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei ASC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei ASC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei ASC, deliveryCity ASC, series ASC"))
  })

  //TC_379
  test(
    "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity, series ORDER   " +
    " BY" +
    " imei DESC, deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId " +
        "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY imei, deliveryCity,    " +
        "series" +
        " ORDER BY imei DESC, deliveryCity ASC, series ASC"),
      sql(
        "SELECT imei, deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, MIN(gamePointId) AS Min_gamePointId " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY GROUP BY imei, deliveryCity,  " +
        "  " +
        "series ORDER BY imei DESC, deliveryCity ASC, series ASC"))
  })

  //TC_380
  test(
    "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId)  " +
    "  " +
    "AS Sum_deviceInformationId, COUNT(imei) AS Count_imei, MIN(gamePointId) AS Min_gamePointId  " +
    "  " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, series ORDER    " +
    "BY " +
    "deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(imei) AS Count_imei, MIN" +
        "(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY " +
        "GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC"),
      sql(
        "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(imei) AS Count_imei, MIN" +
        "(gamePointId) AS Min_gamePointId FROM (select * from Carbon_automation_test6_hive) " +
        "SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity ASC, series ASC"))
  })

  //TC_381
  test(
    "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM(deviceInformationId)  " +
    "  " +
    "AS Sum_deviceInformationId, COUNT(DISTINCT imei) AS DistinctCount_imei, MIN(gamePointId)    " +
    "AS " +
    "Min_gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY GROUP BY    " +
    "deliveryCity, " +
    "series ORDER BY deliveryCity ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT imei) AS " +
        "DistinctCount_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, series ORDER BY deliveryCity " +
        "ASC, series ASC"),
      sql(
        "SELECT deliveryCity, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId, COUNT(DISTINCT imei) AS " +
        "DistinctCount_imei, MIN(gamePointId) AS Min_gamePointId FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY GROUP BY deliveryCity, series ORDER BY " +
        "deliveryCity ASC, series ASC"))
  })

  //TC_382
  test(
    "SELECT deliveryCity, Latest_YEAR, imei, SUM(gamePointId) AS Sum_gamePointId, SUM" +
    "(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, Latest_YEAR, imei ORDER BY " +
    "deliveryCity ASC, Latest_YEAR ASC, imei ASC")({
    checkAnswer(
      sql(
        "SELECT deliveryCity, Latest_YEAR, imei, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, Latest_YEAR, imei ORDER BY " +
        "deliveryCity ASC, Latest_YEAR ASC, imei ASC"),
      sql(
        "SELECT deliveryCity, Latest_YEAR, imei, SUM(gamePointId) AS Sum_gamePointId, SUM" +
        "(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY GROUP BY deliveryCity, Latest_YEAR, imei ORDER    " +
        "BY " +
        "deliveryCity ASC, Latest_YEAR ASC, imei ASC"))
  })

  //TC_383
  test(
    "SELECT deliveryCity, imei, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * from  " +
    " " +
    "Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, imei, series ORDER BY deliveryCity  " +
    "  " +
    "ASC, imei ASC, series ASC")({
    checkAnswer(
      sql(
        "SELECT deliveryCity, imei, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * " +
        "from Carbon_automation_test6) SUB_QRY GROUP BY deliveryCity, imei, series ORDER BY " +
        "deliveryCity ASC, imei ASC, series ASC"),
      sql(
        "SELECT deliveryCity, imei, series, SUM(Latest_YEAR) AS Sum_Latest_YEAR FROM (select * " +
        "from Carbon_automation_test6_hive) SUB_QRY GROUP BY deliveryCity, imei, series ORDER    " +
        "BY " +
        "deliveryCity ASC, imei ASC, series ASC"))
  })


  //TC_389
  test(
    "SELECT imei, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY GROUP BY imei ORDER BY imei ASC")({
    checkAnswer(
      sql(
        "SELECT imei, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY GROUP BY imei ORDER BY imei ASC"),
      sql(
        "SELECT imei, SUM(deviceInformationId) AS Sum_deviceInformationId FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY GROUP BY imei ORDER BY imei ASC"))
  })

  //TC_422
  test(
    "select  min(channelsName) from Carbon_automation_test6 where  deviceinformationid is    null",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select  min(channelsName) from Carbon_automation_test6 where  deviceinformationid is  " +
        "null"),
      sql(
        "select  min(channelsName) from Carbon_automation_test6_hive where  deviceinformationid  " +
        "  " +
        "is  null"))
  })

  //TC_423
  test("select  max(channelsName) from  Carbon_automation_test6 where  deviceinformationid " +
       "is  " +
       "null",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select  max(channelsName) from  Carbon_automation_test6 where  deviceinformationid is   " +
        " " +
        "null"),
      sql(
        "select  max(channelsName) from  Carbon_automation_test6_hive where    " +
        "deviceinformationid " +
        "is  null"))
  })

  //TC_437
  test("SELECT sum(deviceInformationId) FROM Carbon_automation_test6 where imei is NOT " +
       "null")({
    checkAnswer(
      sql("SELECT sum(deviceInformationId) FROM Carbon_automation_test6 where imei is NOT null"),
      sql("SELECT sum(deviceInformationId) FROM Carbon_automation_test6_hive where imei is NOT " +
          "null"))
  })

  //TC_441
  test(
    "select variance(gamepointid), var_pop(gamepointid)  from Carbon_automation_test6 where " +
    "channelsid>2")({
    checkAnswer(
      sql(
        "select variance(gamepointid), var_pop(gamepointid)  from Carbon_automation_test6 where  " +
        "  " +
        "channelsid>2"),
      sql(
        "select variance(gamepointid), var_pop(gamepointid)  from Carbon_automation_test6_hive " +
        "where channelsid>2"))
  })

  //TC_445
  test(
    "select variance(bomcode), var_pop(gamepointid)  from Carbon_automation_test6 where " +
    "activeareaid>3")({
    checkAnswer(
      sql(
        "select variance(bomcode), var_pop(gamepointid)  from Carbon_automation_test6 where " +
        "activeareaid>3"),
      sql(
        "select variance(bomcode), var_pop(gamepointid)  from Carbon_automation_test6_hive    " +
        "where " +
        "activeareaid>3"))
  })

  //TC_447
  test("select var_samp(contractNumber) from Carbon_automation_test6", NonRunningTests)({
    checkAnswer(
      sql("select var_samp(contractNumber) from Carbon_automation_test6"),
      sql("select var_samp(contractNumber) from Carbon_automation_test6_hive"))
  })

  //TC_464
  test("select covar_pop(gamePointId,deviceInformationId ) from Carbon_automation_test6")({
    checkAnswer(
      sql("select covar_pop(gamePointId,deviceInformationId ) from Carbon_automation_test6"),
      sql("select covar_pop(gamePointId,deviceInformationId ) from Carbon_automation_test6_hive"))
  })


  //TC_498
  test("select covar_pop(gamePointId, contractNumber) from Carbon_automation_test6")({
    checkAnswer(
      sql("select covar_pop(gamePointId, contractNumber) from Carbon_automation_test6"),
      sql("select covar_pop(gamePointId, contractNumber) from Carbon_automation_test6_hive"))
  })

  //TC_499
  test("select covar_samp(gamePointId, contractNumber) from Carbon_automation_test6")({
    checkAnswer(
      sql("select covar_samp(gamePointId, contractNumber) from Carbon_automation_test6"),
      sql("select covar_samp(gamePointId, contractNumber) from Carbon_automation_test6_hive"))
  })

  //AllDataTypesTestCase3


  //TC_270
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize,    " +
    "ActiveAreaId " +
    "ORDER BY AMSize ASC, ActiveAreaId ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, " +
        "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"),
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY WHERE AMSize < \"0RAM size\" GROUP BY AMSize, " +
        "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"))
  })

  //TC_612
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize    " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM  " +
    "  " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 RIGHT JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_test61 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_test61.AMSize WHERE Carbon_automation_test6.AMSize IN (\"4RAM size\"," +
    "\"8RAM size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
    ".ActiveCountry, Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId " +
    "ORDER BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize  " +
      "  " +
      "AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry,    " +
      "Carbon_automation_test6" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity    " +
      "FROM" +
      " (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 RIGHT JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
      "SUB_QRY ) Carbon_automation_test61 ON Carbon_automation_test6.AMSize = " +
      "Carbon_automation_test61.AMSize WHERE Carbon_automation_test6.AMSize IN (\"4RAM size\"," +
      "\"8RAM size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
      ".ActiveCountry, Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId " +
      "ORDER BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
      "Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100013, "8RAM size", "1", "Guangdong Province"),
        Row(1, "8RAM size", "2", "Guangdong Province"),
        Row(100016, "8RAM size", "4", "Hunan Province"),
        Row(100038, "8RAM size", "4", "Hunan Province"),
        Row(100081, "8RAM size", "5", "Hunan Province"),
        Row(100063, "8RAM size", "5", "Hunan Province"),
        Row(100052, "8RAM size", "5", "Hunan Province"),
        Row(100004, "8RAM size", "6", "Hubei Province"),
        Row(10002, "8RAM size", "7", "Hubei Province"),
        Row(100023, "8RAM size", "7", "Hubei Province"))
    )
  })


  //VMALL_Per_TC_000
  test("select count(*) from    myvmallTest", NonRunningTests)({
    checkAnswer(
      sql("select count(*) from    myvmallTest"),
      sql("select count(*) from    myvmallTest_hive"))
  })

  //VMALL_Per_TC_001
  test(
    "SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from " +
    "myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from " +
      "myvmallTest) SUB_QRY GROUP BY product_name ORDER BY product_name ASC"),
      sql(
        "SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from " +
        "myvmallTest_hive) SUB_QRY GROUP BY product_name ORDER BY product_name ASC"))

  })

  //VMALL_Per_TC_004
  test(
    "SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER    " +
    "BY " +
    "device_color ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT device_color FROM (select * from myvmallTest) SUB_QRY GROUP BY device_color ORDER  " +
      "  " +
      "BY device_color ASC"),
      sql(
        "SELECT device_color FROM (select * from myvmallTest_hive) SUB_QRY GROUP BY    " +
        "device_color " +
        "ORDER " +
        "BY device_color ASC"))
  })

  //VMALL_Per_TC_005
  test(
    "SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name ORDER   " +
    " BY" +
    "  product_name ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT product_name  FROM (select * from myvmallTest) SUB_QRY GROUP BY product_name    " +
      "ORDER " +
      "BY  product_name ASC"),
      sql(
        "SELECT product_name  FROM (select * from myvmallTest_hive) SUB_QRY GROUP BY    " +
        "product_name" +
        " ORDER " +
        "BY  product_name ASC"))
  })

  //VMALL_Per_TC_007
  test("select count(distinct imei) DistinctCount_imei from myvmallTest", NonRunningTests)({
    checkAnswer(
      sql("select count(distinct imei) DistinctCount_imei from myvmallTest"),
      sql("select count(distinct imei) DistinctCount_imei from myvmallTest_hive"))
  })

  //VMALL_Per_TC_009
  test(
    "select (t1.Latest_MONTH/t2.Latest_DAY)*100 from (select count (imei)  as " +
    "result from Carbon_automation_test6 where imei=\"EmotionUI_2.1\")t1,(select count" +
    "(imei) as totalc from Carbon_automation_test6)t2",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select (t1.result/t2.totalc)*100 from (select count (imei)  as " +
        "result from Carbon_automation_test6 )t1,(select count" +
        "(imei) as totalc from Carbon_automation_test6)t2"),
      sql(
        "select (t1.result/t2.totalc)*100 from (select count (imei)  as " +
        "result from Carbon_automation_test6_hive)t1,(select count" +
        "(imei) as totalc from Carbon_automation_test6_hive)t2"))
  })

  //VMALL_Per_TC_023
  test(
    "SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest   ) SUB_QRY  " +
    "  " +
    "where series LIKE 'series1%' group by series",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmallTest ) SUB_QRY" +
      " where series LIKE 'series1%' group by series"),
      sql(
        "SELECT  count(imei) as distinct_imei,series FROM (select * from  myvmallTest_hive) " +
        "SUB_QRY" +
        " where series LIKE 'series1%' group by series"))
  })

  //VMALL_Per_TC_039
  test(
    "select device_name, count(distinct imei) as imei_number from  myvmallTest  group by " +
    "device_name",
    NonRunningTests)({
    checkAnswer(sql(
      "select device_name, count(distinct imei) as imei_number from  myvmallTest  group by " +
      "device_name"),
      sql(
        "select device_name, count(distinct imei) as imei_number from  myvmallTest_hive  group   " +
        " by" +
        " " +
        "device_name"))
  })


  //VMALL_Per_TC_040
  test(
    "select product_name, count(distinct imei) as imei_number from  myvmallTest  group by " +
    "product_name",
    NonRunningTests)({
    checkAnswer(sql(
      "select product_name, count(distinct imei) as imei_number from  myvmallTest  group by " +
      "product_name"),
      sql(
        "select product_name, count(distinct imei) as imei_number from  myvmallTest_hive  group  " +
        "  " +
        "by " +
        "product_name"))
  })


  //VMALL_Per_TC_043
  test(
    "select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest  " +
    "group by product_name,device_name",
    NonRunningTests)({
    checkAnswer(sql(
      "select product_name, device_name, count(distinct imei) as imei_number from  myvmallTest   " +
      " " +
      "group by product_name,device_name"),
      sql(
        "select product_name, device_name, count(distinct imei) as imei_number from  " +
        "myvmallTest_hive  " +
        "group by product_name,device_name"))
  })

  //VMALL_Per_TC_047
  test(
    "select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest  " +
    "group by device_color,product_name order by product_name limit 1000",
    NonRunningTests)({
    checkAnswer(sql(
      "select device_color,product_name, count(distinct imei) as imei_number from  myvmallTest   " +
      " " +
      "group by device_color,product_name order by product_name limit 1000"),
      sql(
        "select device_color,product_name, count(distinct imei) as imei_number from  " +
        "myvmallTest_hive  " +
        "group by device_color,product_name order by product_name limit 1000"))
  })


  //VMALL_Per_TC_049
  test(
    "SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY " +
    "product_name ORDER BY product_name ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  GROUP BY  " +
      "  " +
      "product_name ORDER BY product_name ASC"),
      sql(
        "SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest_hive  " +
        "GROUP BY " +
        "product_name ORDER BY product_name ASC"))
  })

  //VMALL_Per_TC_051
  test(
    "SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  " +
    "myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name    " +
    "ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  " +
      "myvmallTest  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name  " +
      "  " +
      "ASC"),
      sql(
        "SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  " +
        "myvmallTest_hive  GROUP BY device_color, product_name ORDER BY device_color ASC, " +
        "product_name " +
        "ASC"))
  })

  //VMALL_Per_TC_053
  test(
    "SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY product_name  " +
    "  " +
    "ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT product_name FROM  myvmallTest  SUB_QRY GROUP BY product_name ORDER BY    " +
      "product_name" +
      " ASC"),
      sql(
        "SELECT product_name FROM  myvmallTest_hive  SUB_QRY GROUP BY product_name ORDER BY " +
        "product_name" +
        " ASC"))
  })

  //VMALL_Per_TC_054
  test(
    "SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  " +
    "GROUP BY product_name ORDER BY product_name ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmallTest  " +
      "  " +
      "GROUP BY product_name ORDER BY product_name ASC"),
      sql(
        "SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  " +
        "myvmallTest_hive  " +
        "GROUP BY product_name ORDER BY product_name ASC"))
  })

  //SmartPCC_Perf_TC_002
  test(
    "select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g    " +
    "where" +
    " RAT_NAME='GERAN' group by MSISDN having total < 1073741824 order by total desc",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select imei,sum(Latest_MONTH)+sum(Latest_DAY) as total from  Carbon_automation_test6  " +
        "where deviceColor='2Device Color' group by imei having total < 1073741824 order by    " +
        "total" +
        " desc"),
      Seq(Row("1AA10006", 2022),
        Row("1AA100061", 2022),
        Row("1AA10001", 2022),
        Row("1AA1", 2022),
        Row("1AA1000", 2022),
        Row("1AA100071", 2022),
        Row("1AA100001", 2022),
        Row("1AA100020", 2022)))
  })

  //SmartPCC_Perf_TC_027
  test(
    "select t2.MSISDN,t1.APP_SUB_CATEGORY_NAME,count(t1.MSISDN) as msidn_number,sum(t1" +
    ".UP_THROUGHPUT)+sum(t1.Latest_DAY) as total from  traffic_2g_3g_4g  t1, viptable t2 " +
    "where t1.MSISDN=t2.MSISDN group by t1.APP_SUB_CATEGORY_NAME,t2.MSISDN",
    NonRunningTests)({

    checkAnswer(
      sql(
        "select t2.imei,t1.deviceColor,count(t1.imei) as imei_number,sum(t1" +
        ".Latest_MONTH)+sum(t1.Latest_DAY) as total from  Carbon_automation_test6  t1,    " +
        "Carbon_automation_test6_hive t2" +
        " where t1.imei=t2.imei group by t1.deviceColor,t2.imei"),
      sql(
        "select t2.imei,t1.deviceColor,count(t1.imei) as imei_number,sum(t1" +
        ".Latest_MONTH)+sum(t1.Latest_DAY) as total from  Carbon_automation_test6_hive  t1, " +
        "Carbon_automation_test6 t2 where t1.imei=t2.imei group by t1.deviceColor,t2.imei"))
  })

  //SmartPCC_Perf_TC_030
  test(
    "select t2.MSISDN,t1.deviceColor,count(t1.MSISDN) as msidn_number,sum(t1.UP_THROUGHPUT)+sum  " +
    "  (t1" +
    ".DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  t1, viptable t2 where t1.MSISDN=t2    " +
    ".MSISDN" +
    " group by t1.RAT_NAME,t2.MSISDN",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select t2.imei,t1.deviceColor,count(t1.imei) as imei,sum(t1.Latest_MONTH)+sum" +
        "(t1.Latest_DAY) as total from  Carbon_automation_test6  t1,    " +
        "Carbon_automation_test6_hive t2 where t1.imei = t2" +
        ".imei group by t1.deviceColor,t2.imei"),
      sql(
        "select t2.imei,t1.deviceColor,count(t1.imei) as imei,sum(t1.Latest_MONTH)+sum" +
        "(t1.Latest_DAY) as total from  Carbon_automation_test6_hive  t1,    " +
        "Carbon_automation_test6_hive t2 where t1.imei = t2" +
        ".imei group by t1.deviceColor,t2.imei"))
  })

  //SmartPCC_Perf_TC_031
  test(
    "select level, sum(sumUPdown) as total, count(distinct MSISDN) as MSISDN_count from (select  " +
    "  " +
    "MSISDN, t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT as sumUPdown, if((t1.UP_THROUGHPUT+t1" +
    ".DOWN_THROUGHPUT)>52428800, '>50M', if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>10485760," +
    "'50M~10M',if((t1.UP_THROUGHPUT+t1.DOWN_THROUGHPUT)>1048576, '10M~1M','<1m'))) as level    " +
    "from " +
    " traffic_2g_3g_4g  t1) t2 group by level",
    NonRunningTests)({

    checkAnswer(
      sql(
        "select ROMSize, sum(Latest_DAY) as total, count(distinct imei) as imei_count from " +
        "(select ROMSize,Latest_DAY,imei, t1.Latest_MONTH+t1.Latest_DAY as sumUPdown, if((t1" +
        ".Latest_MONTH+t1.Latest_DAY)>52, '>50M', if((t1.Latest_MONTH+t1.Latest_DAY)>10," +
        "'50M~10M',if((t1.Latest_DAY+t1.Latest_MONTH)>10, '10M~1M','<1m'))) as level from " +
        "Carbon_automation_test6 t1) t2 group by ROMSize"),
      sql(
        "select ROMSize, sum(Latest_DAY) as total, count(distinct imei) as imei_count from " +
        "(select ROMSize,Latest_DAY,imei, t1.Latest_MONTH+t1.Latest_DAY as sumUPdown, if((t1" +
        ".Latest_MONTH+t1.Latest_DAY)>52, '>50M', if((t1.Latest_MONTH+t1.Latest_DAY)>10," +
        "'50M~10M',if((t1.Latest_DAY+t1.Latest_MONTH)>10, '10M~1M','<1m'))) as level from " +
        "Carbon_automation_test6 t1) t2 group by ROMSize"))
  })

  //SmartPCC_Perf_TC_036
  test(
    "select SOURCE_INFO,APP_CATEGORY_ID,APP_CATEGORY_NAME,AREA_CODE,CITY,UP_THROUGHPUT," +
    "DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8615209309657' and APP_CATEGORY_ID='-1'",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select ROMSize,AMSize,imei,ROMSize,channelsName,Latest_MONTH," +
        "Latest_DAY from Carbon_automation_test6 where AMSize='8RAM size' and " +
        "ROMSize='4ROM size'"),
      sql(
        "select ROMSize,AMSize,imei,ROMSize,channelsName,Latest_MONTH," +
        "Latest_DAY from Carbon_automation_test6_hive where AMSize='8RAM size' and " +
        "ROMSize='4ROM size'"))
  })


  //AllDataTypesTestcase4

  //TC_1253
  test("TC_1253") {

    sql("drop table if exists table123")
    sql(
      "create table table123 (imei string,deviceInformationId INT,MAC string,deviceColor    " +
      "string, " +
      "device_backColor string,modelId string, marketName string, AMSize string, ROMSize    " +
      "string, " +
      "CUPAudit string, CPIClocked string, series string, productionDate string, bomCode    " +
      "string, " +
      "internalModels string, deliveryTime string, channelsId string, channelsName string , " +
      "deliveryAreaId string, deliveryCountry string, deliveryProvince  string, deliveryCity " +
      "string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, " +
      "ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince    " +
      "string, " +
      "Activecity string, ActiveDistrict  string, ActiveStreet string, ActiveOperatorId string,  " +
      "  " +
      "Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, " +
      "Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, " +
      "Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,    " +
      "Active_operatorsVersion" +
      " string, Active_phonePADPartitionedVersions string, Latest_YEAR  INT, Latest_MONTH INT, " +
      "Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street    " +
      "string, " +
      "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
      "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
      "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal,contractNumber  " +
      "  " +
      "decimal) stored by 'org.apache.carbondata.format'")
    checkAnswer(
      sql("show segments for table table123"),
      Seq())
    sql("drop table table123")
  }

  //TC_1254
  test("TC_1254", NonRunningTests) {

    sql("drop table if exists table9")
    sql(
      "create table table9 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table9 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table9 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    val dataFrame = sql("show segments for table table9")
    val length = dataFrame.collect().length
    assertEquals(length, 2)
    dataFrame.collect().map(row => {
      assert(row(0) == "0" || row(0) == "1")
      assert(row(1) == "Success")
      assert((row(2) + "").split(":")(0) ==
             (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
      assert((row(3) + "").split(":")(0) ==
             (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
    })
    /*
        checkAnswer(
          sql("show segments for table table9"),
          Seq(Row(0, "Success", "2016-11-28 03:42:05.0", "2016-11-28 03:42:05.0"),
            Row(1, "Success", "2016-11-28 03:42:05.0", "2016-11-28 03:42:05.0 ")))
    */
    sql("drop table table9")
  }

  //TC_1258
  test("TC_1258", NonRunningTests) {
    sql("drop table if exists table13")
    sql(
      "create table table13 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    val startTime: String = new Timestamp(System.currentTimeMillis()).toString();
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table13 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")

    val dataFrame = sql("sHOw segMents for table table13")
    val length = dataFrame.collect().length
    assertEquals(length, 1)
    dataFrame.collect().map(row => {
      assertEquals(row(0), "0")
      assertEquals(row(1), "Success")
      assertEquals((row(2) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
      assertEquals((row(3) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
    })
    /* checkAnswer(
       sql("sHOw segMents for table table13"),
       Seq(Row("0","Success", startTime,startTime)))*/
    sql("drop table table13")
  }

  //DTS2015112006803_02
  test("DTS2015112006803_02", NonRunningTests) {
    sql("drop table if exists table14")
    sql(
      "create table table14 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table14 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("select * from table14")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table14 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("select * from table14")

    val dataFrame = sql("show segments for table table14")
    val length = dataFrame.collect().length
    assertEquals(length, 2)
    dataFrame.collect().map(row => {
      assert(row(0) == "0" || row(0) == "1")
      assertEquals(row(1), "Success")
      assertEquals((row(2) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
      assertEquals((row(3) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
    })

    /*
    checkAnswer(
      sql("show segments for table table14"),
      Seq(Row("0,Success,2016-11-28 03:42:06.0,2016-11-28 03:42:06.0"),
        Row("1,Success,2016-11-28    03: 42: 06.0, 2016 - 11 - 28 03: 42: 06.0 ")))
    */
    sql("drop table table14")
  }

  //DTS2015110901347
  test("DTS2015110901347", NonRunningTests) {

    sql("drop table if exists table15")
    sql(
      "create table table15 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table15 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table15 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table15 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table15 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    val dataFrame = sql("show segments for table table15")
    val length = dataFrame.collect().length
    assertEquals(length, 4)
    dataFrame.collect().map(row => {
      assert(row(0) == "0" || row(0) == "1" || row(0) == "2" || row(0) == "3")
      assertEquals(row(1), "Success")
      assertEquals((row(2) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
      assertEquals((row(3) + "").split(":")(0),
        (new Timestamp(System.currentTimeMillis()) + "").split(":")(0))
    })

    /*checkAnswer(
      sql("show segments for table table15"),
      Seq(Row("0", "Success", "2016-11-28 03:42:06.0,2016-11-28 03:42:06.0"),
        Row("1", "Success", "2016-11-28 03:42:07.0", "2016-11-28 03:42:07.0"),
        Row("2", "Success", "2016-11-28 03:42:07.0", "2016-11-28 03:42:07.0"),
        Row("3", "Success", "2016-11-28 03:42:07.0", "2016-11-28 03:42:07.0")))*/
    sql("drop table table15")
  }

  //DTS2015102211549
  test("DTS2015102211549") {
    sql("drop table if exists table23")
    sql(
      "create table table23 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table23 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")

    checkAnswer(
      sql("select imei,max(gamePointId) FROM table23 where imei=\"1AA10006\" group by imei"),
      Seq(Row("1AA10006", 2478)))
    sql("drop table table23")
  }

  //DTS2015102309588_01
  test("DTS2015102309588_01") {
    sql("drop table if exists table24")
    sql(
      "create table table24 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table24 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")

    checkAnswer(
      sql(
        "select imei,sum(distinct gamePointId) FROM table24 where imei=\"1AA10006\" group by    " +
        "imei" +
        " limit 1"),
      Seq(Row("1AA10006", 2478))
    )
    sql("drop table table24")
  }

  //DTS2015102309588_02
  test("DTS2015102309588_02") {
    sql("drop table if exists table25")
    sql(
      "create table table25 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table25 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")

    checkAnswer(
      sql("select count(imei),count(distinct gamePointId) FROM table25 group by imei limit 1"),
      Seq(Row(1, 1)))
    sql("drop table table25")
  }

  //TC_1293
  test("TC_1293", NonRunningTests) {
    sql("drop table if exists table3")

    sql(
      "create table table3 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table3 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("delete segment 0 from  table table3")
    checkAnswer(
      sql("select gamePointId from table3"),
      Seq())
    sql("drop table table3")
  }

  //TC_1302
  test("TC_1302", NonRunningTests) {
    sql("drop table if exists table12")

    sql(
      "create table table12 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
      "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
      ".carbondata.format'")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table12 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
        "/src/test/resources/TestData1.csv' INTO table table12 OPTIONS('DELIMITER'= ',' ," +
        "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
        "ActiveCountry,Activecity,gamePointId')")
    sql("delete segment 0,1 from  table table12")
    checkAnswer(
      sql("select gamePointId from table12 limit 1"),
      Seq())
    sql("drop table table12")
  }


  /*//TC_1308
    test("TC_1308", NonRunningTests) {
      sql("drop table if exists table18")

      sql(
        "create table table18 (imei string,AMSize string,channelsId string,ActiveCountry string, " +
        "Activecity string,gamePointId decimal,deviceInformationId INT) stored by 'org.apache" +
        ".carbondata.format'")
      sql("LOAD DATA LOCAL INPATH  '" + currentDirectory +
          "/src/test/resources/TestData1.csv' INTO table table18 OPTIONS('DELIMITER'= ',' ," +
          "'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId," +
          "ActiveCountry,Activecity,gamePointId')")
      sql("delete segment 0,1 from  table table18")
      checkAnswer(
        sql("select ActiveCountry from table18 limit 1"),
        Seq())
      sql("drop table table18")
    }*/


  //TC_1311
  test("TC_1311", NonRunningTests) {
    sql("drop table if exists testretention")
    sql(
      "create table testretention (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,productionDate timestamp,gamePointId decimal,    " +
      "deviceInformationId" +
      " INT) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql("delete segment 0 from table testretention")
    checkAnswer(
      sql("select imei,deviceInformationId from testretention"),
      Seq())
    sql("drop table testretention")
  }

  //TC_1314
  test("TC_1314", NonRunningTests) {
    sql("drop table if exists testretention3")
    sql(
      "create table testretention3 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,productionDate timestamp,gamePointId decimal,    " +
      "deviceInformationId" +
      " INT) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention3 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention3 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql("delete segment  0 from table testretention3")
    checkAnswer(
      sql("select imei,AMSize from testretention3 where gamePointId=1407"),
      Seq(Row("1AA100051", "3RAM size"), Row("1AA100061", "0RAM size")))
    sql("drop table testretention3")
  }
  //TC_1316
  test("TC_1316", NonRunningTests) {
    sql("drop table if exists testretention5")
    sql(
      "create table testretention5 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,productionDate timestamp,gamePointId decimal,    " +
      "deviceInformationId" +
      " INT) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention5 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention5 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql("delete segment 0,1 from table testretention5")
    checkAnswer(
      sql("select imei,deviceInformationId from testretention5"),
      Seq())
    sql("drop table testretention5")
  }

  //TC_1318
  test("TC_1318", NonRunningTests) {
    sql("drop table if exists testretention6")
    sql(
      "create table testretention6 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,productionDate timestamp,gamePointId decimal,    " +
      "deviceInformationId" +
      " INT) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention6 " +
      "OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql("delete segment 0 from table testretention6")
    checkAnswer(
      sql("select count(*)  from testretention6"),
      Seq(Row(0)))
    sql("drop table testretention6")
  }

  //TC_1322
  test("TC_1322", NonRunningTests) {
    pending
    sql("drop table if exists testretention9")
    sql(
      "create table testretention9 (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,productionDate timestamp,gamePointId decimal,    " +
      "deviceInformationId" +
      " INT) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table testretention9 " +
      "OPTIONS('DELIMITER'= ',','QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId," +
      "AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate')")
    sql(
      "delete segment 0 from table testretention9 where productionDate before '2015-10-06 " +
      "12:07:28 '")
    checkAnswer(
      sql("select count(*) from testretention9"),
      Seq(Row(3)))
    sql("drop table testretention9")
  }

  //DTS2015110209900
  test("DTS2015110209900", NonRunningTests) {
    sql("drop table if exists table_restructure63")
    sql(
      "create table table_restructure63  (a0 STRING,b0 INT) stored by 'org.apache.carbondata" +
      ".format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/restructure_table.csv' INTO table " +
      "table_restructure63 OPTIONS('DELIMITER'= ',','QUOTECHAR'=  '\"', 'FILEHEADER'= 'a0,b0')")
    sql("delete segment 0 from table table_RESTRUCTURE63")
    checkAnswer(
      sql("select * from table_restructure63 limit 1"),
      Seq())
    sql("drop table table_restructure63")
  }

  //DTS2015112611263
  test("DTS2015112611263", NonRunningTests) {
    sql("drop table if exists makamraghutest002")
    sql(
      "create table makamraghutest002 (imei string,deviceInformationId INT,MAC string,    " +
      "deviceColor" +
      " string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize " +
      "string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode " +
      "string,internalModels string, deliveryTime string, channelsId string, channelsName    " +
      "string " +
      ", deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity " +
      "string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, " +
      "ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince    " +
      "string, " +
      "Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, " +
      "Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, " +
      "Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, " +
      "Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string,    " +
      "Active_operatorsVersion" +
      " string, Active_phonePADPartitionedVersions string, Latest_YEAR INT, Latest_MONTH INT, " +
      "Latest_DAY INT, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
      "Latest_province string, Latest_city string, Latest_district string, Latest_street    " +
      "string, " +
      "Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, " +
      "Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, " +
      "Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
      "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
      "Latest_operatorId string, gamePointDescription string,gamePointId decimal,contractNumber  " +
      "  " +
      "decimal) stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/100.csv' INTO table makamraghutest002 OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
      "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,    " +
      "series, " +
      "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,    " +
      "deliveryAreaId, " +
      "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
      "oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince," +
      "Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId," +
      "Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer," +
      "Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber," +
      "Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH," +
      "Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city," +
      "Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,    " +
      "Latest_operaSysVersion, " +
      "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
      "Latest_operatorId,gamePointId,gamePointDescription')")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/100.csv' INTO table makamraghutest002 OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
      "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,    " +
      "series, " +
      "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,    " +
      "deliveryAreaId, " +
      "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
      "oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince," +
      "Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId," +
      "Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer," +
      "Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber," +
      "Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH," +
      "Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city," +
      "Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,    " +
      "Latest_operaSysVersion, " +
      "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
      "Latest_operatorId,gamePointId,gamePointDescription')")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/100.csv' INTO table makamraghutest002 OPTIONS" +
      "('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC," +
      "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,    " +
      "series, " +
      "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,    " +
      "deliveryAreaId, " +
      "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
      "oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince," +
      "Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId," +
      "Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer," +
      "Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber," +
      "Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH," +
      "Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city," +
      "Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,    " +
      "Latest_operaSysVersion, " +
      "Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
      "Latest_operatorId,gamePointId,gamePointDescription')")
    sql("delete segment 0 from table makamraghutest002")
    checkAnswer(
      sql("select count(*) from makamraghutest002 where series='8Series'"),
      Seq(Row(22)))
    sql("drop table makamraghutest002")
  }

  //TC_1340
  test("TC_1340", NonRunningTests) {
    sql("drop table if exists test60")
    sql(
      "create table test60 (imei string,deviceInformationId INT,AMSize string,gamePointId " +
      "decimal,channelsId string,ActiveCountry string, " +
      "Activecity string,productionDate timestamp) " +
      "stored by 'org.apache.carbondata.format'")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table test60 OPTIONS" +
      "('DELIMITER'= ',','QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
      "gamePointId," +
      "channelsId,ActiveCountry,Activecity,productionDate')")
    sql(
      "LOAD DATA LOCAL INPATH '" + currentDirectory +
      "/src/test/resources/TestData3.csv' INTO table test60 OPTIONS" +
      "('DELIMITER'= ',','QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,AMSize," +
      "gamePointId," +
      "channelsId,ActiveCountry,Activecity,productionDate')")


    sql("delete segment 0 from table test60")
    //   sql("delete segment 1 from table test60 where productionDate before '2015-07-25
    // 12:07:28 ' ")
    checkAnswer(
      sql("select count(*)from test60"),
      Seq(Row(100)))
    sql("drop table test60")
  }



  //TestDataTypes5

  //TC_503
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, ActiveDistrict,  AMSize FROM (select * from Carbon_automation_test6)" +
    " " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize = \"1RAM size\" " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId  ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC")({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry, Activecity FROM (select imei,deviceInformationId,MAC,deviceColor," +
        "device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series," +
        "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
        "deliveryAreaId," +
        "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
        "oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
        "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
        "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
        "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
        "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
        "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street," +
        "Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber," +
        "Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
        "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
        "Latest_operatorId,gamePointId,contractNumber,gamePointDescription from " +
        "Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, ActiveDistrict,  AMSize FROM " +
        "(select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName," +
        "AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels," +
        "deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince," +
        "deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime," +
        "ActiveAreaId," +
        "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
        "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
        "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId," +
        "contractNumber,gamePointDescription from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize = \"1RAM size\" GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId  ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100020l, "1RAM size", "1", "Guangdong Province"),
        Row(100030l, "1RAM size", "2", "Guangdong Province"),
        Row(10000l, "1RAM size", "4", "Hunan Province"),
        Row(100041l, "1RAM size", "4", "Hunan Province"),
        Row(100048l, "1RAM size", "4", "Hunan Province"),
        Row(100011l, "1RAM size", "4", "Hunan Province"),
        Row(100042l, "1RAM size", "5", "Hunan Province"),
        Row(100040l, "1RAM size", "7", "Hubei Province"),
        Row(100066l, "1RAM size", "7", "Hubei Province"))

    )
  })

  //TC_504
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity " +
    "FROM " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize > \"1RAM size\" " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId," +
        "ActiveCountry, Activecity FROM (select imei,deviceInformationId,MAC,deviceColor," +
        "device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series," +
        "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName," +
        "deliveryAreaId," +
        "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
        "oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
        "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
        "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
        "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
        "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
        "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street," +
        "Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber," +
        "Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
        "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
        "Latest_operatorId,gamePointId,contractNumber,gamePointDescription from " +
        "Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName," +
        "AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels," +
        "deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince," +
        "deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime," +
        "ActiveAreaId," +
        "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
        "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
        "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId," +
        "contractNumber,gamePointDescription from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize > \"1RAM size\" GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province"),
        Row(100014, "7RAM size", "3", "Hunan Province"),
        Row(100003, "7RAM size", "5", "Hunan Province"),
        Row(100025, "7RAM size", "6", "Hubei Province"),
        Row(100039, "7RAM size", "6", "Hubei Province"),
        Row(100001, "7RAM size", "6", "Hubei Province"),
        Row(100, "7RAM size", "7", "Hubei Province"),
        Row(100033, "7RAM size", "7", "Hubei Province"),
        Row(100013, "8RAM size", "1", "Guangdong Province"),
        Row(1, "8RAM size", "2", "Guangdong Province"),
        Row(100016, "8RAM size", "4", "Hunan Province"),
        Row(100038, "8RAM size", "4", "Hunan Province"),
        Row(100081, "8RAM size", "5", "Hunan Province"),
        Row(100063, "8RAM size", "5", "Hunan Province"),
        Row(100052, "8RAM size", "5", "Hunan Province"),
        Row(100004, "8RAM size", "6", "Hubei Province"),
        Row(10002, "8RAM size", "7", "Hubei Province"),
        Row(100023, "8RAM size", "7", "Hubei Province"),
        Row(100044, "9RAM size", "1", "Guangdong Province"),
        Row(100054, "9RAM size", "1", "Guangdong Province"),
        Row(100036, "9RAM size", "3", "Hunan Province"),
        Row(100037, "9RAM size", "3", "Hunan Province"),
        Row(100080, "9RAM size", "4", "Hunan Province"),
        Row(100082, "9RAM size", "4", "Hunan Province"),
        Row(10003, "9RAM size", "4", "Hunan Province"),
        Row(100072, "9RAM size", "4", "Hunan Province"),
        Row(100070, "9RAM size", "6", "Hubei Province"),
        Row(100043, "9RAM size", "7", "Hubei Province")))
  })



  //TC_509
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId, Carbon_automation_test6.Activecity AS Activecity, " +
    "Carbon_automation_test6.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ," +
    "deviceInformationId,ActiveCountry, Activecity FROM (select * from Carbon_automation_test6)" +
    " " +
    "SUB_QRY ) Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
    "FROM" +
    " (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({

    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
      ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId, Carbon_automation_test6.Activecity AS Activecity, " +
      "Carbon_automation_test6.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ," +
      "deviceInformationId,ActiveCountry, Activecity FROM (select imei,deviceInformationId,MAC," +
      "deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series," +
      "productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId," +
      "deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet," +
      "oxSingleNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity," +
      "ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion," +
      "Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion," +
      "Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion," +
      "Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR," +
      "Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street," +
      "Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber," +
      "Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer," +
      "Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions," +
      "Latest_operatorId,gamePointId,contractNumber,gamePointDescription from " +
      "Carbon_automation_test6)" +
      " SUB_QRY ) Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
      "FROM (select imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName," +
      "AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels," +
      "deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince," +
      "deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,ActiveCheckTime,ActiveAreaId," +
      "ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId," +
      "Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber," +
      "Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
      "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
      "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
      "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
      "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
      "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
      "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId," +
      "contractNumber,gamePointDescription from Carbon_automation_test6) SUB_QRY ) " +
      "Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
      "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.gamePointId AS gamePointId, " +
        "Carbon_automation_test6_hive" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive.AMSize AS AMSize FROM ( SELECT AMSize, gamePointId ," +
        "deviceInformationId,ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6_hive)" +
        " SUB_QRY ) Carbon_automation_test6_hive INNER JOIN ( SELECT ActiveCountry, Activecity, " +
        "AMSize " +
        "FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC"))
  })

  //TC_510
  test(
    "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS" +
    " AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize LIKE '5RAM %' " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6" +
        ".AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT  Carbon_automation_test6_hive.gamePointId AS gamePointId," +
        "Carbon_automation_test6_hive" +
        ".AMSize AS AMSize, Carbon_automation_test6_hive.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_test6_hive" +
        ".AMSize, " +
        "Carbon_automation_test6_hive.ActiveCountry, Carbon_automation_test6_hive.Activecity," +
        "Carbon_automation_test6_hive.gamePointId ORDER BY Carbon_automation_test6_hive.AMSize " +
        "ASC, " +
        "Carbon_automation_test6_hive.ActiveCountry ASC, Carbon_automation_test6_hive.Activecity " +
        "ASC"))
  })

  //TC_511
  test(
    "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS" +
    " AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize BETWEEN \"2RAM " +
    "size\" AND \"6RAM size\" GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
    ".ActiveCountry, Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId " +
    "ORDER" +
    " BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6" +
        ".AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize BETWEEN \"2RAM size\" AND \"6RAM size\" GROUP BY " +
        "Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
        "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province")))
  })

  //TC_512
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize IN (\"4RAM " +
    "size\"," +
    "\"8RAM size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
    ".ActiveCountry, Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId " +
    "ORDER BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY " +
        "Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
        "Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100013, "8RAM size", "1", "Guangdong Province"),
        Row(1, "8RAM size", "2", "Guangdong Province"),
        Row(100016, "8RAM size", "4", "Hunan Province"),
        Row(100038, "8RAM size", "4", "Hunan Province"),
        Row(100081, "8RAM size", "5", "Hunan Province"),
        Row(100063, "8RAM size", "5", "Hunan Province"),
        Row(100052, "8RAM size", "5", "Hunan Province"),
        Row(100004, "8RAM size", "6", "Hubei Province"),
        Row(10002, "8RAM size", "7", "Hubei Province"),
        Row(100023, "8RAM size", "7", "Hubei Province")))
  })

  //TC_514
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test6.AMSize = \"8RAM " +
    "size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId  ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test6.AMSize = \"8RAM size\") GROUP BY Carbon_automation_test6" +
        ".AMSize, Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId  ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100028, "0RAM size", "1", "Guangdong Province"),
        Row(100010, "0RAM size", "2", "Guangdong Province"),
        Row(100005, "0RAM size", "3", "Hunan Province"),
        Row(100019, "0RAM size", "3", "Hunan Province"),
        Row(100079, "0RAM size", "3", "Hunan Province"),
        Row(100021, "0RAM size", "3", "Hunan Province"),
        Row(100002, "0RAM size", "5", "Hunan Province"),
        Row(100056, "0RAM size", "6", "Hubei Province"),
        Row(100061, "0RAM size", "6", "Hubei Province"),
        Row(100024, "0RAM size", "6", "Hubei Province"),
        Row(100008, "0RAM size", "6", "Hubei Province"),
        Row(100020, "1RAM size", "1", "Guangdong Province"),
        Row(100030, "1RAM size", "2", "Guangdong Province"),
        Row(10000, "1RAM size", "4", "Hunan Province"),
        Row(100041, "1RAM size", "4", "Hunan Province"),
        Row(100048, "1RAM size", "4", "Hunan Province"),
        Row(100011, "1RAM size", "4", "Hunan Province"),
        Row(100042, "1RAM size", "5", "Hunan Province"),
        Row(100040, "1RAM size", "7", "Hubei Province"),
        Row(100066, "1RAM size", "7", "Hubei Province"),
        Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province"),
        Row(100014, "7RAM size", "3", "Hunan Province"),
        Row(100003, "7RAM size", "5", "Hunan Province"),
        Row(100025, "7RAM size", "6", "Hubei Province"),
        Row(100039, "7RAM size", "6", "Hubei Province"),
        Row(100001, "7RAM size", "6", "Hubei Province"),
        Row(100, "7RAM size", "7", "Hubei Province"),
        Row(100033, "7RAM size", "7", "Hubei Province"),
        Row(100044, "9RAM size", "1", "Guangdong Province"),
        Row(100054, "9RAM size", "1", "Guangdong Province"),
        Row(100036, "9RAM size", "3", "Hunan Province"),
        Row(100037, "9RAM size", "3", "Hunan Province"),
        Row(100080, "9RAM size", "4", "Hunan Province"),
        Row(100082, "9RAM size", "4", "Hunan Province"),
        Row(10003, "9RAM size", "4", "Hunan Province"),
        Row(100072, "9RAM size", "4", "Hunan Province"),
        Row(100070, "9RAM size", "6", "Hubei Province"),
        Row(100043, "9RAM size", "7", "Hubei Province")))
  })

  //TC_515
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test6.AMSize > \"6RAM " +
    "size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test6.AMSize > \"6RAM size\") GROUP BY Carbon_automation_test6" +
        ".AMSize, Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ," +
        "Carbon_automation_test6.gamePointId ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100028, "0RAM size", "1", "Guangdong Province"),
        Row(100010, "0RAM size", "2", "Guangdong Province"),
        Row(100005, "0RAM size", "3", "Hunan Province"),
        Row(100019, "0RAM size", "3", "Hunan Province"),
        Row(100079, "0RAM size", "3", "Hunan Province"),
        Row(100021, "0RAM size", "3", "Hunan Province"),
        Row(100002, "0RAM size", "5", "Hunan Province"),
        Row(100056, "0RAM size", "6", "Hubei Province"),
        Row(100061, "0RAM size", "6", "Hubei Province"),
        Row(100024, "0RAM size", "6", "Hubei Province"),
        Row(100008, "0RAM size", "6", "Hubei Province"),
        Row(100020, "1RAM size", "1", "Guangdong Province"),
        Row(100030, "1RAM size", "2", "Guangdong Province"),
        Row(10000, "1RAM size", "4", "Hunan Province"),
        Row(100041, "1RAM size", "4", "Hunan Province"),
        Row(100048, "1RAM size", "4", "Hunan Province"),
        Row(100011, "1RAM size", "4", "Hunan Province"),
        Row(100042, "1RAM size", "5", "Hunan Province"),
        Row(100040, "1RAM size", "7", "Hubei Province"),
        Row(100066, "1RAM size", "7", "Hubei Province"),
        Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province")))
  })

  //TC_516
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity , SUM(Carbon_automation_test6.gamePointId) AS Sum_gamePointId " +
    "FROM" +
    " ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( SELECT " +
    "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test6.AMSize >= \"5RAM " +
    "size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity , SUM(Carbon_automation_test6" +
        ".gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity," +
        "gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test6.AMSize >= \"5RAM size\") GROUP BY Carbon_automation_test6" +
        ".AMSize, Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100028, "0RAM size", "1", "Guangdong Province", 1100308),
        Row(100010, "0RAM size", "2", "Guangdong Province", 1100110),
        Row(100005, "0RAM size", "3", "Hunan Province", 1100055),
        Row(100019, "0RAM size", "3", "Hunan Province", 1100209),
        Row(100079, "0RAM size", "3", "Hunan Province", 1100869),
        Row(100021, "0RAM size", "3", "Hunan Province", 1100231),
        Row(100002, "0RAM size", "5", "Hunan Province", 1100022),
        Row(100056, "0RAM size", "6", "Hubei Province", 1100616),
        Row(100061, "0RAM size", "6", "Hubei Province", 1100671),
        Row(100024, "0RAM size", "6", "Hubei Province", 1100264),
        Row(100008, "0RAM size", "6", "Hubei Province", 1100088),
        Row(100020, "1RAM size", "1", "Guangdong Province", 900180),
        Row(100030, "1RAM size", "2", "Guangdong Province", 900270),
        Row(10000, "1RAM size", "4", "Hunan Province", 90000),
        Row(100041, "1RAM size", "4", "Hunan Province", 900369),
        Row(100048, "1RAM size", "4", "Hunan Province", 900432),
        Row(100011, "1RAM size", "4", "Hunan Province", 900099),
        Row(100042, "1RAM size", "5", "Hunan Province", 900378),
        Row(100040, "1RAM size", "7", "Hubei Province", 900360),
        Row(100066, "1RAM size", "7", "Hubei Province", 900594),
        Row(100071, "2RAM size", "3", "Hunan Province", 200142),
        Row(10007, "2RAM size", "4", "Hunan Province", 20014),
        Row(100032, "3RAM size", "1", "Guangdong Province", 1400448),
        Row(100073, "3RAM size", "1", "Guangdong Province", 1401022),
        Row(100074, "3RAM size", "1", "Guangdong Province", 1401036),
        Row(100083, "3RAM size", "1", "Guangdong Province", 1401162),
        Row(100022, "3RAM size", "2", "Guangdong Province", 1400308),
        Row(100015, "3RAM size", "3", "Hunan Province", 1400210),
        Row(100031, "3RAM size", "4", "Hunan Province", 1400434),
        Row(100051, "3RAM size", "4", "Hunan Province", 1400714),
        Row(100059, "3RAM size", "4", "Hunan Province", 1400826),
        Row(10005, "3RAM size", "5", "Hunan Province", 140070),
        Row(100027, "3RAM size", "5", "Hunan Province", 1400378),
        Row(100053, "3RAM size", "5", "Hunan Province", 1400742),
        Row(100058, "3RAM size", "6", "Hubei Province", 1400812),
        Row(100069, "3RAM size", "7", "Hubei Province", 1400966),
        Row(100060, "4RAM size", "1", "Guangdong Province", 2201320),
        Row(10004, "4RAM size", "1", "Guangdong Province", 220088),
        Row(100055, "4RAM size", "2", "Guangdong Province", 2201210),
        Row(100064, "4RAM size", "3", "Hunan Province", 2201408),
        Row(100065, "4RAM size", "3", "Hunan Province", 2201430),
        Row(100006, "4RAM size", "3", "Hunan Province", 2200132),
        Row(10008, "4RAM size", "3", "Hunan Province", 220176),
        Row(100029, "4RAM size", "3", "Hunan Province", 2200638),
        Row(100057, "4RAM size", "3", "Hunan Province", 2201254),
        Row(1000000, "4RAM size", "4", "Hunan Province", 22000000),
        Row(100084, "4RAM size", "4", "Hunan Province", 2201848),
        Row(100007, "4RAM size", "4", "Hunan Province", 2200154),
        Row(100068, "4RAM size", "4", "Hunan Province", 2201496),
        Row(100009, "4RAM size", "4", "Hunan Province", 2200198),
        Row(100049, "4RAM size", "4", "Hunan Province", 2201078),
        Row(100045, "4RAM size", "6", "Hubei Province", 2200990),
        Row(100046, "4RAM size", "6", "Hubei Province", 2201012),
        Row(10, "4RAM size", "6", "Hubei Province", 220),
        Row(100050, "4RAM size", "7", "Hubei Province", 2201100),
        Row(100076, "4RAM size", "7", "Hubei Province", 2201672),
        Row(100018, "4RAM size", "7", "Hubei Province", 2200396),
        Row(100000, "4RAM size", "7", "Hubei Province", 2200000)))
  })

  //TC_517
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity , SUM(Carbon_automation_test6.gamePointId) AS Sum_gamePointId " +
    "FROM" +
    " ( SELECT AMSize, ActiveCountry, Activecity,gamePointId FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( SELECT " +
    "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE NOT(Carbon_automation_test6.AMSize < \"4RAM " +
    "size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId  ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity , SUM(Carbon_automation_test6" +
        ".gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, ActiveCountry, Activecity," +
        "gamePointId FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE NOT" +
        "(Carbon_automation_test6.AMSize < \"4RAM size\") GROUP BY Carbon_automation_test6" +
        ".AMSize, Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId  ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100060, "4RAM size", "1", "Guangdong Province", 2201320),
        Row(10004, "4RAM size", "1", "Guangdong Province", 220088),
        Row(100055, "4RAM size", "2", "Guangdong Province", 2201210),
        Row(100064, "4RAM size", "3", "Hunan Province", 2201408),
        Row(100065, "4RAM size", "3", "Hunan Province", 2201430),
        Row(100006, "4RAM size", "3", "Hunan Province", 2200132),
        Row(10008, "4RAM size", "3", "Hunan Province", 220176),
        Row(100029, "4RAM size", "3", "Hunan Province", 2200638),
        Row(100057, "4RAM size", "3", "Hunan Province", 2201254),
        Row(1000000, "4RAM size", "4", "Hunan Province", 22000000),
        Row(100084, "4RAM size", "4", "Hunan Province", 2201848),
        Row(100007, "4RAM size", "4", "Hunan Province", 2200154),
        Row(100068, "4RAM size", "4", "Hunan Province", 2201496),
        Row(100009, "4RAM size", "4", "Hunan Province", 2200198),
        Row(100049, "4RAM size", "4", "Hunan Province", 2201078),
        Row(100045, "4RAM size", "6", "Hubei Province", 2200990),
        Row(100046, "4RAM size", "6", "Hubei Province", 2201012),
        Row(10, "4RAM size", "6", "Hubei Province", 220),
        Row(100050, "4RAM size", "7", "Hubei Province", 2201100),
        Row(100076, "4RAM size", "7", "Hubei Province", 2201672),
        Row(100018, "4RAM size", "7", "Hubei Province", 2200396),
        Row(100000, "4RAM size", "7", "Hubei Province", 2200000),
        Row(100075, "5RAM size", "2", "Guangdong Province", 500375),
        Row(100017, "5RAM size", "2", "Guangdong Province", 500085),
        Row(100077, "5RAM size", "3", "Hunan Province", 500385),
        Row(1000, "5RAM size", "3", "Hunan Province", 5000),
        Row(10006, "5RAM size", "6", "Hubei Province", 50030),
        Row(100035, "6RAM size", "1", "Guangdong Province", 900315),
        Row(100026, "6RAM size", "2", "Guangdong Province", 900234),
        Row(100034, "6RAM size", "3", "Hunan Province", 900306),
        Row(100067, "6RAM size", "3", "Hunan Province", 900603),
        Row(100012, "6RAM size", "4", "Hunan Province", 900108),
        Row(10001, "6RAM size", "4", "Hunan Province", 90009),
        Row(100062, "6RAM size", "5", "Hunan Province", 900558),
        Row(100047, "6RAM size", "6", "Hubei Province", 900423),
        Row(100078, "6RAM size", "6", "Hubei Province", 900702),
        Row(100014, "7RAM size", "3", "Hunan Province", 700098),
        Row(100003, "7RAM size", "5", "Hunan Province", 700021),
        Row(100025, "7RAM size", "6", "Hubei Province", 700175),
        Row(100039, "7RAM size", "6", "Hubei Province", 700273),
        Row(100001, "7RAM size", "6", "Hubei Province", 700007),
        Row(100, "7RAM size", "7", "Hubei Province", 700),
        Row(100033, "7RAM size", "7", "Hubei Province", 700231),
        Row(100013, "8RAM size", "1", "Guangdong Province", 1000130),
        Row(1, "8RAM size", "2", "Guangdong Province", 10),
        Row(100016, "8RAM size", "4", "Hunan Province", 1000160),
        Row(100038, "8RAM size", "4", "Hunan Province", 1000380),
        Row(100081, "8RAM size", "5", "Hunan Province", 1000810),
        Row(100063, "8RAM size", "5", "Hunan Province", 1000630),
        Row(100052, "8RAM size", "5", "Hunan Province", 1000520),
        Row(100004, "8RAM size", "6", "Hubei Province", 1000040),
        Row(10002, "8RAM size", "7", "Hubei Province", 100020),
        Row(100023, "8RAM size", "7", "Hubei Province", 1000230),
        Row(100044, "9RAM size", "1", "Guangdong Province", 1000440),
        Row(100054, "9RAM size", "1", "Guangdong Province", 1000540),
        Row(100036, "9RAM size", "3", "Hunan Province", 1000360),
        Row(100037, "9RAM size", "3", "Hunan Province", 1000370),
        Row(100080, "9RAM size", "4", "Hunan Province", 1000800),
        Row(100082, "9RAM size", "4", "Hunan Province", 1000820),
        Row(10003, "9RAM size", "4", "Hunan Province", 100030),
        Row(100072, "9RAM size", "4", "Hunan Province", 1000720),
        Row(100070, "9RAM size", "6", "Hubei Province", 1000700),
        Row(100043, "9RAM size", "7", "Hubei Province", 1000430)))
  })

  //TC_520
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.AMSize AS AMSize, " +
    "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
    "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, AMSize " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.AMSize AS AMSize, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, gamePointId,Activecity, " +
        "AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.gamePointId AS gamePointId, " +
        "Carbon_automation_test6_hive.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive.Activecity " +
        "AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM (select *" +
        " from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_test6_hive INNER JOIN ( " +
        "SELECT ActiveCountry, gamePointId,Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " Carbon_automation_test6_hive.Activecity ASC"))
  })

  //TC_525
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "INNER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "INNER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize"),
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS " +
        "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
        ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
        "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId," +
        " " +
        "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 " +
        "INNER JOIN ( SELECT ActiveCountry, Activecity,gamePointId, AMSize FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1_hive ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1_hive.AMSize"))
  })

  //TC_526
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6" +
    ".deviceInformationId ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.deviceInformationId ASC"),
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS " +
        "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
        ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
        "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId," +
        " " +
        "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 " +
        "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1_hive ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1_hive.AMSize ORDER BY " +
        "Carbon_automation_test6.deviceInformationId ASC"))
  })

  //TC_527
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6" +
    ".deviceInformationId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.deviceInformationId DESC"),
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS " +
        "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
        ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
        "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry," +
        "deviceInformationId, " +
        "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 " +
        "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1_hive ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1_hive.AMSize ORDER BY " +
        "Carbon_automation_test6.deviceInformationId DESC"))
  })

  //TC_532
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
    "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select" +
    " *" +
    " from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC 1",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
      ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
      "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
      "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select" +
      " * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
      "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
        "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
        ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select" +
        " * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1_hive ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1_hive.AMSize ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"))
  })


  //TC_535
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN " +
    "( " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.gamePointId ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.gamePointId ASC"),
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS " +
        "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
        "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
        "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 " +
        "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1_hive ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1_hive.AMSize ORDER BY " +
        "Carbon_automation_test6.gamePointId ASC"))
  })

  //TC_552
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
    "(Carbon_automation_test6" +
    ".deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation_test6" +
    ".gamePointId)" +
    " AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, " +
    "ActiveCountry, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6.AMSize, " +
    "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6.deviceInformationId) AS Sum_deviceInformationId, First" +
        "(Carbon_automation_test6.gamePointId) AS First_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 INNER JOIN ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6_hive.deviceInformationId) AS Sum_deviceInformationId, First" +
        "(Carbon_automation_test6_hive.gamePointId) AS First_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_test6_hive INNER JOIN ( SELECT" +
        " " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6_hive.AMSize, " +
        "Carbon_automation_test6_hive.ActiveCountry, Carbon_automation_test6_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC"))
  })

  //TC_553
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN ( " +
    "SELECT" +
    " ActiveCountry, ActiveDistrict,  AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY" +
    " ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize = \"1RAM size\" " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId  ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, ActiveDistrict,  AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize = \"1RAM size\" GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId  ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100020, "1RAM size", "1", "Guangdong Province"),
        Row(100030, "1RAM size", "2", "Guangdong Province"),
        Row(10000, "1RAM size", "4", "Hunan Province"),
        Row(100041, "1RAM size", "4", "Hunan Province"),
        Row(100048, "1RAM size", "4", "Hunan Province"),
        Row(100011, "1RAM size", "4", "Hunan Province"),
        Row(100042, "1RAM size", "5", "Hunan Province"),
        Row(100040, "1RAM size", "7", "Hubei Province"),
        Row(100066, "1RAM size", "7", "Hubei Province")))
  })

  //TC_555
  test(
    "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS" +
    " AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId,ActiveCountry, Activecity " +
    "FROM " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN ( " +
    "SELECT" +
    " ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize >= \"2RAM size\"" +
    " " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry," +
    "Carbon_automation_test6.gamePointId, Carbon_automation_test6.Activecity ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6" +
        ".AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,  gamePointId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize >= \"2RAM size\" GROUP BY Carbon_automation_test6.AMSize," +
        " Carbon_automation_test6.ActiveCountry,Carbon_automation_test6.gamePointId, " +
        "Carbon_automation_test6.Activecity ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province"),
        Row(100014, "7RAM size", "3", "Hunan Province"),
        Row(100003, "7RAM size", "5", "Hunan Province"),
        Row(100001, "7RAM size", "6", "Hubei Province"),
        Row(100039, "7RAM size", "6", "Hubei Province"),
        Row(100025, "7RAM size", "6", "Hubei Province"),
        Row(100, "7RAM size", "7", "Hubei Province"),
        Row(100033, "7RAM size", "7", "Hubei Province"),
        Row(100013, "8RAM size", "1", "Guangdong Province"),
        Row(1, "8RAM size", "2", "Guangdong Province"),
        Row(100016, "8RAM size", "4", "Hunan Province"),
        Row(100038, "8RAM size", "4", "Hunan Province"),
        Row(100052, "8RAM size", "5", "Hunan Province"),
        Row(100063, "8RAM size", "5", "Hunan Province"),
        Row(100081, "8RAM size", "5", "Hunan Province"),
        Row(100004, "8RAM size", "6", "Hubei Province"),
        Row(100023, "8RAM size", "7", "Hubei Province"),
        Row(10002, "8RAM size", "7", "Hubei Province"),
        Row(100044, "9RAM size", "1", "Guangdong Province"),
        Row(100054, "9RAM size", "1", "Guangdong Province"),
        Row(100037, "9RAM size", "3", "Hunan Province"),
        Row(100036, "9RAM size", "3", "Hunan Province"),
        Row(100080, "9RAM size", "4", "Hunan Province"),
        Row(100072, "9RAM size", "4", "Hunan Province"),
        Row(10003, "9RAM size", "4", "Hunan Province"),
        Row(100082, "9RAM size", "4", "Hunan Province"),
        Row(100070, "9RAM size", "6", "Hubei Province"),
        Row(100043, "9RAM size", "7", "Hubei Province")))
  })

  //TC_560
  test(
    "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS" +
    " AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN ( " +
    "SELECT" +
    " ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize LIKE '5RAM %' " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6" +
        ".AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),

      sql(
        "SELECT  Carbon_automation_test6_hive.gamePointId AS gamePointId," +
        "Carbon_automation_test6_hive" +
        ".AMSize AS AMSize, Carbon_automation_test6_hive.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6_hive.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6_hive.AMSize LIKE '5RAM %' GROUP BY Carbon_automation_test6_hive" +
        ".AMSize, " +
        "Carbon_automation_test6_hive.ActiveCountry, Carbon_automation_test6_hive.Activecity," +
        "Carbon_automation_test6_hive.gamePointId ORDER BY Carbon_automation_test6_hive.AMSize " +
        "ASC, " +
        "Carbon_automation_test6_hive.ActiveCountry ASC, Carbon_automation_test6_hive.Activecity " +
        "ASC"))
  })

  //TC_561
  test(
    "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS" +
    " AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN ( " +
    "SELECT" +
    " ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize BETWEEN \"2RAM " +
    "size\" AND \"6RAM size\" GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
    ".ActiveCountry, Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId " +
    "ORDER" +
    " BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT  Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6" +
        ".AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize BETWEEN \"2RAM size\" AND \"6RAM size\" GROUP BY " +
        "Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
        "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100071, "2RAM size", "3", "Hunan Province"),
        Row(10007, "2RAM size", "4", "Hunan Province"),
        Row(100032, "3RAM size", "1", "Guangdong Province"),
        Row(100073, "3RAM size", "1", "Guangdong Province"),
        Row(100074, "3RAM size", "1", "Guangdong Province"),
        Row(100083, "3RAM size", "1", "Guangdong Province"),
        Row(100022, "3RAM size", "2", "Guangdong Province"),
        Row(100015, "3RAM size", "3", "Hunan Province"),
        Row(100031, "3RAM size", "4", "Hunan Province"),
        Row(100051, "3RAM size", "4", "Hunan Province"),
        Row(100059, "3RAM size", "4", "Hunan Province"),
        Row(10005, "3RAM size", "5", "Hunan Province"),
        Row(100027, "3RAM size", "5", "Hunan Province"),
        Row(100053, "3RAM size", "5", "Hunan Province"),
        Row(100058, "3RAM size", "6", "Hubei Province"),
        Row(100069, "3RAM size", "7", "Hubei Province"),
        Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100075, "5RAM size", "2", "Guangdong Province"),
        Row(100017, "5RAM size", "2", "Guangdong Province"),
        Row(100077, "5RAM size", "3", "Hunan Province"),
        Row(1000, "5RAM size", "3", "Hunan Province"),
        Row(10006, "5RAM size", "6", "Hubei Province"),
        Row(100035, "6RAM size", "1", "Guangdong Province"),
        Row(100026, "6RAM size", "2", "Guangdong Province"),
        Row(100034, "6RAM size", "3", "Hunan Province"),
        Row(100067, "6RAM size", "3", "Hunan Province"),
        Row(100012, "6RAM size", "4", "Hunan Province"),
        Row(10001, "6RAM size", "4", "Hunan Province"),
        Row(100062, "6RAM size", "5", "Hunan Province"),
        Row(100047, "6RAM size", "6", "Hubei Province"),
        Row(100078, "6RAM size", "6", "Hubei Province")))
  })

  //TC_562
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN ( " +
    "SELECT" +
    " ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize IN (\"4RAM " +
    "size\"," +
    "\"8RAM size\") GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6" +
    ".ActiveCountry, Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId " +
    "ORDER BY Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY " +
        "Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
        "Carbon_automation_test6.Activecity ,Carbon_automation_test6.gamePointId ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100060, "4RAM size", "1", "Guangdong Province"),
        Row(10004, "4RAM size", "1", "Guangdong Province"),
        Row(100055, "4RAM size", "2", "Guangdong Province"),
        Row(100064, "4RAM size", "3", "Hunan Province"),
        Row(100065, "4RAM size", "3", "Hunan Province"),
        Row(100006, "4RAM size", "3", "Hunan Province"),
        Row(10008, "4RAM size", "3", "Hunan Province"),
        Row(100029, "4RAM size", "3", "Hunan Province"),
        Row(100057, "4RAM size", "3", "Hunan Province"),
        Row(1000000, "4RAM size", "4", "Hunan Province"),
        Row(100084, "4RAM size", "4", "Hunan Province"),
        Row(100007, "4RAM size", "4", "Hunan Province"),
        Row(100068, "4RAM size", "4", "Hunan Province"),
        Row(100009, "4RAM size", "4", "Hunan Province"),
        Row(100049, "4RAM size", "4", "Hunan Province"),
        Row(100045, "4RAM size", "6", "Hubei Province"),
        Row(100046, "4RAM size", "6", "Hubei Province"),
        Row(10, "4RAM size", "6", "Hubei Province"),
        Row(100050, "4RAM size", "7", "Hubei Province"),
        Row(100076, "4RAM size", "7", "Hubei Province"),
        Row(100018, "4RAM size", "7", "Hubei Province"),
        Row(100000, "4RAM size", "7", "Hubei Province"),
        Row(100013, "8RAM size", "1", "Guangdong Province"),
        Row(1, "8RAM size", "2", "Guangdong Province"),
        Row(100016, "8RAM size", "4", "Hunan Province"),
        Row(100038, "8RAM size", "4", "Hunan Province"),
        Row(100081, "8RAM size", "5", "Hunan Province"),
        Row(100063, "8RAM size", "5", "Hunan Province"),
        Row(100052, "8RAM size", "5", "Hunan Province"),
        Row(100004, "8RAM size", "6", "Hubei Province"),
        Row(10002, "8RAM size", "7", "Hubei Province"),
        Row(100023, "8RAM size", "7", "Hubei Province")))
  })

  //TC_576
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6" +
    ".deviceInformationId ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.deviceInformationId ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".gamePointId AS gamePointId, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId FROM ( SELECT AMSize, gamePointId,ActiveCountry,deviceInformationId," +
        " " +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.deviceInformationId ASC"))
  })

  //TC_577
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6" +
    ".deviceInformationId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".gamePointId AS gamePointId, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry,deviceInformationId, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.deviceInformationId DESC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".gamePointId AS gamePointId, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId FROM ( SELECT  AMSize, gamePointId,ActiveCountry," +
        "deviceInformationId, " +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.deviceInformationId DESC"))
  })

  //TC_582
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
    "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
    "* " +
    "from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC1",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
      ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
      "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
      "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
      "* from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
      "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.gamePointId AS gamePointId, " +
        "Carbon_automation_test6_hive" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId, Carbon_automation_test6_hive.AMSize AS AMSize, " +
        "Carbon_automation_test6_hive" +
        ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select " +
        "* from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC"))
  })

  //TC_585
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN (" +
    " " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.gamePointId ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.gamePointId ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6_hive.gamePointId AS" +
        " " +
        "gamePointId FROM ( SELECT AMSize,deviceInformationId,gamePointId, ActiveCountry, " +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.gamePointId ASC"))
  })

  //TC_586
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId," +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN (" +
    " " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.gamePointId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId," +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.gamePointId DESC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6_hive.gamePointId AS" +
        " " +
        "gamePointId FROM ( SELECT AMSize, ActiveCountry, deviceInformationId,gamePointId," +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.gamePointId DESC"))
  })


  //TC_591
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
    "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
    "* " +
    "from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
      ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId, Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
      "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
      "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
      "* from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
      "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.gamePointId AS gamePointId, " +
        "Carbon_automation_test6_hive" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId, Carbon_automation_test6_hive.AMSize AS AMSize, " +
        "Carbon_automation_test6_hive" +
        ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, deviceInformationId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select " +
        "* from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC"))
  })


  //TC_594
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN (" +
    " " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.AMSize ASC, " +
    "Carbon_automation_test6.gamePointId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.gamePointId DESC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6_hive.gamePointId AS" +
        " " +
        "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.gamePointId DESC"))
  })

  //TC_595
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, " +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN (" +
    " " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.AMSize DESC, " +
    "Carbon_automation_test6.gamePointId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, " +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize DESC, Carbon_automation_test6.gamePointId DESC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6_hive.gamePointId AS" +
        " " +
        "gamePointId FROM ( SELECT AMSize, deviceInformationId,gamePointId,ActiveCountry, " +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize DESC, Carbon_automation_test6_hive.gamePointId DESC"))
  })

  //TC_596
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
    ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
    "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
    "Activecity " +
    "FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 LEFT JOIN (" +
    " " +
    "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize ORDER BY Carbon_automation_test6.AMSize DESC, " +
    "Carbon_automation_test6.gamePointId DESC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, Carbon_automation_test6" +
      ".deviceInformationId AS deviceInformationId, Carbon_automation_test6.gamePointId AS " +
      "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
      "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
      "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
      "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.AMSize DESC, Carbon_automation_test6.gamePointId DESC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry AS " +
        "ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive" +
        ".deviceInformationId AS deviceInformationId, Carbon_automation_test6_hive.gamePointId AS" +
        " " +
        "gamePointId FROM ( SELECT AMSize, ActiveCountry,deviceInformationId, gamePointId," +
        "Activecity FROM (select * from Carbon_automation_test6_hive) SUB_QRY ) " +
        "Carbon_automation_test6_hive " +
        "LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.AMSize DESC, Carbon_automation_test6_hive.gamePointId DESC"))
  })


  //TC_598
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
    ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
    "deviceInformationId, Carbon_automation_test6.Activecity AS Activecity, " +
    "Carbon_automation_test6.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry," +
    "deviceInformationId, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
    "* " +
    "from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
    "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(sql(
      "SELECT Carbon_automation_test6.gamePointId AS gamePointId, Carbon_automation_test6" +
      ".ActiveCountry AS ActiveCountry, Carbon_automation_test6.deviceInformationId AS " +
      "deviceInformationId, Carbon_automation_test6.Activecity AS Activecity, " +
      "Carbon_automation_test6.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, ActiveCountry," +
      "deviceInformationId, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
      "Carbon_automation_test6 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
      "* from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
      "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
      "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.gamePointId AS gamePointId, " +
        "Carbon_automation_test6_hive" +
        ".ActiveCountry AS ActiveCountry, Carbon_automation_test6_hive.deviceInformationId AS " +
        "deviceInformationId, Carbon_automation_test6_hive.Activecity AS Activecity, " +
        "Carbon_automation_test6_hive.AMSize AS AMSize FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry," +
        "deviceInformationId, Activecity FROM (select * from Carbon_automation_test6_hive) " +
        "SUB_QRY ) " +
        "Carbon_automation_test6_hive LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select " +
        "* from Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6_hive.AMSize = Carbon_automation_vmall_test1.AMSize ORDER BY " +
        "Carbon_automation_test6_hive.ActiveCountry ASC, Carbon_automation_test6_hive.Activecity " +
        "ASC"))
  })

  //TC_601
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
    "(Carbon_automation_test6" +
    ".gamePointId) AS Sum_gamePointId, First(Carbon_automation_test6.deviceInformationId) AS " +
    "First_deviceInformationId FROM ( SELECT AMSize,deviceInformationId, gamePointId, " +
    "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
    "Carbon_automation_test6 Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select " +
    "* " +
    "from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY " +
    "Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity ORDER BY Carbon_automation_test6.AMSize ASC, " +
    "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6.gamePointId) AS Sum_gamePointId, First(Carbon_automation_test6" +
        ".deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize," +
        "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6_hive.gamePointId) AS Sum_gamePointId, First" +
        "(Carbon_automation_test6_hive" +
        ".deviceInformationId) AS First_deviceInformationId FROM ( SELECT AMSize," +
        "deviceInformationId, gamePointId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_test6_hive Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6_hive.AMSize, " +
        "Carbon_automation_test6_hive.ActiveCountry, Carbon_automation_test6_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC")
    )
  })

  //TC_602
  test(
    "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry AS " +
    "ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
    "(Carbon_automation_test6" +
    ".deviceInformationId) AS Sum_deviceInformationId, First(Carbon_automation_test6" +
    ".gamePointId)" +
    " AS First_gamePointId FROM ( SELECT AMSize,gamePointId ,deviceInformationId, " +
    "ActiveCountry, " +
    "Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 " +
    "Left join ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from " +
    "Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
    "Carbon_automation_test6" +
    ".AMSize = Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6.AMSize, " +
    "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.AMSize AS AMSize, Carbon_automation_test6.ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6.deviceInformationId) AS Sum_deviceInformationId, First" +
        "(Carbon_automation_test6.gamePointId) AS First_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6) SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity ORDER BY " +
        "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
        "Carbon_automation_test6.Activecity ASC"),
      sql(
        "SELECT Carbon_automation_test6_hive.AMSize AS AMSize, Carbon_automation_test6_hive" +
        ".ActiveCountry " +
        "AS ActiveCountry, Carbon_automation_test6_hive.Activecity AS Activecity, SUM" +
        "(Carbon_automation_test6_hive.deviceInformationId) AS Sum_deviceInformationId, First" +
        "(Carbon_automation_test6_hive.gamePointId) AS First_gamePointId FROM ( SELECT AMSize," +
        "gamePointId ,deviceInformationId, ActiveCountry, Activecity FROM (select * from " +
        "Carbon_automation_test6_hive) SUB_QRY ) Carbon_automation_test6_hive Left join ( SELECT " +
        "ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test6_hive) " +
        "SUB_QRY )" +
        " Carbon_automation_vmall_test1 ON Carbon_automation_test6_hive.AMSize = " +
        "Carbon_automation_vmall_test1.AMSize GROUP BY Carbon_automation_test6_hive.AMSize, " +
        "Carbon_automation_test6_hive.ActiveCountry, Carbon_automation_test6_hive.Activecity " +
        "ORDER BY " +
        "Carbon_automation_test6_hive.AMSize ASC, Carbon_automation_test6_hive.ActiveCountry ASC," +
        " " +
        "Carbon_automation_test6_hive.Activecity ASC")
    )
  })

  //TC_603
  test(
    "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize " +
    "AS " +
    "AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, Carbon_automation_test6" +
    ".Activecity AS Activecity FROM ( SELECT AMSize, gamePointId,ActiveCountry, Activecity FROM" +
    " " +
    "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_test6 RIGHT JOIN ( " +
    "SELECT ActiveCountry, ActiveDistrict,  AMSize FROM (select * from Carbon_automation_test6)" +
    " " +
    "SUB_QRY ) Carbon_automation_vmall_test1 ON Carbon_automation_test6.AMSize = " +
    "Carbon_automation_vmall_test1.AMSize WHERE Carbon_automation_test6.AMSize = \"1RAM size\" " +
    "GROUP BY Carbon_automation_test6.AMSize, Carbon_automation_test6.ActiveCountry, " +
    "Carbon_automation_test6.Activecity,Carbon_automation_test6.gamePointId  ORDER BY " +
    "Carbon_automation_test6.AMSize ASC, Carbon_automation_test6.ActiveCountry ASC, " +
    "Carbon_automation_test6.Activecity ASC",
    NonRunningTests)({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test6.gamePointId AS gamePointId,Carbon_automation_test6.AMSize" +
        " AS AMSize, Carbon_automation_test6.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test6.Activecity AS Activecity FROM ( SELECT AMSize, gamePointId," +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test6) SUB_QRY ) " +
        "Carbon_automation_test6 RIGHT JOIN ( SELECT ActiveCountry, ActiveDistrict,  AMSize FROM " +
        "(select * from Carbon_automation_test6) SUB_QRY ) Carbon_automation_vmall_test1 ON " +
        "Carbon_automation_test6.AMSize = Carbon_automation_vmall_test1.AMSize WHERE " +
        "Carbon_automation_test6.AMSize = \"1RAM size\" GROUP BY Carbon_automation_test6.AMSize, " +
        "Carbon_automation_test6.ActiveCountry, Carbon_automation_test6.Activecity," +
        "Carbon_automation_test6.gamePointId  ORDER BY Carbon_automation_test6.AMSize ASC, " +
        "Carbon_automation_test6.ActiveCountry ASC, Carbon_automation_test6.Activecity ASC"),
      Seq(Row(100020, "1RAM size", "1", "Guangdong Province"),
        Row(100030, "1RAM size", "2", "Guangdong Province"),
        Row(10000, "1RAM size", "4", "Hunan Province"),
        Row(100041, "1RAM size", "4", "Hunan Province"),
        Row(100048, "1RAM size", "4", "Hunan Province"),
        Row(100011, "1RAM size", "4", "Hunan Province"),
        Row(100042, "1RAM size", "5", "Hunan Province"),
        Row(100040, "1RAM size", "7", "Hubei Province"),
        Row(100066, "1RAM size", "7", "Hubei Province")))
  })


  //Test-16
  test("select imei, Latest_DAY+ 10 as a  from Carbon_automation_test6", NonRunningTests) {
    checkAnswer(sql("select imei, Latest_DAY+ 10 as a  from Carbon_automation_test6"),
      sql("select imei, Latest_DAY+ 10 as a  from Carbon_automation_test6_hive"));
  }
  //Test-17
  test("select imei, gamePointId+ 10 as Total from Carbon_automation_test6", NonRunningTests) {
    checkAnswer(sql("select imei, gamePointId+ 10 as Total from Carbon_automation_test6"),
      sql("select imei, gamePointId+ 10 as Total from Carbon_automation_test6_hive"));
  }
  //Test-18
  test("select imei, modelId+ 10 Total from Carbon_automation_test6 ", NonRunningTests) {
    checkAnswer(sql("select imei, modelId+ 10 Total from Carbon_automation_test6 "),
      sql("select imei, modelId+ 10 Total from Carbon_automation_test6_hive "));
  }
  //Test-19

  test("select imei, gamePointId+contractNumber as a  from Carbon_automation_test6 ",
    NonRunningTests) {
    checkAnswer(sql("select imei, gamePointId+contractNumber as a  from " +
                    "Carbon_automation_test6 "),
      sql("select imei, gamePointId+contractNumber as a  from " +
          "Carbon_automation_test6_hive "));
  }

  //Test-20
  test("select imei, deviceInformationId+gamePointId as Total from Carbon_automation_test6",
    NonRunningTests) {
    checkAnswer(sql(
      "select imei, deviceInformationId+gamePointId as Total from Carbon_automation_test6"),
      sql(
        "select imei, deviceInformationId+gamePointId as Total from Carbon_automation_test6_hive"));
  }
  //Test-21
  test("select imei, deviceInformationId+deviceInformationId Total from " +
       "Carbon_automation_test6",
    NonRunningTests) {
    checkAnswer(sql(
      "select imei, deviceInformationId+deviceInformationId Total from Carbon_automation_test6"),
      sql(
        "select imei, deviceInformationId+deviceInformationId Total from " +
        "Carbon_automation_test6_hive"));
  }

  test(
    "select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from " +
    "Carbon_automation_test6",
    NonRunningTests) {
    checkAnswer(
      sql(
        "select percentile(gamePointId,array(0,0.2,0.3,1))  as  a from " +
        "Carbon_automation_test6"),
      sql(
        "select percentile(gamePointId,array(0,0.2,0.3,1))  as  a from " +
        "Carbon_automation_test6_hive")
    )

    //     checkAnswer(sql("select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a
    // from Carbon_automation_test6"),"TC_112.csv");
  }


}
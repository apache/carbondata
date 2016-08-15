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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.{NonRunningTests, QueryTest}

import org.apache.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for all queries on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase3 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate", "false")
    try {
      sql(
        "create table Carbon_automation_test3 (imei string,deviceInformationId int,MAC string," +
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
        ".carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='Latest_MONTH," +
        "deviceInformationId')")
      sql("LOAD DATA LOCAL INPATH '" + currentDirectory +
          "/src/test/resources/100_olap.csv' INTO table Carbon_automation_test3 OPTIONS" +
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
        "/src/test/resources/FACT_UNITED_DATA_INFO_sample_table.csv' INTO table traffic_2g_3g_4g " +
        "OPTIONS('DELIMITER'= ',' ,'QUOTECHAR'= '\"', 'FILEHEADER'= '')")

      sql(
        "create table hivetable(imei string,deviceInformationId int,MAC string,deviceColor " +
          "string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize " +
          "string,CUPAudit string,CPIClocked string,series string,productionDate timestamp," +
          "bomCode string,internalModels string, deliveryTime string, channelsId string, " +
          "channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince " +
          "string, deliveryCity string,deliveryDistrict string, deliveryStreet string, " +
          "oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry " +
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
          "Latest_phonePADPartitionedVersions string, Latest_operatorId string, " +
          "gamePointDescription string, gamePointId int,contractNumber int) row format " +
          "delimited fields terminated by ','"
      )

      sql(
        "LOAD DATA local inpath'" + currentDirectory + "/src/test/resources/100_olap.csv'" +
          " overwrite INTO table hivetable"
      )

    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }

  }

  override def afterAll {
    try {
      sql("drop table Carbon_automation_test3")
      sql("drop table myvmallTest")
      sql("drop table traffic_2g_3g_4g")
      sql("drop table hivetable")
    } catch {
      case e: Exception => print("ERROR : " + e.getMessage)
    }
  }

  //TC_222
  test(
    "select imei, Latest_DAY from Carbon_automation_test3 where Latest_DAY  LIKE Latest_areaId " +
      "AND  Latest_DAY  LIKE Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test3 where Latest_DAY  LIKE " +
          "Latest_areaId AND  Latest_DAY  LIKE Latest_HOUR"
      ),
      Seq()
    )
  }
  )

  //TC_224
  test(
    "select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from " +
      "Carbon_automation_test3) qq where babu NOT LIKE   Latest_MONTH"
  )({
    checkAnswer(
      sql(
        "select * from (select if( Latest_areaId=7,7,NULL) as babu,Latest_MONTH from " +
          "Carbon_automation_test3) qq where babu NOT LIKE   Latest_MONTH"
      ),
      Seq()
    )
  }
  )

  //TC_262
  test(
    "select count(imei) ,series from Carbon_automation_test3 group by series having sum " +
      "(Latest_DAY) == 99"
  )({
    checkAnswer(
      sql(
        "select count(imei) ,series from Carbon_automation_test3 group by series having sum " +
          "(Latest_DAY) == 99"
      ),
      Seq()
    )
  }
  )

  //TC_264
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test3) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId ORDER " +
      "BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test3) SUB_QRY WHERE AMSize = \"\" GROUP BY AMSize, ActiveAreaId " +
          "ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq()
    )
  }
  )

  //TC_320
  test(
    "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId FROM " +
      "(select * from Carbon_automation_test3) SUB_QRY WHERE gamePointId > 1.0E9 GROUP BY " +
      "ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, ActiveDistrict ASC, " +
      "Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, Activecity, SUM(gamePointId) AS Sum_gamePointId " +
          "FROM (select * from Carbon_automation_test3) SUB_QRY WHERE gamePointId > 1.0E9 GROUP " +
          "BY ActiveCountry, ActiveDistrict, Activecity ORDER BY ActiveCountry ASC, " +
          "ActiveDistrict ASC, Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_343
  test(
    "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
      "deliverycity FROM (select * from Carbon_automation_test3) SUB_QRY WHERE deliverycity IS NULL"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_test3) SUB_QRY WHERE deliverycity " +
          "IS NULL"
      ),
      Seq()
    )
  }
  )

  //TC_409
  test("select  gamePointId from Carbon_automation_test3 where  modelId is  null")({
    checkAnswer(
      sql("select  gamePointId from Carbon_automation_test3 where  modelId is  null"),
      Seq()
    )
  }
  )

  //TC_410
  test("select  contractNumber from Carbon_automation_test3 where bomCode is  null")({
    checkAnswer(
      sql("select  contractNumber from Carbon_automation_test3 where bomCode is  null"),
      Seq()
    )
  }
  )

  //TC_411
  test("select  imei from Carbon_automation_test3 where AMSIZE is  null")({
    checkAnswer(
      sql("select  imei from Carbon_automation_test3 where AMSIZE is  null"),
      Seq()
    )
  }
  )

  //TC_412
  test("select  bomCode from Carbon_automation_test3 where contractnumber is  null")({
    checkAnswer(
      sql("select  bomCode from Carbon_automation_test3 where contractnumber is  null"),
      Seq()
    )
  }
  )

  //TC_413
  test("select  latest_day from Carbon_automation_test3 where  modelId is  null")({
    checkAnswer(
      sql("select  latest_day from Carbon_automation_test3 where  modelId is  null"),
      Seq()
    )
  }
  )

  //TC_414
  test("select  latest_day from Carbon_automation_test3 where  deviceinformationid is  null")({
    checkAnswer(
      sql("select  latest_day from Carbon_automation_test3 where  deviceinformationid is  null"),
      Seq()
    )
  }
  )

  //TC_415
  test("select  deviceinformationid from Carbon_automation_test3 where  modelId is  null")({
    checkAnswer(
      sql("select  deviceinformationid from Carbon_automation_test3 where  modelId is  null"),
      Seq()
    )
  }
  )

  //TC_416
  test(
    "select  deviceinformationid from Carbon_automation_test3 where  deviceinformationid is  null"
  )({
    checkAnswer(
      sql(
        "select  deviceinformationid from Carbon_automation_test3 where  deviceinformationid is  " +
          "null"
      ),
      Seq()
    )
  }
  )

  //TC_417
  test("select  imei from Carbon_automation_test3 where  modelId is  null")({
    checkAnswer(
      sql("select  imei from Carbon_automation_test3 where  modelId is  null"),
      Seq()
    )
  }
  )

  //TC_418
  test("select  imei from Carbon_automation_test3 where  deviceinformationid is  null")({
    checkAnswer(
      sql("select  imei from Carbon_automation_test3 where  deviceinformationid is  null"),
      Seq()
    )
  }
  )

  //TC_115
  test(
    "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from " +
      "Carbon_automation_test3"
  )({
    checkAnswer(
      sql(
        "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from " +
          "Carbon_automation_test3"
      ),
      sql(
        "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99))  as a from " +
          "hivetable"
      )
    )
  }
  )

  //TC_116
  test(
    "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from " +
      "Carbon_automation_test3")({
    checkAnswer(
      sql(
        "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from " +
          "Carbon_automation_test3"
      ),
      sql(
        "select percentile_approx(deviceInformationId,array(0.2,0.3,0.99),5) as a from " +
          "hivetable"
      )
    )
  }
  )

  //TC_117
  test("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test3")({
    checkAnswer(
      sql("select histogram_numeric(deviceInformationId,2)  as a from Carbon_automation_test3"),
      sql("select histogram_numeric(deviceInformationId,2)  as a from hivetable")
    )
  }
  )

  //TC_128
  test(
    "select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from Carbon_automation_test3"
  )({
    checkAnswer(
      sql(
        "select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from " +
          "Carbon_automation_test3"
      ),
      sql(
        "select percentile(deviceInformationId,array(0,0.2,0.3,1))  as  a from " +
          "hivetable"
      )
    )
  }
  )


  //TC_131
  test(
    "select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from Carbon_automation_test3"
  )({
    checkAnswer(
      sql(
        "select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from " +
          "Carbon_automation_test3"
      ),
      sql(
        "select percentile_approx(gamePointId,array(0.2,0.3,0.99))  as a from " +
          "hivetable"
      )
    )
  }
  )

  //TC_132
  test(
    "select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from Carbon_automation_test3",
    NonRunningTests)({
    checkAnswer(
      sql(
        "select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from " +
          "Carbon_automation_test3"
      ),
      sql(
        "select percentile_approx(gamePointId,array(0.2,0.3,0.99),5) as a from " +
          "hivetable"
      )
    )
  }
  )

  //TC_133
  test("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test3",
    NonRunningTests)({
    validateResult(
      sql("select histogram_numeric(gamePointId,2)  as a from Carbon_automation_test3"),
      "TC_133.csv"
    )
  }
  )

  //TC_477
  test("select percentile(1,array(1)) from Carbon_automation_test3")({
    checkAnswer(
      sql("select percentile(1,array(1)) from Carbon_automation_test3"),
      sql("select percentile(1,array(1)) from hivetable")
    )
  }
  )

  //TC_479
  test("select percentile(1,array('0.5')) from Carbon_automation_test3")({
    checkAnswer(
      sql("select percentile(1,array('0.5')) from Carbon_automation_test3"),
      sql("select percentile(1,array('0.5')) from hivetable")
    )
  }
  )

  //TC_480
  test("select percentile(1,array('1')) from Carbon_automation_test3")({
    checkAnswer(
      sql("select percentile(1,array('1')) from Carbon_automation_test3"),
      sql("select percentile(1,array('1')) from hivetable")
    )
  }
  )

  //TC_486
  test("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test32")({
    checkAnswer(
      sql("select percentile_approx(1,array(0.5),5000) from Carbon_automation_test3"),
      sql("select percentile_approx(1,array(0.5),5000) from hivetable")
    )
  }
  )

  //TC_487
  test("select histogram_numeric(1, 5000)from Carbon_automation_test3")({
    checkAnswer(
      sql("select histogram_numeric(1, 5000)from Carbon_automation_test3"),
      sql("select histogram_numeric(1, 5000)from hivetable")
    )
  }
  )

  //TC_488
  test("select histogram_numeric(1, 1000)from Carbon_automation_test3")({
    checkAnswer(
      sql("select histogram_numeric(1, 1000)from Carbon_automation_test3"),
      sql("select histogram_numeric(1, 1000)from hivetable")
    )
  }
  )

  //TC_489
  test("select histogram_numeric(1, 500)from Carbon_automation_test31")({
    checkAnswer(
      sql("select histogram_numeric(1, 500)from Carbon_automation_test3"),
      sql("select histogram_numeric(1, 500)from hivetable")
    )
  }
  )

  //TC_490
  test("select histogram_numeric(1, 500)from Carbon_automation_test3")({
    checkAnswer(
      sql("select histogram_numeric(1, 500)from Carbon_automation_test3"),
      sql("select histogram_numeric(1, 500)from hivetable")
    )
  }
  )

  //TC_491
  test("select collect_set(gamePointId) from Carbon_automation_test3",
    NonRunningTests)({
    validateResult(
      sql("select collect_set(gamePointId) from Carbon_automation_test3"),
      "TC_491.csv"
    )
  }
  )

  //TC_492
  test("select collect_set(AMSIZE) from Carbon_automation_test3")({
    checkAnswer(
      sql("select collect_set(AMSIZE) from Carbon_automation_test3"),
      sql("select collect_set(AMSIZE) from hivetable")
    )
  }
  )

  //TC_493
  test("select collect_set(bomcode) from Carbon_automation_test3")({
    checkAnswer(
      sql("select collect_set(bomcode) from Carbon_automation_test3"),
      sql("select collect_set(bomcode) from hivetable")
    )
  }
  )

  //TC_494
  test("select collect_set(imei) from Carbon_automation_test3")({
    checkAnswer(
      sql("select collect_set(imei) from Carbon_automation_test3"),
      sql("select collect_set(imei) from hivetable")
    )
  }
  )

  //TC_500
  test("select percentile(1,array('0.5')) from Carbon_automation_test31")({
    checkAnswer(
      sql("select percentile(1,array('0.5')) from Carbon_automation_test3"),
      sql("select percentile(1,array('0.5')) from hivetable")
    )
  }
  )

  //TC_513
  test(
    "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize AS " +
      "AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, Carbon_automation_test3" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test3 INNER JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test31 ON Carbon_automation_test3.AMSize = " +
      "Carbon_automation_test31.AMSize WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY " +
      "Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
      "Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId ORDER BY " +
      "Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize" +
          " AS AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, " +
          "Carbon_automation_test3.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test3 INNER JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test31 ON " +
          "Carbon_automation_test3.AMSize = Carbon_automation_test31.AMSize WHERE " +
          "Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3.AMSize, " +
          "Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ," +
          "Carbon_automation_test3.gamePointId ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_563
  test(
    "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize AS " +
      "AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, Carbon_automation_test3" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test3 LEFT JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test31 ON Carbon_automation_test3.AMSize = " +
      "Carbon_automation_test31.AMSize WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY " +
      "Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
      "Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId ORDER BY " +
      "Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize" +
          " AS AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, " +
          "Carbon_automation_test3.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test3 LEFT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test31 ON " +
          "Carbon_automation_test3.AMSize = Carbon_automation_test31.AMSize WHERE " +
          "Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3.AMSize, " +
          "Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ," +
          "Carbon_automation_test3.gamePointId ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_612
  test(
    "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize AS " +
      "AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, Carbon_automation_test3" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test3 RIGHT JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test31 ON Carbon_automation_test3.AMSize = " +
      "Carbon_automation_test31.AMSize WHERE Carbon_automation_test3.AMSize IN (\"4RAM size\"," +
      "\"8RAM size\") GROUP BY Carbon_automation_test3.AMSize, Carbon_automation_test3" +
      ".ActiveCountry, Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId " +
      "ORDER BY Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC", NonRunningTests
  )({
    validateResult(sql(
      "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize " +
        "AS AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, " +
        "Carbon_automation_test3.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
        "ActiveCountry, Activecity FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
        "Carbon_automation_test3 RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
        "(select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test31 ON " +
        "Carbon_automation_test3.AMSize = Carbon_automation_test31.AMSize WHERE " +
        "Carbon_automation_test3.AMSize IN (\"4RAM size\",\"8RAM size\") GROUP BY " +
        "Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
        "Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId ORDER BY " +
        "Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
        "Carbon_automation_test3.Activecity ASC"
    ), "TC_612.csv"
    )
  }
  )

  //TC_613
  test(
    "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize AS " +
      "AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, Carbon_automation_test3" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test3 RIGHT JOIN ( " +
      "SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test31 ON Carbon_automation_test3.AMSize = " +
      "Carbon_automation_test31.AMSize WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY " +
      "Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
      "Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId ORDER BY " +
      "Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize" +
          " AS AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, " +
          "Carbon_automation_test3.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test3 RIGHT JOIN ( SELECT ActiveCountry, Activecity, AMSize FROM " +
          "(select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test31 ON " +
          "Carbon_automation_test3.AMSize = Carbon_automation_test31.AMSize WHERE " +
          "Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3.AMSize, " +
          "Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ," +
          "Carbon_automation_test3.gamePointId ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_663
  test(
    "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize AS " +
      "AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, Carbon_automation_test3" +
      ".Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, ActiveCountry, Activecity FROM" +
      " (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test3 FULL OUTER JOIN" +
      " ( SELECT ActiveCountry, Activecity, AMSize FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test31 ON Carbon_automation_test3.AMSize = " +
      "Carbon_automation_test31.AMSize WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY " +
      "Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
      "Carbon_automation_test3.Activecity ,Carbon_automation_test3.gamePointId ORDER BY " +
      "Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.gamePointId AS gamePointId,Carbon_automation_test3.AMSize" +
          " AS AMSize, Carbon_automation_test3.ActiveCountry AS ActiveCountry, " +
          "Carbon_automation_test3.Activecity AS Activecity FROM ( SELECT AMSize,gamePointId, " +
          "ActiveCountry, Activecity FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test3 FULL OUTER JOIN ( SELECT ActiveCountry, Activecity, AMSize " +
          "FROM (select * from Carbon_automation_test3) SUB_QRY ) Carbon_automation_test31 ON " +
          "Carbon_automation_test3.AMSize = Carbon_automation_test31.AMSize WHERE " +
          "Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3.AMSize, " +
          "Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ," +
          "Carbon_automation_test3.gamePointId ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_712
  test(
    "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test3 INNER JOIN ( SELECT AMSize, ActiveCountry, Activecity, " +
      "gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
      "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = Carbon_automation_test31" +
      ".gamePointId WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3" +
      ".AMSize, Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ORDER " +
      "BY Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
          "SUB_QRY ) Carbon_automation_test3 INNER JOIN ( SELECT AMSize, ActiveCountry, " +
          "Activecity, gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = " +
          "Carbon_automation_test31.gamePointId WHERE Carbon_automation_test3.AMSize IS NULL " +
          "GROUP BY Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
          "Carbon_automation_test3.Activecity ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_760
  test(
    "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test3 LEFT JOIN ( SELECT AMSize, ActiveCountry, Activecity, " +
      "gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
      "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = Carbon_automation_test31" +
      ".gamePointId WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3" +
      ".AMSize, Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ORDER " +
      "BY Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
          "SUB_QRY ) Carbon_automation_test3 LEFT JOIN ( SELECT AMSize, ActiveCountry, " +
          "Activecity, gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = " +
          "Carbon_automation_test31.gamePointId WHERE Carbon_automation_test3.AMSize IS NULL " +
          "GROUP BY Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
          "Carbon_automation_test3.Activecity ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //TC_856
  test(
    "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry AS " +
      "ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
      "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
      "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
      "SUB_QRY ) Carbon_automation_test3 FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, " +
      "Activecity, gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
      "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = Carbon_automation_test31" +
      ".gamePointId WHERE Carbon_automation_test3.AMSize IS NULL GROUP BY Carbon_automation_test3" +
      ".AMSize, Carbon_automation_test3.ActiveCountry, Carbon_automation_test3.Activecity ORDER " +
      "BY Carbon_automation_test3.AMSize ASC, Carbon_automation_test3.ActiveCountry ASC, " +
      "Carbon_automation_test3.Activecity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Carbon_automation_test3.AMSize AS AMSize, Carbon_automation_test3.ActiveCountry " +
          "AS ActiveCountry, Carbon_automation_test3.Activecity AS Activecity, SUM" +
          "(Carbon_automation_test3.gamePointId) AS Sum_gamePointId FROM ( SELECT AMSize, " +
          "ActiveCountry, Activecity, gamePointId FROM (select * from Carbon_automation_test3) " +
          "SUB_QRY ) Carbon_automation_test3 FULL OUTER JOIN ( SELECT AMSize, ActiveCountry, " +
          "Activecity, gamePointId FROM (select * from Carbon_automation_test3) SUB_QRY ) " +
          "Carbon_automation_test31 ON Carbon_automation_test3.gamePointId = " +
          "Carbon_automation_test31.gamePointId WHERE Carbon_automation_test3.AMSize IS NULL " +
          "GROUP BY Carbon_automation_test3.AMSize, Carbon_automation_test3.ActiveCountry, " +
          "Carbon_automation_test3.Activecity ORDER BY Carbon_automation_test3.AMSize ASC, " +
          "Carbon_automation_test3.ActiveCountry ASC, Carbon_automation_test3.Activecity ASC"
      ),
      Seq()
    )
  }
  )

  //VMALL_Per_TC_003
  test(
    "SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from " +
      "myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  " +
      "product_name ASC"
  )({
    checkAnswer(
      sql(
        "SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from " +
          "myvmallTest) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  " +
          "product_name ASC"
      ),
      Seq(Row("Huawei4009", 1))
    )
  }
  )

  //VMALL_Per_TC_010
  test(
    "select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest " +
      "where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2"
  )({
    checkAnswer(
      sql(
        "select (t1.hnor4xi/t2.totalc)*100 from (select count (imei)  as hnor4xi from myvmallTest" +
          " where device_name=\"Honor2\")t1,(select count (imei) as totalc from myvmallTest)t2"
      ),
      Seq(Row(0.0))
    )
  }
  )

  //VMALL_Per_TC_011
  test(
    "select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) " +
      "mydates,imei from myvmallTest) sub where mydates<1000"
  )({
    checkAnswer(
      sql(
        "select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) " +
          "mydates,imei from myvmallTest) sub where mydates<1000"
      ),
      Seq(Row(1000))
    )
  }
  )

  //VMALL_Per_TC_013
  test(
    "select count(imei)  DistinctCount_imei from myvmallTest where " +
      "(Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3.863972\") " +
      "OR (Active_emui_version=\"EmotionUI_2.843\" and Latest_emui_version=\"EmotionUI_3.863843\")"
  )({
    checkAnswer(
      sql(
        "select count(imei)  DistinctCount_imei from myvmallTest where " +
          "(Active_emui_version=\"EmotionUI_2.972\" and Latest_emui_version=\"EmotionUI_3" +
          ".863972\") OR (Active_emui_version=\"EmotionUI_2.843\" and " +
          "Latest_emui_version=\"EmotionUI_3.863843\")"
      ),
      Seq(Row(2))
    )
  }
  )

  //VMALL_Per_TC_014
  test(
    "select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4.3' " +
      "and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and " +
      "Active_emui_version='EmotionUI_2.2')"
  )({
    checkAnswer(
      sql(
        "select count(imei) as imeicount from myvmallTest where (Active_os_version='Android 4.4" +
          ".3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and " +
          "Active_emui_version='EmotionUI_2.2')"
      ),
      Seq(Row(2))
    )
  }
  )

  //VMALL_Per_TC_B017
  test(
    "SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest) " +
      "SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"
  )({
    checkAnswer(
      sql(
        "SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmallTest)" +
          " SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC"
      ),
      Seq(Row("SmartPhone_3998", 1))
    )
  }
  )

  //VMALL_Per_TC_021
  test(
    "SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY " +
      "where device_name='Honor63011'  and product_name='Huawei3011'"
  )({
    checkAnswer(
      sql(
        "SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) " +
          "SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'"
      ),
      Seq(Row("imeiA009863011", "Honor63011"))
    )
  }
  )

  //VMALL_Per_TC_022
  test(
    "SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) SUB_QRY " +
      "where imei='imeiA009863011' or imei='imeiA009863012'"
  )({
    checkAnswer(
      sql(
        "SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmallTest   ) " +
          "SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'"
      ),
      Seq(Row("imeiA009863011", "Honor63011"), Row("imeiA009863012", "Honor63012"))
    )
  }
  )

  //VMALL_Per_TC_024
  test(
    "select product_name, count(distinct imei)  as imei_number from     myvmallTest    where " +
      "imei='imeiA009863017' group by product_name"
  )({
    checkAnswer(
      sql(
        "select product_name, count(distinct imei)  as imei_number from     myvmallTest    where " +
          "imei='imeiA009863017' group by product_name"
      ),
      Seq(Row("Huawei3017", 1))
    )
  }
  )

  //VMALL_Per_TC_025
  test(
    "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where " +
      "deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where" +
          " deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number " +
          "desc"
      ),
      Seq(Row("Huawei3017", 1))
    )
  }
  )

  //VMALL_Per_TC_026
  test(
    "select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where " +
      "deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select deliveryCity, count(distinct imei)  as imei_number from     myvmallTest     where" +
          " deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc"
      ),
      Seq(Row("deliveryCity17", 2))
    )
  }
  )

  //VMALL_Per_TC_027
  test(
    "select device_color, count(distinct imei)  as imei_number from     myvmallTest     where " +
      "bom='51090576_63017' group by device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select device_color, count(distinct imei)  as imei_number from     myvmallTest     where" +
          " bom='51090576_63017' group by device_color order by imei_number desc"
      ),
      Seq(Row("black3017", 1))
    )
  }
  )

  //VMALL_Per_TC_028
  test(
    "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where " +
      "product_name='Huawei3017' group by product_name order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where" +
          " product_name='Huawei3017' group by product_name order by imei_number desc"
      ),
      Seq(Row("Huawei3017", 1))
    )
  }
  )

  //VMALL_Per_TC_029
  test(
    "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where " +
      "deliveryprovince='Province_17' group by product_name order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name, count(distinct imei)  as imei_number from     myvmallTest     where" +
          " deliveryprovince='Province_17' group by product_name order by imei_number desc"
      ),
      Seq(Row("Huawei3017", 1), Row("Huawei3517", 1))
    )
  }
  )

  //VMALL_Per_TC_030
  test(
    "select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     where  " +
      "deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmallTest     " +
          "where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc"
      ),
      Seq(Row("517_GB", "cpu_clock517", 1), Row("17_GB", "cpu_clock17", 1))
    )
  }
  )

  //VMALL_Per_TC_031
  test(
    "select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  " +
      "imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac," +
      "device_color"
  )({
    checkAnswer(
      sql(
        "select uuid,mac,device_color,count(distinct imei) from    myvmallTest    where  " +
          "imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac," +
          "device_color"
      ),
      Seq(Row("uuidA009863017", "MAC09863017", "black3017", 1))
    )
  }
  )

  //VMALL_Per_TC_032
  test(
    "select device_color,count(distinct imei)as imei_number  from     myvmallTest   where " +
      "product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987" +
      "' group by device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select device_color,count(distinct imei)as imei_number  from     myvmallTest   where " +
          "product_name='Huawei3987' and " +
          "Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color " +
          "order by imei_number desc"
      ),
      Seq(Row("black3987", 1))
    )
  }
  )

  //VMALL_Per_TC_033
  test(
    "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  " +
      "where product_name='Huawei3993' and " +
      "Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name," +
      "device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest " +
          " where product_name='Huawei3993' and " +
          "Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name," +
          "device_color order by imei_number desc"
      ),
      Seq(Row("Huawei3993", "black3993", 1))
    )
  }
  )

  //VMALL_Per_TC_034
  test(
    "select device_color, count(distinct imei) as imei_number from  myvmallTest  where " +
      "product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order " +
      "by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select device_color, count(distinct imei) as imei_number from  myvmallTest  where " +
          "product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color " +
          "order by imei_number desc"
      ),
      Seq(Row("black3972", 1))
    )
  }
  )

  //VMALL_Per_TC_035
  test(
    "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  " +
      "where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name," +
      "device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest " +
          " where product_name='Huawei3972' and deliveryprovince='Province_472' group by " +
          "product_name,device_color order by imei_number desc"
      ),
      Seq(Row("Huawei3972", "black3972", 1))
    )
  }
  )

  //VMALL_Per_TC_036
  test(
    "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  " +
      "where product_name='Huawei3987' and deliveryprovince='Province_487' and " +
      "deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest " +
          " where product_name='Huawei3987' and deliveryprovince='Province_487' and " +
          "deliverycity='deliveryCity487' group by product_name,device_color order by imei_number" +
          " desc"
      ),
      Seq(Row("Huawei3987", "black3987", 1))
    )
  }
  )

  //VMALL_Per_TC_037
  test(
    "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest  " +
      "where product_name='Huawei3987' and deliveryprovince='Province_487' and " +
      "deliverycity='deliveryCity487' and device_color='black3987' group by product_name," +
      "device_color order by imei_number desc"
  )({
    checkAnswer(
      sql(
        "select product_name,device_color, count(distinct imei) as imei_number from  myvmallTest " +
          " where product_name='Huawei3987' and deliveryprovince='Province_487' and " +
          "deliverycity='deliveryCity487' and device_color='black3987' group by product_name," +
          "device_color order by imei_number desc"
      ),
      Seq(Row("Huawei3987", "black3987", 1))
    )
  }
  )

  //VMALL_Per_TC_050
  test(
    "SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY " +
      "where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"
  )({
    checkAnswer(
      sql(
        "SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmallTest  SUB_QRY " +
          "where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"
      ),
      Seq(Row("Huawei3987", 1))
    )
  }
  )

  //VMALL_Per_TC_052
  test(
    "SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where " +
      "product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"
  )({
    checkAnswer(
      sql(
        "SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmallTest  where " +
          "product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC"
      ),
      Seq(Row("Huawei3987", 1))
    )
  }
  )

  //SmartPCC_Perf_TC_001
  test(
    "select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where" +
      " TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN"
  )({
    checkAnswer(
      sql(
        "select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  " +
          "where TERMINAL_BRAND='HTC' and APP_CATEGORY_NAME='Web_Browsing' group by MSISDN"
      ),
      Seq(Row("8613649905753", 2381))
    )
  }
  )

  //SmartPCC_Perf_TC_003
  test(
    "select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where" +
      " RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc"
  )({
    checkAnswer(
      sql(
        "select MSISDN,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  " +
          "where RAT_NAME='GERAN' group by MSISDN having total > 23865 order by total desc"
      ),
      Seq(Row("8613893462639", 2874640), Row("8613993676885", 73783))
    )
  }
  )

  //SmartPCC_Perf_TC_007
  test(
    "select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum" +
      "(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where APP_SUB_CATEGORY_NAME='HTTP' and " +
      "TERMINAL_BRAND='MARCONI' group by APP_SUB_CATEGORY_NAME order by msidn_number desc"
  )({
    checkAnswer(
      sql(
        "select APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)" +
          "+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where " +
          "APP_SUB_CATEGORY_NAME='HTTP' and TERMINAL_BRAND='MARCONI' group by " +
          "APP_SUB_CATEGORY_NAME order by msidn_number desc"
      ),
      Seq(Row("HTTP", 1, 2874640))
    )
  }
  )

  //SmartPCC_Perf_TC_009
  test(
    "select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum" +
      "(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' group by" +
      " TERMINAL_BRAND order by msidn_number desc"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_BRAND,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum" +
          "(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where TERMINAL_BRAND='MARCONI' " +
          "group by TERMINAL_BRAND order by msidn_number desc"
      ),
      Seq(Row("MARCONI", 1, 2874640))
    )
  }
  )

  //SmartPCC_Perf_TC_013
  test(
    "select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as" +
      " total from  traffic_2g_3g_4g  where CGI='460003772902063' group by CGI order by total desc"
  )({
    checkAnswer(
      sql(
        "select CGI,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+sum" +
          "(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where CGI='460003772902063' group " +
          "by CGI order by total desc"
      ),
      Seq(Row("460003772902063", 1, 73783))
    )
  }
  )

  //SmartPCC_Perf_TC_020
  test(
    "select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum" +
      "(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  " +
      "RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND,APP_SUB_CATEGORY_NAME " +
      "order by total desc"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_BRAND,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum" +
          "(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  " +
          "RAT_NAME='GERAN' and TERMINAL_BRAND='HTC' group by TERMINAL_BRAND," +
          "APP_SUB_CATEGORY_NAME order by total desc"
      ),
      Seq(Row("HTC", "HTTP_Browsing", 1, 2381), Row("HTC", "DNS", 1, 280))
    )
  }
  )

  //SmartPCC_Perf_TC_024
  test(
    "select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum" +
      "(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in" +
      "('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE order " +
      "by total desc"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_TYPE,APP_SUB_CATEGORY_NAME,count(distinct MSISDN) as msidn_number,sum" +
          "(UP_THROUGHPUT)+sum(DOWN_THROUGHPUT) as total from  traffic_2g_3g_4g  where  CGI in" +
          "('460003772902063','460003773401611') group by APP_SUB_CATEGORY_NAME,TERMINAL_TYPE " +
          "order by total desc"
      ),
      Seq(Row("SMARTPHONE", "HTTPS", 1, 73783), Row("SMARTPHONE", "HTTP_Browsing", 1, 2381))
    )
  }
  )

  //SmartPCC_Perf_TC_032
  test(
    "select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum" +
      "(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group by " +
      "TERMINAL_TYPE"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_TYPE,count(distinct MSISDN) as msidn_number,sum(UP_THROUGHPUT)+ sum" +
          "(DOWN_THROUGHPUT) as total from  traffic_2G_3G_4G where MSISDN='8613993800024' group " +
          "by TERMINAL_TYPE"
      ),
      Seq(Row("SMARTPHONE", 1, 5044))
    )
  }
  )

  //SmartPCC_Perf_TC_033
  test(
    "select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  " +
      "traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_TYPE,sum(UP_THROUGHPUT)+ sum(DOWN_THROUGHPUT) as total from  " +
          "traffic_2g_3g_4g where MSISDN='8613519003078' group by TERMINAL_TYPE"
      ),
      Seq(Row("FEATURE PHONE", 2485))
    )
  }
  )

  //SmartPCC_Perf_TC_034
  test(
    "select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where " +
      "MSISDN='8613993104233'"
  )({
    checkAnswer(
      sql(
        "select TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT as total from  Traffic_2G_3G_4G where MSISDN='8613993104233'"
      ),
      Seq(Row("SMARTPHONE", 134, 146))
    )
  }
  )

  //SmartPCC_Perf_TC_035
  test(
    "select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'"
  )({
    checkAnswer(
      sql(
        "select SOURCE_INFO,APP_CATEGORY_ID,RAT_NAME,TERMINAL_TYPE,UP_THROUGHPUT, DOWN_THROUGHPUT from  Traffic_2G_3G_4G where MSISDN='8618394185970' and APP_CATEGORY_ID='2'"
      ),
      Seq(Row("GN", "2", "GERAN", "SMARTPHONE", 13934, 9931))
    )
  }
  )

  //SmartPCC_Perf_TC_037
  test(
    "select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'"
  )({
    checkAnswer(
      sql(
        "select SOURCE_INFO,APP_CATEGORY_ID from Traffic_2G_3G_4G where MSISDN='8615120474362' and APP_CATEGORY_ID='-1'"
      ),
      Seq(Row("GN", "-1"))
    )
  }
  )

  //SmartPCC_Perf_TC_038
  test(
    "select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'"
  )({
    checkAnswer(
      sql(
        "select SOURCE_INFO,APP_CATEGORY_ID,TERMINAL_BRAND,TERMINAL_MODEL,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where MSISDN='8613893600602' and APP_CATEGORY_ID='-1'"
      ),
      Seq(Row("GN", "-1", "LENOVO", "LENOVO A60", 1662, 684))
    )
  }
  )

  //SmartPCC_Perf_TC_039
  test(
    "select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'"
  )({
    checkAnswer(
      sql(
        "select SOURCE_INFO,APP_CATEGORY_ID,CGI,DAY,HOUR,MIN,UP_THROUGHPUT,DOWN_THROUGHPUT from Traffic_2G_3G_4G where  APP_CATEGORY_ID='16' and MSISDN='8613993899110' and CGI='460003776906411' and DAY='8-1' and HOUR='23'"
      ),
      Seq(Row("GN", "16", "460003776906411", "8-1", "23", "0", 1647, 2717))
    )
  }
  )

}
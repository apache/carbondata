
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
 * Test Class for myvmall to verify all scenerios
 */

class MYVMALLTestCase extends QueryTest with BeforeAndAfterAll {
         

//VMALL_Per_CreateCube_001_Drop
test("VMALL_Per_CreateCube_001_Drop", Include) {
  sql(s"""drop table if exists  myvmall""").collect

  sql(s"""drop table if exists  myvmall_hive""").collect

}
       

//VMALL_Per_CreateCube_001
test("VMALL_Per_CreateCube_001", Include) {
  sql(s"""create table myvmall (imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year int,check_month int ,check_day int,check_hour int,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year int,Active_check_month int,Active_check_day int,Active_check_hour int,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year int,Latest_check_month int,Latest_check_day int,Latest_check_hour int,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES ('DICTIONARY_INCLUDE'='check_year,check_month,check_day,check_hour,Active_check_year,Active_check_month,Active_check_day,Active_check_hour,Latest_check_year,Latest_check_month,Latest_check_day')""").collect

  sql(s"""create table myvmall_hive (imei String,uuid String,MAC String,device_color String,device_shell_color String,device_name String,product_name String,ram String,rom String,cpu_clock String,series String,check_date String,check_year int,check_month int ,check_day int,check_hour int,bom String,inside_name String,packing_date String,packing_year String,packing_month String,packing_day String,packing_hour String,customer_name String,deliveryAreaId String,deliveryCountry String,deliveryProvince String,deliveryCity String,deliveryDistrict String,packing_list_no String,order_no String,Active_check_time String,Active_check_year int,Active_check_month int,Active_check_day int,Active_check_hour int,ActiveAreaId String,ActiveCountry String,ActiveProvince String,Activecity String,ActiveDistrict String,Active_network String,Active_firmware_version String,Active_emui_version String,Active_os_version String,Latest_check_time String,Latest_check_year int,Latest_check_month int,Latest_check_day int,Latest_check_hour int,Latest_areaId String,Latest_country String,Latest_province String,Latest_city String,Latest_district String,Latest_firmware_version String,Latest_emui_version String,Latest_os_version String,Latest_network String,site String,site_desc String,product String,product_desc String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//VMALL_Per_DataLoad_001
test("VMALL_Per_DataLoad_001", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO table myvmall options('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,uuid,MAC,device_color,device_shell_color,device_name,product_name,ram,rom,cpu_clock,series,check_date,check_year,check_month,check_day,check_hour,bom,inside_name,packing_date,packing_year,packing_month,packing_day,packing_hour,customer_name,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,packing_list_no,order_no,Active_check_time,Active_check_year,Active_check_month,Active_check_day,Active_check_hour,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,Active_network,Active_firmware_version,Active_emui_version,Active_os_version,Latest_check_time,Latest_check_year,Latest_check_month,Latest_check_day,Latest_check_hour,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_firmware_version,Latest_emui_version,Latest_os_version,Latest_network,site,site_desc,product,product_desc')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/100_VMALL_1_Day_DATA_2015-09-15.csv' INTO table myvmall_hive """).collect

}
       

//VMALL_Per_TC_000
test("VMALL_Per_TC_000", Include) {
  checkAnswer(s"""select count(*) from  myvmall   """,
    s"""select count(*) from  myvmall_hive   """)
}
       

//VMALL_Per_TC_001
test("VMALL_Per_TC_001", Include) {
  checkAnswer(s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY GROUP BY product_name ORDER BY product_name ASC""")
}
       

//VMALL_Per_TC_002
test("VMALL_Per_TC_002", Include) {
  checkAnswer(s"""SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC""",
    s"""SELECT device_name, product, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY GROUP BY device_name, product, product_name ORDER BY device_name ASC, product ASC, product_name ASC""")
}
       

//VMALL_Per_TC_003
test("VMALL_Per_TC_003", Include) {
  checkAnswer(s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC""",
    s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY where product_name='Huawei4009' GROUP BY product_name ORDER BY  product_name ASC""")
}
       

//VMALL_Per_TC_004
test("VMALL_Per_TC_004", Include) {
  checkAnswer(s"""SELECT device_color FROM (select * from myvmall) SUB_QRY GROUP BY device_color ORDER BY device_color ASC""",
    s"""SELECT device_color FROM (select * from myvmall_hive) SUB_QRY GROUP BY device_color ORDER BY device_color ASC""")
}
       

//VMALL_Per_TC_005
test("VMALL_Per_TC_005", Include) {
  checkAnswer(s"""SELECT product_name  FROM (select * from myvmall) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC""",
    s"""SELECT product_name  FROM (select * from myvmall_hive) SUB_QRY GROUP BY product_name ORDER BY  product_name ASC""")
}
       

//VMALL_Per_TC_006
test("VMALL_Per_TC_006", Include) {
  checkAnswer(s"""SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, COUNT(DISTINCT packing_list_no) AS LONG_COL_0 FROM (select * from myvmall_hive) SUB_QRY GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_007
test("VMALL_Per_TC_007", Include) {
  checkAnswer(s"""select count(distinct imei) DistinctCount_imei from myvmall""",
    s"""select count(distinct imei) DistinctCount_imei from myvmall_hive""")
}
       

//VMALL_Per_TC_008
test("VMALL_Per_TC_008", Include) {
  checkAnswer(s"""Select count(imei),deliveryCountry  from myvmall group by deliveryCountry order by deliveryCountry asc""",
    s"""Select count(imei),deliveryCountry  from myvmall_hive group by deliveryCountry order by deliveryCountry asc""")
}
       

//VMALL_Per_TC_011
test("VMALL_Per_TC_011", Include) {
  checkAnswer(s"""select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmall) sub where mydates<1000""",
    s"""select count(imei) from (select DATEDIFF(from_unixtime(unix_timestamp()),packing_date) mydates,imei from myvmall_hive) sub where mydates<1000""")
}
       

//VMALL_Per_TC_012
test("VMALL_Per_TC_012", Include) {
  checkAnswer(s"""SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC""",
    s"""SELECT Active_os_version, count(distinct imei) DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY GROUP BY Active_os_version ORDER BY Active_os_version ASC""")
}
       

//VMALL_Per_TC_013
test("VMALL_Per_TC_013", Include) {
  checkAnswer(s"""select count(imei)  DistinctCount_imei from myvmall where (Active_emui_version="EmotionUI_2.972" and Latest_emui_version="EmotionUI_3.863972") OR (Active_emui_version="EmotionUI_2.843" and Latest_emui_version="EmotionUI_3.863843")""",
    s"""select count(imei)  DistinctCount_imei from myvmall_hive where (Active_emui_version="EmotionUI_2.972" and Latest_emui_version="EmotionUI_3.863972") OR (Active_emui_version="EmotionUI_2.843" and Latest_emui_version="EmotionUI_3.863843")""")
}
       

//VMALL_Per_TC_014
test("VMALL_Per_TC_014", Include) {
  checkAnswer(s"""select count(imei) as imeicount from myvmall where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')""",
    s"""select count(imei) as imeicount from myvmall_hive where (Active_os_version='Android 4.4.3' and Active_emui_version='EmotionUI_2.3')or (Active_os_version='Android 4.4.2' and Active_emui_version='EmotionUI_2.2')""")
}
       

//VMALL_Per_TC_B015
test("VMALL_Per_TC_B015", Include) {
  checkAnswer(s"""SELECT product, count(distinct imei) DistinctCount_imei FROM myvmall GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, count(distinct imei) DistinctCount_imei FROM myvmall_hive GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_B016
test("VMALL_Per_TC_B016", Include) {
  checkAnswer(s"""SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC""",
    s"""SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC""")
}
       

//VMALL_Per_TC_B017
test("VMALL_Per_TC_B017", Include) {
  checkAnswer(s"""SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmall) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from myvmall_hive) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_B018
test("VMALL_Per_TC_B018", Include) {
  checkAnswer(s"""SELECT Active_emui_version FROM (select * from myvmall) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC""",
    s"""SELECT Active_emui_version FROM (select * from myvmall_hive) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC""")
}
       

//VMALL_Per_TC_B019
test("VMALL_Per_TC_B019", Include) {
  checkAnswer(s"""SELECT product FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC""",
    s"""SELECT product FROM (select * from myvmall_hive) SUB_QRY GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_B020
test("VMALL_Per_TC_B020", Include) {
  checkAnswer(s"""SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmall) SUB_QRY GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from myvmall_hive) SUB_QRY GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_015
test("VMALL_Per_TC_015", Include) {
  checkAnswer(s"""SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmall    GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, count(distinct imei) DistinctCount_imei FROM    myvmall_hive    GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_016
test("VMALL_Per_TC_016", Include) {
  checkAnswer(s"""SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC""",
    s"""SELECT Active_emui_version, product, product_desc, COUNT(DISTINCT imei) AS DistinctCount_imei FROM (select * from    myvmall_hive   ) SUB_QRY GROUP BY Active_emui_version, product, product_desc ORDER BY Active_emui_version ASC, product ASC, product_desc ASC""")
}
       

//VMALL_Per_TC_017
test("VMALL_Per_TC_017", Include) {
  checkAnswer(s"""SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, count(distinct imei) DistinctCount_imei FROM (select * from    myvmall_hive   ) SUB_QRY where product='SmartPhone_3998' GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_018
test("VMALL_Per_TC_018", Include) {
  checkAnswer(s"""SELECT Active_emui_version FROM (select * from    myvmall   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC""",
    s"""SELECT Active_emui_version FROM (select * from    myvmall_hive   ) SUB_QRY GROUP BY Active_emui_version ORDER BY Active_emui_version ASC""")
}
       

//VMALL_Per_TC_019
test("VMALL_Per_TC_019", Include) {
  checkAnswer(s"""SELECT product FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC""",
    s"""SELECT product FROM (select * from    myvmall_hive   ) SUB_QRY GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_020
test("VMALL_Per_TC_020", Include) {
  checkAnswer(s"""SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmall   ) SUB_QRY GROUP BY product ORDER BY product ASC""",
    s"""SELECT product, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM (select * from    myvmall_hive   ) SUB_QRY GROUP BY product ORDER BY product ASC""")
}
       

//VMALL_Per_TC_021
test("VMALL_Per_TC_021", Include) {
  checkAnswer(s"""SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'""",
    s"""SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall_hive   ) SUB_QRY where device_name='Honor63011'  and product_name='Huawei3011'""")
}
       

//VMALL_Per_TC_022
test("VMALL_Per_TC_022", Include) {
  checkAnswer(s"""SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'""",
    s"""SELECT  imei,device_name DistinctCount_imei FROM (select * from    myvmall_hive   ) SUB_QRY where imei='imeiA009863011' or imei='imeiA009863012'""")
}
       

//VMALL_Per_TC_023
test("VMALL_Per_TC_023", Include) {
  checkAnswer(s"""SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmall   ) SUB_QRY where series LIKE 'series1%' group by series""",
    s"""SELECT  count(imei) as distinct_imei,series FROM (select * from    myvmall_hive   ) SUB_QRY where series LIKE 'series1%' group by series""")
}
       

//VMALL_Per_TC_024
test("VMALL_Per_TC_024", Include) {
  checkAnswer(s"""select product_name, count(distinct imei)  as imei_number from     myvmall    where imei='imeiA009863017' group by product_name""",
    s"""select product_name, count(distinct imei)  as imei_number from     myvmall_hive    where imei='imeiA009863017' group by product_name""")
}
       

//VMALL_Per_TC_025
test("VMALL_Per_TC_025", Include) {
  checkAnswer(s"""select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc""",
    s"""select product_name, count(distinct imei)  as imei_number from     myvmall_hive     where deliveryAreaId ='500280121000000_9863017' group by product_name order by imei_number desc""")
}
       

//VMALL_Per_TC_026
test("VMALL_Per_TC_026", Include) {
  checkAnswer(s"""select deliveryCity, count(distinct imei)  as imei_number from     myvmall     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc""",
    s"""select deliveryCity, count(distinct imei)  as imei_number from     myvmall_hive     where deliveryCity='deliveryCity17' group by deliveryCity order by imei_number desc""")
}
       

//VMALL_Per_TC_027
test("VMALL_Per_TC_027", Include) {
  checkAnswer(s"""select device_color, count(distinct imei)  as imei_number from     myvmall     where bom='51090576_63017' group by device_color order by imei_number desc""",
    s"""select device_color, count(distinct imei)  as imei_number from     myvmall_hive     where bom='51090576_63017' group by device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_028
test("VMALL_Per_TC_028", Include) {
  checkAnswer(s"""select product_name, count(distinct imei)  as imei_number from     myvmall     where product_name='Huawei3017' group by product_name order by imei_number desc""",
    s"""select product_name, count(distinct imei)  as imei_number from     myvmall_hive     where product_name='Huawei3017' group by product_name order by imei_number desc""")
}
       

//VMALL_Per_TC_029
test("VMALL_Per_TC_029", Include) {
  checkAnswer(s"""select product_name, count(distinct imei)  as imei_number from     myvmall     where deliveryprovince='Province_17' group by product_name order by product_name,imei_number desc""",
    s"""select product_name, count(distinct imei)  as imei_number from     myvmall_hive     where deliveryprovince='Province_17' group by product_name order by product_name,imei_number desc""")
}
       

//VMALL_Per_TC_030
test("VMALL_Per_TC_030", Include) {
  checkAnswer(s"""select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmall     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc""",
    s"""select rom,cpu_clock, count(distinct imei)  as imei_number from     myvmall_hive     where  deliveryprovince='Province_17' group by rom,cpu_clock order by imei_number desc""")
}
       

//VMALL_Per_TC_031
test("VMALL_Per_TC_031", Include) {
  checkAnswer(s"""select uuid,mac,device_color,count(distinct imei) from    myvmall    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color""",
    s"""select uuid,mac,device_color,count(distinct imei) from    myvmall_hive    where  imei='imeiA009863017' and deliveryareaid='500280121000000_9863017' group by uuid,mac,device_color""")
}
       

//VMALL_Per_TC_032
test("VMALL_Per_TC_032", Include) {
  checkAnswer(s"""select device_color,count(distinct imei)as imei_number  from     myvmall   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc""",
    s"""select device_color,count(distinct imei)as imei_number  from     myvmall_hive   where product_name='Huawei3987' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863987' group by device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_033
test("VMALL_Per_TC_033", Include) {
  checkAnswer(s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc""",
    s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall_hive  where product_name='Huawei3993' and Active_firmware_version='H60-L01V100R001CHNC00B121SP0_863993' group by product_name,device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_034
test("VMALL_Per_TC_034", Include) {
  checkAnswer(s"""select device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc""",
    s"""select device_color, count(distinct imei) as imei_number from  myvmall_hive  where product_name='Huawei3972' and deliveryprovince='Province_472' group by device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_035
test("VMALL_Per_TC_035", Include) {
  checkAnswer(s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc""",
    s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall_hive  where product_name='Huawei3972' and deliveryprovince='Province_472' group by product_name,device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_036
test("VMALL_Per_TC_036", Include) {
  checkAnswer(s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc""",
    s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall_hive  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' group by product_name,device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_037
test("VMALL_Per_TC_037", Include) {
  checkAnswer(s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc""",
    s"""select product_name,device_color, count(distinct imei) as imei_number from  myvmall_hive  where product_name='Huawei3987' and deliveryprovince='Province_487' and deliverycity='deliveryCity487' and device_color='black3987' group by product_name,device_color order by imei_number desc""")
}
       

//VMALL_Per_TC_038
test("VMALL_Per_TC_038", Include) {
  checkAnswer(s"""select Latest_network, count(distinct imei) as imei_number from  myvmall  group by Latest_network""",
    s"""select Latest_network, count(distinct imei) as imei_number from  myvmall_hive  group by Latest_network""")
}
       

//VMALL_Per_TC_039
test("VMALL_Per_TC_039", Include) {
  checkAnswer(s"""select device_name, count(distinct imei) as imei_number from  myvmall  group by device_name""",
    s"""select device_name, count(distinct imei) as imei_number from  myvmall_hive  group by device_name""")
}
       

//VMALL_Per_TC_040
test("VMALL_Per_TC_040", Include) {
  checkAnswer(s"""select product_name, count(distinct imei) as imei_number from  myvmall  group by product_name""",
    s"""select product_name, count(distinct imei) as imei_number from  myvmall_hive  group by product_name""")
}
       

//VMALL_Per_TC_041
test("VMALL_Per_TC_041", Include) {
  checkAnswer(s"""select deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity""",
    s"""select deliverycity, count(distinct imei) as imei_number from  myvmall_hive  group by deliverycity""")
}
       

//VMALL_Per_TC_042
test("VMALL_Per_TC_042", Include) {
  checkAnswer(s"""select device_name, deliverycity,count(distinct imei) as imei_number from  myvmall  group by device_name,deliverycity """,
    s"""select device_name, deliverycity,count(distinct imei) as imei_number from  myvmall_hive  group by device_name,deliverycity """)
}
       

//VMALL_Per_TC_043
test("VMALL_Per_TC_043", Include) {
  checkAnswer(s"""select product_name, device_name, count(distinct imei) as imei_number from  myvmall  group by product_name,device_name """,
    s"""select product_name, device_name, count(distinct imei) as imei_number from  myvmall_hive  group by product_name,device_name """)
}
       

//VMALL_Per_TC_044
test("VMALL_Per_TC_044", Include) {
  checkAnswer(s"""select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name""",
    s"""select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall_hive  group by deliverycity,product_name""")
}
       

//VMALL_Per_TC_045
test("VMALL_Per_TC_045", Include) {
  checkAnswer(s"""select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall  group by deliverycity,product_name""",
    s"""select product_name,deliverycity, count(distinct imei) as imei_number from  myvmall_hive  group by deliverycity,product_name""")
}
       

//VMALL_Per_TC_046
test("VMALL_Per_TC_046", Include) {
  checkAnswer(s"""select check_day,check_hour, count(distinct imei) as imei_number from  myvmall  group by check_day,check_hour""",
    s"""select check_day,check_hour, count(distinct imei) as imei_number from  myvmall_hive  group by check_day,check_hour""")
}
       

//VMALL_Per_TC_047
test("VMALL_Per_TC_047", Include) {
  checkAnswer(s"""select device_color,product_name, count(distinct imei) as imei_number from  myvmall  group by device_color,product_name order by product_name limit 1000""",
    s"""select device_color,product_name, count(distinct imei) as imei_number from  myvmall_hive  group by device_color,product_name order by product_name limit 1000""")
}
       

//VMALL_Per_TC_048
test("VMALL_Per_TC_048", Include) {
  checkAnswer(s"""select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmall  group by packing_hour,deliveryCity,device_color order by packing_hour,deliveryCity,device_color  limit 1000""",
    s"""select packing_hour,deliveryCity,device_color,count(distinct imei) as imei_number from  myvmall_hive  group by packing_hour,deliveryCity,device_color order by packing_hour,deliveryCity,device_color  limit 1000""")
}
       

//VMALL_Per_TC_049
test("VMALL_Per_TC_049", Include) {
  checkAnswer(s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall_hive  GROUP BY product_name ORDER BY product_name ASC""")
}
       

//VMALL_Per_TC_050
test("VMALL_Per_TC_050", Include) {
  checkAnswer(s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name, count(distinct imei) DistinctCount_imei FROM  myvmall_hive  SUB_QRY where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC""")
}
       

//VMALL_Per_TC_051
test("VMALL_Per_TC_051", Include) {
  checkAnswer(s"""SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmall  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC""",
    s"""SELECT device_color, product_name, COUNT(DISTINCT imei) AS DistinctCount_imei FROM  myvmall_hive  GROUP BY device_color, product_name ORDER BY device_color ASC, product_name ASC""")
}
       

//VMALL_Per_TC_052
test("VMALL_Per_TC_052", Include) {
  checkAnswer(s"""SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmall  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name, count(distinct imei) DistinctCount_imei from  myvmall_hive  where product_name='Huawei3987' GROUP BY product_name ORDER BY product_name ASC""")
}
       

//VMALL_Per_TC_053
test("VMALL_Per_TC_053", Include) {
  checkAnswer(s"""SELECT product_name FROM  myvmall  SUB_QRY GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name FROM  myvmall_hive  SUB_QRY GROUP BY product_name ORDER BY product_name ASC""")
}
       

//VMALL_Per_TC_054
test("VMALL_Per_TC_054", Include) {
  checkAnswer(s"""SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmall  GROUP BY product_name ORDER BY product_name ASC""",
    s"""SELECT product_name, COUNT(DISTINCT Active_emui_version) AS LONG_COL_0 FROM  myvmall_hive  GROUP BY product_name ORDER BY product_name ASC""")
}
       

//PushUP_FILTER_myvmall_TC001
test("PushUP_FILTER_myvmall_TC001", Include) {
  checkAnswer(s"""select check_year,check_month from myvmall where check_year=2015 or  check_month=9 or check_hour=-1""",
    s"""select check_year,check_month from myvmall_hive where check_year=2015 or  check_month=9 or check_hour=-1""")
}
       

//PushUP_FILTER_myvmall_TC002
test("PushUP_FILTER_myvmall_TC002", Include) {
  checkAnswer(s"""select check_year,check_month from myvmall where check_year=2015 and  check_month=9 and check_hour=-1""",
    s"""select check_year,check_month from myvmall_hive where check_year=2015 and  check_month=9 and check_hour=-1""")
}
       

//PushUP_FILTER_myvmall_TC003
test("PushUP_FILTER_myvmall_TC003", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei =='imeiA009863011') and (check_year== 2015) and (check_month==9) and (check_day==15)""",
    s"""select imei from myvmall_hive where  (imei =='imeiA009863011') and (check_year== 2015) and (check_month==9) and (check_day==15)""")
}
       

//PushUP_FILTER_myvmall_TC004
test("PushUP_FILTER_myvmall_TC004", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei =='imeiA009863011') or (check_year== 2015) or (check_month==9) or (check_day==15)""",
    s"""select imei from myvmall_hive where  (imei =='imeiA009863011') or (check_year== 2015) or (check_month==9) or (check_day==15)""")
}
       

//PushUP_FILTER_myvmall_TC005
test("PushUP_FILTER_myvmall_TC005", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei =='imeiA009863011') or (check_year== 2015) and (check_month==9) or (check_day==15)""",
    s"""select imei from myvmall_hive where  (imei =='imeiA009863011') or (check_year== 2015) and (check_month==9) or (check_day==15)""")
}
       

//PushUP_FILTER_myvmall_TC006
test("PushUP_FILTER_myvmall_TC006", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei !='imeiA009863015') and (check_year != 2016) and (check_month!=10) and (check_day!=15)""",
    s"""select imei from myvmall_hive where  (imei !='imeiA009863015') and (check_year != 2016) and (check_month!=10) and (check_day!=15)""")
}
       

//PushUP_FILTER_myvmall_TC007
test("PushUP_FILTER_myvmall_TC007", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei !='imeiA009863015') or (check_year != 2016) or (check_month!=10) or (check_day!=15)""",
    s"""select imei from myvmall_hive where  (imei !='imeiA009863015') or (check_year != 2016) or (check_month!=10) or (check_day!=15)""")
}
       

//PushUP_FILTER_myvmall_TC008
test("PushUP_FILTER_myvmall_TC008", Include) {
  checkAnswer(s"""select imei from myvmall where  (imei !='imeiA009863015') or (check_year != 2016) and (check_month!=10) or (check_day!=15)""",
    s"""select imei from myvmall_hive where  (imei !='imeiA009863015') or (check_year != 2016) and (check_month!=10) or (check_day!=15)""")
}
       

//PushUP_FILTER_myvmall_TC009
test("PushUP_FILTER_myvmall_TC009", Include) {
  checkAnswer(s"""select imei,check_year,check_month from myvmall where active_firmware_version IS NOT NULL and activeareaid IS NOT NULL and activedistrict IS NOT NULL""",
    s"""select imei,check_year,check_month from myvmall_hive where active_firmware_version IS NOT NULL and activeareaid IS NOT NULL and activedistrict IS NOT NULL""")
}
       

//PushUP_FILTER_myvmall_TC010
test("PushUP_FILTER_myvmall_TC010", Include) {
  checkAnswer(s"""select imei,check_year,check_month from myvmall where active_firmware_version IS NOT NULL or activeareaid IS NOT NULL or activedistrict IS NOT NULL""",
    s"""select imei,check_year,check_month from myvmall_hive where active_firmware_version IS NOT NULL or activeareaid IS NOT NULL or activedistrict IS NOT NULL""")
}
       

//PushUP_FILTER_myvmall_TC011
test("PushUP_FILTER_myvmall_TC011", Include) {
  checkAnswer(s"""select imei,check_year,check_month from myvmall where active_firmware_version IS NOT NULL and activeareaid IS NOT NULL or activedistrict IS NOT NULL""",
    s"""select imei,check_year,check_month from myvmall_hive where active_firmware_version IS NOT NULL and activeareaid IS NOT NULL or activedistrict IS NOT NULL""")
}
       

//PushUP_FILTER_myvmall_TC012
test("PushUP_FILTER_myvmall_TC012", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where myvmall.latest_check_hour  NOT IN (1,10) and myvmall.imei NOT IN ('imeiA009945257','imeiA009945258') and myvmall.check_year NOT IN (2014,2016)""",
    s"""select imei,latest_check_hour from myvmall_hive where myvmall_hive.latest_check_hour  NOT IN (1,10) and myvmall_hive.imei NOT IN ('imeiA009945257','imeiA009945258') and myvmall_hive.check_year NOT IN (2014,2016)""")
}
       

//PushUP_FILTER_myvmall_TC013
test("PushUP_FILTER_myvmall_TC013", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where myvmall.latest_check_hour  IN (10,14) and myvmall.imei IN ('imeiA009945257','imeiA009945258') and myvmall.check_year IN (2015)""",
    s"""select imei,latest_check_hour from myvmall_hive where myvmall_hive.latest_check_hour  IN (10,14) and myvmall_hive.imei IN ('imeiA009945257','imeiA009945258') and myvmall_hive.check_year IN (2015)""")
}
       

//PushUP_FILTER_myvmall_TC014
test("PushUP_FILTER_myvmall_TC014", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where myvmall.latest_check_hour  NOT IN (1,10) and myvmall.imei NOT IN ('imeiA009945257','imeiA009945258') and myvmall.check_year IN (2015)""",
    s"""select imei,latest_check_hour from myvmall_hive where myvmall_hive.latest_check_hour  NOT IN (1,10) and myvmall_hive.imei NOT IN ('imeiA009945257','imeiA009945258') and myvmall_hive.check_year IN (2015)""")
}
       

//PushUP_FILTER_myvmall_TC015
test("PushUP_FILTER_myvmall_TC015", Include) {
  checkAnswer(s"""select imei,latest_check_hour+0.1, Latest_check_year+10,Latest_check_month+9999999999.999 from myvmall""",
    s"""select imei,latest_check_hour+0.1, Latest_check_year+10,Latest_check_month+9999999999.999 from myvmall_hive""")
}
       

//PushUP_FILTER_myvmall_TC016
test("PushUP_FILTER_myvmall_TC016", Include) {
  checkAnswer(s"""select imei,latest_check_hour-0.99, Latest_check_year-91,Latest_check_month-9999999999.999 from myvmall""",
    s"""select imei,latest_check_hour-0.99, Latest_check_year-91,Latest_check_month-9999999999.999 from myvmall_hive""")
}
       

//PushUP_FILTER_myvmall_TC017
test("PushUP_FILTER_myvmall_TC017", Include) {
  checkAnswer(s"""select imei,latest_check_hour*0.99, Latest_check_year*91,Latest_check_month*9999999999.999 from myvmall""",
    s"""select imei,latest_check_hour*0.99, Latest_check_year*91,Latest_check_month*9999999999.999 from myvmall_hive""")
}
       

//PushUP_FILTER_myvmall_TC018
test("PushUP_FILTER_myvmall_TC018", Include) {
  checkAnswer(s"""select imei,latest_check_hour/0.99, Latest_check_year/91,Latest_check_month/9999999999.999 from myvmall""",
    s"""select imei,latest_check_hour/0.99, Latest_check_year/91,Latest_check_month/9999999999.999 from myvmall_hive""")
}
       

//PushUP_FILTER_myvmall_TC019
test("PushUP_FILTER_myvmall_TC019", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where latest_check_hour >10 and check_year >2014 and check_month >8""",
    s"""select imei,latest_check_hour from myvmall_hive where latest_check_hour >10 and check_year >2014 and check_month >8""")
}
       

//PushUP_FILTER_myvmall_TC020
test("PushUP_FILTER_myvmall_TC020", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where latest_check_hour <15 and check_year <2016 and check_month <10""",
    s"""select imei,latest_check_hour from myvmall_hive where latest_check_hour <15 and check_year <2016 and check_month <10""")
}
       

//PushUP_FILTER_myvmall_TC021
test("PushUP_FILTER_myvmall_TC021", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where latest_check_hour >=8 and check_year >=2014 and check_month >=1""",
    s"""select imei,latest_check_hour from myvmall_hive where latest_check_hour >=8 and check_year >=2014 and check_month >=1""")
}
       

//PushUP_FILTER_myvmall_TC022
test("PushUP_FILTER_myvmall_TC022", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where latest_check_hour <=15 and check_year <=2016 and check_month <=10""",
    s"""select imei,latest_check_hour from myvmall_hive where latest_check_hour <=15 and check_year <=2016 and check_month <=10""")
}
       

//PushUP_FILTER_myvmall_TC023
test("PushUP_FILTER_myvmall_TC023", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where check_year LIKE 2015 and check_day LIKE 15 and check_month LIKE 9""",
    s"""select imei,latest_check_hour from myvmall_hive where check_year LIKE 2015 and check_day LIKE 15 and check_month LIKE 9""")
}
       

//PushUP_FILTER_myvmall_TC024
test("PushUP_FILTER_myvmall_TC024", Include) {
  checkAnswer(s"""select imei,latest_check_hour from myvmall where check_year NOT LIKE 2014 and check_day NOT LIKE 14 and check_month NOT LIKE 10""",
    s"""select imei,latest_check_hour from myvmall_hive where check_year NOT LIKE 2014 and check_day NOT LIKE 14 and check_month NOT LIKE 10""")
}
       
override def afterAll {
sql("drop table if exists myvmall")
sql("drop table if exists myvmall_hive")
}
}
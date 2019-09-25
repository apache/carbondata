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

package org.apache.carbondata.spark.testsuite.allqueries

import org.apache.spark.sql.CarbonDictionaryCatalystDecoder
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.test.util.CarbonQueryTest

/**
  * Test Class for all query on multiple datatypes
  *
  */
class AllDataTypesTestCase extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll {
    clean

    sql("create table if not exists Carbon_automation_test (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string, gamePointId int,contractNumber int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='Latest_MONTH,Latest_DAY,deviceInformationId')")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/100_olap.csv' INTO table Carbon_automation_test options('DELIMITER'= ',', 'QUOTECHAR'= '\"', 'FILEHEADER'= 'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""")

    //hive table
    sql("create table if not exists Carbon_automation_test_hive (imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string,contractNumber int, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId int,gamePointDescription string)row format delimited fields terminated by ','")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/100_olap.csv' INTO table Carbon_automation_test_hive""")

  }

  def clean {
    sql("drop table if exists carbonunion")
    sql("drop table if exists Carbon_automation_test")
    sql("drop table if exists Carbon_automation_test_hive")
  }

  override def afterAll {
    clean
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }

  ignore("TPCH query issue with not joining with decoded values") {
    sql("drop table if exists SUPPLIER")
    sql("drop table if exists PARTSUPP")
    sql("drop table if exists CUSTOMER")
    sql("drop table if exists NATION")
    sql("drop table if exists REGION")
    sql("drop table if exists PART")
    sql("drop table if exists LINEITEM")
    sql("drop table if exists ORDERS")
    sql("create table if not exists SUPPLIER(S_COMMENT string,S_SUPPKEY string,S_NAME string, S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_EXCLUDE'='S_COMMENT, S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE','table_blocksize'='300','SORT_COLUMNS'='')")
    sql("create table if not exists PARTSUPP (  PS_PARTKEY int,  PS_SUPPKEY  string,  PS_AVAILQTY  int,  PS_SUPPLYCOST  double,  PS_COMMENT  string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_EXCLUDE'='PS_SUPPKEY,PS_COMMENT', 'table_blocksize'='300', 'no_inverted_index'='PS_SUPPKEY, PS_COMMENT','SORT_COLUMNS'='')")
    sql("create table if not exists CUSTOMER(  C_MKTSEGMENT string,  C_NATIONKEY string,  C_CUSTKEY string,  C_NAME string,  C_ADDRESS string,  C_PHONE string,  C_ACCTBAL double,  C_COMMENT string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='C_MKTSEGMENT,C_NATIONKEY','DICTIONARY_EXCLUDE'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT', 'table_blocksize'='300', 'no_inverted_index'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT','SORT_COLUMNS'='C_MKTSEGMENT')")
    sql("create table if not exists NATION (  N_NAME string,  N_NATIONKEY string,  N_REGIONKEY string,  N_COMMENT  string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='N_REGIONKEY','DICTIONARY_EXCLUDE'='N_COMMENT', 'table_blocksize'='300','no_inverted_index'='N_COMMENT','SORT_COLUMNS'='N_NAME')")
    sql("create table if not exists REGION(  R_NAME string,  R_REGIONKEY string,  R_COMMENT string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='R_NAME,R_REGIONKEY','DICTIONARY_EXCLUDE'='R_COMMENT', 'table_blocksize'='300','no_inverted_index'='R_COMMENT','SORT_COLUMNS'='R_NAME')")
    sql("create table if not exists PART(  P_BRAND string,  P_SIZE int,  P_CONTAINER string,  P_TYPE string,  P_PARTKEY INT ,  P_NAME string,  P_MFGR string,  P_RETAILPRICE double,  P_COMMENT string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='P_BRAND,P_SIZE,P_CONTAINER,P_MFGR','DICTIONARY_EXCLUDE'='P_NAME, P_COMMENT', 'table_blocksize'='300','no_inverted_index'='P_NAME,P_COMMENT,P_MFGR','SORT_COLUMNS'='P_SIZE,P_TYPE,P_NAME,P_BRAND,P_CONTAINER')")
    sql("create table if not exists LINEITEM(  L_SHIPDATE date,  L_SHIPMODE string,  L_SHIPINSTRUCT string,  L_RETURNFLAG string,  L_RECEIPTDATE date,  L_ORDERKEY INT ,  L_PARTKEY INT ,  L_SUPPKEY   string,  L_LINENUMBER int,  L_QUANTITY double,  L_EXTENDEDPRICE double,  L_DISCOUNT double,  L_TAX double,  L_LINESTATUS string,  L_COMMITDATE date,  L_COMMENT  string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='L_SHIPDATE,L_SHIPMODE,L_SHIPINSTRUCT,L_RECEIPTDATE,L_COMMITDATE,L_RETURNFLAG,L_LINESTATUS','DICTIONARY_EXCLUDE'='L_SUPPKEY, L_COMMENT', 'table_blocksize'='300', 'no_inverted_index'='L_SUPPKEY,L_COMMENT','SORT_COLUMNS'='L_SHIPDATE,L_RETURNFLAG,L_SHIPMODE,L_RECEIPTDATE,L_SHIPINSTRUCT')")
    sql("create table if not exists ORDERS(  O_ORDERDATE date,  O_ORDERPRIORITY string,  O_ORDERSTATUS string,  O_ORDERKEY int,  O_CUSTKEY string,  O_TOTALPRICE double,  O_CLERK string,  O_SHIPPRIORITY int,  O_COMMENT string) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES ('DICTIONARY_INCLUDE'='O_ORDERDATE,O_ORDERSTATUS','DICTIONARY_EXCLUDE'='O_ORDERPRIORITY, O_CUSTKEY, O_CLERK, O_COMMENT', 'table_blocksize'='300','no_inverted_index'='O_ORDERPRIORITY, O_CUSTKEY, O_CLERK, O_COMMENT', 'SORT_COLUMNS'='O_ORDERDATE')")
    val df = sql(
      "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from " +
      "part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = " +
      "ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and " +
      "n_regionkey = r_regionkey and r_name = 'EUROPE' and ps_supplycost = ( select min" +
      "(ps_supplycost) from partsupp, supplier,nation, region where p_partkey = ps_partkey and " +
      "s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and " +
      "r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey limit 100")

    val decoders = df.queryExecution.optimizedPlan.collect {
      case p: CarbonDictionaryCatalystDecoder => p
    }

    assertResult(5)(decoders.length)

    sql("drop table if exists SUPPLIER")
    sql("drop table if exists PARTSUPP")
    sql("drop table if exists CUSTOMER")
    sql("drop table if exists NATION")
    sql("drop table if exists REGION")
    sql("drop table if exists PART")
    sql("drop table if exists LINEITEM")
    sql("drop table if exists ORDERS")
  }
}
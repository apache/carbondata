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

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class DoubleDataTypeTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS oscon_carbon_old")
    sql("drop table if exists carbon_datatype_double_byte")
    sql("drop table if exists carbon_datatype_double_short")
    sql("drop table if exists carbon_datatype_double_sint")
    sql("drop table if exists carbon_datatype_double_int")
    sql("drop table if exists carbon_datatype_double_long")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_byte")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_short")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_sint")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_int")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_long")
    sql("DROP TABLE IF EXISTS oscon_carbon_old1")
    sql("""create table oscon_carbon_old (CUST_PRFRD_FLG String,PROD_BRAND_NAME String,PROD_COLOR String,CUST_LAST_RVW_DATE String,CUST_COUNTRY String,CUST_CITY String,PRODUCT_NAME String,CUST_JOB_TITLE String,CUST_STATE String,CUST_BUY_POTENTIAL String,PRODUCT_MODEL String,ITM_ID String,ITM_NAME String,PRMTION_ID String,PRMTION_NAME String,SHP_MODE_ID String,SHP_MODE String,DELIVERY_COUNTRY String,DELIVERY_STATE String,DELIVERY_CITY String,DELIVERY_DISTRICT String,ACTIVE_EMUI_VERSION String,WH_NAME String,STR_ORDER_DATE String,OL_ORDER_NO String,OL_ORDER_DATE String,OL_SITE String,CUST_FIRST_NAME String,CUST_LAST_NAME String,CUST_BIRTH_DY String,CUST_BIRTH_MM String,CUST_BIRTH_YR String,CUST_BIRTH_COUNTRY String,CUST_SEX String,CUST_ADDRESS_ID String,CUST_STREET_NO String,CUST_STREET_NAME String,CUST_AGE String,CUST_SUITE_NO String,CUST_ZIP String,CUST_COUNTY String,PRODUCT_ID String,PROD_SHELL_COLOR String,DEVICE_NAME String,PROD_SHORT_DESC String,PROD_LONG_DESC String,PROD_THUMB String,PROD_IMAGE String,PROD_UPDATE_DATE String,PROD_LIVE String,PROD_LOC String,PROD_RAM String,PROD_ROM String,PROD_CPU_CLOCK String,PROD_SERIES String,ITM_REC_START_DATE String,ITM_REC_END_DATE String,ITM_BRAND_ID String,ITM_BRAND String,ITM_CLASS_ID String,ITM_CLASS String,ITM_CATEGORY_ID String,ITM_CATEGORY String,ITM_MANUFACT_ID String,ITM_MANUFACT String,ITM_FORMULATION String,ITM_COLOR String,ITM_CONTAINER String,ITM_MANAGER_ID String,PRM_START_DATE String,PRM_END_DATE String,PRM_CHANNEL_DMAIL String,PRM_CHANNEL_EMAIL String,PRM_CHANNEL_CAT String,PRM_CHANNEL_TV String,PRM_CHANNEL_RADIO String,PRM_CHANNEL_PRESS String,PRM_CHANNEL_EVENT String,PRM_CHANNEL_DEMO String,PRM_CHANNEL_DETAILS String,PRM_PURPOSE String,PRM_DSCNT_ACTIVE String,SHP_CODE String,SHP_CARRIER String,SHP_CONTRACT String,CHECK_DATE String,CHECK_YR String,CHECK_MM String,CHECK_DY String,CHECK_HOUR String,BOM String,INSIDE_NAME String,PACKING_DATE String,PACKING_YR String,PACKING_MM String,PACKING_DY String,PACKING_HOUR String,DELIVERY_PROVINCE String,PACKING_LIST_NO String,ACTIVE_CHECK_TIME String,ACTIVE_CHECK_YR String,ACTIVE_CHECK_MM String,ACTIVE_CHECK_DY String,ACTIVE_CHECK_HOUR String,ACTIVE_AREA_ID String,ACTIVE_COUNTRY String,ACTIVE_PROVINCE String,ACTIVE_CITY String,ACTIVE_DISTRICT String,ACTIVE_NETWORK String,ACTIVE_FIRMWARE_VER String,ACTIVE_OS_VERSION String,LATEST_CHECK_TIME String,LATEST_CHECK_YR String,LATEST_CHECK_MM String,LATEST_CHECK_DY String,LATEST_CHECK_HOUR String,LATEST_AREAID String,LATEST_COUNTRY String,LATEST_PROVINCE String,LATEST_CITY String,LATEST_DISTRICT String,LATEST_FIRMWARE_VER String,LATEST_EMUI_VERSION String,LATEST_OS_VERSION String,LATEST_NETWORK String,WH_ID String,WH_STREET_NO String,WH_STREET_NAME String,WH_STREET_TYPE String,WH_SUITE_NO String,WH_CITY String,WH_COUNTY String,WH_STATE String,WH_ZIP String,WH_COUNTRY String,OL_SITE_DESC String,OL_RET_ORDER_NO String,OL_RET_DATE String,PROD_MODEL_ID String,CUST_ID String,PROD_UNQ_MDL_ID String,CUST_NICK_NAME String,CUST_LOGIN String,CUST_EMAIL_ADDR String,PROD_UNQ_DEVICE_ADDR String,PROD_UQ_UUID String,PROD_BAR_CODE String,TRACKING_NO String,STR_ORDER_NO String,CUST_DEP_COUNT double,CUST_VEHICLE_COUNT double,CUST_ADDRESS_CNT double,CUST_CRNT_CDEMO_CNT double,CUST_CRNT_HDEMO_CNT double,CUST_CRNT_ADDR_DM double,CUST_FIRST_SHIPTO_CNT double,CUST_FIRST_SALES_CNT double,CUST_GMT_OFFSET double,CUST_DEMO_CNT double,CUST_INCOME double,PROD_UNLIMITED double,PROD_OFF_PRICE double,PROD_UNITS double,TOTAL_PRD_COST double,TOTAL_PRD_DISC double,PROD_WEIGHT double,REG_UNIT_PRICE double,EXTENDED_AMT double,UNIT_PRICE_DSCNT_PCT double,DSCNT_AMT double,PROD_STD_CST double,TOTAL_TX_AMT double,FREIGHT_CHRG double,WAITING_PERIOD double,DELIVERY_PERIOD double,ITM_CRNT_PRICE double,ITM_UNITS double,ITM_WSLE_CST double,ITM_SIZE double,PRM_CST double,PRM_RESPONSE_TARGET double,PRM_ITM_DM double,SHP_MODE_CNT double,WH_GMT_OFFSET double,WH_SQ_FT double,STR_ORD_QTY double,STR_WSLE_CST double,STR_LIST_PRICE double,STR_SALES_PRICE double,STR_EXT_DSCNT_AMT double,STR_EXT_SALES_PRICE double,STR_EXT_WSLE_CST double,STR_EXT_LIST_PRICE double,STR_EXT_TX double,STR_COUPON_AMT double,STR_NET_PAID double,STR_NET_PAID_INC_TX double,STR_NET_PRFT double,STR_SOLD_YR_CNT double,STR_SOLD_MM_CNT double,STR_SOLD_ITM_CNT double,STR_TOTAL_CUST_CNT double,STR_AREA_CNT double,STR_DEMO_CNT double,STR_OFFER_CNT double,STR_PRM_CNT double,STR_TICKET_CNT double,STR_NET_PRFT_DM_A double,STR_NET_PRFT_DM_B double,STR_NET_PRFT_DM_C double,STR_NET_PRFT_DM_D double,STR_NET_PRFT_DM_E double,STR_RET_STR_ID double,STR_RET_REASON_CNT double,STR_RET_TICKET_NO double,STR_RTRN_QTY double,STR_RTRN_AMT double,STR_RTRN_TX double,STR_RTRN_AMT_INC_TX double,STR_RET_FEE double,STR_RTRN_SHIP_CST double,STR_RFNDD_CSH double,STR_REVERSED_CHRG double,STR_STR_CREDIT double,STR_RET_NET_LOSS double,STR_RTRNED_YR_CNT double,STR_RTRN_MM_CNT double,STR_RET_ITM_CNT double,STR_RET_CUST_CNT double,STR_RET_AREA_CNT double,STR_RET_OFFER_CNT double,STR_RET_PRM_CNT double,STR_RET_NET_LOSS_DM_A double,STR_RET_NET_LOSS_DM_B double,STR_RET_NET_LOSS_DM_C double,STR_RET_NET_LOSS_DM_D double,OL_ORD_QTY double,OL_WSLE_CST double,OL_LIST_PRICE double,OL_SALES_PRICE double,OL_EXT_DSCNT_AMT double,OL_EXT_SALES_PRICE double,OL_EXT_WSLE_CST double,OL_EXT_LIST_PRICE double,OL_EXT_TX double,OL_COUPON_AMT double,OL_EXT_SHIP_CST double,OL_NET_PAID double,OL_NET_PAID_INC_TX double,OL_NET_PAID_INC_SHIP double,OL_NET_PAID_INC_SHIP_TX double,OL_NET_PRFT double,OL_SOLD_YR_CNT double,OL_SOLD_MM_CNT double,OL_SHIP_DATE_CNT double,OL_ITM_CNT double,OL_BILL_CUST_CNT double,OL_BILL_AREA_CNT double,OL_BILL_DEMO_CNT double,OL_BILL_OFFER_CNT double,OL_SHIP_CUST_CNT double,OL_SHIP_AREA_CNT double,OL_SHIP_DEMO_CNT double,OL_SHIP_OFFER_CNT double,OL_WEB_PAGE_CNT double,OL_WEB_SITE_CNT double,OL_SHIP_MODE_CNT double,OL_WH_CNT double,OL_PRM_CNT double,OL_NET_PRFT_DM_A double,OL_NET_PRFT_DM_B double,OL_NET_PRFT_DM_C double,OL_NET_PRFT_DM_D double,OL_RET_RTRN_QTY double,OL_RTRN_AMT double,OL_RTRN_TX double,OL_RTRN_AMT_INC_TX double,OL_RET_FEE double,OL_RTRN_SHIP_CST double,OL_RFNDD_CSH double,OL_REVERSED_CHRG double,OL_ACCOUNT_CREDIT double,OL_RTRNED_YR_CNT double,OL_RTRNED_MM_CNT double,OL_RTRITM_CNT double,OL_RFNDD_CUST_CNT double,OL_RFNDD_AREA_CNT double,OL_RFNDD_DEMO_CNT double,OL_RFNDD_OFFER_CNT double,OL_RTRNING_CUST_CNT double,OL_RTRNING_AREA_CNT double,OL_RTRNING_DEMO_CNT double,OL_RTRNING_OFFER_CNT double,OL_RTRWEB_PAGE_CNT double,OL_REASON_CNT double,OL_NET_LOSS double,OL_NET_LOSS_DM_A double,OL_NET_LOSS_DM_B double,OL_NET_LOSS_DM_C double) STORED AS carbondata tblproperties ('table_blocksize'='256')""")
    sql(s"""load data LOCAL inpath '$resourcesPath/oscon_10.csv' into table oscon_carbon_old options('DELIMITER'=',', 'QUOTECHAR'='\"','FILEHEADER'='ACTIVE_AREA_ID, ACTIVE_CHECK_DY, ACTIVE_CHECK_HOUR, ACTIVE_CHECK_MM, ACTIVE_CHECK_TIME, ACTIVE_CHECK_YR, ACTIVE_CITY, ACTIVE_COUNTRY, ACTIVE_DISTRICT, ACTIVE_EMUI_VERSION, ACTIVE_FIRMWARE_VER, ACTIVE_NETWORK, ACTIVE_OS_VERSION, ACTIVE_PROVINCE, BOM, CHECK_DATE, CHECK_DY, CHECK_HOUR, CHECK_MM, CHECK_YR, CUST_ADDRESS_ID, CUST_AGE, CUST_BIRTH_COUNTRY, CUST_BIRTH_DY, CUST_BIRTH_MM, CUST_BIRTH_YR, CUST_BUY_POTENTIAL, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_COUNTY, CUST_EMAIL_ADDR, CUST_LAST_RVW_DATE, CUST_FIRST_NAME, CUST_ID, CUST_JOB_TITLE, CUST_LAST_NAME, CUST_LOGIN, CUST_NICK_NAME, CUST_PRFRD_FLG, CUST_SEX, CUST_STREET_NAME, CUST_STREET_NO, CUST_SUITE_NO, CUST_ZIP, DELIVERY_CITY, DELIVERY_STATE, DELIVERY_COUNTRY, DELIVERY_DISTRICT, DELIVERY_PROVINCE, DEVICE_NAME, INSIDE_NAME, ITM_BRAND, ITM_BRAND_ID, ITM_CATEGORY, ITM_CATEGORY_ID, ITM_CLASS, ITM_CLASS_ID, ITM_COLOR, ITM_CONTAINER, ITM_FORMULATION, ITM_MANAGER_ID, ITM_MANUFACT, ITM_MANUFACT_ID, ITM_ID, ITM_NAME, ITM_REC_END_DATE, ITM_REC_START_DATE, LATEST_AREAID, LATEST_CHECK_DY, LATEST_CHECK_HOUR, LATEST_CHECK_MM, LATEST_CHECK_TIME, LATEST_CHECK_YR, LATEST_CITY, LATEST_COUNTRY, LATEST_DISTRICT, LATEST_EMUI_VERSION, LATEST_FIRMWARE_VER, LATEST_NETWORK, LATEST_OS_VERSION, LATEST_PROVINCE, OL_ORDER_DATE, OL_ORDER_NO, OL_RET_ORDER_NO, OL_RET_DATE, OL_SITE, OL_SITE_DESC, PACKING_DATE, PACKING_DY, PACKING_HOUR, PACKING_LIST_NO, PACKING_MM, PACKING_YR, PRMTION_ID, PRMTION_NAME, PRM_CHANNEL_CAT, PRM_CHANNEL_DEMO, PRM_CHANNEL_DETAILS, PRM_CHANNEL_DMAIL, PRM_CHANNEL_EMAIL, PRM_CHANNEL_EVENT, PRM_CHANNEL_PRESS, PRM_CHANNEL_RADIO, PRM_CHANNEL_TV, PRM_DSCNT_ACTIVE, PRM_END_DATE, PRM_PURPOSE, PRM_START_DATE, PRODUCT_ID, PROD_BAR_CODE, PROD_BRAND_NAME, PRODUCT_NAME, PRODUCT_MODEL, PROD_MODEL_ID, PROD_COLOR, PROD_SHELL_COLOR, PROD_CPU_CLOCK, PROD_IMAGE, PROD_LIVE, PROD_LOC, PROD_LONG_DESC, PROD_RAM, PROD_ROM, PROD_SERIES, PROD_SHORT_DESC, PROD_THUMB, PROD_UNQ_DEVICE_ADDR, PROD_UNQ_MDL_ID, PROD_UPDATE_DATE, PROD_UQ_UUID, SHP_CARRIER, SHP_CODE, SHP_CONTRACT, SHP_MODE_ID, SHP_MODE, STR_ORDER_DATE, STR_ORDER_NO, TRACKING_NO, WH_CITY, WH_COUNTRY, WH_COUNTY, WH_ID, WH_NAME, WH_STATE, WH_STREET_NAME, WH_STREET_NO, WH_STREET_TYPE, WH_SUITE_NO, WH_ZIP, CUST_DEP_COUNT, CUST_VEHICLE_COUNT, CUST_ADDRESS_CNT, CUST_CRNT_CDEMO_CNT, CUST_CRNT_HDEMO_CNT, CUST_CRNT_ADDR_DM, CUST_FIRST_SHIPTO_CNT, CUST_FIRST_SALES_CNT, CUST_GMT_OFFSET, CUST_DEMO_CNT, CUST_INCOME, PROD_UNLIMITED, PROD_OFF_PRICE, PROD_UNITS, TOTAL_PRD_COST, TOTAL_PRD_DISC, PROD_WEIGHT, REG_UNIT_PRICE, EXTENDED_AMT, UNIT_PRICE_DSCNT_PCT, DSCNT_AMT, PROD_STD_CST, TOTAL_TX_AMT, FREIGHT_CHRG, WAITING_PERIOD, DELIVERY_PERIOD, ITM_CRNT_PRICE, ITM_UNITS, ITM_WSLE_CST, ITM_SIZE, PRM_CST, PRM_RESPONSE_TARGET, PRM_ITM_DM, SHP_MODE_CNT, WH_GMT_OFFSET, WH_SQ_FT, STR_ORD_QTY, STR_WSLE_CST, STR_LIST_PRICE, STR_SALES_PRICE, STR_EXT_DSCNT_AMT, STR_EXT_SALES_PRICE, STR_EXT_WSLE_CST, STR_EXT_LIST_PRICE, STR_EXT_TX, STR_COUPON_AMT, STR_NET_PAID, STR_NET_PAID_INC_TX, STR_NET_PRFT, STR_SOLD_YR_CNT, STR_SOLD_MM_CNT, STR_SOLD_ITM_CNT, STR_TOTAL_CUST_CNT, STR_AREA_CNT, STR_DEMO_CNT, STR_OFFER_CNT, STR_PRM_CNT, STR_TICKET_CNT, STR_NET_PRFT_DM_A, STR_NET_PRFT_DM_B, STR_NET_PRFT_DM_C, STR_NET_PRFT_DM_D, STR_NET_PRFT_DM_E, STR_RET_STR_ID, STR_RET_REASON_CNT, STR_RET_TICKET_NO, STR_RTRN_QTY, STR_RTRN_AMT, STR_RTRN_TX, STR_RTRN_AMT_INC_TX, STR_RET_FEE, STR_RTRN_SHIP_CST, STR_RFNDD_CSH, STR_REVERSED_CHRG, STR_STR_CREDIT, STR_RET_NET_LOSS, STR_RTRNED_YR_CNT, STR_RTRN_MM_CNT, STR_RET_ITM_CNT, STR_RET_CUST_CNT, STR_RET_AREA_CNT, STR_RET_OFFER_CNT, STR_RET_PRM_CNT, STR_RET_NET_LOSS_DM_A, STR_RET_NET_LOSS_DM_B, STR_RET_NET_LOSS_DM_C, STR_RET_NET_LOSS_DM_D, OL_ORD_QTY, OL_WSLE_CST, OL_LIST_PRICE, OL_SALES_PRICE, OL_EXT_DSCNT_AMT, OL_EXT_SALES_PRICE, OL_EXT_WSLE_CST, OL_EXT_LIST_PRICE, OL_EXT_TX, OL_COUPON_AMT, OL_EXT_SHIP_CST, OL_NET_PAID, OL_NET_PAID_INC_TX, OL_NET_PAID_INC_SHIP, OL_NET_PAID_INC_SHIP_TX, OL_NET_PRFT, OL_SOLD_YR_CNT, OL_SOLD_MM_CNT, OL_SHIP_DATE_CNT, OL_ITM_CNT, OL_BILL_CUST_CNT, OL_BILL_AREA_CNT, OL_BILL_DEMO_CNT, OL_BILL_OFFER_CNT, OL_SHIP_CUST_CNT, OL_SHIP_AREA_CNT, OL_SHIP_DEMO_CNT, OL_SHIP_OFFER_CNT, OL_WEB_PAGE_CNT, OL_WEB_SITE_CNT, OL_SHIP_MODE_CNT, OL_WH_CNT, OL_PRM_CNT, OL_NET_PRFT_DM_A, OL_NET_PRFT_DM_B, OL_NET_PRFT_DM_C, OL_NET_PRFT_DM_D, OL_RET_RTRN_QTY, OL_RTRN_AMT, OL_RTRN_TX, OL_RTRN_AMT_INC_TX, OL_RET_FEE, OL_RTRN_SHIP_CST, OL_RFNDD_CSH, OL_REVERSED_CHRG, OL_ACCOUNT_CREDIT, OL_RTRNED_YR_CNT, OL_RTRNED_MM_CNT, OL_RTRITM_CNT, OL_RFNDD_CUST_CNT, OL_RFNDD_AREA_CNT, OL_RFNDD_DEMO_CNT, OL_RFNDD_OFFER_CNT, OL_RTRNING_CUST_CNT, OL_RTRNING_AREA_CNT, OL_RTRNING_DEMO_CNT, OL_RTRNING_OFFER_CNT, OL_RTRWEB_PAGE_CNT, OL_REASON_CNT, OL_NET_LOSS, OL_NET_LOSS_DM_A, OL_NET_LOSS_DM_B, OL_NET_LOSS_DM_C','BAD_RECORDS_ACTION'='FORCE','BAD_RECORDS_LOGGER_ENABLE'='FALSE')""")
    sql(
      """create table oscon_carbon_old1 (CUST_PRFRD_FLG String,PROD_BRAND_NAME String,PROD_COLOR String,CUST_LAST_RVW_DATE String,CUST_COUNTRY String,CUST_CITY String,PRODUCT_NAME String,CUST_JOB_TITLE String,CUST_STATE String,CUST_BUY_POTENTIAL String,PRODUCT_MODEL String,ITM_ID String,ITM_NAME String,PRMTION_ID String,PRMTION_NAME String,SHP_MODE_ID String,SHP_MODE String,DELIVERY_COUNTRY String,DELIVERY_STATE String,DELIVERY_CITY String,DELIVERY_DISTRICT String,ACTIVE_EMUI_VERSION String,WH_NAME String,STR_ORDER_DATE String,OL_ORDER_NO String,OL_ORDER_DATE String,OL_SITE String,CUST_FIRST_NAME String,CUST_LAST_NAME String,CUST_BIRTH_DY String,CUST_BIRTH_MM String,CUST_BIRTH_YR String,CUST_BIRTH_COUNTRY String,CUST_SEX String,CUST_ADDRESS_ID String,CUST_STREET_NO String,CUST_STREET_NAME String,CUST_AGE String,CUST_SUITE_NO String,CUST_ZIP String,CUST_COUNTY String,PRODUCT_ID String,PROD_SHELL_COLOR String,DEVICE_NAME String,PROD_SHORT_DESC String,PROD_LONG_DESC String,PROD_THUMB String,PROD_IMAGE String,PROD_UPDATE_DATE String,PROD_LIVE String,PROD_LOC String,PROD_RAM String,PROD_ROM String,PROD_CPU_CLOCK String,PROD_SERIES String,ITM_REC_START_DATE String,ITM_REC_END_DATE String,ITM_BRAND_ID String,ITM_BRAND String,ITM_CLASS_ID String,ITM_CLASS String,ITM_CATEGORY_ID String,ITM_CATEGORY String,ITM_MANUFACT_ID String,ITM_MANUFACT String,ITM_FORMULATION String,ITM_COLOR String,ITM_CONTAINER String,ITM_MANAGER_ID String,PRM_START_DATE String,PRM_END_DATE String,PRM_CHANNEL_DMAIL String,PRM_CHANNEL_EMAIL String,PRM_CHANNEL_CAT String,PRM_CHANNEL_TV String,PRM_CHANNEL_RADIO String,PRM_CHANNEL_PRESS String,PRM_CHANNEL_EVENT String,PRM_CHANNEL_DEMO String,PRM_CHANNEL_DETAILS String,PRM_PURPOSE String,PRM_DSCNT_ACTIVE String,SHP_CODE String,SHP_CARRIER String,SHP_CONTRACT String,CHECK_DATE String,CHECK_YR String,CHECK_MM String,CHECK_DY String,CHECK_HOUR String,BOM String,INSIDE_NAME String,PACKING_DATE String,PACKING_YR String,PACKING_MM String,PACKING_DY String,PACKING_HOUR String,DELIVERY_PROVINCE String,PACKING_LIST_NO String,ACTIVE_CHECK_TIME String,ACTIVE_CHECK_YR String,ACTIVE_CHECK_MM String,ACTIVE_CHECK_DY String,ACTIVE_CHECK_HOUR String,ACTIVE_AREA_ID String,ACTIVE_COUNTRY String,ACTIVE_PROVINCE String,ACTIVE_CITY String,ACTIVE_DISTRICT String,ACTIVE_NETWORK String,ACTIVE_FIRMWARE_VER String,ACTIVE_OS_VERSION String,LATEST_CHECK_TIME String,LATEST_CHECK_YR String,LATEST_CHECK_MM String,LATEST_CHECK_DY String,LATEST_CHECK_HOUR String,LATEST_AREAID String,LATEST_COUNTRY String,LATEST_PROVINCE String,LATEST_CITY String,LATEST_DISTRICT String,LATEST_FIRMWARE_VER String,LATEST_EMUI_VERSION String,LATEST_OS_VERSION String,LATEST_NETWORK String,WH_ID String,WH_STREET_NO String,WH_STREET_NAME String,WH_STREET_TYPE String,WH_SUITE_NO String,WH_CITY String,WH_COUNTY String,WH_STATE String,WH_ZIP String,WH_COUNTRY String,OL_SITE_DESC String,OL_RET_ORDER_NO String,OL_RET_DATE String,PROD_MODEL_ID String,CUST_ID String,PROD_UNQ_MDL_ID String,CUST_NICK_NAME String,CUST_LOGIN String,CUST_EMAIL_ADDR String,PROD_UNQ_DEVICE_ADDR String,PROD_UQ_UUID String,PROD_BAR_CODE String,TRACKING_NO String,STR_ORDER_NO String,CUST_DEP_COUNT double,CUST_VEHICLE_COUNT double,CUST_ADDRESS_CNT double,CUST_CRNT_CDEMO_CNT double,CUST_CRNT_HDEMO_CNT double,CUST_CRNT_ADDR_DM double,CUST_FIRST_SHIPTO_CNT double,CUST_FIRST_SALES_CNT double,CUST_GMT_OFFSET double,CUST_DEMO_CNT double,CUST_INCOME double,PROD_UNLIMITED double,PROD_OFF_PRICE double,PROD_UNITS double,TOTAL_PRD_COST double,TOTAL_PRD_DISC double,PROD_WEIGHT double,REG_UNIT_PRICE double,EXTENDED_AMT double,UNIT_PRICE_DSCNT_PCT double,DSCNT_AMT double,PROD_STD_CST double,TOTAL_TX_AMT double,FREIGHT_CHRG double,WAITING_PERIOD double,DELIVERY_PERIOD double,ITM_CRNT_PRICE double,ITM_UNITS double,ITM_WSLE_CST double,ITM_SIZE double,PRM_CST double,PRM_RESPONSE_TARGET double,PRM_ITM_DM double,SHP_MODE_CNT double,WH_GMT_OFFSET double,WH_SQ_FT double,STR_ORD_QTY double,STR_WSLE_CST double,STR_LIST_PRICE double,STR_SALES_PRICE double,STR_EXT_DSCNT_AMT double,STR_EXT_SALES_PRICE double,STR_EXT_WSLE_CST double,STR_EXT_LIST_PRICE double,STR_EXT_TX double,STR_COUPON_AMT double,STR_NET_PAID double,STR_NET_PAID_INC_TX double,STR_NET_PRFT double,STR_SOLD_YR_CNT double,STR_SOLD_MM_CNT double,STR_SOLD_ITM_CNT double,STR_TOTAL_CUST_CNT double,STR_AREA_CNT double,STR_DEMO_CNT double,STR_OFFER_CNT double,STR_PRM_CNT double,STR_TICKET_CNT double,STR_NET_PRFT_DM_A double,STR_NET_PRFT_DM_B double,STR_NET_PRFT_DM_C double,STR_NET_PRFT_DM_D double,STR_NET_PRFT_DM_E double,STR_RET_STR_ID double,STR_RET_REASON_CNT double,STR_RET_TICKET_NO double,STR_RTRN_QTY double,STR_RTRN_AMT double,STR_RTRN_TX double,STR_RTRN_AMT_INC_TX double,STR_RET_FEE double,STR_RTRN_SHIP_CST double,STR_RFNDD_CSH double,STR_REVERSED_CHRG double,STR_STR_CREDIT double,STR_RET_NET_LOSS double,STR_RTRNED_YR_CNT double,STR_RTRN_MM_CNT double,STR_RET_ITM_CNT double,STR_RET_CUST_CNT double,STR_RET_AREA_CNT double,STR_RET_OFFER_CNT double,STR_RET_PRM_CNT double,STR_RET_NET_LOSS_DM_A double,STR_RET_NET_LOSS_DM_B double,STR_RET_NET_LOSS_DM_C double,STR_RET_NET_LOSS_DM_D double,OL_ORD_QTY double,OL_WSLE_CST double,OL_LIST_PRICE double,OL_SALES_PRICE double,OL_EXT_DSCNT_AMT double,OL_EXT_SALES_PRICE double,OL_EXT_WSLE_CST double,OL_EXT_LIST_PRICE double,OL_EXT_TX double,OL_COUPON_AMT double,OL_EXT_SHIP_CST double,OL_NET_PAID double,OL_NET_PAID_INC_TX double,OL_NET_PAID_INC_SHIP double,OL_NET_PAID_INC_SHIP_TX double,OL_NET_PRFT double,OL_SOLD_YR_CNT double,OL_SOLD_MM_CNT double,OL_SHIP_DATE_CNT double,OL_ITM_CNT double,OL_BILL_CUST_CNT double,OL_BILL_AREA_CNT double,OL_BILL_DEMO_CNT double,OL_BILL_OFFER_CNT double,OL_SHIP_CUST_CNT double,OL_SHIP_AREA_CNT double,OL_SHIP_DEMO_CNT double,OL_SHIP_OFFER_CNT double,OL_WEB_PAGE_CNT double,OL_WEB_SITE_CNT double,OL_SHIP_MODE_CNT double,OL_WH_CNT double,OL_PRM_CNT double,OL_NET_PRFT_DM_A double,OL_NET_PRFT_DM_B double,OL_NET_PRFT_DM_C double,OL_NET_PRFT_DM_D double,OL_RET_RTRN_QTY double,OL_RTRN_AMT double,OL_RTRN_TX double,OL_RTRN_AMT_INC_TX double,OL_RET_FEE double,OL_RTRN_SHIP_CST double,OL_RFNDD_CSH double,OL_REVERSED_CHRG double,OL_ACCOUNT_CREDIT double,OL_RTRNED_YR_CNT double,OL_RTRNED_MM_CNT double,OL_RTRITM_CNT double,OL_RFNDD_CUST_CNT double,OL_RFNDD_AREA_CNT double,OL_RFNDD_DEMO_CNT double,OL_RFNDD_OFFER_CNT double,OL_RTRNING_CUST_CNT double,OL_RTRNING_AREA_CNT double,OL_RTRNING_DEMO_CNT double,OL_RTRNING_OFFER_CNT double,OL_RTRWEB_PAGE_CNT double,OL_REASON_CNT double,OL_NET_LOSS double,OL_NET_LOSS_DM_A double,OL_NET_LOSS_DM_B double,OL_NET_LOSS_DM_C double) STORED AS carbondata TBLPROPERTIES ('table_blocksize'='256')""".stripMargin)
    sql(s"""load data LOCAL inpath '$resourcesPath/oscon_10.csv' into table oscon_carbon_old1 options('DELIMITER'=',', 'QUOTECHAR'='\"','FILEHEADER'='ACTIVE_AREA_ID, ACTIVE_CHECK_DY, ACTIVE_CHECK_HOUR, ACTIVE_CHECK_MM, ACTIVE_CHECK_TIME, ACTIVE_CHECK_YR, ACTIVE_CITY, ACTIVE_COUNTRY, ACTIVE_DISTRICT, ACTIVE_EMUI_VERSION, ACTIVE_FIRMWARE_VER, ACTIVE_NETWORK, ACTIVE_OS_VERSION, ACTIVE_PROVINCE, BOM, CHECK_DATE, CHECK_DY, CHECK_HOUR, CHECK_MM, CHECK_YR, CUST_ADDRESS_ID, CUST_AGE, CUST_BIRTH_COUNTRY, CUST_BIRTH_DY, CUST_BIRTH_MM, CUST_BIRTH_YR, CUST_BUY_POTENTIAL, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_COUNTY, CUST_EMAIL_ADDR, CUST_LAST_RVW_DATE, CUST_FIRST_NAME, CUST_ID, CUST_JOB_TITLE, CUST_LAST_NAME, CUST_LOGIN, CUST_NICK_NAME, CUST_PRFRD_FLG, CUST_SEX, CUST_STREET_NAME, CUST_STREET_NO, CUST_SUITE_NO, CUST_ZIP, DELIVERY_CITY, DELIVERY_STATE, DELIVERY_COUNTRY, DELIVERY_DISTRICT, DELIVERY_PROVINCE, DEVICE_NAME, INSIDE_NAME, ITM_BRAND, ITM_BRAND_ID, ITM_CATEGORY, ITM_CATEGORY_ID, ITM_CLASS, ITM_CLASS_ID, ITM_COLOR, ITM_CONTAINER, ITM_FORMULATION, ITM_MANAGER_ID, ITM_MANUFACT, ITM_MANUFACT_ID, ITM_ID, ITM_NAME, ITM_REC_END_DATE, ITM_REC_START_DATE, LATEST_AREAID, LATEST_CHECK_DY, LATEST_CHECK_HOUR, LATEST_CHECK_MM, LATEST_CHECK_TIME, LATEST_CHECK_YR, LATEST_CITY, LATEST_COUNTRY, LATEST_DISTRICT, LATEST_EMUI_VERSION, LATEST_FIRMWARE_VER, LATEST_NETWORK, LATEST_OS_VERSION, LATEST_PROVINCE, OL_ORDER_DATE, OL_ORDER_NO, OL_RET_ORDER_NO, OL_RET_DATE, OL_SITE, OL_SITE_DESC, PACKING_DATE, PACKING_DY, PACKING_HOUR, PACKING_LIST_NO, PACKING_MM, PACKING_YR, PRMTION_ID, PRMTION_NAME, PRM_CHANNEL_CAT, PRM_CHANNEL_DEMO, PRM_CHANNEL_DETAILS, PRM_CHANNEL_DMAIL, PRM_CHANNEL_EMAIL, PRM_CHANNEL_EVENT, PRM_CHANNEL_PRESS, PRM_CHANNEL_RADIO, PRM_CHANNEL_TV, PRM_DSCNT_ACTIVE, PRM_END_DATE, PRM_PURPOSE, PRM_START_DATE, PRODUCT_ID, PROD_BAR_CODE, PROD_BRAND_NAME, PRODUCT_NAME, PRODUCT_MODEL, PROD_MODEL_ID, PROD_COLOR, PROD_SHELL_COLOR, PROD_CPU_CLOCK, PROD_IMAGE, PROD_LIVE, PROD_LOC, PROD_LONG_DESC, PROD_RAM, PROD_ROM, PROD_SERIES, PROD_SHORT_DESC, PROD_THUMB, PROD_UNQ_DEVICE_ADDR, PROD_UNQ_MDL_ID, PROD_UPDATE_DATE, PROD_UQ_UUID, SHP_CARRIER, SHP_CODE, SHP_CONTRACT, SHP_MODE_ID, SHP_MODE, STR_ORDER_DATE, STR_ORDER_NO, TRACKING_NO, WH_CITY, WH_COUNTRY, WH_COUNTY, WH_ID, WH_NAME, WH_STATE, WH_STREET_NAME, WH_STREET_NO, WH_STREET_TYPE, WH_SUITE_NO, WH_ZIP, CUST_DEP_COUNT, CUST_VEHICLE_COUNT, CUST_ADDRESS_CNT, CUST_CRNT_CDEMO_CNT, CUST_CRNT_HDEMO_CNT, CUST_CRNT_ADDR_DM, CUST_FIRST_SHIPTO_CNT, CUST_FIRST_SALES_CNT, CUST_GMT_OFFSET, CUST_DEMO_CNT, CUST_INCOME, PROD_UNLIMITED, PROD_OFF_PRICE, PROD_UNITS, TOTAL_PRD_COST, TOTAL_PRD_DISC, PROD_WEIGHT, REG_UNIT_PRICE, EXTENDED_AMT, UNIT_PRICE_DSCNT_PCT, DSCNT_AMT, PROD_STD_CST, TOTAL_TX_AMT, FREIGHT_CHRG, WAITING_PERIOD, DELIVERY_PERIOD, ITM_CRNT_PRICE, ITM_UNITS, ITM_WSLE_CST, ITM_SIZE, PRM_CST, PRM_RESPONSE_TARGET, PRM_ITM_DM, SHP_MODE_CNT, WH_GMT_OFFSET, WH_SQ_FT, STR_ORD_QTY, STR_WSLE_CST, STR_LIST_PRICE, STR_SALES_PRICE, STR_EXT_DSCNT_AMT, STR_EXT_SALES_PRICE, STR_EXT_WSLE_CST, STR_EXT_LIST_PRICE, STR_EXT_TX, STR_COUPON_AMT, STR_NET_PAID, STR_NET_PAID_INC_TX, STR_NET_PRFT, STR_SOLD_YR_CNT, STR_SOLD_MM_CNT, STR_SOLD_ITM_CNT, STR_TOTAL_CUST_CNT, STR_AREA_CNT, STR_DEMO_CNT, STR_OFFER_CNT, STR_PRM_CNT, STR_TICKET_CNT, STR_NET_PRFT_DM_A, STR_NET_PRFT_DM_B, STR_NET_PRFT_DM_C, STR_NET_PRFT_DM_D, STR_NET_PRFT_DM_E, STR_RET_STR_ID, STR_RET_REASON_CNT, STR_RET_TICKET_NO, STR_RTRN_QTY, STR_RTRN_AMT, STR_RTRN_TX, STR_RTRN_AMT_INC_TX, STR_RET_FEE, STR_RTRN_SHIP_CST, STR_RFNDD_CSH, STR_REVERSED_CHRG, STR_STR_CREDIT, STR_RET_NET_LOSS, STR_RTRNED_YR_CNT, STR_RTRN_MM_CNT, STR_RET_ITM_CNT, STR_RET_CUST_CNT, STR_RET_AREA_CNT, STR_RET_OFFER_CNT, STR_RET_PRM_CNT, STR_RET_NET_LOSS_DM_A, STR_RET_NET_LOSS_DM_B, STR_RET_NET_LOSS_DM_C, STR_RET_NET_LOSS_DM_D, OL_ORD_QTY, OL_WSLE_CST, OL_LIST_PRICE, OL_SALES_PRICE, OL_EXT_DSCNT_AMT, OL_EXT_SALES_PRICE, OL_EXT_WSLE_CST, OL_EXT_LIST_PRICE, OL_EXT_TX, OL_COUPON_AMT, OL_EXT_SHIP_CST, OL_NET_PAID, OL_NET_PAID_INC_TX, OL_NET_PAID_INC_SHIP, OL_NET_PAID_INC_SHIP_TX, OL_NET_PRFT, OL_SOLD_YR_CNT, OL_SOLD_MM_CNT, OL_SHIP_DATE_CNT, OL_ITM_CNT, OL_BILL_CUST_CNT, OL_BILL_AREA_CNT, OL_BILL_DEMO_CNT, OL_BILL_OFFER_CNT, OL_SHIP_CUST_CNT, OL_SHIP_AREA_CNT, OL_SHIP_DEMO_CNT, OL_SHIP_OFFER_CNT, OL_WEB_PAGE_CNT, OL_WEB_SITE_CNT, OL_SHIP_MODE_CNT, OL_WH_CNT, OL_PRM_CNT, OL_NET_PRFT_DM_A, OL_NET_PRFT_DM_B, OL_NET_PRFT_DM_C, OL_NET_PRFT_DM_D, OL_RET_RTRN_QTY, OL_RTRN_AMT, OL_RTRN_TX, OL_RTRN_AMT_INC_TX, OL_RET_FEE, OL_RTRN_SHIP_CST, OL_RFNDD_CSH, OL_REVERSED_CHRG, OL_ACCOUNT_CREDIT, OL_RTRNED_YR_CNT, OL_RTRNED_MM_CNT, OL_RTRITM_CNT, OL_RFNDD_CUST_CNT, OL_RFNDD_AREA_CNT, OL_RFNDD_DEMO_CNT, OL_RFNDD_OFFER_CNT, OL_RTRNING_CUST_CNT, OL_RTRNING_AREA_CNT, OL_RTRNING_DEMO_CNT, OL_RTRNING_OFFER_CNT, OL_RTRWEB_PAGE_CNT, OL_REASON_CNT, OL_NET_LOSS, OL_NET_LOSS_DM_A, OL_NET_LOSS_DM_B, OL_NET_LOSS_DM_C','BAD_RECORDS_ACTION'='FORCE','BAD_RECORDS_LOGGER_ENABLE'='FALSE')""")
  }

  test("test to check result for double data type") {
    val result = sql("select OL_SALES_PRICE from oscon_carbon_old limit 10").count()
    assert(result.equals(10L))
  }
  test("test to check result for double data type as dimension") {
    val result = sql("select CUST_DEP_COUNT from oscon_carbon_old1 limit 10").count()
    assert(result.equals(10L))
  }

  test("Double Datatype Check with AdaptiveDeltaFloating Codec (BYTE)") {

    sql(
      """create table carbon_datatype_double_byte (c_double double) STORED AS carbondata""".stripMargin)
    sql(s"load data inpath '$resourcesPath/double/data_notitle_byte.csv' into table " +
        s"carbon_datatype_double_byte options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_double_byte"),
      Seq(Row(3.1412), Row(3.1413), Row(3.1414), Row(3.1415)))
  }

  test("Double Datatype Check with AdaptiveDeltaFloating Codec (Short)") {

    sql(
      """create table carbon_datatype_double_short (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_short.csv' into table " +
        s"carbon_datatype_double_short options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_double_short"),
      Seq(Row(3.1412), Row(3.1413), Row(3.1414), Row(5.1415)))
  }

  test("Double Datatype Check with AdaptiveDeltaFloating Codec (Short_Int)") {

    sql(
      """create table carbon_datatype_double_sint (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_short_int.csv' into table " +
        s"carbon_datatype_double_sint options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_double_sint"),
      Seq(Row(300.1412), Row(300.1413), Row(300.1414), Row(900.1415)))
  }

  test("Double Datatype Check with AdaptiveDeltaFloating Codec (Int)") {

    sql(
      """create table carbon_datatype_double_int (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_int.csv' into table " +
        s"carbon_datatype_double_int options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_double_int"),
      Seq(Row(199161.2812), Row(199161.2813), Row(199161.2814), Row(200000.1415)))
  }

  test("Double Datatype Check with AdaptiveDeltaFloating Codec (Long)") {

    sql(
      """create table carbon_datatype_double_long (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_long.csv' into table " +
        s"carbon_datatype_double_long options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_double_long"),
      Seq(Row(100000.1412), Row(100000.1413), Row(100000.1414), Row(200000.1415)))
  }

  test("Double Datatype Check with AdaptiveFloatingCodec Codec (BYTE)") {

    sql(
      """create table carbon_datatype_AdaptiveFloatingCodec_double_byte (c_double double) STORED AS carbondata""".stripMargin)
    sql(s"load data inpath '$resourcesPath/double/data_notitle_AdaptiveFloating_byte.csv' into table " +
        s"carbon_datatype_AdaptiveFloatingCodec_double_byte options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_AdaptiveFloatingCodec_double_byte"),
      Seq(Row(0.0012), Row(0.0013), Row(0.0014), Row(0.0015)))
  }

  test("Double Datatype Check with AdaptiveFloatingCodec Codec (Short)") {

    sql(
      """create table carbon_datatype_AdaptiveFloatingCodec_double_short (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_AdaptiveFloating_short.csv' into table " +
        s"carbon_datatype_AdaptiveFloatingCodec_double_short options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_AdaptiveFloatingCodec_double_short"),
      Seq(Row(0.1412), Row(0.1413), Row(0.1414), Row(0.1415)))
  }

  test("Double Datatype Check with AdaptiveFloatingCodec Codec (Short_Int)") {

    sql(
      """create table carbon_datatype_AdaptiveFloatingCodec_double_sint (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_AdaptiveFloating_short_int.csv' into table " +
        s"carbon_datatype_AdaptiveFloatingCodec_double_sint options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_AdaptiveFloatingCodec_double_sint"),
      Seq(Row(300.1412), Row(300.1413), Row(300.1414), Row(800.1415)))
  }

  test("Double Datatype Check with AdaptiveFloatingCodec Codec (Int)") {

    sql(
      """create table carbon_datatype_AdaptiveFloatingCodec_double_int (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_AdaptiveFloating_int.csv' into table " +
        s"carbon_datatype_AdaptiveFloatingCodec_double_int options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_AdaptiveFloatingCodec_double_int"),
      Seq(Row(100000.2812), Row(100000.2813), Row(100000.2814), Row(200000.1415)))
  }

  test("Double Datatype Check with AdaptiveFloatingCodec Codec (Long)") {

    sql(
      """create table carbon_datatype_AdaptiveFloatingCodec_double_long (c_double double) STORED AS carbondata""")
    sql(s"load data inpath '$resourcesPath/double/data_notitle_long.csv' into table " +
        s"carbon_datatype_AdaptiveFloatingCodec_double_long options('delimiter'='|', 'fileheader'='id, c_string, " +
        s"c_bigint, c_decimal, c_double, c_timestamp')")
    checkAnswer(sql("select * from carbon_datatype_AdaptiveFloatingCodec_double_long"),
      Seq(Row(100000.1412), Row(100000.1413), Row(100000.1414), Row(200000.1415)))
  }


  override def afterAll: Unit = {
    sql("drop table if exists carbon_datatype_double_byte")
    sql("drop table if exists carbon_datatype_double_short")
    sql("drop table if exists carbon_datatype_double_sint")
    sql("drop table if exists carbon_datatype_double_int")
    sql("drop table if exists carbon_datatype_double_long")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_byte")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_short")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_sint")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_int")
    sql("drop table if exists carbon_datatype_AdaptiveFloatingCodec_double_long")
    sql("DROP TABLE IF EXISTS oscon_carbon_old")
  }

}

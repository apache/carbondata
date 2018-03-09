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

package org.apache.carbondata.mv.testutil

object Tpcds_1_4_Tables {
  val tpcds1_4Tables = Seq[String](
    s"""
       |CREATE TABLE catalog_sales (
       |  `cs_sold_date_sk` int,
       |  `cs_sold_time_sk` int,
       |  `cs_ship_date_sk` int,
       |  `cs_bill_customer_sk` int,
       |  `cs_bill_cdemo_sk` int,
       |  `cs_bill_hdemo_sk` int,
       |  `cs_bill_addr_sk` int,
       |  `cs_ship_customer_sk` int,
       |  `cs_ship_cdemo_sk` int,
       |  `cs_ship_hdemo_sk` int,
       |  `cs_ship_addr_sk` int,
       |  `cs_call_center_sk` int,
       |  `cs_catalog_page_sk` int,
       |  `cs_ship_mode_sk` int,
       |  `cs_warehouse_sk` int,
       |  `cs_item_sk` int,
       |  `cs_promo_sk` int,
       |  `cs_order_number` bigint,
       |  `cs_quantity` int,
       |  `cs_wholesale_cost` decimal(7,2),
       |  `cs_list_price` decimal(7,2),
       |  `cs_sales_price` decimal(7,2),
       |  `cs_ext_discount_amt` decimal(7,2),
       |  `cs_ext_sales_price` decimal(7,2),
       |  `cs_ext_wholesale_cost` decimal(7,2),
       |  `cs_ext_list_price` decimal(7,2),
       |  `cs_ext_tax` decimal(7,2),
       |  `cs_coupon_amt` decimal(7,2),
       |  `cs_ext_ship_cost` decimal(7,2),
       |  `cs_net_paid` decimal(7,2),
       |  `cs_net_paid_inc_tax` decimal(7,2),
       |  `cs_net_paid_inc_ship` decimal(7,2),
       |  `cs_net_paid_inc_ship_tax` decimal(7,2),
       |  `cs_net_profit` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE catalog_returns (
       |  `cr_returned_date_sk` int,
       |  `cr_returned_time_sk` int,
       |  `cr_item_sk` int,
       |  `cr_refunded_customer_sk` int,
       |  `cr_refunded_cdemo_sk` int,
       |  `cr_refunded_hdemo_sk` int,
       |  `cr_refunded_addr_sk` int,
       |  `cr_returning_customer_sk` int,
       |  `cr_returning_cdemo_sk` int,
       |  `cr_returning_hdemo_sk` int,
       |  `cr_returning_addr_sk` int,
       |  `cr_call_center_sk` int,
       |  `cr_catalog_page_sk` int,
       |  `cr_ship_mode_sk` int,
       |  `cr_warehouse_sk` int,
       |  `cr_reason_sk` int,
       |  `cr_order_number` bigint,
       |  `cr_return_quantity` int,
       |  `cr_return_amount` decimal(7,2),
       |  `cr_return_tax` decimal(7,2),
       |  `cr_return_amt_inc_tax` decimal(7,2),
       |  `cr_fee` decimal(7,2),
       |  `cr_return_ship_cost` decimal(7,2),
       |  `cr_refunded_cash` decimal(7,2),
       |  `cr_reversed_charge` decimal(7,2),
       |  `cr_store_credit` decimal(7,2),
       |  `cr_net_loss` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE inventory (
       |  `inv_date_sk` int,
       |  `inv_item_sk` int,
       |  `inv_warehouse_sk` int,
       |  `inv_quantity_on_hand` int
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE store_sales (
       |  `ss_sold_date_sk` int,
       |  `ss_sold_time_sk` int,
       |  `ss_item_sk` int,
       |  `ss_customer_sk` int,
       |  `ss_cdemo_sk` int,
       |  `ss_hdemo_sk` int,
       |  `ss_addr_sk` int,
       |  `ss_store_sk` int,
       |  `ss_promo_sk` int,
       |  `ss_ticket_number` bigint,
       |  `ss_quantity` int,
       |  `ss_wholesale_cost` decimal(7,2),
       |  `ss_list_price` decimal(7,2),
       |  `ss_sales_price` decimal(7,2),
       |  `ss_ext_discount_amt` decimal(7,2),
       |  `ss_ext_sales_price` decimal(7,2),
       |  `ss_ext_wholesale_cost` decimal(7,2),
       |  `ss_ext_list_price` decimal(7,2),
       |  `ss_ext_tax` decimal(7,2),
       |  `ss_coupon_amt` decimal(7,2),
       |  `ss_net_paid` decimal(7,2),
       |  `ss_net_paid_inc_tax` decimal(7,2),
       |  `ss_net_profit` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE store_returns (
       |  `sr_returned_date_sk` int,
       |  `sr_return_time_sk` int,
       |  `sr_item_sk` int,
       |  `sr_customer_sk` int,
       |  `sr_cdemo_sk` int,
       |  `sr_hdemo_sk` int,
       |  `sr_addr_sk` int,
       |  `sr_store_sk` int,
       |  `sr_reason_sk` int,
       |  `sr_ticket_number` bigint,
       |  `sr_return_quantity` int,
       |  `sr_return_amt` decimal(7,2),
       |  `sr_return_tax` decimal(7,2),
       |  `sr_return_amt_inc_tax` decimal(7,2),
       |  `sr_fee` decimal(7,2),
       |  `sr_return_ship_cost` decimal(7,2),
       |  `sr_refunded_cash` decimal(7,2),
       |  `sr_reversed_charge` decimal(7,2),
       |  `sr_store_credit` decimal(7,2),
       |  `sr_net_loss` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE web_sales (
       |  `ws_sold_date_sk` int,
       |  `ws_sold_time_sk` int,
       |  `ws_ship_date_sk` int,
       |  `ws_item_sk` int,
       |  `ws_bill_customer_sk` int,
       |  `ws_bill_cdemo_sk` int,
       |  `ws_bill_hdemo_sk` int,
       |  `ws_bill_addr_sk` int,
       |  `ws_ship_customer_sk` int,
       |  `ws_ship_cdemo_sk` int,
       |  `ws_ship_hdemo_sk` int,
       |  `ws_ship_addr_sk` int,
       |  `ws_web_page_sk` int,
       |  `ws_web_site_sk` int,
       |  `ws_ship_mode_sk` int,
       |  `ws_warehouse_sk` int,
       |  `ws_promo_sk` int,
       |  `ws_order_number` bigint,
       |  `ws_quantity` int,
       |  `ws_wholesale_cost` decimal(7,2),
       |  `ws_list_price` decimal(7,2),
       |  `ws_sales_price` decimal(7,2),
       |  `ws_ext_discount_amt` decimal(7,2),
       |  `ws_ext_sales_price` decimal(7,2),
       |  `ws_ext_wholesale_cost` decimal(7,2),
       |  `ws_ext_list_price` decimal(7,2),
       |  `ws_ext_tax` decimal(7,2),
       |  `ws_coupon_amt` decimal(7,2),
       |  `ws_ext_ship_cost` decimal(7,2),
       |  `ws_net_paid` decimal(7,2),
       |  `ws_net_paid_inc_tax` decimal(7,2),
       |  `ws_net_paid_inc_ship` decimal(7,2),
       |  `ws_net_paid_inc_ship_tax` decimal(7,2),
       |  `ws_net_profit` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE web_returns (
       |  `wr_returned_date_sk` int,
       |  `wr_returned_time_sk` int,
       |  `wr_item_sk` int,
       |  `wr_refunded_customer_sk` int,
       |  `wr_refunded_cdemo_sk` int,
       |  `wr_refunded_hdemo_sk` int,
       |  `wr_refunded_addr_sk` int,
       |  `wr_returning_customer_sk` int,
       |  `wr_returning_cdemo_sk` int,
       |  `wr_returning_hdemo_sk` int,
       |  `wr_returning_addr_sk` int,
       |  `wr_web_page_sk` int,
       |  `wr_reason_sk` int,
       |  `wr_order_number` bigint,
       |  `wr_return_quantity` int,
       |  `wr_return_amt` decimal(7,2),
       |  `wr_return_tax` decimal(7,2),
       |  `wr_return_amt_inc_tax` decimal(7,2),
       |  `wr_fee` decimal(7,2),
       |  `wr_return_ship_cost` decimal(7,2),
       |  `wr_refunded_cash` decimal(7,2),
       |  `wr_reversed_charge` decimal(7,2),
       |  `wr_account_credit` decimal(7,2),
       |  `wr_net_loss` decimal(7,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE call_center (
       |  `cc_call_center_sk` int,
       |  `cc_call_center_id` string,
       |  `cc_rec_start_date` date,
       |  `cc_rec_end_date` date,
       |  `cc_closed_date_sk` int,
       |  `cc_open_date_sk` int,
       |  `cc_name` string,
       |  `cc_class` string,
       |  `cc_employees` int,
       |  `cc_sq_ft` int,
       |  `cc_hours` string,
       |  `cc_manager` string,
       |  `cc_mkt_id` int,
       |  `cc_mkt_class` string,
       |  `cc_mkt_desc` string,
       |  `cc_market_manager` string,
       |  `cc_division` int,
       |  `cc_division_name` string,
       |  `cc_company` int,
       |  `cc_company_name` string,
       |  `cc_street_number` string,
       |  `cc_street_name` string,
       |  `cc_street_type` string,
       |  `cc_suite_number` string,
       |  `cc_city` string,
       |  `cc_county` string,
       |  `cc_state` string,
       |  `cc_zip` string,
       |  `cc_country` string,
       |  `cc_gmt_offset` decimal(5,2),
       |  `cc_tax_percentage` decimal(5,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE catalog_page (
       |  `cp_catalog_page_sk` int,
       |  `cp_catalog_page_id` string,
       |  `cp_start_date_sk` int,
       |  `cp_end_date_sk` int,
       |  `cp_department` string,
       |  `cp_catalog_number` int,
       |  `cp_catalog_page_number` int,
       |  `cp_description` string,
       |  `cp_type` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE customer (
       |  `c_customer_sk` int,
       |  `c_customer_id` string,
       |  `c_current_cdemo_sk` int,
       |  `c_current_hdemo_sk` int,
       |  `c_current_addr_sk` int,
       |  `c_first_shipto_date_sk` int,
       |  `c_first_sales_date_sk` int,
       |  `c_salutation` string,
       |  `c_first_name` string,
       |  `c_last_name` string,
       |  `c_preferred_cust_flag` string,
       |  `c_birth_day` int,
       |  `c_birth_month` int,
       |  `c_birth_year` int,
       |  `c_birth_country` string,
       |  `c_login` string,
       |  `c_email_address` string,
       |  `c_last_review_date` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE customer_address (
       |  `ca_address_sk` int,
       |  `ca_address_id` string,
       |  `ca_street_number` string,
       |  `ca_street_name` string,
       |  `ca_street_type` string,
       |  `ca_suite_number` string,
       |  `ca_city` string,
       |  `ca_county` string,
       |  `ca_state` string,
       |  `ca_zip` string,
       |  `ca_country` string,
       |  `ca_gmt_offset` decimal(5,2),
       |  `ca_location_type` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE customer_demographics (
       |  `cd_demo_sk` int,
       |  `cd_gender` string,
       |  `cd_marital_status` string,
       |  `cd_education_status` string,
       |  `cd_purchase_estimate` int,
       |  `cd_credit_rating` string,
       |  `cd_dep_count` int,
       |  `cd_dep_employed_count` int,
       |  `cd_dep_college_count` int
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE date_dim (
       |  `d_date_sk` int,
       |  `d_date_id` string,
       |  `d_date` date,
       |  `d_month_seq` int,
       |  `d_week_seq` int,
       |  `d_quarter_seq` int,
       |  `d_year` int,
       |  `d_dow` int,
       |  `d_moy` int,
       |  `d_dom` int,
       |  `d_qoy` int,
       |  `d_fy_year` int,
       |  `d_fy_quarter_seq` int,
       |  `d_fy_week_seq` int,
       |  `d_day_name` string,
       |  `d_quarter_name` string,
       |  `d_holiday` string,
       |  `d_weekend` string,
       |  `d_following_holiday` string,
       |  `d_first_dom` int,
       |  `d_last_dom` int,
       |  `d_same_day_ly` int,
       |  `d_same_day_lq` int,
       |  `d_current_day` string,
       |  `d_current_week` string,
       |  `d_current_month` string,
       |  `d_current_quarter` string,
       |  `d_current_year` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE household_demographics (
       |  `hd_demo_sk` int,
       |  `hd_income_band_sk` int,
       |  `hd_buy_potential` string,
       |  `hd_dep_count` int,
       |  `hd_vehicle_count` int
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE income_band (
       |  `ib_income_band_sk` int,
       |  `ib_lower_bound` int,
       |  `ib_upper_bound` int
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE item (
       |  `i_item_sk` int,
       |  `i_item_id` string,
       |  `i_rec_start_date` date,
       |  `i_rec_end_date` date,
       |  `i_item_desc` string,
       |  `i_current_price` decimal(7,2),
       |  `i_wholesale_cost` decimal(7,2),
       |  `i_brand_id` int,
       |  `i_brand` string,
       |  `i_class_id` int,
       |  `i_class` string,
       |  `i_category_id` int,
       |  `i_category` string,
       |  `i_manufact_id` int,
       |  `i_manufact` string,
       |  `i_size` string,
       |  `i_formulation` string,
       |  `i_color` string,
       |  `i_units` string,
       |  `i_container` string,
       |  `i_manager_id` int,
       |  `i_product_name` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE promotion (
       |  `p_promo_sk` int,
       |  `p_promo_id` string,
       |  `p_start_date_sk` int,
       |  `p_end_date_sk` int,
       |  `p_item_sk` int,
       |  `p_cost` decimal(15,2),
       |  `p_response_target` int,
       |  `p_promo_name` string,
       |  `p_channel_dmail` string,
       |  `p_channel_email` string,
       |  `p_channel_catalog` string,
       |  `p_channel_tv` string,
       |  `p_channel_radio` string,
       |  `p_channel_press` string,
       |  `p_channel_event` string,
       |  `p_channel_demo` string,
       |  `p_channel_details` string,
       |  `p_purpose` string,
       |  `p_discount_active` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE reason (
       |  `r_reason_sk` int,
       |  `r_reason_id` string,
       |  `r_reason_desc` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE ship_mode (
       |  `sm_ship_mode_sk` int,
       |  `sm_ship_mode_id` string,
       |  `sm_type` string,
       |  `sm_code` string,
       |  `sm_carrier` string,
       |  `sm_contract` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE store (
       |  `s_store_sk` int,
       |  `s_store_id` string,
       |  `s_rec_start_date` date,
       |  `s_rec_end_date` date,
       |  `s_closed_date_sk` int,
       |  `s_store_name` string,
       |  `s_number_employees` int,
       |  `s_floor_space` int,
       |  `s_hours` string,
       |  `s_manager` string,
       |  `s_market_id` int,
       |  `s_geography_class` string,
       |  `s_market_desc` string,
       |  `s_market_manager` string,
       |  `s_division_id` int,
       |  `s_division_name` string,
       |  `s_company_id` int,
       |  `s_company_name` string,
       |  `s_street_number` string,
       |  `s_street_name` string,
       |  `s_street_type` string,
       |  `s_suite_number` string,
       |  `s_city` string,
       |  `s_county` string,
       |  `s_state` string,
       |  `s_zip` string,
       |  `s_country ` string,
       |  `s_gmt_offset` decimal(5,2),
       |  `s_tax_precentage` decimal(5,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE time_dim (
       |  `t_time_sk` int,
       |  `t_time_id` string,
       |  `t_time` int,
       |  `t_hour` int,
       |  `t_minute` int,
       |  `t_second` int,
       |  `t_am_pm` string,
       |  `t_shift` string,
       |  `t_sub_shift` string,
       |  `t_meal_time` string
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE warehouse (
       |  `w_warehouse_sk` int,
       |  `w_warehouse_id` string,
       |  `w_warehouse_name` string,
       |  `w_warehouse_sq_ft` int,
       |  `w_street_number` string,
       |  `w_street_name` string,
       |  `w_street_type` string,
       |  `w_suite_number` string,
       |  `w_city` string,
       |  `w_county` string,
       |  `w_state` string,
       |  `w_zip` string,
       |  `w_country` string,
       |  `w_gmt_offset` decimal(5,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE web_page (
       |  `wp_web_page_sk` int,
       |  `wp_web_page_id` string,
       |  `wp_rec_start_date` date,
       |  `wp_rec_end_date` date,
       |  `wp_creation_date_sk` int,
       |  `wp_access_date_sk` int,
       |  `wp_autogen_flag` string,
       |  `wp_customer_sk` int,
       |  `wp_url` string,
       |  `wp_type` string,
       |  `wp_char_count` int,
       |  `wp_link_count` int,
       |  `wp_image_count` int,
       |  `wp_max_ad_count` int
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE web_site (
       |  `web_site_sk` int,
       |  `web_site_id` string,
       |  `web_rec_start_date` date,
       |  `web_rec_end_date` date,
       |  `web_name` string,
       |  `web_open_date_sk` int,
       |  `web_close_date_sk` int,
       |  `web_class` string,
       |  `web_manager` string,
       |  `web_mkt_id` int,
       |  `web_mkt_class` string,
       |  `web_mkt_desc` string,
       |  `web_market_manager` string,
       |  `web_company_id` int,
       |  `web_company_name` string,
       |  `web_street_number` string,
       |  `web_street_name` string,
       |  `web_street_type` string,
       |  `web_suite_number` string,
       |  `web_city` string,
       |  `web_county` string,
       |  `web_state` string,
       |  `web_zip` string,
       |  `web_country` string,
       |  `web_gmt_offset` decimal(5,2),
       |  `web_tax_percentage` decimal(5,2)
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
       |STORED AS TEXTFILE
      """.stripMargin.trim,
    s"""
       |CREATE TABLE sdr_dyn_seq_custer_iot_all_hour_60min
       |(
       |    `dim_1`       String,
       |    `dim_51`      String,
       |    `starttime`   String,
       |    `dim_2`       String,
       |    `dim_3`       String,
       |    `dim_4`       String,
       |    `dim_5`       String,
       |    `dim_6`       String,
       |    `dim_7`       String,
       |    `dim_8`       String,
       |    `dim_9`       String,
       |    `dim_10`      String,
       |    `dim_11`      String,
       |    `dim_12`      String,
       |    `dim_13`      String,
       |    `dim_14`      String,
       |    `dim_15`      String,
       |    `dim_16`      String,
       |    `dim_17`      String,
       |    `dim_18`      String,
       |    `dim_19`      String,
       |    `dim_20`      String,
       |    `dim_21`      String,
       |    `dim_22`      String,
       |    `dim_23`      String,
       |    `dim_24`      String,
       |    `dim_25`      String,
       |    `dim_26`      String,
       |    `dim_27`      String,
       |    `dim_28`      String,
       |    `dim_29`      String,
       |    `dim_30`      String,
       |    `dim_31`      String,
       |    `dim_32`      String,
       |    `dim_33`      String,
       |    `dim_34`      String,
       |    `dim_35`      String,
       |    `dim_36`      String,
       |    `dim_37`      String,
       |    `dim_38`      String,
       |    `dim_39`      String,
       |    `dim_40`      String,
       |    `dim_41`      String,
       |    `dim_42`      String,
       |    `dim_43`      String,
       |    `dim_44`      String,
       |    `dim_45`      String,
       |    `dim_46`      String,
       |    `dim_47`      String,
       |    `dim_48`      String,
       |    `dim_49`      String,
       |    `dim_50`      String,
       |    `dim_52`      String,
       |    `dim_53`      String,
       |    `dim_54`      String,
       |    `dim_55`      String,
       |    `dim_56`      String,
       |    `dim_57`      String,
       |    `dim_58`      String,
       |    `dim_59`      String,
       |    `dim_60`      String,
       |    `dim_61`      String,
       |    `dim_62`      String,
       |    `dim_63`      String,
       |    `dim_64`      String,
       |    `dim_65`      String,
       |    `dim_66`      String,
       |    `dim_67`      String,
       |    `dim_68`      String,
       |    `dim_69`      String,
       |    `dim_70`      String,
       |    `dim_71`      String,
       |    `dim_72`      String,
       |    `dim_73`      String,
       |    `dim_74`      String,
       |    `dim_75`      String,
       |    `dim_76`      String,
       |    `dim_77`      String,
       |    `dim_78`      String,
       |    `dim_79`      String,
       |    `dim_80`      String,
       |    `dim_81`      String,
       |    `dim_82`      String,
       |    `dim_83`      String,
       |    `dim_84`      String,
       |    `dim_85`      String,
       |    `dim_86`      String,
       |    `dim_87`      String,
       |    `dim_88`      String,
       |    `dim_89`      String,
       |    `dim_90`      String,
       |    `dim_91`      String,
       |    `dim_92`      String,
       |    `dim_93`      String,
       |    `dim_94`      String,
       |    `dim_95`      String,
       |    `dim_96`      String,
       |    `dim_97`      String,
       |    `dim_98`      String,
       |    `dim_99`      String,
       |    `dim_100`     String,
       |    `counter_1`   double,
       |    `counter_2`   double,
       |    `counter_3`   double,
       |    `counter_4`   double,
       |    `counter_5`   double,
       |    `counter_6`   double,
       |    `counter_7`   double,
       |    `counter_8`   double,
       |    `counter_9`   double,
       |    `counter_10`  double,
       |    `counter_11`  double,
       |    `counter_12`  double,
       |    `counter_13`  double,
       |    `counter_14`  double,
       |    `counter_15`  double,
       |    `counter_16`  double,
       |    `counter_17`  double,
       |    `counter_18`  double,
       |    `counter_19`  double,
       |    `counter_20`  double,
       |    `counter_21`  double,
       |    `counter_22`  double,
       |    `counter_23`  double,
       |    `counter_24`  double,
       |    `counter_25`  double,
       |    `counter_26`  double,
       |    `counter_27`  double,
       |    `counter_28`  double,
       |    `counter_29`  double,
       |    `counter_30`  double,
       |    `counter_31`  double,
       |    `counter_32`  double,
       |    `counter_33`  double,
       |    `counter_34`  double,
       |    `counter_35`  double,
       |    `counter_36`  double,
       |    `counter_37`  double,
       |    `counter_38`  double,
       |    `counter_39`  double,
       |    `counter_40`  double,
       |    `counter_41`  double,
       |    `counter_42`  double,
       |    `counter_43`  double,
       |    `counter_44`  double,
       |    `counter_45`  double,
       |    `counter_46`  double,
       |    `counter_47`  double,
       |    `counter_48`  double,
       |    `counter_49`  double,
       |    `counter_50`  double,
       |    `counter_51`  double,
       |    `counter_52`  double,
       |    `counter_53`  double,
       |    `counter_54`  double,
       |    `counter_55`  double,
       |    `counter_56`  double,
       |    `counter_57`  double,
       |    `counter_58`  double,
       |    `counter_59`  double,
       |    `counter_60`  double,
       |    `counter_61`  double,
       |    `counter_62`  double,
       |    `counter_63`  double,
       |    `counter_64`  double,
       |    `counter_65`  double,
       |    `counter_66`  double,
       |    `counter_67`  double,
       |    `counter_68`  double,
       |    `counter_69`  double,
       |    `counter_70`  double,
       |    `counter_71`  double,
       |    `counter_72`  double,
       |    `counter_73`  double,
       |    `counter_74`  double,
       |    `counter_75`  double,
       |    `counter_76`  double,
       |    `counter_77`  double,
       |    `counter_78`  double,
       |    `counter_79`  double,
       |    `counter_80`  double,
       |    `counter_81`  double,
       |    `counter_82`  double,
       |    `counter_83`  double,
       |    `counter_84`  double,
       |    `counter_85`  double,
       |    `counter_86`  double,
       |    `counter_87`  double,
       |    `counter_88`  double,
       |    `counter_89`  double,
       |    `counter_90`  double,
       |    `counter_91`  double,
       |    `counter_92`  double,
       |    `counter_93`  double,
       |    `counter_94`  double,
       |    `counter_95`  double,
       |    `counter_96`  double,
       |    `counter_97`  double,
       |    `counter_98`  double,
       |    `counter_99`  double,
       |    `counter_100` double,
       |    `batchno`     double
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
       |STORED AS TEXTFILE
          """.stripMargin.trim,
    s"""
       |CREATE TABLE dim_apn_iot
       |(
       |    `city_ascription`     String,
       |    `industry`            String,
       |    `apn_name`            String,
       |    `service_level`       String,
       |    `customer_name`       String,
       |    `id`                  bigint
       |)
       |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
       |STORED AS TEXTFILE
          """.stripMargin.trim,
    s"""
       |CREATE TABLE tradeflow_all (
       | m_month      smallint,
       | hs_code      string  ,
       | country      smallint,
       | dollar_value double  ,
       | quantity     double  ,
       | unit         smallint,
       | b_country    smallint,
       | imex         smallint,
       | y_year       smallint)
       |STORED AS parquet
          """.stripMargin.trim,
    s"""
       |CREATE TABLE country (
       | countryid   smallint ,
       | country_en  string   ,
       | country_cn  string   )
       |STORED AS parquet
          """.stripMargin.trim,
    s"""
       |CREATE TABLE updatetime (
       | countryid     smallint ,
       | imex          smallint ,
       | hs_len        smallint ,
       | minstartdate  string   ,
       | startdate     string   ,
       | newdate       string   ,
       | minnewdate    string   )
       |STORED AS parquet
          """.stripMargin.trim
  )
}

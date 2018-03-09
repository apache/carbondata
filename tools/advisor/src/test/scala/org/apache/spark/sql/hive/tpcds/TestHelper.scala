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

package org.apache.spark.sql.hive.tpcds

import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.spark.sql.internal.SQLConf

import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor

object TestCommonSubexpressionManager extends TestCommonSubexpressionManager 

class TestCommonSubexpressionManager extends CommonSubexpressionManager (
    TestHive.sparkSession,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false, 
                       SQLConf.CBO_ENABLED -> true, 
                       SQLConf.buildConf("spark.mv.recommend.speedup.threshold").doubleConf.createWithDefault(0.5) -> 0.5,
                       SQLConf.buildConf("spark.mv.recommend.rowcount.threshold").doubleConf.createWithDefault(0.1) -> 0.1,
                       SQLConf.buildConf("spark.mv.recommend.frequency.threshold").doubleConf.createWithDefault(2) -> 2,
                       SQLConf.buildConf("spark.mv.tableCluster").stringConf.createWithDefault(s"""""") -> s"""{"fact":["default.store_returns","default.catalog_sales","default.web_sales","default.store_sales","default.sdr_dyn_seq_custer_iot_all_hour_60min","default.tradeflow_all"],"dimension":["default.time_dim","default.inventory","default.web_page","default.customer_demographics","default.web_site","default.ship_mode","default.store","default.customer_address","default.reason","default.catalog_page","default.promotion","default.customer","default.catalog_returns","default.call_center","default.web_returns","default.household_demographics","default.date_dim","default.income_band","default.warehouse","default.item","default.dim_apn_iot","default.country","default.updatetime"]}""")) {
}

object TestQueryBatchPreprocessor extends QueryBatchPreprocessor
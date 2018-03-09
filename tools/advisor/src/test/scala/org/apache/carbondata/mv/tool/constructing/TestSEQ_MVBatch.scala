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

package org.apache.carbondata.mv.tool.constructing

object TestSEQ_MVBatch {
  val seq_testCases = Seq(
         ("case_1",
          Seq(       
              """
               |SELECT AT.a3600 AS START_TIME
               |	,SUM(CUSTER_IOT_GRP_USER_NUMBER_M) AS USER_NUMBER
               |	,AT.a12575873557 AS CITY_ASCRIPTION
               |	,AT.a12575847251 AS SERVICE_LEVEL
               |	,AT.a12575903189 AS INDUSTRY
               |FROM (
               |	SELECT MT.a3600 AS a3600
               |		,MT.a12575873557 AS a12575873557
               |		,MT.a12575847251 AS a12575847251
               |		,MT.a12575903189 AS a12575903189
               |		,SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_CA
               |		,(
               |			CASE 
               |				WHEN (SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0))) > 0
               |					THEN 1
               |				ELSE 0
               |				END
               |			) AS CUSTER_IOT_GRP_USER_NUMBER_M
               |		,MT.a204010101 AS a204010101
               |	FROM (
               |		SELECT cast(floor((STARTTIME + 28800) / 3600) * 3600 - 28800 AS INT) AS a3600
               |			,SUM(COALESCE(1, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_C
               |			,D12575657700_H104.a12575903189 AS a12575903189
               |			,DIM_52 AS a204010101
               |			,D12575657700_H104.a12575873557 AS a12575873557
               |			,D12575657700_H104.a12575847251 AS a12575847251
               |		FROM SDR_DYN_SEQ_CUSTER_IOT_ALL_HOUR_60MIN
               |		LEFT JOIN (
               |			SELECT INDUSTRY AS a12575903189
               |				,APN_NAME AS a12575817396
               |				,CITY_ASCRIPTION AS a12575873557
               |				,SERVICE_LEVEL AS a12575847251
               |			FROM DIM_APN_IOT
               |			GROUP BY INDUSTRY
               |				,APN_NAME
               |				,CITY_ASCRIPTION
               |				,SERVICE_LEVEL
               |			) D12575657700_H104 ON DIM_51 = D12575657700_H104.a12575817396
               |		WHERE (
               |				D12575657700_H104.a12575873557 IN (
               |					'金华'
               |					,'丽水'
               |					,'台州'
               |					,'舟山'
               |					,'嘉兴'
               |					,'宁波'
               |					,'温州'
               |					,'绍兴'
               |					,'湖州'
               |					,'杭州'
               |					,'衢州'
               |					,'省直管'
               |					,'外省地市'
               |					,'测试'
               |					)
               |				AND D12575657700_H104.a12575903189 IN (
               |					'公共管理'
               |					,'卫生社保'
               |					,'电力供应'
               |					,'金融业'
               |					,'软件业'
               |					,'文体娱业'
               |					,'居民服务'
               |					,'科研技术'
               |					,'交运仓储'
               |					,'建筑业'
               |					,'租赁服务'
               |					,'制造业'
               |					,'住宿餐饮'
               |					,'公共服务'
               |					,'批发零售'
               |					,'农林牧渔'
               |					)
               |				AND D12575657700_H104.a12575847251 IN (
               |					'金'
               |					,'标准'
               |					,'银'
               |					,'铜'
               |					)
               |				AND DIM_1 IN (
               |					'1'
               |					,'2'
               |					,'5'
               |					)
               |				)
               |		GROUP BY STARTTIME
               |			,D12575657700_H104.a12575903189
               |			,DIM_52
               |			,D12575657700_H104.a12575873557
               |			,D12575657700_H104.a12575847251
               |		) MT
               |	GROUP BY MT.a3600
               |		,MT.a12575873557
               |		,MT.a12575847251
               |		,MT.a12575903189
               |		,MT.a204010101
               |	) AT
               |GROUP BY AT.a3600
               |	,AT.a12575873557
               |	,AT.a12575847251
               |	,AT.a12575903189
               |ORDER BY START_TIME ASC
              """.stripMargin.trim,
              """
               |SELECT AT.a3600 AS START_TIME
               |	,SUM(CUSTER_IOT_GRP_USER_NUMBER_M) AS USER_NUMBER
               |	,AT.a12575873557 AS CITY_ASCRIPTION
               |	,AT.a12575847251 AS SERVICE_LEVEL
               |	,AT.a12575903189 AS INDUSTRY
               |FROM (
               |	SELECT MT.a3600 AS a3600
               |		,MT.a12575873557 AS a12575873557
               |		,MT.a12575847251 AS a12575847251
               |		,MT.a12575903189 AS a12575903189
               |		,SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_CA
               |		,(
               |			CASE 
               |				WHEN (SUM(COALESCE(CUSTER_IOT_GRP_USER_NUM_STREAM_C, 0))) > 0
               |					THEN 1
               |				ELSE 0
               |				END
               |			) AS CUSTER_IOT_GRP_USER_NUMBER_M
               |		,MT.a204010101 AS a204010101
               |	FROM (
               |		SELECT cast(floor((STARTTIME + 28800) / 3600) * 3600 - 28800 AS INT) AS a3600
               |			,SUM(COALESCE(1, 0)) AS CUSTER_IOT_GRP_USER_NUM_STREAM_C
               |			,D12575657700_H104.a12575903189 AS a12575903189
               |			,DIM_52 AS a204010101
               |			,D12575657700_H104.a12575873557 AS a12575873557
               |			,D12575657700_H104.a12575847251 AS a12575847251
               |      ,D12575657700_H104.a12575817396 AS a12575817396
               |		FROM SDR_DYN_SEQ_CUSTER_IOT_ALL_HOUR_60MIN
               |		LEFT JOIN (
               |			SELECT INDUSTRY AS a12575903189
               |				,APN_NAME AS a12575817396
               |				,CITY_ASCRIPTION AS a12575873557
               |				,SERVICE_LEVEL AS a12575847251
               |			FROM DIM_APN_IOT
               |			GROUP BY INDUSTRY
               |				,APN_NAME
               |				,CITY_ASCRIPTION
               |				,SERVICE_LEVEL
               |			) D12575657700_H104 ON DIM_51 = D12575657700_H104.a12575817396
               |		WHERE (
               |				D12575657700_H104.a12575873557 IN (
               |					'金华'
               |					,'丽水'
               |					,'台州'
               |					,'舟山'
               |					,'嘉兴'
               |					,'宁波'
               |					,'温州'
               |					,'绍兴'
               |					,'湖州'
               |					,'杭州'
               |					,'衢州'
               |					,'省直管'
               |					,'外省地市'
               |					,'测试'
               |					)
               |				AND D12575657700_H104.a12575903189 IN (
               |					'公共管理'
               |					,'卫生社保'
               |					,'电力供应'
               |					,'金融业'
               |					,'软件业'
               |					,'文体娱业'
               |					,'居民服务'
               |					,'科研技术'
               |					,'交运仓储'
               |					,'建筑业'
               |					,'租赁服务'
               |					,'制造业'
               |					,'住宿餐饮'
               |					,'公共服务'
               |					,'批发零售'
               |					,'农林牧渔'
               |					)
               |				AND D12575657700_H104.a12575847251 IN (
               |					'金'
               |					,'标准'
               |					,'银'
               |					,'铜'
               |					)
               |				AND DIM_1 IN (
               |					'1'
               |					,'2'
               |					,'5'
               |					)
               |				)
               |		GROUP BY STARTTIME
               |			,D12575657700_H104.a12575903189
               |			,DIM_52
               |			,D12575657700_H104.a12575873557
               |			,D12575657700_H104.a12575847251
               |      ,D12575657700_H104.a12575817396
               |		) MT
               |	GROUP BY MT.a3600
               |		,MT.a12575873557
               |		,MT.a12575847251
               |		,MT.a12575903189
               |		,MT.a204010101
               |	) AT
               |GROUP BY AT.a3600
               |	,AT.a12575873557
               |	,AT.a12575847251
               |	,AT.a12575903189
               |ORDER BY START_TIME ASC
              """.stripMargin.trim
          ),
          Seq(
              """
               | 
              """.stripMargin.trim
          )
        )
   )
}

  

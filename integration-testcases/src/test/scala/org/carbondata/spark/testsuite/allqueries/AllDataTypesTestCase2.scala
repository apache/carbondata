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

package org.carbondata.spark.testsuite.allqueries

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

import org.carbondata.core.constants.CarbonCommonConstants

/**
  * Test Class for all queries on multiple datatypes
  * Manohar
  */
class AllDataTypesTestCase2 extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    try {
      sql(
        "create cube Carbon_automation_test2 dimensions(imei string,deviceInformationId integer," +
          "MAC string,deviceColor string,device_backColor string,modelId string,marketName " +
          "string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string," +
          "productionDate timestamp,bomCode string,internalModels string, deliveryTime string, " +
          "channelsId string, channelsName string , deliveryAreaId string, deliveryCountry " +
          "string, deliveryProvince string, deliveryCity string,deliveryDistrict string, " +
          "deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId " +
          "string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict" +
          " string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, " +
          "Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, " +
          "Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string," +
          "Active_webTypeDataVerNumber string, Active_operatorsVersion string, " +
          "Active_phonePADPartitionedVersions string, Latest_YEAR integer, Latest_MONTH integer, " +
          "Latest_DAY integer, Latest_HOUR string, Latest_areaId string, Latest_country string, " +
          "Latest_province string, Latest_city string, Latest_district string, Latest_street " +
          "string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion " +
          "string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion " +
          "string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, " +
          "Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, " +
          "Latest_operatorId string, gamePointDescription string)  measures(gamePointId numeric," +
          "contractNumber numeric) OPTIONS (PARTITIONER [CLASS = 'org.carbondata" +
          ".spark.partition.api.impl.SampleDataPartitionerImpl' ,COLUMNS= (imei) , " +
          "PARTITION_COUNT=2] )"

      )
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
        )
      sql("LOAD DATA FACT FROM '" + currentDirectory + "/src/test/resources/100_olap.csv' INTO " +
        "Cube Carbon_automation_test2 partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER " +
        "'imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize," +
        "ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime," +
        "channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity," +
        "deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime," +
        "ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet," +
        "ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion," +
        "Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer," +
        "Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions," +
        "Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country," +
        "Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId," +
        "Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer," +
        "Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber," +
        "Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId," +
        "gamePointId,gamePointDescription')")
    } catch {
      case e: Exception => print("ERROR: DROP Carbon_automation_test2 ")
    }
  }

  override def afterAll {
    //CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    try {
      sql("drop cube Carbon_automation_test2")
    } catch {
      case e: Exception => print("ERROR: DROP Carbon_automation_test2 ")
    }
  }

  //Test-23
  test(
    "select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, sum(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      Seq(Row("2", 18659),
        Row("7", 20484),
        Row("1", 20934),
        Row("4", 20991.197),
        Row("5", 23024),
        Row("3", 25241),
        Row("6", 28542)
      )
    )

  }






  //Test-28
  test(
    "select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, avg(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      Seq(Row("7", 1138),
        Row("6", 1502.2105263157894),
        Row("3", 1577.5625),
        Row("1", 1744.5),
        Row("4", 1749.2664166666666),
        Row("2", 1865.9),
        Row("5", 1918.66666666666667)
      )
    )

  }


  //Test-32
  test(
    "select channelsId, count(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, count(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      Seq(Row("2", 10),
        Row("1", 12),
        Row("4", 12),
        Row("5", 12),
        Row("3", 16),
        Row("7", 18),
        Row("6", 19)
      )
    )

  }


  //Test-36
  test(
    "select channelsId, min(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, min(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      Seq(Row("1", 39),
        Row("6", 89),
        Row("3", 161),
        Row("7", 266),
        Row("5", 308),
        Row("4", 451),
        Row("2", 578)
      )
    )

  }



  //Test-40
  test(
    "select channelsId, max(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
      "channelsId order by Total"
  ) {
    checkAnswer(
      sql(
        "select channelsId, max(gamePointId+ 10) Total from Carbon_automation_test2 group by  " +
          "channelsId order by Total"
      ),
      Seq(Row("3", 2249),
        Row("7", 2358),
        Row("4", 2836),
        Row("1", 2873),
        Row("6", 2962),
        Row("2", 2980),
        Row("5", 2982)
      )
    )

  }

  //Test-45
  test(
    "select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_test2 group by " +
      "Latest_YEAR"
  )({

    checkAnswer(
      sql(
        "select Latest_YEAR ,count(distinct Latest_YEAR) from Carbon_automation_test2 group by " +
          "Latest_YEAR"
      ),
      Seq(Row(2015, 1))
    )
  }
  )
  //TC_053
  test("select avg(gamepointid) +10 as a ,series  from Carbon_automation_test2 group by series")({
    checkAnswer(
      sql("select avg(gamepointid) +10 as a ,series  from Carbon_automation_test2 group by series"),
      Seq(Row(1445.7777777777778, "6Series"),
        Row(1735.3333333333333, "0Series"),
        Row(1553.0, "4Series"),
        Row(1243.3636363636363, "8Series"),
        Row(1700.1088181818182, "7Series"),
        Row(1343.6666666666667, "1Series"),
        Row(1720.0588235294117, "5Series"),
        Row(1625.0, "9Series"),
        Row(1914.375, "3Series"),
        Row(1382.6666666666667, "2Series")
      )
    )
  }
  )

  //TC_059
  test(
    "select count(distinct gamepointid)+10 as a,series from Carbon_automation_test2 group by series"
  )({
    checkAnswer(
      sql(
        "select count(distinct gamepointid)+10 as a,series from Carbon_automation_test2 group by " +
          "series"
      ),
      Seq(Row(19, "6Series"),
        Row(25, "0Series"),
        Row(18, "4Series"),
        Row(21, "8Series"),
        Row(21, "7Series"),
        Row(13, "1Series"),
        Row(26, "5Series"),
        Row(18, "9Series"),
        Row(18, "3Series"),
        Row(19, "2Series")
      )
    )
  }
  )

  //TC_068
  test("select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select count (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      Seq(Row(2))
    )
  }
  )

  //TC_070
  test("select count(DISTINCT gamePointId) as a from Carbon_automation_test2")({
    checkAnswer(
      sql("select count(DISTINCT gamePointId) as a from Carbon_automation_test2"),
      Seq(Row(97))
    )
  }
  )

  //TC_076
  test("select sum( DISTINCT gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select sum( DISTINCT gamePointId) a  from Carbon_automation_test2"),
      Seq(Row(153284.19700000001))
    )
  }
  )

  //TC_079
  test("select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select sum (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      Seq(Row(108.0))
    )
  }
  )

  //TC_082
  test("select avg(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select avg(gamePointId) a  from Carbon_automation_test2"),
      Seq(Row(1584.6989595959594))
    )
  }
  )

  //TC_087
  test("select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select avg (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      Seq(Row(54.0))
    )
  }
  )


  //TC_089
  test("select min(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select min(gamePointId) a  from Carbon_automation_test2"),
      Seq(Row(29.0))
    )
  }
  )


  //TC_094
  test("select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select min (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      Seq(Row(29.0))
    )
  }
  )

  //TC_096
  test("select max(gamePointId) a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select max(gamePointId) a  from Carbon_automation_test2"),
      Seq(Row(2972.0))
    )
  }
  )

  //TC_101
  test("select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2")({
    checkAnswer(
      sql("select max (if(gamePointId>100,NULL,gamePointId))  a from Carbon_automation_test2"),
      Seq(Row(79.0))
    )
  }
  )

  //TC_122
  test("select stddev_pop(gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(gamePointId) as a  from Carbon_automation_test2"),
      Seq(Row(809.1896217395077))
    )
  }
  )

  //TC_124
  test("select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_pop(gamePointId,gamePointId) as a  from Carbon_automation_test2"),
      Seq(Row(654787.8439309277))
    )
  }
  )

  //TC_125
  test("select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId,gamePointId) as a  from Carbon_automation_test2"),
      Seq(Row(661469.3525424678))
    )
  }
  )

  //TC_129
  test("select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_test2")({
    checkAnswer(
      sql("select percentile_approx(gamePointId,0.2) as a  from Carbon_automation_test2"),
      Seq(Row(746.4))
    )
  }
  )

  //TC_140
  test(
    "select count(distinct deviceColor) a,deliveryProvince,series from Carbon_automation_test2 " +
      "group by deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select count(distinct deviceColor) a,deliveryProvince,series from " +
          "Carbon_automation_test2 group by deliveryProvince,series order by deliveryProvince," +
          "series"
      ),
      Seq(Row(5, "Guangdong Province", "0Series"),
        Row(1, "Guangdong Province", "1Series"),
        Row(2, "Guangdong Province", "2Series"),
        Row(3, "Guangdong Province", "3Series"),
        Row(1, "Guangdong Province", "4Series"),
        Row(6, "Guangdong Province", "5Series"),
        Row(1, "Guangdong Province", "6Series"),
        Row(1, "Guangdong Province", "7Series"),
        Row(3, "Guangdong Province", "8Series"),
        Row(2, "Guangdong Province", "9Series"),
        Row(3, "Hubei Province", "0Series"),
        Row(1, "Hubei Province", "1Series"),
        Row(4, "Hubei Province", "2Series"),
        Row(1, "Hubei Province", "3Series"),
        Row(2, "Hubei Province", "4Series"),
        Row(1, "Hubei Province", "5Series"),
        Row(2, "Hubei Province", "6Series"),
        Row(6, "Hubei Province", "7Series"),
        Row(3, "Hubei Province", "8Series"),
        Row(2, "Hubei Province", "9Series"),
        Row(6, "Hunan Province", "0Series"),
        Row(1, "Hunan Province", "1Series"),
        Row(3, "Hunan Province", "2Series"),
        Row(1, "Hunan Province", "3Series"),
        Row(4, "Hunan Province", "4Series"),
        Row(7, "Hunan Province", "5Series"),
        Row(6, "Hunan Province", "6Series"),
        Row(3, "Hunan Province", "7Series"),
        Row(5, "Hunan Province", "8Series"),
        Row(3, "Hunan Province", "9Series")
      )
    )
  }
  )

  //TC_142
  test(
    "select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,sum(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ),
      Seq(Row("Chinese", "Guangdong Province", "0Series", 11182.0),
        Row("Chinese", "Guangdong Province", "1Series", 1053.0),
        Row("Chinese", "Guangdong Province", "2Series", 3468.0),
        Row("Chinese", "Guangdong Province", "3Series", 11206.0),
        Row("Chinese", "Guangdong Province", "4Series", 1337.0),
        Row("Chinese", "Guangdong Province", "5Series", 10962.0),
        Row("Chinese", "Guangdong Province", "6Series", 1229.0),
        Row("Chinese", "Guangdong Province", "7Series", 3543.0),
        Row("Chinese", "Guangdong Province", "8Series", 3859.0),
        Row("Chinese", "Guangdong Province", "9Series", 1525.0),
        Row("Chinese", "Hubei Province", "0Series", 5545.0),
        Row("Chinese", "Hubei Province", "1Series", 2593.0),
        Row("Chinese", "Hubei Province", "2Series", 4344.0),
        Row("Chinese", "Hubei Province", "3Series", 2077.0),
        Row("Chinese", "Hubei Province", "4Series", 3542.0),
        Row("Chinese", "Hubei Province", "5Series", 692.0),
        Row("Chinese", "Hubei Province", "6Series", 1657.0),
        Row("Chinese", "Hubei Province", "7Series", 10629.197),
        Row("Chinese", "Hubei Province", "8Series", 3279.0),
        Row("Chinese", "Hubei Province", "9Series", 3088.0),
        Row("Chinese", "Hunan Province", "0Series", 9153.0),
        Row("Chinese", "Hunan Province", "1Series", 355.0),
        Row("Chinese", "Hunan Province", "2Series", 4542.0),
        Row("Chinese", "Hunan Province", "3Series", 1952.0),
        Row("Chinese", "Hunan Province", "4Series", 7465.0),
        Row("Chinese", "Hunan Province", "5Series", 17417.0),
        Row("Chinese", "Hunan Province", "6Series", 10036.0),
        Row("Chinese", "Hunan Province", "7Series", 4419.0),
        Row("Chinese", "Hunan Province", "8Series", 6429.0),
        Row("Chinese", "Hunan Province", "9Series", 8307.0)
      )
    )
  }
  )

  //TC_146
  test(
    "select series,avg(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,avg(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ),
      Seq(Row("0Series", 1725.3333333333333),
        Row("1Series", 1333.6666666666667),
        Row("2Series", 1372.6666666666667),
        Row("3Series", 1904.375),
        Row("4Series", 1543.0),
        Row("5Series", 1710.0588235294117),
        Row("6Series", 1435.7777777777778),
        Row("7Series", 1690.1088181818182),
        Row("8Series", 1233.3636363636363),
        Row("9Series", 1615.0)
      )
    )
  }
  )

  //TC_147
  test(
    "select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,avg(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ),
      Seq(Row("Chinese", "Guangdong Province", "0Series", 1863.6666666666667),
        Row("Chinese", "Guangdong Province", "1Series", 1053.0),
        Row("Chinese", "Guangdong Province", "2Series", 1734.0),
        Row("Chinese", "Guangdong Province", "3Series", 2241.2),
        Row("Chinese", "Guangdong Province", "4Series", 1337.0),
        Row("Chinese", "Guangdong Province", "5Series", 1566.0),
        Row("Chinese", "Guangdong Province", "6Series", 1229.0),
        Row("Chinese", "Guangdong Province", "7Series", 1771.5),
        Row("Chinese", "Guangdong Province", "8Series", 1286.3333333333333),
        Row("Chinese", "Guangdong Province", "9Series", 762.5),
        Row("Chinese", "Hubei Province", "0Series", 1848.3333333333333),
        Row("Chinese", "Hubei Province", "1Series", 2593.0),
        Row("Chinese", "Hubei Province", "2Series", 1086.0),
        Row("Chinese", "Hubei Province", "3Series", 2077.0),
        Row("Chinese", "Hubei Province", "4Series", 1771.0),
        Row("Chinese", "Hubei Province", "5Series", 692.0),
        Row("Chinese", "Hubei Province", "6Series", 828.5),
        Row("Chinese", "Hubei Province", "7Series", 1771.5328333333334),
        Row("Chinese", "Hubei Province", "8Series", 1093.0),
        Row("Chinese", "Hubei Province", "9Series", 1544.0),
        Row("Chinese", "Hunan Province", "0Series", 1525.5),
        Row("Chinese", "Hunan Province", "1Series", 355.0),
        Row("Chinese", "Hunan Province", "2Series", 1514.0),
        Row("Chinese", "Hunan Province", "3Series", 976.0),
        Row("Chinese", "Hunan Province", "4Series", 1493.0),
        Row("Chinese", "Hunan Province", "5Series", 1935.2222222222222),
        Row("Chinese", "Hunan Province", "6Series", 1672.6666666666667),
        Row("Chinese", "Hunan Province", "7Series", 1473.0),
        Row("Chinese", "Hunan Province", "8Series", 1285.8),
        Row("Chinese", "Hunan Province", "9Series", 2076.75)
      )
    )
  }
  )

  //TC_151
  test(
    "select series,min(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,min(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ),
      Seq(Row("0Series", 202.0),
        Row("1Series", 355.0),
        Row("2Series", 29.0),
        Row("3Series", 79.0),
        Row("4Series", 448.0),
        Row("5Series", 151.0),
        Row("6Series", 750.0),
        Row("7Series", 1015.0),
        Row("8Series", 412.0),
        Row("9Series", 136.0)
      )
    )
  }
  )

  //TC_152
  test(
    "select deliveryCountry,deliveryProvince,series,min(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,min(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ),
      Seq(Row("Chinese", "Guangdong Province", "0Series", 202.0),
        Row("Chinese", "Guangdong Province", "1Series", 1053.0),
        Row("Chinese", "Guangdong Province", "2Series", 1407.0),
        Row("Chinese", "Guangdong Province", "3Series", 1077.0),
        Row("Chinese", "Guangdong Province", "4Series", 1337.0),
        Row("Chinese", "Guangdong Province", "5Series", 256.0),
        Row("Chinese", "Guangdong Province", "6Series", 1229.0),
        Row("Chinese", "Guangdong Province", "7Series", 1368.0),
        Row("Chinese", "Guangdong Province", "8Series", 412.0),
        Row("Chinese", "Guangdong Province", "9Series", 571.0),
        Row("Chinese", "Hubei Province", "0Series", 732.0),
        Row("Chinese", "Hubei Province", "1Series", 2593.0),
        Row("Chinese", "Hubei Province", "2Series", 29.0),
        Row("Chinese", "Hubei Province", "3Series", 2077.0),
        Row("Chinese", "Hubei Province", "4Series", 572.0),
        Row("Chinese", "Hubei Province", "5Series", 692.0),
        Row("Chinese", "Hubei Province", "6Series", 750.0),
        Row("Chinese", "Hubei Province", "7Series", 1080.0),
        Row("Chinese", "Hubei Province", "8Series", 441.0),
        Row("Chinese", "Hubei Province", "9Series", 136.0),
        Row("Chinese", "Hunan Province", "0Series", 505.0),
        Row("Chinese", "Hunan Province", "1Series", 355.0),
        Row("Chinese", "Hunan Province", "2Series", 298.0),
        Row("Chinese", "Hunan Province", "3Series", 79.0),
        Row("Chinese", "Hunan Province", "4Series", 448.0),
        Row("Chinese", "Hunan Province", "5Series", 151.0),
        Row("Chinese", "Hunan Province", "6Series", 845.0),
        Row("Chinese", "Hunan Province", "7Series", 1015.0),
        Row("Chinese", "Hunan Province", "8Series", 538.0),
        Row("Chinese", "Hunan Province", "9Series", 1823.0)
      )
    )
  }
  )

  //TC_153
  test(
    "select deliveryProvince,series,min(deviceInformationId) a from Carbon_automation_test2 group" +
      " by deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,min(deviceInformationId) a from Carbon_automation_test2 " +
          "group by deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", 100011),
        Row("Guangdong Province", "1Series", 100032),
        Row("Guangdong Province", "2Series", 100034),
        Row("Guangdong Province", "3Series", 10006),
        Row("Guangdong Province", "4Series", 100059),
        Row("Guangdong Province", "5Series", 10004),
        Row("Guangdong Province", "6Series", 100038),
        Row("Guangdong Province", "7Series", 10000),
        Row("Guangdong Province", "8Series", 100039),
        Row("Guangdong Province", "9Series", 100043),
        Row("Hubei Province", "0Series", 10002),
        Row("Hubei Province", "1Series", 100005),
        Row("Hubei Province", "2Series", 100050),
        Row("Hubei Province", "3Series", 100077),
        Row("Hubei Province", "4Series", 100004),
        Row("Hubei Province", "5Series", 1000),
        Row("Hubei Province", "6Series", 100056),
        Row("Hubei Province", "7Series", 1),
        Row("Hubei Province", "8Series", 100018),
        Row("Hubei Province", "9Series", 100000),
        Row("Hunan Province", "0Series", 100001),
        Row("Hunan Province", "1Series", 100013),
        Row("Hunan Province", "2Series", 10001),
        Row("Hunan Province", "3Series", 100010),
        Row("Hunan Province", "4Series", 100012),
        Row("Hunan Province", "5Series", 100),
        Row("Hunan Province", "6Series", 100006),
        Row("Hunan Province", "7Series", 10003),
        Row("Hunan Province", "8Series", 10005),
        Row("Hunan Province", "9Series", 100007)
      )
    )
  }
  )

  //TC_154
  test(
    "select deliveryProvince,series,min(channelsId) a from Carbon_automation_test2 group by " +
      "deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,min(channelsId) a from Carbon_automation_test2 group by " +
          "deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", "1"),
        Row("Guangdong Province", "1Series", "7"),
        Row("Guangdong Province", "2Series", "6"),
        Row("Guangdong Province", "3Series", "1"),
        Row("Guangdong Province", "4Series", "7"),
        Row("Guangdong Province", "5Series", "1"),
        Row("Guangdong Province", "6Series", "3"),
        Row("Guangdong Province", "7Series", "1"),
        Row("Guangdong Province", "8Series", "1"),
        Row("Guangdong Province", "9Series", "6"),
        Row("Hubei Province", "0Series", "3"),
        Row("Hubei Province", "1Series", "1"),
        Row("Hubei Province", "2Series", "1"),
        Row("Hubei Province", "3Series", "6"),
        Row("Hubei Province", "4Series", "2"),
        Row("Hubei Province", "5Series", "3"),
        Row("Hubei Province", "6Series", "5"),
        Row("Hubei Province", "7Series", "1"),
        Row("Hubei Province", "8Series", "4"),
        Row("Hubei Province", "9Series", "6"),
        Row("Hunan Province", "0Series", "2"),
        Row("Hunan Province", "1Series", "6"),
        Row("Hunan Province", "2Series", "5"),
        Row("Hunan Province", "3Series", "3"),
        Row("Hunan Province", "4Series", "1"),
        Row("Hunan Province", "5Series", "3"),
        Row("Hunan Province", "6Series", "2"),
        Row("Hunan Province", "7Series", "3"),
        Row("Hunan Province", "8Series", "1"),
        Row("Hunan Province", "9Series", "1")
      )
    )
  }
  )

  //TC_155
  test(
    "select deliveryProvince,series,min(bomCode) a from Carbon_automation_test2 group by " +
      "deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,min(bomCode) a from Carbon_automation_test2 group by " +
          "deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", "100011"),
        Row("Guangdong Province", "1Series", "100032"),
        Row("Guangdong Province", "2Series", "100034"),
        Row("Guangdong Province", "3Series", "100042"),
        Row("Guangdong Province", "4Series", "100059"),
        Row("Guangdong Province", "5Series", "100020"),
        Row("Guangdong Province", "6Series", "100038"),
        Row("Guangdong Province", "7Series", "10000"),
        Row("Guangdong Province", "8Series", "100039"),
        Row("Guangdong Province", "9Series", "100043"),
        Row("Hubei Province", "0Series", "100009"),
        Row("Hubei Province", "1Series", "100005"),
        Row("Hubei Province", "2Series", "100050"),
        Row("Hubei Province", "3Series", "100077"),
        Row("Hubei Province", "4Series", "100004"),
        Row("Hubei Province", "5Series", "1000"),
        Row("Hubei Province", "6Series", "100056"),
        Row("Hubei Province", "7Series", "1"),
        Row("Hubei Province", "8Series", "100018"),
        Row("Hubei Province", "9Series", "100000"),
        Row("Hunan Province", "0Series", "100001"),
        Row("Hunan Province", "1Series", "100013"),
        Row("Hunan Province", "2Series", "10001"),
        Row("Hunan Province", "3Series", "100010"),
        Row("Hunan Province", "4Series", "100012"),
        Row("Hunan Province", "5Series", "100"),
        Row("Hunan Province", "6Series", "100006"),
        Row("Hunan Province", "7Series", "10003"),
        Row("Hunan Province", "8Series", "100008"),
        Row("Hunan Province", "9Series", "100007")
      )
    )
  }
  )

  //TC_156
  test(
    "select series,max(gamePointId) a from Carbon_automation_test2 group by series order by series"
  )({
    checkAnswer(
      sql(
        "select series,max(gamePointId) a from Carbon_automation_test2 group by series order by " +
          "series"
      ),
      Seq(Row("0Series", 2972.0),
        Row("1Series", 2593.0),
        Row("2Series", 2553.0),
        Row("3Series", 2745.0),
        Row("4Series", 2970.0),
        Row("5Series", 2849.0),
        Row("6Series", 2572.0),
        Row("7Series", 2738.562),
        Row("8Series", 2078.0),
        Row("9Series", 2952.0)
      )
    )
  }
  )

  //TC_157
  test(
    "select deliveryCountry,deliveryProvince,series,max(gamePointId) a from " +
      "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
      "deliveryCountry,deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryCountry,deliveryProvince,series,max(gamePointId) a from " +
          "Carbon_automation_test2 group by deliveryCountry,deliveryProvince,series order by " +
          "deliveryCountry,deliveryProvince,series"
      ),
      Seq(Row("Chinese", "Guangdong Province", "0Series", 2890.0),
        Row("Chinese", "Guangdong Province", "1Series", 1053.0),
        Row("Chinese", "Guangdong Province", "2Series", 2061.0),
        Row("Chinese", "Guangdong Province", "3Series", 2745.0),
        Row("Chinese", "Guangdong Province", "4Series", 1337.0),
        Row("Chinese", "Guangdong Province", "5Series", 2734.0),
        Row("Chinese", "Guangdong Province", "6Series", 1229.0),
        Row("Chinese", "Guangdong Province", "7Series", 2175.0),
        Row("Chinese", "Guangdong Province", "8Series", 1750.0),
        Row("Chinese", "Guangdong Province", "9Series", 954.0),
        Row("Chinese", "Hubei Province", "0Series", 2972.0),
        Row("Chinese", "Hubei Province", "1Series", 2593.0),
        Row("Chinese", "Hubei Province", "2Series", 1655.0),
        Row("Chinese", "Hubei Province", "3Series", 2077.0),
        Row("Chinese", "Hubei Province", "4Series", 2970.0),
        Row("Chinese", "Hubei Province", "5Series", 692.0),
        Row("Chinese", "Hubei Province", "6Series", 907.0),
        Row("Chinese", "Hubei Province", "7Series", 2738.562),
        Row("Chinese", "Hubei Province", "8Series", 2078.0),
        Row("Chinese", "Hubei Province", "9Series", 2952.0),
        Row("Chinese", "Hunan Province", "0Series", 2436.0),
        Row("Chinese", "Hunan Province", "1Series", 355.0),
        Row("Chinese", "Hunan Province", "2Series", 2553.0),
        Row("Chinese", "Hunan Province", "3Series", 1873.0),
        Row("Chinese", "Hunan Province", "4Series", 2863.0),
        Row("Chinese", "Hunan Province", "5Series", 2849.0),
        Row("Chinese", "Hunan Province", "6Series", 2572.0),
        Row("Chinese", "Hunan Province", "7Series", 2071.0),
        Row("Chinese", "Hunan Province", "8Series", 1608.0),
        Row("Chinese", "Hunan Province", "9Series", 2288.0)
      )
    )
  }
  )

  //TC_158
  test(
    "select deliveryProvince,series,max(deviceInformationId) a from Carbon_automation_test2 group" +
      " by deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,max(deviceInformationId) a from Carbon_automation_test2 " +
          "group by deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", 100084),
        Row("Guangdong Province", "1Series", 100032),
        Row("Guangdong Province", "2Series", 100051),
        Row("Guangdong Province", "3Series", 100075),
        Row("Guangdong Province", "4Series", 100059),
        Row("Guangdong Province", "5Series", 100081),
        Row("Guangdong Province", "6Series", 100038),
        Row("Guangdong Province", "7Series", 100054),
        Row("Guangdong Province", "8Series", 100068),
        Row("Guangdong Province", "9Series", 100080),
        Row("Hubei Province", "0Series", 100076),
        Row("Hubei Province", "1Series", 100005),
        Row("Hubei Province", "2Series", 100078),
        Row("Hubei Province", "3Series", 100077),
        Row("Hubei Province", "4Series", 100067),
        Row("Hubei Province", "5Series", 1000),
        Row("Hubei Province", "6Series", 100074),
        Row("Hubei Province", "7Series", 1000000),
        Row("Hubei Province", "8Series", 100040),
        Row("Hubei Province", "9Series", 100062),
        Row("Hunan Province", "0Series", 100083),
        Row("Hunan Province", "1Series", 100013),
        Row("Hunan Province", "2Series", 100045),
        Row("Hunan Province", "3Series", 100016),
        Row("Hunan Province", "4Series", 100079),
        Row("Hunan Province", "5Series", 100082),
        Row("Hunan Province", "6Series", 100066),
        Row("Hunan Province", "7Series", 100037),
        Row("Hunan Province", "8Series", 100069),
        Row("Hunan Province", "9Series", 100057)
      )
    )
  }
  )

  //TC_159
  test(
    "select deliveryProvince,series,max(channelsId) a from Carbon_automation_test2 group by " +
      "deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,max(channelsId) a from Carbon_automation_test2 group by " +
          "deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", "6"),
        Row("Guangdong Province", "1Series", "7"),
        Row("Guangdong Province", "2Series", "7"),
        Row("Guangdong Province", "3Series", "6"),
        Row("Guangdong Province", "4Series", "7"),
        Row("Guangdong Province", "5Series", "7"),
        Row("Guangdong Province", "6Series", "3"),
        Row("Guangdong Province", "7Series", "2"),
        Row("Guangdong Province", "8Series", "7"),
        Row("Guangdong Province", "9Series", "6"),
        Row("Hubei Province", "0Series", "7"),
        Row("Hubei Province", "1Series", "1"),
        Row("Hubei Province", "2Series", "5"),
        Row("Hubei Province", "3Series", "6"),
        Row("Hubei Province", "4Series", "7"),
        Row("Hubei Province", "5Series", "3"),
        Row("Hubei Province", "6Series", "7"),
        Row("Hubei Province", "7Series", "7"),
        Row("Hubei Province", "8Series", "7"),
        Row("Hubei Province", "9Series", "6"),
        Row("Hunan Province", "0Series", "7"),
        Row("Hunan Province", "1Series", "6"),
        Row("Hunan Province", "2Series", "6"),
        Row("Hunan Province", "3Series", "6"),
        Row("Hunan Province", "4Series", "7"),
        Row("Hunan Province", "5Series", "7"),
        Row("Hunan Province", "6Series", "7"),
        Row("Hunan Province", "7Series", "6"),
        Row("Hunan Province", "8Series", "5"),
        Row("Hunan Province", "9Series", "6")
      )
    )
  }
  )

  //TC_160
  test(
    "select deliveryProvince,series,max(bomCode) a from Carbon_automation_test2 group by " +
      "deliveryProvince,series order by deliveryProvince,series"
  )({
    checkAnswer(
      sql(
        "select deliveryProvince,series,max(bomCode) a from Carbon_automation_test2 group by " +
          "deliveryProvince,series order by deliveryProvince,series"
      ),
      Seq(Row("Guangdong Province", "0Series", "100084"),
        Row("Guangdong Province", "1Series", "100032"),
        Row("Guangdong Province", "2Series", "100051"),
        Row("Guangdong Province", "3Series", "100075"),
        Row("Guangdong Province", "4Series", "100059"),
        Row("Guangdong Province", "5Series", "100081"),
        Row("Guangdong Province", "6Series", "100038"),
        Row("Guangdong Province", "7Series", "100054"),
        Row("Guangdong Province", "8Series", "100068"),
        Row("Guangdong Province", "9Series", "100080"),
        Row("Hubei Province", "0Series", "100076"),
        Row("Hubei Province", "1Series", "100005"),
        Row("Hubei Province", "2Series", "100078"),
        Row("Hubei Province", "3Series", "100077"),
        Row("Hubei Province", "4Series", "100067"),
        Row("Hubei Province", "5Series", "1000"),
        Row("Hubei Province", "6Series", "100074"),
        Row("Hubei Province", "7Series", "100055"),
        Row("Hubei Province", "8Series", "100040"),
        Row("Hubei Province", "9Series", "100062"),
        Row("Hunan Province", "0Series", "100083"),
        Row("Hunan Province", "1Series", "100013"),
        Row("Hunan Province", "2Series", "100045"),
        Row("Hunan Province", "3Series", "100016"),
        Row("Hunan Province", "4Series", "100079"),
        Row("Hunan Province", "5Series", "100082"),
        Row("Hunan Province", "6Series", "100066"),
        Row("Hunan Province", "7Series", "100037"),
        Row("Hunan Province", "8Series", "10007"),
        Row("Hunan Province", "9Series", "100057")
      )
    )
  }
  )

  //TC_169
  test("select imei,series,gamePointId from Carbon_automation_test2 limit 10")({
    checkAnswer(
      sql("select imei,series,gamePointId from Carbon_automation_test2 limit 10"),
      Seq(Row("1AA1", "7Series", 2738.562),
        Row("1AA10", "7Series", 1714.635),
        Row("1AA100", "5Series", 1271.0),
        Row("1AA1000", "5Series", 692.0),
        Row("1AA10000", "7Series", 2175.0),
        Row("1AA100000", "9Series", 136.0),
        Row("1AA1000000", "7Series", 1600.0),
        Row("1AA100001", "0Series", 505.0),
        Row("1AA100002", "0Series", 1341.0),
        Row("1AA100003", "5Series", 2239.0)
      )
    )
  }
  )

  //TC_174
  test("select distinct gamePointId from Carbon_automation_test2")({
    checkAnswer(
      sql("select distinct gamePointId from Carbon_automation_test2"),
      Seq(Row(1350.0),
        Row(412.0),
        Row(2952.0),
        Row(2077.0),
        Row(1750.0),
        Row(1600.0),
        Row(2436.0),
        Row(2061.0),
        Row(1442.0),
        Row(1717.0),
        Row(1567.0),
        Row(1434.0),
        Row(2745.0),
        Row(954.0),
        Row(2970.0),
        Row(1226.0),
        Row(750.0),
        Row(256.0),
        Row(2488.0),
        Row(1768.0),
        Row(1368.0),
        Row(571.0),
        Row(2863.0),
        Row(79.0),
        Row(2288.0),
        Row(2972.0),
        Row(2572.0),
        Row(692.0),
        Row(1077.0),
        Row(613.0),
        Row(813.0),
        Row(538.0),
        Row(2890.0),
        Row(202.0),
        Row(448.0),
        Row(298.0),
        Row(2399.0),
        Row(2849.0),
        Row(2224.0),
        Row(151.0),
        Row(1778.0),
        Row(2483.0),
        Row(901.0),
        Row(1053.0),
        Row(1728.0),
        Row(2192.0),
        Row(2142.0),
        Row(572.0),
        Row(29.0),
        Row(1337.0),
        Row(568.0),
        Row(2826.0),
        Row(2738.562),
        Row(2635.0),
        Row(1229.0),
        Row(1271.0),
        Row(2194.0),
        Row(760.0),
        Row(2553.0),
        Row(2078.0),
        Row(2478.0),
        Row(1655.0),
        Row(1080.0),
        Row(505.0),
        Row(355.0),
        Row(1697.0),
        Row(2071.0),
        Row(2205.0),
        Row(1864.0),
        Row(1015.0),
        Row(2239.0),
        Row(865.0),
        Row(1873.0),
        Row(1098.0),
        Row(2348.0),
        Row(1823.0),
        Row(1973.0),
        Row(2507.0),
        Row(732.0),
        Row(907.0),
        Row(1714.635),
        Row(1407.0),
        Row(1724.0),
        Row(1999.0),
        Row(2175.0),
        Row(1991.0),
        Row(1691.0),
        Row(441.0),
        Row(136.0),
        Row(1341.0),
        Row(845.0),
        Row(2734.0),
        Row(1841.0),
        Row(1491.0),
        Row(1333.0),
        Row(2593.0),
        Row(1608.0)
      )
    )
  }
  )

  //TC_188
  test("select series,gamePointId as a from Carbon_automation_test2  order by a asc limit 10")({
    checkAnswer(
      sql("select series,gamePointId as a from Carbon_automation_test2  order by a asc limit 10"),
      Seq(Row("2Series", 29.0),
        Row("3Series", 79.0),
        Row("9Series", 136.0),
        Row("5Series", 151.0),
        Row("0Series", 202.0),
        Row("5Series", 256.0),
        Row("2Series", 298.0),
        Row("1Series", 355.0),
        Row("8Series", 412.0),
        Row("8Series", 441.0)
      )
    )
  }
  )

  //TC_189
  test("select series,gamePointId as a from Carbon_automation_test2  order by a desc limit 10")({
    checkAnswer(
      sql("select series,gamePointId as a from Carbon_automation_test2  order by a desc limit 10"),
      Seq(Row("0Series", 2972.0),
        Row("4Series", 2970.0),
        Row("9Series", 2952.0),
        Row("0Series", 2890.0),
        Row("4Series", 2863.0),
        Row("5Series", 2849.0),
        Row("0Series", 2826.0),
        Row("3Series", 2745.0),
        Row("7Series", 2738.562),
        Row("5Series", 2734.0)
      )
    )
  }
  )

  //TC_192
  test(
    "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
      "(gamePointId==2738.562)1 "
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
          "(gamePointId==2738.562)"
      ),
      Seq(Row("1AA1"))
    )
  }
  )

  //TC_193
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where  (contractNumber" +
      " == 5281803) and (gamePointId==2738.562)"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where  " +
          "(contractNumber == 5281803) and (gamePointId==2738.562)"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"))
    )
  }
  )

  //TC_194
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series='8Series'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "series='8Series'"
      ),
      Seq(Row(1070757.0, 1442.0, "8Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series")
      )
    )
  }
  )

  //TC_195
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series='8Series'" +
      " and internalModels='8Internal models'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "series='8Series' and internalModels='8Internal models'"
      ),
      Seq(Row(1070757.0, 1442.0, "8Series"), Row(7880439.0, 2078.0, "8Series"))
    )
  }
  )

  //TC_196
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series='8Series'" +
      " or  internalModels='8Internal models'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "series='8Series' or  internalModels='8Internal models'"
      ),
      Seq(Row(88231.0, 2239.0, "5Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(1070757.0, 1442.0, "8Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series")
      )
    )
  }
  )

  //TC_197
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series='8Series'" +
      " or series='7Series'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "series='8Series' or series='7Series'"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(6805600.0, 1714.635, "7Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(1070757.0, 1442.0, "8Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series")
      )
    )
  }
  )

  //TC_198
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId=2738" +
      ".562"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "gamePointId=2738.562"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"))
    )
  }
  )

  //TC_199
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
      "deviceInformationId=10"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "deviceInformationId=10"
      ),
      Seq(Row(6805600.0, 1714.635, "7Series"))
    )
  }
  )

  //TC_200
  test(
    "select contractNumber,gamePointId,series  from Carbon_automation_test2 where " +
      "(deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 and " +
      "deviceColor='0Device Color')"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series  from Carbon_automation_test2 where " +
          "(deviceInformationId=100 and deviceColor='1Device Color') OR (deviceInformationId=10 " +
          "and deviceColor='0Device Color')"
      ),
      Seq(Row(6805600.0, 1714.635, "7Series"), Row(8231335.0, 1271.0, "5Series"))
    )
  }
  )

  //TC_201
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series !='8Series'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series'"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(6805600.0, 1714.635, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(88231.0, 2239.0, "5Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_202
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
      "!='8Series' and internalModels !='8Internal models'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series' and internalModels !='8Internal models'"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(6805600.0, 1714.635, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_203
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
      "!='8Series' or  internalModels !='8Internal models'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series' or  internalModels !='8Internal models'"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(6805600.0, 1714.635, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(88231.0, 2239.0, "5Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_204
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
      "!='8Series' or series !='7Series'"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where series " +
          "!='8Series' or series !='7Series'"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(6805600.0, 1714.635, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(88231.0, 2239.0, "5Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(1070757.0, 1442.0, "8Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_205
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
      "!=2738.562"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
          "!=2738.562"
      ),
      Seq(Row(6805600.0, 1714.635, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(88231.0, 2239.0, "5Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(1070757.0, 1442.0, "8Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_206
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
      "deviceInformationId !=10"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where " +
          "deviceInformationId !=10"
      ),
      Seq(Row(5281803.0, 2738.562, "7Series"),
        Row(8231335.0, 1271.0, "5Series"),
        Row(8978765.0, 692.0, "5Series"),
        Row(3784858.0, 2175.0, "7Series"),
        Row(1602458.0, 136.0, "9Series"),
        Row(9737768.0, 1600.0, "7Series"),
        Row(2919786.0, 505.0, "0Series"),
        Row(9455612.0, 1341.0, "0Series"),
        Row(88231.0, 2239.0, "5Series"),
        Row(1439363.0, 2970.0, "4Series"),
        Row(3940720.0, 2593.0, "1Series"),
        Row(4451217.0, 2572.0, "6Series"),
        Row(335583.0, 1991.0, "9Series"),
        Row(1070757.0, 1442.0, "8Series"),
        Row(2389657.0, 1841.0, "0Series"),
        Row(5986189.0, 298.0, "2Series"),
        Row(8543280.0, 79.0, "3Series"),
        Row(4816260.0, 202.0, "0Series"),
        Row(8453995.0, 568.0, "4Series"),
        Row(2051539.0, 355.0, "1Series"),
        Row(7610075.0, 151.0, "5Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(6495292.0, 1873.0, "3Series"),
        Row(2611464.0, 2205.0, "9Series"),
        Row(574375.0, 441.0, "8Series"),
        Row(4459076.0, 2194.0, "5Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(833654.0, 256.0, "5Series"),
        Row(566917.0, 1778.0, "0Series"),
        Row(832387.0, 1999.0, "5Series"),
        Row(2850246.0, 2194.0, "5Series"),
        Row(6169467.0, 2483.0, "6Series"),
        Row(6533899.0, 1724.0, "0Series"),
        Row(7487134.0, 1768.0, "7Series"),
        Row(4750239.0, 2436.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(7774590.0, 1691.0, "2Series"),
        Row(5586718.0, 2071.0, "7Series"),
        Row(5857263.0, 1333.0, "7Series"),
        Row(6416074.0, 1080.0, "7Series"),
        Row(6994063.0, 1053.0, "1Series"),
        Row(8229807.0, 760.0, "8Series"),
        Row(5797079.0, 2061.0, "2Series"),
        Row(6283062.0, 2142.0, "5Series"),
        Row(8431770.0, 2224.0, "5Series"),
        Row(3311312.0, 1015.0, "7Series"),
        Row(2843881.0, 1229.0, "6Series"),
        Row(1901889.0, 1750.0, "8Series"),
        Row(3077303.0, 1717.0, "5Series"),
        Row(7880439.0, 2078.0, "8Series"),
        Row(3454331.0, 2734.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(3278167.0, 571.0, "9Series"),
        Row(5659107.0, 1697.0, "8Series"),
        Row(9952232.0, 2553.0, "2Series"),
        Row(424923.0, 1077.0, "3Series"),
        Row(7839922.0, 1823.0, "9Series"),
        Row(9500486.0, 2399.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(6190068.0, 1608.0, "8Series"),
        Row(7768468.0, 29.0, "2Series"),
        Row(7236919.0, 1407.0, "2Series"),
        Row(167725.0, 845.0, "6Series"),
        Row(2651084.0, 1655.0, "2Series"),
        Row(6283156.0, 1368.0, "7Series"),
        Row(7342321.0, 1728.0, "7Series"),
        Row(1753823.0, 750.0, "6Series"),
        Row(5451533.0, 2288.0, "9Series"),
        Row(5403108.0, 2635.0, "5Series"),
        Row(168757.0, 1337.0, "4Series"),
        Row(9394732.0, 2478.0, "3Series"),
        Row(7420815.0, 538.0, "8Series"),
        Row(4358621.0, 1407.0, "6Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(9318234.0, 1226.0, "2Series"),
        Row(5565240.0, 865.0, "6Series"),
        Row(3166724.0, 901.0, "0Series"),
        Row(5592457.0, 1864.0, "6Series"),
        Row(7575196.0, 572.0, "4Series"),
        Row(3235086.0, 412.0, "8Series"),
        Row(7917206.0, 1491.0, "8Series"),
        Row(4156339.0, 1350.0, "8Series"),
        Row(4202614.0, 1567.0, "0Series"),
        Row(2199957.0, 1973.0, "0Series"),
        Row(511128.0, 448.0, "4Series"),
        Row(580612.0, 2488.0, "4Series"),
        Row(275342.0, 907.0, "6Series"),
        Row(3215327.0, 2507.0, "3Series"),
        Row(8069859.0, 732.0, "0Series"),
        Row(6383562.0, 2077.0, "3Series"),
        Row(6428516.0, 1434.0, "2Series"),
        Row(5159121.0, 1098.0, "4Series"),
        Row(3360388.0, 813.0, "5Series"),
        Row(5152985.0, 954.0, "9Series"),
        Row(3335480.0, 613.0, "5Series"),
        Row(994815.0, 2348.0, "5Series"),
        Row(507229.0, 2192.0, "0Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_207
  test(
    "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
      ">2738.562"
  )({
    checkAnswer(
      sql(
        "select contractNumber,gamePointId,series from Carbon_automation_test2 where gamePointId " +
          ">2738.562"
      ),
      Seq(Row(1439363.0, 2970.0, "4Series"),
        Row(6663091.0, 2863.0, "4Series"),
        Row(5204739.0, 2972.0, "0Series"),
        Row(8880112.0, 2849.0, "5Series"),
        Row(23250.0, 2745.0, "3Series"),
        Row(1952050.0, 2890.0, "0Series"),
        Row(2362114.0, 2952.0, "9Series"),
        Row(8976568.0, 2826.0, "0Series")
      )
    )
  }
  )

  //TC_217
  test(
    "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY BETWEEN Latest_areaId " +
      "AND  Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY BETWEEN " +
          "Latest_areaId AND  Latest_HOUR"
      ),
      Seq(Row("1AA1000000", 1),
        Row("1AA100005", 1),
        Row("1AA100025", 1),
        Row("1AA100026", 1),
        Row("1AA100027", 1),
        Row("1AA100028", 1),
        Row("1AA100034", 1),
        Row("1AA100039", 1),
        Row("1AA100050", 1),
        Row("1AA100052", 1),
        Row("1AA100061", 1),
        Row("1AA100074", 1)
      )
    )
  }
  )

  //TC_221
  test(
    "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY NOT LIKE Latest_areaId" +
      " AND Latest_DAY NOT LIKE  Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select imei, Latest_DAY from Carbon_automation_test2 where Latest_DAY NOT LIKE " +
          "Latest_areaId AND Latest_DAY NOT LIKE  Latest_HOUR"
      ),
      Seq(Row("1AA1", 1),
        Row("1AA10", 1),
        Row("1AA100", 1),
        Row("1AA1000", 1),
        Row("1AA10000", 1),
        Row("1AA100000", 1),
        Row("1AA100001", 1),
        Row("1AA100002", 1),
        Row("1AA100003", 1),
        Row("1AA100004", 1),
        Row("1AA100006", 1),
        Row("1AA100007", 1),
        Row("1AA100008", 1),
        Row("1AA100009", 1),
        Row("1AA10001", 1),
        Row("1AA100010", 1),
        Row("1AA100011", 1),
        Row("1AA100012", 1),
        Row("1AA100013", 1),
        Row("1AA100014", 1),
        Row("1AA100015", 1),
        Row("1AA100016", 1),
        Row("1AA100017", 1),
        Row("1AA100018", 1),
        Row("1AA100019", 1),
        Row("1AA10002", 1),
        Row("1AA100020", 1),
        Row("1AA100021", 1),
        Row("1AA100022", 1),
        Row("1AA100023", 1),
        Row("1AA100024", 1),
        Row("1AA100029", 1),
        Row("1AA10003", 1),
        Row("1AA100030", 1),
        Row("1AA100031", 1),
        Row("1AA100032", 1),
        Row("1AA100033", 1),
        Row("1AA100035", 1),
        Row("1AA100036", 1),
        Row("1AA100037", 1),
        Row("1AA100038", 1),
        Row("1AA10004", 1),
        Row("1AA100040", 1),
        Row("1AA100041", 1),
        Row("1AA100042", 1),
        Row("1AA100043", 1),
        Row("1AA100044", 1),
        Row("1AA100045", 1),
        Row("1AA100046", 1),
        Row("1AA100047", 1),
        Row("1AA100048", 1),
        Row("1AA100049", 1),
        Row("1AA10005", 1),
        Row("1AA100051", 1),
        Row("1AA100053", 1),
        Row("1AA100054", 1),
        Row("1AA100055", 1),
        Row("1AA100056", 1),
        Row("1AA100057", 1),
        Row("1AA100058", 1),
        Row("1AA100059", 1),
        Row("1AA10006", 1),
        Row("1AA100060", 1),
        Row("1AA100062", 1),
        Row("1AA100063", 1),
        Row("1AA100064", 1),
        Row("1AA100065", 1),
        Row("1AA100066", 1),
        Row("1AA100067", 1),
        Row("1AA100068", 1),
        Row("1AA100069", 1),
        Row("1AA10007", 1),
        Row("1AA100070", 1),
        Row("1AA100071", 1),
        Row("1AA100072", 1),
        Row("1AA100073", 1),
        Row("1AA100075", 1),
        Row("1AA100076", 1),
        Row("1AA100077", 1),
        Row("1AA100078", 1),
        Row("1AA100079", 1),
        Row("1AA10008", 1),
        Row("1AA100080", 1),
        Row("1AA100081", 1),
        Row("1AA100082", 1),
        Row("1AA100083", 1),
        Row("1AA100084", 1)
      )
    )
  }
  )

  //TC_225
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId >505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId >505"),
      Seq(Row("1AA1", 2738.562),
        Row("1AA10", 1714.635),
        Row("1AA100", 1271.0),
        Row("1AA1000", 692.0),
        Row("1AA10000", 2175.0),
        Row("1AA1000000", 1600.0),
        Row("1AA100002", 1341.0),
        Row("1AA100003", 2239.0),
        Row("1AA100004", 2970.0),
        Row("1AA100005", 2593.0),
        Row("1AA100006", 2572.0),
        Row("1AA100007", 1991.0),
        Row("1AA100008", 1442.0),
        Row("1AA100009", 1841.0),
        Row("1AA100012", 568.0),
        Row("1AA100015", 2863.0),
        Row("1AA100016", 1873.0),
        Row("1AA100017", 2205.0),
        Row("1AA100019", 2194.0),
        Row("1AA10002", 2972.0),
        Row("1AA100021", 1778.0),
        Row("1AA100022", 1999.0),
        Row("1AA100023", 2194.0),
        Row("1AA100024", 2483.0),
        Row("1AA100025", 1724.0),
        Row("1AA100026", 1768.0),
        Row("1AA100027", 2436.0),
        Row("1AA100028", 2849.0),
        Row("1AA100029", 1691.0),
        Row("1AA10003", 2071.0),
        Row("1AA100030", 1333.0),
        Row("1AA100031", 1080.0),
        Row("1AA100032", 1053.0),
        Row("1AA100033", 760.0),
        Row("1AA100034", 2061.0),
        Row("1AA100035", 2142.0),
        Row("1AA100036", 2224.0),
        Row("1AA100037", 1015.0),
        Row("1AA100038", 1229.0),
        Row("1AA100039", 1750.0),
        Row("1AA10004", 1717.0),
        Row("1AA100040", 2078.0),
        Row("1AA100041", 2734.0),
        Row("1AA100042", 2745.0),
        Row("1AA100043", 571.0),
        Row("1AA100044", 1697.0),
        Row("1AA100045", 2553.0),
        Row("1AA100046", 1077.0),
        Row("1AA100047", 1823.0),
        Row("1AA100048", 2399.0),
        Row("1AA100049", 2890.0),
        Row("1AA10005", 1608.0),
        Row("1AA100051", 1407.0),
        Row("1AA100052", 845.0),
        Row("1AA100053", 1655.0),
        Row("1AA100054", 1368.0),
        Row("1AA100055", 1728.0),
        Row("1AA100056", 750.0),
        Row("1AA100057", 2288.0),
        Row("1AA100058", 2635.0),
        Row("1AA100059", 1337.0),
        Row("1AA10006", 2478.0),
        Row("1AA100060", 538.0),
        Row("1AA100061", 1407.0),
        Row("1AA100062", 2952.0),
        Row("1AA100063", 1226.0),
        Row("1AA100064", 865.0),
        Row("1AA100065", 901.0),
        Row("1AA100066", 1864.0),
        Row("1AA100067", 572.0),
        Row("1AA100069", 1491.0),
        Row("1AA10007", 1350.0),
        Row("1AA100070", 1567.0),
        Row("1AA100071", 1973.0),
        Row("1AA100073", 2488.0),
        Row("1AA100074", 907.0),
        Row("1AA100075", 2507.0),
        Row("1AA100076", 732.0),
        Row("1AA100077", 2077.0),
        Row("1AA100078", 1434.0),
        Row("1AA100079", 1098.0),
        Row("1AA10008", 813.0),
        Row("1AA100080", 954.0),
        Row("1AA100081", 613.0),
        Row("1AA100082", 2348.0),
        Row("1AA100083", 2192.0),
        Row("1AA100084", 2826.0)
      )
    )
  }
  )

  //TC_226
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId <505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId <505"),
      Seq(Row("1AA100000", 136.0),
        Row("1AA10001", 298.0),
        Row("1AA100010", 79.0),
        Row("1AA100011", 202.0),
        Row("1AA100013", 355.0),
        Row("1AA100014", 151.0),
        Row("1AA100018", 441.0),
        Row("1AA100020", 256.0),
        Row("1AA100050", 29.0),
        Row("1AA100068", 412.0),
        Row("1AA100072", 448.0)
      )
    )
  }
  )

  //TC_227
  test("select imei,gamePointId from Carbon_automation_test2 where channelsId <2")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where channelsId <2"),
      Seq(Row("1AA10000", 2175.0),
        Row("1AA100005", 2593.0),
        Row("1AA100008", 1442.0),
        Row("1AA100011", 202.0),
        Row("1AA100015", 2863.0),
        Row("1AA100025", 1724.0),
        Row("1AA100031", 1080.0),
        Row("1AA100039", 1750.0),
        Row("1AA100041", 2734.0),
        Row("1AA100047", 1823.0),
        Row("1AA100048", 2399.0),
        Row("1AA100050", 29.0)
      )
    )
  }
  )

  //TC_228
  test("select imei,gamePointId from Carbon_automation_test2 where channelsId >2")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where channelsId >2"),
      Seq(Row("1AA1", 2738.562),
        Row("1AA10", 1714.635),
        Row("1AA100", 1271.0),
        Row("1AA1000", 692.0),
        Row("1AA100000", 136.0),
        Row("1AA1000000", 1600.0),
        Row("1AA100001", 505.0),
        Row("1AA100002", 1341.0),
        Row("1AA100003", 2239.0),
        Row("1AA100007", 1991.0),
        Row("1AA100009", 1841.0),
        Row("1AA10001", 298.0),
        Row("1AA100010", 79.0),
        Row("1AA100013", 355.0),
        Row("1AA100014", 151.0),
        Row("1AA100016", 1873.0),
        Row("1AA100017", 2205.0),
        Row("1AA100018", 441.0),
        Row("1AA100019", 2194.0),
        Row("1AA10002", 2972.0),
        Row("1AA100020", 256.0),
        Row("1AA100021", 1778.0),
        Row("1AA100022", 1999.0),
        Row("1AA100023", 2194.0),
        Row("1AA100026", 1768.0),
        Row("1AA100027", 2436.0),
        Row("1AA100028", 2849.0),
        Row("1AA100029", 1691.0),
        Row("1AA10003", 2071.0),
        Row("1AA100030", 1333.0),
        Row("1AA100032", 1053.0),
        Row("1AA100033", 760.0),
        Row("1AA100034", 2061.0),
        Row("1AA100035", 2142.0),
        Row("1AA100036", 2224.0),
        Row("1AA100037", 1015.0),
        Row("1AA100038", 1229.0),
        Row("1AA10004", 1717.0),
        Row("1AA100040", 2078.0),
        Row("1AA100042", 2745.0),
        Row("1AA100043", 571.0),
        Row("1AA100045", 2553.0),
        Row("1AA100046", 1077.0),
        Row("1AA100049", 2890.0),
        Row("1AA100051", 1407.0),
        Row("1AA100052", 845.0),
        Row("1AA100053", 1655.0),
        Row("1AA100055", 1728.0),
        Row("1AA100056", 750.0),
        Row("1AA100057", 2288.0),
        Row("1AA100058", 2635.0),
        Row("1AA100059", 1337.0),
        Row("1AA10006", 2478.0),
        Row("1AA100060", 538.0),
        Row("1AA100061", 1407.0),
        Row("1AA100062", 2952.0),
        Row("1AA100063", 1226.0),
        Row("1AA100064", 865.0),
        Row("1AA100065", 901.0),
        Row("1AA100066", 1864.0),
        Row("1AA100067", 572.0),
        Row("1AA100068", 412.0),
        Row("1AA100069", 1491.0),
        Row("1AA10007", 1350.0),
        Row("1AA100070", 1567.0),
        Row("1AA100071", 1973.0),
        Row("1AA100072", 448.0),
        Row("1AA100074", 907.0),
        Row("1AA100075", 2507.0),
        Row("1AA100076", 732.0),
        Row("1AA100077", 2077.0),
        Row("1AA100078", 1434.0),
        Row("1AA100079", 1098.0),
        Row("1AA10008", 813.0),
        Row("1AA100080", 954.0),
        Row("1AA100082", 2348.0),
        Row("1AA100084", 2826.0)
      )
    )
  }
  )

  //TC_229
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId >=505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId >=505"),
      Seq(Row("1AA1", 2738.562),
        Row("1AA10", 1714.635),
        Row("1AA100", 1271.0),
        Row("1AA1000", 692.0),
        Row("1AA10000", 2175.0),
        Row("1AA1000000", 1600.0),
        Row("1AA100001", 505.0),
        Row("1AA100002", 1341.0),
        Row("1AA100003", 2239.0),
        Row("1AA100004", 2970.0),
        Row("1AA100005", 2593.0),
        Row("1AA100006", 2572.0),
        Row("1AA100007", 1991.0),
        Row("1AA100008", 1442.0),
        Row("1AA100009", 1841.0),
        Row("1AA100012", 568.0),
        Row("1AA100015", 2863.0),
        Row("1AA100016", 1873.0),
        Row("1AA100017", 2205.0),
        Row("1AA100019", 2194.0),
        Row("1AA10002", 2972.0),
        Row("1AA100021", 1778.0),
        Row("1AA100022", 1999.0),
        Row("1AA100023", 2194.0),
        Row("1AA100024", 2483.0),
        Row("1AA100025", 1724.0),
        Row("1AA100026", 1768.0),
        Row("1AA100027", 2436.0),
        Row("1AA100028", 2849.0),
        Row("1AA100029", 1691.0),
        Row("1AA10003", 2071.0),
        Row("1AA100030", 1333.0),
        Row("1AA100031", 1080.0),
        Row("1AA100032", 1053.0),
        Row("1AA100033", 760.0),
        Row("1AA100034", 2061.0),
        Row("1AA100035", 2142.0),
        Row("1AA100036", 2224.0),
        Row("1AA100037", 1015.0),
        Row("1AA100038", 1229.0),
        Row("1AA100039", 1750.0),
        Row("1AA10004", 1717.0),
        Row("1AA100040", 2078.0),
        Row("1AA100041", 2734.0),
        Row("1AA100042", 2745.0),
        Row("1AA100043", 571.0),
        Row("1AA100044", 1697.0),
        Row("1AA100045", 2553.0),
        Row("1AA100046", 1077.0),
        Row("1AA100047", 1823.0),
        Row("1AA100048", 2399.0),
        Row("1AA100049", 2890.0),
        Row("1AA10005", 1608.0),
        Row("1AA100051", 1407.0),
        Row("1AA100052", 845.0),
        Row("1AA100053", 1655.0),
        Row("1AA100054", 1368.0),
        Row("1AA100055", 1728.0),
        Row("1AA100056", 750.0),
        Row("1AA100057", 2288.0),
        Row("1AA100058", 2635.0),
        Row("1AA100059", 1337.0),
        Row("1AA10006", 2478.0),
        Row("1AA100060", 538.0),
        Row("1AA100061", 1407.0),
        Row("1AA100062", 2952.0),
        Row("1AA100063", 1226.0),
        Row("1AA100064", 865.0),
        Row("1AA100065", 901.0),
        Row("1AA100066", 1864.0),
        Row("1AA100067", 572.0),
        Row("1AA100069", 1491.0),
        Row("1AA10007", 1350.0),
        Row("1AA100070", 1567.0),
        Row("1AA100071", 1973.0),
        Row("1AA100073", 2488.0),
        Row("1AA100074", 907.0),
        Row("1AA100075", 2507.0),
        Row("1AA100076", 732.0),
        Row("1AA100077", 2077.0),
        Row("1AA100078", 1434.0),
        Row("1AA100079", 1098.0),
        Row("1AA10008", 813.0),
        Row("1AA100080", 954.0),
        Row("1AA100081", 613.0),
        Row("1AA100082", 2348.0),
        Row("1AA100083", 2192.0),
        Row("1AA100084", 2826.0)
      )
    )
  }
  )

  //TC_230
  test("select imei,gamePointId from Carbon_automation_test2 where gamePointId <=505")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where gamePointId <=505"),
      Seq(Row("1AA100000", 136.0),
        Row("1AA100001", 505.0),
        Row("1AA10001", 298.0),
        Row("1AA100010", 79.0),
        Row("1AA100011", 202.0),
        Row("1AA100013", 355.0),
        Row("1AA100014", 151.0),
        Row("1AA100018", 441.0),
        Row("1AA100020", 256.0),
        Row("1AA100050", 29.0),
        Row("1AA100068", 412.0),
        Row("1AA100072", 448.0)
      )
    )
  }
  )

  //TC_231
  test("select imei,gamePointId from Carbon_automation_test2 where channelsId <=2")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where channelsId <=2"),
      Seq(Row("1AA10000", 2175.0),
        Row("1AA100004", 2970.0),
        Row("1AA100005", 2593.0),
        Row("1AA100006", 2572.0),
        Row("1AA100008", 1442.0),
        Row("1AA100011", 202.0),
        Row("1AA100012", 568.0),
        Row("1AA100015", 2863.0),
        Row("1AA100024", 2483.0),
        Row("1AA100025", 1724.0),
        Row("1AA100031", 1080.0),
        Row("1AA100039", 1750.0),
        Row("1AA100041", 2734.0),
        Row("1AA100044", 1697.0),
        Row("1AA100047", 1823.0),
        Row("1AA100048", 2399.0),
        Row("1AA10005", 1608.0),
        Row("1AA100050", 29.0),
        Row("1AA100054", 1368.0),
        Row("1AA100073", 2488.0),
        Row("1AA100081", 613.0),
        Row("1AA100083", 2192.0)
      )
    )
  }
  )

  //TC_232
  test("select imei,gamePointId from Carbon_automation_test2 where channelsId >=2")({
    checkAnswer(
      sql("select imei,gamePointId from Carbon_automation_test2 where channelsId >=2"),
      Seq(Row("1AA1", 2738.562),
        Row("1AA10", 1714.635),
        Row("1AA100", 1271.0),
        Row("1AA1000", 692.0),
        Row("1AA100000", 136.0),
        Row("1AA1000000", 1600.0),
        Row("1AA100001", 505.0),
        Row("1AA100002", 1341.0),
        Row("1AA100003", 2239.0),
        Row("1AA100004", 2970.0),
        Row("1AA100006", 2572.0),
        Row("1AA100007", 1991.0),
        Row("1AA100009", 1841.0),
        Row("1AA10001", 298.0),
        Row("1AA100010", 79.0),
        Row("1AA100012", 568.0),
        Row("1AA100013", 355.0),
        Row("1AA100014", 151.0),
        Row("1AA100016", 1873.0),
        Row("1AA100017", 2205.0),
        Row("1AA100018", 441.0),
        Row("1AA100019", 2194.0),
        Row("1AA10002", 2972.0),
        Row("1AA100020", 256.0),
        Row("1AA100021", 1778.0),
        Row("1AA100022", 1999.0),
        Row("1AA100023", 2194.0),
        Row("1AA100024", 2483.0),
        Row("1AA100026", 1768.0),
        Row("1AA100027", 2436.0),
        Row("1AA100028", 2849.0),
        Row("1AA100029", 1691.0),
        Row("1AA10003", 2071.0),
        Row("1AA100030", 1333.0),
        Row("1AA100032", 1053.0),
        Row("1AA100033", 760.0),
        Row("1AA100034", 2061.0),
        Row("1AA100035", 2142.0),
        Row("1AA100036", 2224.0),
        Row("1AA100037", 1015.0),
        Row("1AA100038", 1229.0),
        Row("1AA10004", 1717.0),
        Row("1AA100040", 2078.0),
        Row("1AA100042", 2745.0),
        Row("1AA100043", 571.0),
        Row("1AA100044", 1697.0),
        Row("1AA100045", 2553.0),
        Row("1AA100046", 1077.0),
        Row("1AA100049", 2890.0),
        Row("1AA10005", 1608.0),
        Row("1AA100051", 1407.0),
        Row("1AA100052", 845.0),
        Row("1AA100053", 1655.0),
        Row("1AA100054", 1368.0),
        Row("1AA100055", 1728.0),
        Row("1AA100056", 750.0),
        Row("1AA100057", 2288.0),
        Row("1AA100058", 2635.0),
        Row("1AA100059", 1337.0),
        Row("1AA10006", 2478.0),
        Row("1AA100060", 538.0),
        Row("1AA100061", 1407.0),
        Row("1AA100062", 2952.0),
        Row("1AA100063", 1226.0),
        Row("1AA100064", 865.0),
        Row("1AA100065", 901.0),
        Row("1AA100066", 1864.0),
        Row("1AA100067", 572.0),
        Row("1AA100068", 412.0),
        Row("1AA100069", 1491.0),
        Row("1AA10007", 1350.0),
        Row("1AA100070", 1567.0),
        Row("1AA100071", 1973.0),
        Row("1AA100072", 448.0),
        Row("1AA100073", 2488.0),
        Row("1AA100074", 907.0),
        Row("1AA100075", 2507.0),
        Row("1AA100076", 732.0),
        Row("1AA100077", 2077.0),
        Row("1AA100078", 1434.0),
        Row("1AA100079", 1098.0),
        Row("1AA10008", 813.0),
        Row("1AA100080", 954.0),
        Row("1AA100081", 613.0),
        Row("1AA100082", 2348.0),
        Row("1AA100083", 2192.0),
        Row("1AA100084", 2826.0)
      )
    )
  }
  )

  //TC_238
  test(
    "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
      "(gamePointId==2738.562)"
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) and " +
          "(gamePointId==2738.562)"
      ),
      Seq(Row("1AA1"))
    )
  }
  )

  //TC_239
  test(
    "select deliveryCity from Carbon_automation_test2 where  (deliveryCity == 'yichang') and ( " +
      "deliveryStreet=='yichang')"
  )({
    checkAnswer(
      sql(
        "select deliveryCity from Carbon_automation_test2 where  (deliveryCity == 'yichang') and " +
          "( deliveryStreet=='yichang')"
      ),
      Seq(Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang")
      )
    )
  }
  )

  //TC_240
  test(
    "select channelsId  from Carbon_automation_test2 where  (channelsId == '4') and " +
      "(gamePointId==2738.562)"
  )({
    checkAnswer(
      sql(
        "select channelsId  from Carbon_automation_test2 where  (channelsId == '4') and " +
          "(gamePointId==2738.562)"
      ),
      Seq(Row("4"))
    )
  }
  )

  //TC_241
  test(
    "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) OR " +
      "(gamePointId==2738.562) order by contractNumber"
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where  (contractNumber == 5281803) OR " +
          "(gamePointId==2738.562) order by contractNumber"
      ),
      Seq(Row("1AA1"))
    )
  }
  )

  //TC_242
  test(
    "select channelsId from Carbon_automation_test2 where  (channelsId == '4') OR " +
      "(gamePointId==2738.562) order by channelsId"
  )({
    checkAnswer(
      sql(
        "select channelsId from Carbon_automation_test2 where  (channelsId == '4') OR " +
          "(gamePointId==2738.562) order by channelsId"
      ),
      Seq(Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4")
      )
    )
  }
  )

  //TC_243
  test(
    "select deliveryCity  from Carbon_automation_test2 where  (deliveryCity == 'yichang') OR ( " +
      "deliveryStreet=='yichang') order by deliveryCity"
  )({
    checkAnswer(
      sql(
        "select deliveryCity  from Carbon_automation_test2 where  (deliveryCity == 'yichang') OR " +
          "( deliveryStreet=='yichang') order by deliveryCity"
      ),
      Seq(Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang")
      )
    )
  }
  )

  //TC_244
  test(
    "select imei, gamePointId from Carbon_automation_test2 where contractNumber in (5281803) and " +
      "gamePointId IN (2738.562) ORDER BY gamePointId"
  )({
    checkAnswer(
      sql(
        "select imei, gamePointId from Carbon_automation_test2 where contractNumber in (5281803) " +
          "and gamePointId IN (2738.562) ORDER BY gamePointId"
      ),
      Seq(Row("1AA1", 2738.562))
    )
  }
  )

  //TC_245
  test(
    "select channelsId from Carbon_automation_test2 where  channelsId in ('4') or gamePointId IN " +
      "(2738.562) ORDER BY channelsId"
  )({
    checkAnswer(
      sql(
        "select channelsId from Carbon_automation_test2 where  channelsId in ('4') or gamePointId" +
          " IN (2738.562) ORDER BY channelsId"
      ),
      Seq(Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4"),
        Row("4")
      )
    )
  }
  )

  //TC_246
  test(
    "select deliveryCity from Carbon_automation_test2 where  deliveryCity IN ('yichang') AND  " +
      "deliveryStreet IN ('yichang') ORDER BY deliveryCity"
  )({
    checkAnswer(
      sql(
        "select deliveryCity from Carbon_automation_test2 where  deliveryCity IN ('yichang') AND " +
          " deliveryStreet IN ('yichang') ORDER BY deliveryCity"
      ),
      Seq(Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang"),
        Row("yichang")
      )
    )
  }
  )

  //TC_247
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId > " +
      "4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId > 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100010", 79.0, "6", "3Series"),
        Row("1AA100000", 136.0, "6", "9Series"),
        Row("1AA100020", 256.0, "7", "5Series"),
        Row("1AA10001", 298.0, "5", "2Series"),
        Row("1AA100013", 355.0, "6", "1Series")
      )
    )
  }
  )

  //TC_248
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId < " +
      "4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId < 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100050", 29.0, "1", "2Series"),
        Row("1AA100014", 151.0, "3", "5Series"),
        Row("1AA100011", 202.0, "1", "0Series"),
        Row("1AA100012", 568.0, "2", "4Series"),
        Row("1AA100081", 613.0, "2", "5Series")
      )
    )
  }
  )

  //TC_249
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId >=" +
      " 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId >= 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100010", 79.0, "6", "3Series"),
        Row("1AA100000", 136.0, "6", "9Series"),
        Row("1AA100020", 256.0, "7", "5Series"),
        Row("1AA10001", 298.0, "5", "2Series"),
        Row("1AA100013", 355.0, "6", "1Series")
      )
    )
  }
  )

  //TC_250
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId <=" +
      " 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId <= 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100050", 29.0, "1", "2Series"),
        Row("1AA100014", 151.0, "3", "5Series"),
        Row("1AA100011", 202.0, "1", "0Series"),
        Row("1AA100018", 441.0, "4", "8Series"),
        Row("1AA100060", 538.0, "4", "8Series")
      )
    )
  }
  )

  //TC_251
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA10001", 298.0, "5", "2Series"),
        Row("1AA100018", 441.0, "4", "8Series"),
        Row("1AA100060", 538.0, "4", "8Series"),
        Row("1AA100056", 750.0, "5", "6Series"),
        Row("1AA10008", 813.0, "4", "5Series")
      )
    )
  }
  )

  //TC_252
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT BETWEEN 4 AND 5 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100050", 29.0, "1", "2Series"),
        Row("1AA100010", 79.0, "6", "3Series"),
        Row("1AA100000", 136.0, "6", "9Series"),
        Row("1AA100014", 151.0, "3", "5Series"),
        Row("1AA100011", 202.0, "1", "0Series")
      )
    )
  }
  )

  //TC_253
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "LIKE 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId LIKE 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100018", 441.0, "4", "8Series"),
        Row("1AA100060", 538.0, "4", "8Series"),
        Row("1AA10008", 813.0, "4", "5Series"),
        Row("1AA100046", 1077.0, "4", "3Series"),
        Row("1AA10", 1714.635, "4", "7Series")
      )
    )
  }
  )

  //TC_254
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT LIKE 4 AND channelsId NOT LIKE 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT LIKE 4 AND channelsId NOT LIKE 5 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100050", 29.0, "1", "2Series"),
        Row("1AA100010", 79.0, "6", "3Series"),
        Row("1AA100000", 136.0, "6", "9Series"),
        Row("1AA100014", 151.0, "3", "5Series"),
        Row("1AA100011", 202.0, "1", "0Series")
      )
    )
  }
  )

  //TC_255
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "RLIKE 4 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId RLIKE 4 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100018", 441.0, "4", "8Series"),
        Row("1AA100060", 538.0, "4", "8Series"),
        Row("1AA10008", 813.0, "4", "5Series"),
        Row("1AA100046", 1077.0, "4", "3Series"),
        Row("1AA10", 1714.635, "4", "7Series")
      )
    )
  }
  )

  //TC_256
  test(
    "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  channelsId " +
      "NOT RLIKE 4 AND channelsId NOT RLIKE 5 ORDER BY gamePointId limit 5"
  )({
    checkAnswer(
      sql(
        "select imei,gamePointId, channelsId,series from Carbon_automation_test2 where  " +
          "channelsId NOT RLIKE 4 AND channelsId NOT RLIKE 5 ORDER BY gamePointId limit 5"
      ),
      Seq(Row("1AA100050", 29.0, "1", "2Series"),
        Row("1AA100010", 79.0, "6", "3Series"),
        Row("1AA100000", 136.0, "6", "9Series"),
        Row("1AA100014", 151.0, "3", "5Series"),
        Row("1AA100011", 202.0, "1", "0Series")
      )
    )
  }
  )

  //TC_257
  test(
    "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' and " +
      "internalModels='8Internal models') OR (series='7Series' and internalModels='7Internal " +
      "models')"
  )({
    checkAnswer(
      sql(
        "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' " +
          "and internalModels='8Internal models') OR (series='7Series' and " +
          "internalModels='7Internal models')"
      ),
      Seq(Row("1AA1000000", "7Internal models", "7Series"),
        Row("1AA100008", "8Internal models", "8Series"),
        Row("1AA100040", "8Internal models", "8Series")
      )
    )
  }
  )

  //TC_258
  test(
    "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' or " +
      "internalModels='8Internal models') or (series='7Series' or internalModels='7Internal " +
      "models')"
  )({
    checkAnswer(
      sql(
        "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' " +
          "or internalModels='8Internal models') or (series='7Series' or " +
          "internalModels='7Internal models')"
      ),
      Seq(Row("1AA1", "9Internal models", "7Series"),
        Row("1AA10", "2Internal models", "7Series"),
        Row("1AA1000", "7Internal models", "5Series"),
        Row("1AA10000", "4Internal models", "7Series"),
        Row("1AA1000000", "7Internal models", "7Series"),
        Row("1AA100003", "8Internal models", "5Series"),
        Row("1AA100007", "8Internal models", "9Series"),
        Row("1AA100008", "8Internal models", "8Series"),
        Row("1AA100011", "7Internal models", "0Series"),
        Row("1AA100018", "3Internal models", "8Series"),
        Row("1AA100020", "8Internal models", "5Series"),
        Row("1AA100021", "7Internal models", "0Series"),
        Row("1AA100022", "7Internal models", "5Series"),
        Row("1AA100025", "8Internal models", "0Series"),
        Row("1AA100026", "5Internal models", "7Series"),
        Row("1AA100029", "7Internal models", "2Series"),
        Row("1AA10003", "2Internal models", "7Series"),
        Row("1AA100030", "1Internal models", "7Series"),
        Row("1AA100031", "4Internal models", "7Series"),
        Row("1AA100033", "1Internal models", "8Series"),
        Row("1AA100037", "4Internal models", "7Series"),
        Row("1AA100039", "9Internal models", "8Series"),
        Row("1AA100040", "8Internal models", "8Series"),
        Row("1AA100044", "4Internal models", "8Series"),
        Row("1AA100047", "8Internal models", "9Series"),
        Row("1AA100048", "8Internal models", "3Series"),
        Row("1AA10005", "5Internal models", "8Series"),
        Row("1AA100051", "8Internal models", "2Series"),
        Row("1AA100054", "2Internal models", "7Series"),
        Row("1AA100055", "8Internal models", "7Series"),
        Row("1AA100060", "0Internal models", "8Series"),
        Row("1AA100063", "8Internal models", "2Series"),
        Row("1AA100064", "7Internal models", "6Series"),
        Row("1AA100068", "6Internal models", "8Series"),
        Row("1AA100069", "1Internal models", "8Series"),
        Row("1AA10007", "7Internal models", "8Series"),
        Row("1AA100078", "7Internal models", "2Series"),
        Row("1AA10008", "7Internal models", "5Series"),
        Row("1AA100084", "7Internal models", "0Series")
      )
    )
  }
  )

  //TC_259
  test(
    "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' or " +
      "internalModels='8Internal models') and (series='7Series' or internalModels='7Internal " +
      "models')"
  )({
    checkAnswer(
      sql(
        "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' " +
          "or internalModels='8Internal models') and (series='7Series' or " +
          "internalModels='7Internal models')"
      ),
      Seq(Row("1AA100055", "8Internal models", "7Series"),
        Row("1AA10007", "7Internal models", "8Series")
      )
    )
  }
  )

  //TC_260
  test(
    "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' and " +
      "internalModels='8Internal models') or (deviceInformationId is not NULL)"
  )({
    checkAnswer(
      sql(
        "select imei,internalModels,series from Carbon_automation_test2 where (series='8Series' " +
          "and internalModels='8Internal models') or (deviceInformationId is not NULL)"
      ),
      Seq(Row("1AA1", "9Internal models", "7Series"),
        Row("1AA10", "2Internal models", "7Series"),
        Row("1AA100", "0Internal models", "5Series"),
        Row("1AA1000", "7Internal models", "5Series"),
        Row("1AA10000", "4Internal models", "7Series"),
        Row("1AA100000", "9Internal models", "9Series"),
        Row("1AA1000000", "7Internal models", "7Series"),
        Row("1AA100001", "2Internal models", "0Series"),
        Row("1AA100002", "2Internal models", "0Series"),
        Row("1AA100003", "8Internal models", "5Series"),
        Row("1AA100004", "0Internal models", "4Series"),
        Row("1AA100005", "9Internal models", "1Series"),
        Row("1AA100006", "9Internal models", "6Series"),
        Row("1AA100007", "8Internal models", "9Series"),
        Row("1AA100008", "8Internal models", "8Series"),
        Row("1AA100009", "1Internal models", "0Series"),
        Row("1AA10001", "2Internal models", "2Series"),
        Row("1AA100010", "6Internal models", "3Series"),
        Row("1AA100011", "7Internal models", "0Series"),
        Row("1AA100012", "5Internal models", "4Series"),
        Row("1AA100013", "3Internal models", "1Series"),
        Row("1AA100014", "5Internal models", "5Series"),
        Row("1AA100015", "0Internal models", "4Series"),
        Row("1AA100016", "9Internal models", "3Series"),
        Row("1AA100017", "1Internal models", "9Series"),
        Row("1AA100018", "3Internal models", "8Series"),
        Row("1AA100019", "2Internal models", "5Series"),
        Row("1AA10002", "6Internal models", "0Series"),
        Row("1AA100020", "8Internal models", "5Series"),
        Row("1AA100021", "7Internal models", "0Series"),
        Row("1AA100022", "7Internal models", "5Series"),
        Row("1AA100023", "0Internal models", "5Series"),
        Row("1AA100024", "6Internal models", "6Series"),
        Row("1AA100025", "8Internal models", "0Series"),
        Row("1AA100026", "5Internal models", "7Series"),
        Row("1AA100027", "2Internal models", "0Series"),
        Row("1AA100028", "1Internal models", "5Series"),
        Row("1AA100029", "7Internal models", "2Series"),
        Row("1AA10003", "2Internal models", "7Series"),
        Row("1AA100030", "1Internal models", "7Series"),
        Row("1AA100031", "4Internal models", "7Series"),
        Row("1AA100032", "3Internal models", "1Series"),
        Row("1AA100033", "1Internal models", "8Series"),
        Row("1AA100034", "5Internal models", "2Series"),
        Row("1AA100035", "0Internal models", "5Series"),
        Row("1AA100036", "6Internal models", "5Series"),
        Row("1AA100037", "4Internal models", "7Series"),
        Row("1AA100038", "3Internal models", "6Series"),
        Row("1AA100039", "9Internal models", "8Series"),
        Row("1AA10004", "1Internal models", "5Series"),
        Row("1AA100040", "8Internal models", "8Series"),
        Row("1AA100041", "1Internal models", "5Series"),
        Row("1AA100042", "1Internal models", "3Series"),
        Row("1AA100043", "3Internal models", "9Series"),
        Row("1AA100044", "4Internal models", "8Series"),
        Row("1AA100045", "0Internal models", "2Series"),
        Row("1AA100046", "4Internal models", "3Series"),
        Row("1AA100047", "8Internal models", "9Series"),
        Row("1AA100048", "8Internal models", "3Series"),
        Row("1AA100049", "2Internal models", "0Series"),
        Row("1AA10005", "5Internal models", "8Series"),
        Row("1AA100050", "9Internal models", "2Series"),
        Row("1AA100051", "8Internal models", "2Series"),
        Row("1AA100052", "3Internal models", "6Series"),
        Row("1AA100053", "1Internal models", "2Series"),
        Row("1AA100054", "2Internal models", "7Series"),
        Row("1AA100055", "8Internal models", "7Series"),
        Row("1AA100056", "3Internal models", "6Series"),
        Row("1AA100057", "3Internal models", "9Series"),
        Row("1AA100058", "9Internal models", "5Series"),
        Row("1AA100059", "3Internal models", "4Series"),
        Row("1AA10006", "6Internal models", "3Series"),
        Row("1AA100060", "0Internal models", "8Series"),
        Row("1AA100061", "2Internal models", "6Series"),
        Row("1AA100062", "3Internal models", "9Series"),
        Row("1AA100063", "8Internal models", "2Series"),
        Row("1AA100064", "7Internal models", "6Series"),
        Row("1AA100065", "5Internal models", "0Series"),
        Row("1AA100066", "3Internal models", "6Series"),
        Row("1AA100067", "3Internal models", "4Series"),
        Row("1AA100068", "6Internal models", "8Series"),
        Row("1AA100069", "1Internal models", "8Series"),
        Row("1AA10007", "7Internal models", "8Series"),
        Row("1AA100070", "2Internal models", "0Series"),
        Row("1AA100071", "0Internal models", "0Series"),
        Row("1AA100072", "0Internal models", "4Series"),
        Row("1AA100073", "5Internal models", "4Series"),
        Row("1AA100074", "3Internal models", "6Series"),
        Row("1AA100075", "3Internal models", "3Series"),
        Row("1AA100076", "0Internal models", "0Series"),
        Row("1AA100077", "5Internal models", "3Series"),
        Row("1AA100078", "7Internal models", "2Series"),
        Row("1AA100079", "5Internal models", "4Series"),
        Row("1AA10008", "7Internal models", "5Series"),
        Row("1AA100080", "4Internal models", "9Series"),
        Row("1AA100081", "4Internal models", "5Series"),
        Row("1AA100082", "9Internal models", "5Series"),
        Row("1AA100083", "9Internal models", "0Series"),
        Row("1AA100084", "7Internal models", "0Series")
      )
    )
  }
  )

  //TC_261
  test(
    "select  imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE'"
  )({
    checkAnswer(
      sql(
        "select  imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG " +
          "PROVINCE'"
      ),
      Seq(Row("1AA1"),
        Row("1AA1000000"),
        Row("1AA100001"),
        Row("1AA100002"),
        Row("1AA100005"),
        Row("1AA100008"),
        Row("1AA100015"),
        Row("1AA100019"),
        Row("1AA100020"),
        Row("1AA100022"),
        Row("1AA100025"),
        Row("1AA100026"),
        Row("1AA100027"),
        Row("1AA100028"),
        Row("1AA100032"),
        Row("1AA100033"),
        Row("1AA100034"),
        Row("1AA100037"),
        Row("1AA100039"),
        Row("1AA100044"),
        Row("1AA100048"),
        Row("1AA10005"),
        Row("1AA100050"),
        Row("1AA100052"),
        Row("1AA100054"),
        Row("1AA100061"),
        Row("1AA10007"),
        Row("1AA100074"),
        Row("1AA100075"),
        Row("1AA100077"),
        Row("1AA100082")
      )
    )
  }
  )


  //TC_266
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"\" GROUP BY AMSize, ActiveAreaId ORDER " +
      "BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"\" GROUP BY AMSize, ActiveAreaId " +
          "ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq(Row("0RAM size", "1", 2849.0),
        Row("0RAM size", "2", 79.0),
        Row("0RAM size", "3", 7663.0),
        Row("0RAM size", "5", 1341.0),
        Row("0RAM size", "6", 6082.0),
        Row("1RAM size", "1", 256.0),
        Row("1RAM size", "2", 1333.0),
        Row("1RAM size", "4", 7510.0),
        Row("1RAM size", "5", 2745.0),
        Row("1RAM size", "7", 3942.0),
        Row("2RAM size", "3", 1973.0),
        Row("2RAM size", "4", 1350.0),
        Row("3RAM size", "1", 6640.0),
        Row("3RAM size", "2", 1999.0),
        Row("3RAM size", "3", 2863.0),
        Row("3RAM size", "4", 3824.0),
        Row("3RAM size", "5", 5699.0),
        Row("3RAM size", "6", 2635.0),
        Row("3RAM size", "7", 1491.0),
        Row("4RAM size", "1", 2255.0),
        Row("4RAM size", "2", 1728.0),
        Row("4RAM size", "3", 9130.0),
        Row("4RAM size", "4", 11560.0),
        Row("4RAM size", "6", 5344.635),
        Row("4RAM size", "7", 1338.0),
        Row("5RAM size", "2", 4712.0),
        Row("5RAM size", "3", 2769.0),
        Row("5RAM size", "6", 2478.0),
        Row("6RAM size", "1", 2142.0),
        Row("6RAM size", "2", 1768.0),
        Row("6RAM size", "3", 2633.0),
        Row("6RAM size", "4", 866.0),
        Row("6RAM size", "5", 2952.0),
        Row("6RAM size", "6", 3257.0),
        Row("7RAM size", "3", 151.0),
        Row("7RAM size", "5", 2239.0),
        Row("7RAM size", "6", 3979.0),
        Row("7RAM size", "7", 2031.0),
        Row("8RAM size", "1", 355.0),
        Row("8RAM size", "2", 2738.562),
        Row("8RAM size", "4", 3102.0),
        Row("8RAM size", "5", 2684.0),
        Row("8RAM size", "6", 2970.0),
        Row("8RAM size", "7", 5166.0),
        Row("9RAM size", "1", 3065.0),
        Row("9RAM size", "3", 3239.0),
        Row("9RAM size", "4", 5821.0),
        Row("9RAM size", "6", 1567.0),
        Row("9RAM size", "7", 571.0)
      )
    )
  }
  )

  //TC_267
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"1RAM size\" GROUP BY AMSize, " +
      "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize > \"1RAM size\" GROUP BY AMSize, " +
          "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq(Row("2RAM size", "3", 1973.0),
        Row("2RAM size", "4", 1350.0),
        Row("3RAM size", "1", 6640.0),
        Row("3RAM size", "2", 1999.0),
        Row("3RAM size", "3", 2863.0),
        Row("3RAM size", "4", 3824.0),
        Row("3RAM size", "5", 5699.0),
        Row("3RAM size", "6", 2635.0),
        Row("3RAM size", "7", 1491.0),
        Row("4RAM size", "1", 2255.0),
        Row("4RAM size", "2", 1728.0),
        Row("4RAM size", "3", 9130.0),
        Row("4RAM size", "4", 11560.0),
        Row("4RAM size", "6", 5344.635),
        Row("4RAM size", "7", 1338.0),
        Row("5RAM size", "2", 4712.0),
        Row("5RAM size", "3", 2769.0),
        Row("5RAM size", "6", 2478.0),
        Row("6RAM size", "1", 2142.0),
        Row("6RAM size", "2", 1768.0),
        Row("6RAM size", "3", 2633.0),
        Row("6RAM size", "4", 866.0),
        Row("6RAM size", "5", 2952.0),
        Row("6RAM size", "6", 3257.0),
        Row("7RAM size", "3", 151.0),
        Row("7RAM size", "5", 2239.0),
        Row("7RAM size", "6", 3979.0),
        Row("7RAM size", "7", 2031.0),
        Row("8RAM size", "1", 355.0),
        Row("8RAM size", "2", 2738.562),
        Row("8RAM size", "4", 3102.0),
        Row("8RAM size", "5", 2684.0),
        Row("8RAM size", "6", 2970.0),
        Row("8RAM size", "7", 5166.0),
        Row("9RAM size", "1", 3065.0),
        Row("9RAM size", "3", 3239.0),
        Row("9RAM size", "4", 5821.0),
        Row("9RAM size", "6", 1567.0),
        Row("9RAM size", "7", 571.0)
      )
    )
  }
  )

  //TC_268
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize >= \"\" GROUP BY AMSize, ActiveAreaId ORDER " +
      "BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize >= \"\" GROUP BY AMSize, ActiveAreaId " +
          "ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq(Row("0RAM size", "1", 2849.0),
        Row("0RAM size", "2", 79.0),
        Row("0RAM size", "3", 7663.0),
        Row("0RAM size", "5", 1341.0),
        Row("0RAM size", "6", 6082.0),
        Row("1RAM size", "1", 256.0),
        Row("1RAM size", "2", 1333.0),
        Row("1RAM size", "4", 7510.0),
        Row("1RAM size", "5", 2745.0),
        Row("1RAM size", "7", 3942.0),
        Row("2RAM size", "3", 1973.0),
        Row("2RAM size", "4", 1350.0),
        Row("3RAM size", "1", 6640.0),
        Row("3RAM size", "2", 1999.0),
        Row("3RAM size", "3", 2863.0),
        Row("3RAM size", "4", 3824.0),
        Row("3RAM size", "5", 5699.0),
        Row("3RAM size", "6", 2635.0),
        Row("3RAM size", "7", 1491.0),
        Row("4RAM size", "1", 2255.0),
        Row("4RAM size", "2", 1728.0),
        Row("4RAM size", "3", 9130.0),
        Row("4RAM size", "4", 11560.0),
        Row("4RAM size", "6", 5344.635),
        Row("4RAM size", "7", 1338.0),
        Row("5RAM size", "2", 4712.0),
        Row("5RAM size", "3", 2769.0),
        Row("5RAM size", "6", 2478.0),
        Row("6RAM size", "1", 2142.0),
        Row("6RAM size", "2", 1768.0),
        Row("6RAM size", "3", 2633.0),
        Row("6RAM size", "4", 866.0),
        Row("6RAM size", "5", 2952.0),
        Row("6RAM size", "6", 3257.0),
        Row("7RAM size", "3", 151.0),
        Row("7RAM size", "5", 2239.0),
        Row("7RAM size", "6", 3979.0),
        Row("7RAM size", "7", 2031.0),
        Row("8RAM size", "1", 355.0),
        Row("8RAM size", "2", 2738.562),
        Row("8RAM size", "4", 3102.0),
        Row("8RAM size", "5", 2684.0),
        Row("8RAM size", "6", 2970.0),
        Row("8RAM size", "7", 5166.0),
        Row("9RAM size", "1", 3065.0),
        Row("9RAM size", "3", 3239.0),
        Row("9RAM size", "4", 5821.0),
        Row("9RAM size", "6", 1567.0),
        Row("9RAM size", "7", 571.0)
      )
    )
  }
  )

  //TC_269
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize >= \"0RAM size\" GROUP BY AMSize, " +
      "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamePointId) AS Sum_gamePointId FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize >= \"0RAM size\" GROUP BY AMSize, " +
          "ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq(Row("0RAM size", "1", 2849.0),
        Row("0RAM size", "2", 79.0),
        Row("0RAM size", "3", 7663.0),
        Row("0RAM size", "5", 1341.0),
        Row("0RAM size", "6", 6082.0),
        Row("1RAM size", "1", 256.0),
        Row("1RAM size", "2", 1333.0),
        Row("1RAM size", "4", 7510.0),
        Row("1RAM size", "5", 2745.0),
        Row("1RAM size", "7", 3942.0),
        Row("2RAM size", "3", 1973.0),
        Row("2RAM size", "4", 1350.0),
        Row("3RAM size", "1", 6640.0),
        Row("3RAM size", "2", 1999.0),
        Row("3RAM size", "3", 2863.0),
        Row("3RAM size", "4", 3824.0),
        Row("3RAM size", "5", 5699.0),
        Row("3RAM size", "6", 2635.0),
        Row("3RAM size", "7", 1491.0),
        Row("4RAM size", "1", 2255.0),
        Row("4RAM size", "2", 1728.0),
        Row("4RAM size", "3", 9130.0),
        Row("4RAM size", "4", 11560.0),
        Row("4RAM size", "6", 5344.635),
        Row("4RAM size", "7", 1338.0),
        Row("5RAM size", "2", 4712.0),
        Row("5RAM size", "3", 2769.0),
        Row("5RAM size", "6", 2478.0),
        Row("6RAM size", "1", 2142.0),
        Row("6RAM size", "2", 1768.0),
        Row("6RAM size", "3", 2633.0),
        Row("6RAM size", "4", 866.0),
        Row("6RAM size", "5", 2952.0),
        Row("6RAM size", "6", 3257.0),
        Row("7RAM size", "3", 151.0),
        Row("7RAM size", "5", 2239.0),
        Row("7RAM size", "6", 3979.0),
        Row("7RAM size", "7", 2031.0),
        Row("8RAM size", "1", 355.0),
        Row("8RAM size", "2", 2738.562),
        Row("8RAM size", "4", 3102.0),
        Row("8RAM size", "5", 2684.0),
        Row("8RAM size", "6", 2970.0),
        Row("8RAM size", "7", 5166.0),
        Row("9RAM size", "1", 3065.0),
        Row("9RAM size", "3", 3239.0),
        Row("9RAM size", "4", 5821.0),
        Row("9RAM size", "6", 1567.0),
        Row("9RAM size", "7", 571.0)
      )
    )
  }
  )

  //TC_271
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, " +
      "ActiveOperatorId, ActiveProvince, ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict " +
      "ASC, ActiveOperatorId ASC, ActiveProvince ASC, ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
          "FROM (select * from Carbon_automation_test2) SUB_QRY GROUP BY ActiveCountry, " +
          "ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet ORDER BY ActiveCountry " +
          "ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, ActiveStreet ASC"
      ),
      Seq(Row("Chinese", "hongshan", "10", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100001", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100004", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100008", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100024", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100025", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100039", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100045", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100046", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100047", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100056", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100058", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "10006", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100061", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100070", "Hubei Province", "hongshan"),
        Row("Chinese", "hongshan", "100078", "Hubei Province", "hongshan"),
        Row("Chinese", "longgang", "100013", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100020", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100028", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100032", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100035", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "10004", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100044", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100054", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100060", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100073", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100074", "Guangdong Province", "matishan"),
        Row("Chinese", "longgang", "100083", "Guangdong Province", "matishan"),
        Row("Chinese", "longhua", "1", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100010", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100017", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100022", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100026", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100030", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100055", "Guangdong Province", "mingzhi"),
        Row("Chinese", "longhua", "100075", "Guangdong Province", "mingzhi"),
        Row("Chinese", "tianyuan", "100002", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100003", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100027", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100042", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "10005", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100052", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100053", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100062", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100063", "Hunan Province", "tianyua"),
        Row("Chinese", "tianyuan", "100081", "Hunan Province", "tianyua"),
        Row("Chinese", "xiangtan", "10000", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "1000000", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100007", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100009", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "10001", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100011", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100012", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100016", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "10003", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100031", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100038", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100041", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100048", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100049", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100051", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100059", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100068", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "10007", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100072", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100080", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100082", "Hunan Province", "jianshelu"),
        Row("Chinese", "xiangtan", "100084", "Hunan Province", "jianshelu"),
        Row("Chinese", "yichang", "100", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100000", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100018", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "10002", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100023", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100033", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100040", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100043", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100050", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100066", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100069", "Hubei Province", "yichang"),
        Row("Chinese", "yichang", "100076", "Hubei Province", "yichang"),
        Row("Chinese", "yuhua", "1000", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100005", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100006", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100014", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100015", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100019", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100021", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100029", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100034", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100036", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100037", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100057", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100064", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100065", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100067", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100071", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100077", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "100079", "Hunan Province", "shazitang"),
        Row("Chinese", "yuhua", "10008", "Hunan Province", "shazitang")
      )
    )
  }
  )

  //TC_272
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, SUM" +
      "(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY " +
      "GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
      "ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, " +
      "ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "SUM(gamePointId) AS Sum_gamePointId FROM (select * from Carbon_automation_test2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      ),
      Seq(Row("Chinese", "hongshan", "10", "Hubei Province", "hongshan", 1714.635),
        Row("Chinese", "hongshan", "100001", "Hubei Province", "hongshan", 505.0),
        Row("Chinese", "hongshan", "100004", "Hubei Province", "hongshan", 2970.0),
        Row("Chinese", "hongshan", "100008", "Hubei Province", "hongshan", 1442.0),
        Row("Chinese", "hongshan", "100024", "Hubei Province", "hongshan", 2483.0),
        Row("Chinese", "hongshan", "100025", "Hubei Province", "hongshan", 1724.0),
        Row("Chinese", "hongshan", "100039", "Hubei Province", "hongshan", 1750.0),
        Row("Chinese", "hongshan", "100045", "Hubei Province", "hongshan", 2553.0),
        Row("Chinese", "hongshan", "100046", "Hubei Province", "hongshan", 1077.0),
        Row("Chinese", "hongshan", "100047", "Hubei Province", "hongshan", 1823.0),
        Row("Chinese", "hongshan", "100056", "Hubei Province", "hongshan", 750.0),
        Row("Chinese", "hongshan", "100058", "Hubei Province", "hongshan", 2635.0),
        Row("Chinese", "hongshan", "10006", "Hubei Province", "hongshan", 2478.0),
        Row("Chinese", "hongshan", "100061", "Hubei Province", "hongshan", 1407.0),
        Row("Chinese", "hongshan", "100070", "Hubei Province", "hongshan", 1567.0),
        Row("Chinese", "hongshan", "100078", "Hubei Province", "hongshan", 1434.0),
        Row("Chinese", "longgang", "100013", "Guangdong Province", "matishan", 355.0),
        Row("Chinese", "longgang", "100020", "Guangdong Province", "matishan", 256.0),
        Row("Chinese", "longgang", "100028", "Guangdong Province", "matishan", 2849.0),
        Row("Chinese", "longgang", "100032", "Guangdong Province", "matishan", 1053.0),
        Row("Chinese", "longgang", "100035", "Guangdong Province", "matishan", 2142.0),
        Row("Chinese", "longgang", "10004", "Guangdong Province", "matishan", 1717.0),
        Row("Chinese", "longgang", "100044", "Guangdong Province", "matishan", 1697.0),
        Row("Chinese", "longgang", "100054", "Guangdong Province", "matishan", 1368.0),
        Row("Chinese", "longgang", "100060", "Guangdong Province", "matishan", 538.0),
        Row("Chinese", "longgang", "100073", "Guangdong Province", "matishan", 2488.0),
        Row("Chinese", "longgang", "100074", "Guangdong Province", "matishan", 907.0),
        Row("Chinese", "longgang", "100083", "Guangdong Province", "matishan", 2192.0),
        Row("Chinese", "longhua", "1", "Guangdong Province", "mingzhi", 2738.562),
        Row("Chinese", "longhua", "100010", "Guangdong Province", "mingzhi", 79.0),
        Row("Chinese", "longhua", "100017", "Guangdong Province", "mingzhi", 2205.0),
        Row("Chinese", "longhua", "100022", "Guangdong Province", "mingzhi", 1999.0),
        Row("Chinese", "longhua", "100026", "Guangdong Province", "mingzhi", 1768.0),
        Row("Chinese", "longhua", "100030", "Guangdong Province", "mingzhi", 1333.0),
        Row("Chinese", "longhua", "100055", "Guangdong Province", "mingzhi", 1728.0),
        Row("Chinese", "longhua", "100075", "Guangdong Province", "mingzhi", 2507.0),
        Row("Chinese", "tianyuan", "100002", "Hunan Province", "tianyua", 1341.0),
        Row("Chinese", "tianyuan", "100003", "Hunan Province", "tianyua", 2239.0),
        Row("Chinese", "tianyuan", "100027", "Hunan Province", "tianyua", 2436.0),
        Row("Chinese", "tianyuan", "100042", "Hunan Province", "tianyua", 2745.0),
        Row("Chinese", "tianyuan", "10005", "Hunan Province", "tianyua", 1608.0),
        Row("Chinese", "tianyuan", "100052", "Hunan Province", "tianyua", 845.0),
        Row("Chinese", "tianyuan", "100053", "Hunan Province", "tianyua", 1655.0),
        Row("Chinese", "tianyuan", "100062", "Hunan Province", "tianyua", 2952.0),
        Row("Chinese", "tianyuan", "100063", "Hunan Province", "tianyua", 1226.0),
        Row("Chinese", "tianyuan", "100081", "Hunan Province", "tianyua", 613.0),
        Row("Chinese", "xiangtan", "10000", "Hunan Province", "jianshelu", 2175.0),
        Row("Chinese", "xiangtan", "1000000", "Hunan Province", "jianshelu", 1600.0),
        Row("Chinese", "xiangtan", "100007", "Hunan Province", "jianshelu", 1991.0),
        Row("Chinese", "xiangtan", "100009", "Hunan Province", "jianshelu", 1841.0),
        Row("Chinese", "xiangtan", "10001", "Hunan Province", "jianshelu", 298.0),
        Row("Chinese", "xiangtan", "100011", "Hunan Province", "jianshelu", 202.0),
        Row("Chinese", "xiangtan", "100012", "Hunan Province", "jianshelu", 568.0),
        Row("Chinese", "xiangtan", "100016", "Hunan Province", "jianshelu", 1873.0),
        Row("Chinese", "xiangtan", "10003", "Hunan Province", "jianshelu", 2071.0),
        Row("Chinese", "xiangtan", "100031", "Hunan Province", "jianshelu", 1080.0),
        Row("Chinese", "xiangtan", "100038", "Hunan Province", "jianshelu", 1229.0),
        Row("Chinese", "xiangtan", "100041", "Hunan Province", "jianshelu", 2734.0),
        Row("Chinese", "xiangtan", "100048", "Hunan Province", "jianshelu", 2399.0),
        Row("Chinese", "xiangtan", "100049", "Hunan Province", "jianshelu", 2890.0),
        Row("Chinese", "xiangtan", "100051", "Hunan Province", "jianshelu", 1407.0),
        Row("Chinese", "xiangtan", "100059", "Hunan Province", "jianshelu", 1337.0),
        Row("Chinese", "xiangtan", "100068", "Hunan Province", "jianshelu", 412.0),
        Row("Chinese", "xiangtan", "10007", "Hunan Province", "jianshelu", 1350.0),
        Row("Chinese", "xiangtan", "100072", "Hunan Province", "jianshelu", 448.0),
        Row("Chinese", "xiangtan", "100080", "Hunan Province", "jianshelu", 954.0),
        Row("Chinese", "xiangtan", "100082", "Hunan Province", "jianshelu", 2348.0),
        Row("Chinese", "xiangtan", "100084", "Hunan Province", "jianshelu", 2826.0),
        Row("Chinese", "yichang", "100", "Hubei Province", "yichang", 1271.0),
        Row("Chinese", "yichang", "100000", "Hubei Province", "yichang", 136.0),
        Row("Chinese", "yichang", "100018", "Hubei Province", "yichang", 441.0),
        Row("Chinese", "yichang", "10002", "Hubei Province", "yichang", 2972.0),
        Row("Chinese", "yichang", "100023", "Hubei Province", "yichang", 2194.0),
        Row("Chinese", "yichang", "100033", "Hubei Province", "yichang", 760.0),
        Row("Chinese", "yichang", "100040", "Hubei Province", "yichang", 2078.0),
        Row("Chinese", "yichang", "100043", "Hubei Province", "yichang", 571.0),
        Row("Chinese", "yichang", "100050", "Hubei Province", "yichang", 29.0),
        Row("Chinese", "yichang", "100066", "Hubei Province", "yichang", 1864.0),
        Row("Chinese", "yichang", "100069", "Hubei Province", "yichang", 1491.0),
        Row("Chinese", "yichang", "100076", "Hubei Province", "yichang", 732.0),
        Row("Chinese", "yuhua", "1000", "Hunan Province", "shazitang", 692.0),
        Row("Chinese", "yuhua", "100005", "Hunan Province", "shazitang", 2593.0),
        Row("Chinese", "yuhua", "100006", "Hunan Province", "shazitang", 2572.0),
        Row("Chinese", "yuhua", "100014", "Hunan Province", "shazitang", 151.0),
        Row("Chinese", "yuhua", "100015", "Hunan Province", "shazitang", 2863.0),
        Row("Chinese", "yuhua", "100019", "Hunan Province", "shazitang", 2194.0),
        Row("Chinese", "yuhua", "100021", "Hunan Province", "shazitang", 1778.0),
        Row("Chinese", "yuhua", "100029", "Hunan Province", "shazitang", 1691.0),
        Row("Chinese", "yuhua", "100034", "Hunan Province", "shazitang", 2061.0),
        Row("Chinese", "yuhua", "100036", "Hunan Province", "shazitang", 2224.0),
        Row("Chinese", "yuhua", "100037", "Hunan Province", "shazitang", 1015.0),
        Row("Chinese", "yuhua", "100057", "Hunan Province", "shazitang", 2288.0),
        Row("Chinese", "yuhua", "100064", "Hunan Province", "shazitang", 865.0),
        Row("Chinese", "yuhua", "100065", "Hunan Province", "shazitang", 901.0),
        Row("Chinese", "yuhua", "100067", "Hunan Province", "shazitang", 572.0),
        Row("Chinese", "yuhua", "100071", "Hunan Province", "shazitang", 1973.0),
        Row("Chinese", "yuhua", "100077", "Hunan Province", "shazitang", 2077.0),
        Row("Chinese", "yuhua", "100079", "Hunan Province", "shazitang", 1098.0),
        Row("Chinese", "yuhua", "10008", "Hunan Province", "shazitang", 813.0)
      )
    )
  }
  )

  //TC_273
  test(
    "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, AVG" +
      "(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY " +
      "GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet " +
      "ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, ActiveProvince ASC, " +
      "ActiveStreet ASC"
  )({
    checkAnswer(
      sql(
        "SELECT ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, ActiveStreet, " +
          "AVG(gamePointId) AS Avg_gamePointId FROM (select * from Carbon_automation_test2) " +
          "SUB_QRY GROUP BY ActiveCountry, ActiveDistrict, ActiveOperatorId, ActiveProvince, " +
          "ActiveStreet ORDER BY ActiveCountry ASC, ActiveDistrict ASC, ActiveOperatorId ASC, " +
          "ActiveProvince ASC, ActiveStreet ASC"
      ),
      Seq(Row("Chinese", "hongshan", "10", "Hubei Province", "hongshan", 1714.635),
        Row("Chinese", "hongshan", "100001", "Hubei Province", "hongshan", 505.0),
        Row("Chinese", "hongshan", "100004", "Hubei Province", "hongshan", 2970.0),
        Row("Chinese", "hongshan", "100008", "Hubei Province", "hongshan", 1442.0),
        Row("Chinese", "hongshan", "100024", "Hubei Province", "hongshan", 2483.0),
        Row("Chinese", "hongshan", "100025", "Hubei Province", "hongshan", 1724.0),
        Row("Chinese", "hongshan", "100039", "Hubei Province", "hongshan", 1750.0),
        Row("Chinese", "hongshan", "100045", "Hubei Province", "hongshan", 2553.0),
        Row("Chinese", "hongshan", "100046", "Hubei Province", "hongshan", 1077.0),
        Row("Chinese", "hongshan", "100047", "Hubei Province", "hongshan", 1823.0),
        Row("Chinese", "hongshan", "100056", "Hubei Province", "hongshan", 750.0),
        Row("Chinese", "hongshan", "100058", "Hubei Province", "hongshan", 2635.0),
        Row("Chinese", "hongshan", "10006", "Hubei Province", "hongshan", 2478.0),
        Row("Chinese", "hongshan", "100061", "Hubei Province", "hongshan", 1407.0),
        Row("Chinese", "hongshan", "100070", "Hubei Province", "hongshan", 1567.0),
        Row("Chinese", "hongshan", "100078", "Hubei Province", "hongshan", 1434.0),
        Row("Chinese", "longgang", "100013", "Guangdong Province", "matishan", 355.0),
        Row("Chinese", "longgang", "100020", "Guangdong Province", "matishan", 256.0),
        Row("Chinese", "longgang", "100028", "Guangdong Province", "matishan", 2849.0),
        Row("Chinese", "longgang", "100032", "Guangdong Province", "matishan", 1053.0),
        Row("Chinese", "longgang", "100035", "Guangdong Province", "matishan", 2142.0),
        Row("Chinese", "longgang", "10004", "Guangdong Province", "matishan", 1717.0),
        Row("Chinese", "longgang", "100044", "Guangdong Province", "matishan", 1697.0),
        Row("Chinese", "longgang", "100054", "Guangdong Province", "matishan", 1368.0),
        Row("Chinese", "longgang", "100060", "Guangdong Province", "matishan", 538.0),
        Row("Chinese", "longgang", "100073", "Guangdong Province", "matishan", 2488.0),
        Row("Chinese", "longgang", "100074", "Guangdong Province", "matishan", 907.0),
        Row("Chinese", "longgang", "100083", "Guangdong Province", "matishan", 2192.0),
        Row("Chinese", "longhua", "1", "Guangdong Province", "mingzhi", 2738.562),
        Row("Chinese", "longhua", "100010", "Guangdong Province", "mingzhi", 79.0),
        Row("Chinese", "longhua", "100017", "Guangdong Province", "mingzhi", 2205.0),
        Row("Chinese", "longhua", "100022", "Guangdong Province", "mingzhi", 1999.0),
        Row("Chinese", "longhua", "100026", "Guangdong Province", "mingzhi", 1768.0),
        Row("Chinese", "longhua", "100030", "Guangdong Province", "mingzhi", 1333.0),
        Row("Chinese", "longhua", "100055", "Guangdong Province", "mingzhi", 1728.0),
        Row("Chinese", "longhua", "100075", "Guangdong Province", "mingzhi", 2507.0),
        Row("Chinese", "tianyuan", "100002", "Hunan Province", "tianyua", 1341.0),
        Row("Chinese", "tianyuan", "100003", "Hunan Province", "tianyua", 2239.0),
        Row("Chinese", "tianyuan", "100027", "Hunan Province", "tianyua", 2436.0),
        Row("Chinese", "tianyuan", "100042", "Hunan Province", "tianyua", 2745.0),
        Row("Chinese", "tianyuan", "10005", "Hunan Province", "tianyua", 1608.0),
        Row("Chinese", "tianyuan", "100052", "Hunan Province", "tianyua", 845.0),
        Row("Chinese", "tianyuan", "100053", "Hunan Province", "tianyua", 1655.0),
        Row("Chinese", "tianyuan", "100062", "Hunan Province", "tianyua", 2952.0),
        Row("Chinese", "tianyuan", "100063", "Hunan Province", "tianyua", 1226.0),
        Row("Chinese", "tianyuan", "100081", "Hunan Province", "tianyua", 613.0),
        Row("Chinese", "xiangtan", "10000", "Hunan Province", "jianshelu", 2175.0),
        Row("Chinese", "xiangtan", "1000000", "Hunan Province", "jianshelu", 1600.0),
        Row("Chinese", "xiangtan", "100007", "Hunan Province", "jianshelu", 1991.0),
        Row("Chinese", "xiangtan", "100009", "Hunan Province", "jianshelu", 1841.0),
        Row("Chinese", "xiangtan", "10001", "Hunan Province", "jianshelu", 298.0),
        Row("Chinese", "xiangtan", "100011", "Hunan Province", "jianshelu", 202.0),
        Row("Chinese", "xiangtan", "100012", "Hunan Province", "jianshelu", 568.0),
        Row("Chinese", "xiangtan", "100016", "Hunan Province", "jianshelu", 1873.0),
        Row("Chinese", "xiangtan", "10003", "Hunan Province", "jianshelu", 2071.0),
        Row("Chinese", "xiangtan", "100031", "Hunan Province", "jianshelu", 1080.0),
        Row("Chinese", "xiangtan", "100038", "Hunan Province", "jianshelu", 1229.0),
        Row("Chinese", "xiangtan", "100041", "Hunan Province", "jianshelu", 2734.0),
        Row("Chinese", "xiangtan", "100048", "Hunan Province", "jianshelu", 2399.0),
        Row("Chinese", "xiangtan", "100049", "Hunan Province", "jianshelu", 2890.0),
        Row("Chinese", "xiangtan", "100051", "Hunan Province", "jianshelu", 1407.0),
        Row("Chinese", "xiangtan", "100059", "Hunan Province", "jianshelu", 1337.0),
        Row("Chinese", "xiangtan", "100068", "Hunan Province", "jianshelu", 412.0),
        Row("Chinese", "xiangtan", "10007", "Hunan Province", "jianshelu", 1350.0),
        Row("Chinese", "xiangtan", "100072", "Hunan Province", "jianshelu", 448.0),
        Row("Chinese", "xiangtan", "100080", "Hunan Province", "jianshelu", 954.0),
        Row("Chinese", "xiangtan", "100082", "Hunan Province", "jianshelu", 2348.0),
        Row("Chinese", "xiangtan", "100084", "Hunan Province", "jianshelu", 2826.0),
        Row("Chinese", "yichang", "100", "Hubei Province", "yichang", 1271.0),
        Row("Chinese", "yichang", "100000", "Hubei Province", "yichang", 136.0),
        Row("Chinese", "yichang", "100018", "Hubei Province", "yichang", 441.0),
        Row("Chinese", "yichang", "10002", "Hubei Province", "yichang", 2972.0),
        Row("Chinese", "yichang", "100023", "Hubei Province", "yichang", 2194.0),
        Row("Chinese", "yichang", "100033", "Hubei Province", "yichang", 760.0),
        Row("Chinese", "yichang", "100040", "Hubei Province", "yichang", 2078.0),
        Row("Chinese", "yichang", "100043", "Hubei Province", "yichang", 571.0),
        Row("Chinese", "yichang", "100050", "Hubei Province", "yichang", 29.0),
        Row("Chinese", "yichang", "100066", "Hubei Province", "yichang", 1864.0),
        Row("Chinese", "yichang", "100069", "Hubei Province", "yichang", 1491.0),
        Row("Chinese", "yichang", "100076", "Hubei Province", "yichang", 732.0),
        Row("Chinese", "yuhua", "1000", "Hunan Province", "shazitang", 692.0),
        Row("Chinese", "yuhua", "100005", "Hunan Province", "shazitang", 2593.0),
        Row("Chinese", "yuhua", "100006", "Hunan Province", "shazitang", 2572.0),
        Row("Chinese", "yuhua", "100014", "Hunan Province", "shazitang", 151.0),
        Row("Chinese", "yuhua", "100015", "Hunan Province", "shazitang", 2863.0),
        Row("Chinese", "yuhua", "100019", "Hunan Province", "shazitang", 2194.0),
        Row("Chinese", "yuhua", "100021", "Hunan Province", "shazitang", 1778.0),
        Row("Chinese", "yuhua", "100029", "Hunan Province", "shazitang", 1691.0),
        Row("Chinese", "yuhua", "100034", "Hunan Province", "shazitang", 2061.0),
        Row("Chinese", "yuhua", "100036", "Hunan Province", "shazitang", 2224.0),
        Row("Chinese", "yuhua", "100037", "Hunan Province", "shazitang", 1015.0),
        Row("Chinese", "yuhua", "100057", "Hunan Province", "shazitang", 2288.0),
        Row("Chinese", "yuhua", "100064", "Hunan Province", "shazitang", 865.0),
        Row("Chinese", "yuhua", "100065", "Hunan Province", "shazitang", 901.0),
        Row("Chinese", "yuhua", "100067", "Hunan Province", "shazitang", 572.0),
        Row("Chinese", "yuhua", "100071", "Hunan Province", "shazitang", 1973.0),
        Row("Chinese", "yuhua", "100077", "Hunan Province", "shazitang", 2077.0),
        Row("Chinese", "yuhua", "100079", "Hunan Province", "shazitang", 1098.0),
        Row("Chinese", "yuhua", "10008", "Hunan Province", "shazitang", 813.0)
      )
    )
  }
  )


  //TC_277
  test(
    "SELECT Activecity, ActiveCountry, ActiveDistrict, MAX(gamepointid) AS Max_gmaepointid FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY GROUP BY Activecity, ActiveCountry, " +
      "ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, MAX(gamepointid) AS Max_gmaepointid " +
          "FROM (select * from Carbon_automation_test2) SUB_QRY GROUP BY Activecity, " +
          "ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, " +
          "ActiveDistrict ASC"
      ),
      Seq(Row("changsha", "Chinese", "yuhua", 2863.0),
        Row("guangzhou", "Chinese", "longhua", 2738.562),
        Row("shenzhen", "Chinese", "longgang", 2849.0),
        Row("wuhan", "Chinese", "hongshan", 2970.0),
        Row("xiangtan", "Chinese", "xiangtan", 2890.0),
        Row("yichang", "Chinese", "yichang", 2972.0),
        Row("zhuzhou", "Chinese", "tianyuan", 2952.0)
      )
    )
  }
  )

  //TC_278
  test(
    "SELECT Activecity, ActiveCountry, ActiveDistrict, MIN(gamepointid) AS Min_gamepointid FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY GROUP BY Activecity, ActiveCountry, " +
      "ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, ActiveDistrict ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Activecity, ActiveCountry, ActiveDistrict, MIN(gamepointid) AS Min_gamepointid " +
          "FROM (select * from Carbon_automation_test2) SUB_QRY GROUP BY Activecity, " +
          "ActiveCountry, ActiveDistrict ORDER BY Activecity ASC, ActiveCountry ASC, " +
          "ActiveDistrict ASC"
      ),
      Seq(Row("changsha", "Chinese", "yuhua", 151.0),
        Row("guangzhou", "Chinese", "longhua", 79.0),
        Row("shenzhen", "Chinese", "longgang", 256.0),
        Row("wuhan", "Chinese", "hongshan", 505.0),
        Row("xiangtan", "Chinese", "xiangtan", 202.0),
        Row("yichang", "Chinese", "yichang", 29.0),
        Row("zhuzhou", "Chinese", "tianyuan", 613.0)
      )
    )
  }
  )

  //TC_283
  test(
    "select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_test2 where" +
      " AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize"
  )({
    checkAnswer(
      sql(
        "select  AMSize,sum( gamePointId+ contractNumber) as total from Carbon_automation_test2 " +
          "where AMSize='0RAM size' and  ActiveProvince='Guangdong Province' group by AMSize"
      ),
      Seq(Row("0RAM size", 1.742632E7))
    )
  }
  )

  //TC_284
  test(
    "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
      "CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
  )({
    checkAnswer(
      sql(
        "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 " +
          "where  CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
      ),
      Seq(Row("4RAM size", 1.6792444E7),
        Row("6RAM size", 1.0819629E7),
        Row("7RAM size", 8232606.0),
        Row("3RAM size", 6417154.0),
        Row("5RAM size", 6385639.0),
        Row("0RAM size", 6171950.0),
        Row("9RAM size", 5660804.0),
        Row("8RAM size", 5284541.562)
      )
    )
  }
  )

  //TC_285
  test(
    "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
      "CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
  )({
    checkAnswer(
      sql(
        "select AMSize,sum(gamePointId+contractNumber) as total from Carbon_automation_test2 " +
          "where CUPAudit='0CPU Audit' group by AMSize having total > 10 order by total desc"
      ),
      Seq(Row("4RAM size", 1.6792444E7),
        Row("6RAM size", 1.0819629E7),
        Row("7RAM size", 8232606.0),
        Row("3RAM size", 6417154.0),
        Row("5RAM size", 6385639.0),
        Row("0RAM size", 6171950.0),
        Row("9RAM size", 5660804.0),
        Row("8RAM size", 5284541.562)
      )
    )
  }
  )

  //TC_286
  test(
    "select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
  )({
    checkAnswer(
      sql(
        "select  ActiveAreaId,count(distinct AMSize) as AMSize_number, sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
      ),
      Seq(Row("1", 7, 4.8863556E7),
        Row("2", 7, 4.1185336562E7),
        Row("3", 8, 1.00876754E8),
        Row("4", 7, 1.02219772E8),
        Row("5", 6, 3.8359697E7),
        Row("6", 8, 7.662756463499999E7),
        Row("7", 6, 6.7214095E7)
      )
    )
  }
  )

  //TC_287
  test(
    "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) " +
      "as total from Carbon_automation_test2 where ActiveAreaId='6'group by ActiveAreaId order by" +
      " AMSize_number desc"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
          "ActiveAreaId='6'group by ActiveAreaId order by AMSize_number desc"
      ),
      Seq(Row("6", 8, 7.662756463499999E7))
    )
  }
  )

  //TC_288
  test(
    "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) " +
      "as total from Carbon_automation_test2 group by ActiveAreaId"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
      ),
      Seq(Row("1", 7, 4.8863556E7),
        Row("2", 7, 4.1185336562E7),
        Row("3", 8, 1.00876754E8),
        Row("4", 7, 1.02219772E8),
        Row("5", 6, 3.8359697E7),
        Row("6", 8, 7.662756463499999E7),
        Row("7", 6, 6.7214095E7)
      )
    )
  }
  )

  //TC_289
  test(
    "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) " +
      "as total from Carbon_automation_test2 where ActiveAreaId='6' and ActiveProvince='Hubei " +
      "Province' group by ActiveAreaId order by AMSize_number desc"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
          "ActiveAreaId='6' and ActiveProvince='Hubei Province' group by ActiveAreaId order by " +
          "AMSize_number desc"
      ),
      Seq(Row("6", 8, 7.662756463499999E7))
    )
  }
  )

  //TC_290
  test(
    "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveProvince1"
  )({
    checkAnswer(
      sql(
        "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "ActiveProvince"
      ),
      Seq(Row("Hunan Province", 10, 2.41456223E8),
        Row("Guangdong Province", 8, 9.0048892562E7),
        Row("Hubei Province", 9, 1.43841659635E8)
      )
    )
  }
  )

  //TC_291
  test(
    "select AMSize,count(distinct imei) as imei_number,sum(gamePointId+contractNumber) as total " +
      "from Carbon_automation_test2 where AMSize='0RAM size' group by AMSize order by imei_number" +
      " desc"
  )({
    checkAnswer(
      sql(
        "select AMSize,count(distinct imei) as imei_number,sum(gamePointId+contractNumber) as " +
          "total from Carbon_automation_test2 where AMSize='0RAM size' group by AMSize order by " +
          "imei_number desc"
      ),
      Seq(Row("0RAM size", 11, 5.437552E7))
    )
  }
  )

  //TC_292
  test(
    "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveProvince"
  )({
    checkAnswer(
      sql(
        "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "ActiveProvince"
      ),
      Seq(Row("Hunan Province", 10, 2.41456223E8),
        Row("Guangdong Province", 8, 9.0048892562E7),
        Row("Hubei Province", 9, 1.43841659635E8)
      )
    )
  }
  )

  //TC_293
  test(
    "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where CUPAudit='0CPU " +
      "Audit' group by ActiveProvince order by total desc"
  )({
    checkAnswer(
      sql(
        "select ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
          "CUPAudit='0CPU Audit' group by ActiveProvince order by total desc"
      ),
      Seq(Row("Hubei Province", 3, 2.3775647E7),
        Row("Hunan Province", 3, 2.3622422E7),
        Row("Guangdong Province", 3, 1.8366698562E7)
      )
    )
  }
  )

  //TC_294
  test(
    "select  ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by  " +
      "ActiveOperatorId"
  )({
    checkAnswer(
      sql(
        "select  ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by  " +
          "ActiveOperatorId"
      ),
      Seq(Row("100007", 1, 337574.0),
        Row("100008", 1, 1072199.0),
        Row("100009", 1, 2391498.0),
        Row("100070", 1, 4204181.0),
        Row("100071", 1, 2201930.0),
        Row("100072", 1, 511576.0),
        Row("100073", 1, 583100.0),
        Row("100074", 1, 276249.0),
        Row("100075", 1, 3217834.0),
        Row("100076", 1, 8070591.0),
        Row("100077", 1, 6385639.0),
        Row("100078", 1, 6429950.0),
        Row("100079", 1, 5160219.0),
        Row("100010", 1, 8543359.0),
        Row("100011", 1, 4816462.0),
        Row("100012", 1, 8454563.0),
        Row("100013", 1, 2051894.0),
        Row("100014", 1, 7610226.0),
        Row("100015", 1, 6665954.0),
        Row("100016", 1, 6497165.0),
        Row("100017", 1, 2613669.0),
        Row("100018", 1, 574816.0),
        Row("100019", 1, 4461270.0),
        Row("100080", 1, 5153939.0),
        Row("100081", 1, 3336093.0),
        Row("100082", 1, 997163.0),
        Row("100083", 1, 509421.0),
        Row("100084", 1, 8979394.0),
        Row("1", 1, 5284541.562),
        Row("100020", 1, 833910.0),
        Row("100021", 1, 568695.0),
        Row("100022", 1, 834386.0),
        Row("100023", 1, 2852440.0),
        Row("100024", 1, 6171950.0),
        Row("100025", 1, 6535623.0),
        Row("100026", 1, 7488902.0),
        Row("100027", 1, 4752675.0),
        Row("100028", 1, 8882961.0),
        Row("100029", 1, 7776281.0),
        Row("10000", 1, 3787033.0),
        Row("10001", 1, 5986487.0),
        Row("10002", 1, 5207711.0),
        Row("10003", 1, 5588789.0),
        Row("10004", 1, 3079020.0),
        Row("100030", 1, 5858596.0),
        Row("10005", 1, 6191676.0),
        Row("100031", 1, 6417154.0),
        Row("10006", 1, 9397210.0),
        Row("10007", 1, 4157689.0),
        Row("100032", 1, 6995116.0),
        Row("10008", 1, 3361201.0),
        Row("100033", 1, 8230567.0),
        Row("100034", 1, 5799140.0),
        Row("100035", 1, 6285204.0),
        Row("100036", 1, 8433994.0),
        Row("100037", 1, 3312327.0),
        Row("100038", 1, 2845110.0),
        Row("100039", 1, 1903639.0),
        Row("10", 1, 6807314.635),
        Row("1000000", 1, 9739368.0),
        Row("1000", 1, 8979457.0),
        Row("100040", 1, 7882517.0),
        Row("100041", 1, 3457065.0),
        Row("100042", 1, 25995.0),
        Row("100043", 1, 3278738.0),
        Row("100044", 1, 5660804.0),
        Row("100045", 1, 9954785.0),
        Row("100046", 1, 426000.0),
        Row("100047", 1, 7841745.0),
        Row("100048", 1, 9502885.0),
        Row("100049", 1, 1954940.0),
        Row("100050", 1, 7768497.0),
        Row("100051", 1, 7238326.0),
        Row("100052", 1, 168570.0),
        Row("100053", 1, 2652739.0),
        Row("100054", 1, 6284524.0),
        Row("100055", 1, 7344049.0),
        Row("100056", 1, 1754573.0),
        Row("100057", 1, 5453821.0),
        Row("100058", 1, 5405743.0),
        Row("100059", 1, 170094.0),
        Row("100060", 1, 7421353.0),
        Row("100061", 1, 4360028.0),
        Row("100062", 1, 2365066.0),
        Row("100063", 1, 9319460.0),
        Row("100064", 1, 5566105.0),
        Row("100065", 1, 3167625.0),
        Row("100066", 1, 5594321.0),
        Row("100", 1, 8232606.0),
        Row("100067", 1, 7575768.0),
        Row("100068", 1, 3235498.0),
        Row("100069", 1, 7918697.0),
        Row("100000", 1, 1602594.0),
        Row("100001", 1, 2920291.0),
        Row("100002", 1, 9456953.0),
        Row("100003", 1, 90470.0),
        Row("100004", 1, 1442333.0),
        Row("100005", 1, 3943313.0),
        Row("100006", 1, 4453789.0)
      )
    )
  }
  )

  //TC_295
  test(
    "select ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
      "ActiveOperatorId='100000' group by ActiveOperatorId order by total desc"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where " +
          "ActiveOperatorId='100000' group by ActiveOperatorId order by total desc"
      ),
      Seq(Row("100000", 1, 1602594.0))
    )
  }
  )

  //TC_296
  test(
    "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum(gamePointId+contractNumber) " +
      "as total from Carbon_automation_test2 group by ActiveAreaId1"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by ActiveAreaId"
      ),
      Seq(Row("1", 7, 4.8863556E7),
        Row("2", 7, 4.1185336562E7),
        Row("3", 8, 1.00876754E8),
        Row("4", 7, 1.02219772E8),
        Row("5", 6, 3.8359697E7),
        Row("6", 8, 7.662756463499999E7),
        Row("7", 6, 6.7214095E7)
      )
    )
  }
  )

  //TC_297
  test(
    "select  Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by Latest_DAY," +
      "Latest_HOUR"
  )({
    checkAnswer(
      sql(
        "select  Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "Latest_DAY,Latest_HOUR"
      ),
      Seq(Row(1, "12", 10, 4.75346775197E8))
    )
  }
  )

  //TC_298
  test(
    "select Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where Latest_HOUR " +
      "between 12 and 15 group by Latest_DAY,Latest_HOUR order by total desc"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,Latest_HOUR,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where Latest_HOUR " +
          "between 12 and 15 group by Latest_DAY,Latest_HOUR order by total desc"
      ),
      Seq(Row(1, "12", 10, 4.75346775197E8))
    )
  }
  )

  //TC_299
  test(
    "select  Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by Activecity," +
      "ActiveProvince"
  )({
    checkAnswer(
      sql(
        "select  Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "Activecity,ActiveProvince"
      ),
      Seq(Row("xiangtan", "Hunan Province", 7, 1.02219772E8),
        Row("yichang", "Hubei Province", 6, 6.7214095E7),
        Row("shenzhen", "Guangdong Province", 7, 4.8863556E7),
        Row("zhuzhou", "Hunan Province", 6, 3.8359697E7),
        Row("changsha", "Hunan Province", 8, 1.00876754E8),
        Row("guangzhou", "Guangdong Province", 7, 4.1185336562E7),
        Row("wuhan", "Hubei Province", 8, 7.662756463499999E7)
      )
    )
  }
  )

  //TC_300
  test(
    "select Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
      "ActiveProvince='Guangdong Province' group by Activecity,ActiveProvince order by total desc"
  )({
    checkAnswer(
      sql(
        "select Activecity,ActiveProvince,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
          "ActiveProvince='Guangdong Province' group by Activecity,ActiveProvince order by total " +
          "desc"
      ),
      Seq(Row("shenzhen", "Guangdong Province", 7, 4.8863556E7),
        Row("guangzhou", "Guangdong Province", 7, 4.1185336562E7)
      )
    )
  }
  )

  //TC_301
  test(
    "select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by Activecity," +
      "ActiveOperatorId"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "Activecity,ActiveOperatorId"
      ),
      Seq(Row("100066", "yichang", 1, 5594321.0),
        Row("10002", "yichang", 1, 5207711.0),
        Row("100069", "yichang", 1, 7918697.0),
        Row("100021", "changsha", 1, 568695.0),
        Row("100002", "zhuzhou", 1, 9456953.0),
        Row("100000", "yichang", 1, 1602594.0),
        Row("100003", "zhuzhou", 1, 90470.0),
        Row("100029", "changsha", 1, 7776281.0),
        Row("100083", "shenzhen", 1, 509421.0),
        Row("100061", "wuhan", 1, 4360028.0),
        Row("100022", "guangzhou", 1, 834386.0),
        Row("100026", "guangzhou", 1, 7488902.0),
        Row("100020", "shenzhen", 1, 833910.0),
        Row("10000", "xiangtan", 1, 3787033.0),
        Row("10001", "xiangtan", 1, 5986487.0),
        Row("100001", "wuhan", 1, 2920291.0),
        Row("100068", "xiangtan", 1, 3235498.0),
        Row("10003", "xiangtan", 1, 5588789.0),
        Row("100004", "wuhan", 1, 1442333.0),
        Row("100076", "yichang", 1, 8070591.0),
        Row("100028", "shenzhen", 1, 8882961.0),
        Row("10007", "xiangtan", 1, 4157689.0),
        Row("100008", "wuhan", 1, 1072199.0),
        Row("100034", "changsha", 1, 5799140.0),
        Row("100036", "changsha", 1, 8433994.0),
        Row("100037", "changsha", 1, 3312327.0),
        Row("100007", "xiangtan", 1, 337574.0),
        Row("100009", "xiangtan", 1, 2391498.0),
        Row("100070", "wuhan", 1, 4204181.0),
        Row("100030", "guangzhou", 1, 5858596.0),
        Row("100018", "yichang", 1, 574816.0),
        Row("1000000", "xiangtan", 1, 9739368.0),
        Row("100078", "wuhan", 1, 6429950.0),
        Row("100072", "xiangtan", 1, 511576.0),
        Row("10004", "shenzhen", 1, 3079020.0),
        Row("100081", "zhuzhou", 1, 3336093.0),
        Row("100032", "shenzhen", 1, 6995116.0),
        Row("100035", "shenzhen", 1, 6285204.0),
        Row("100011", "xiangtan", 1, 4816462.0),
        Row("100012", "xiangtan", 1, 8454563.0),
        Row("100016", "xiangtan", 1, 6497165.0),
        Row("100023", "yichang", 1, 2852440.0),
        Row("100027", "zhuzhou", 1, 4752675.0),
        Row("100080", "xiangtan", 1, 5153939.0),
        Row("100082", "xiangtan", 1, 997163.0),
        Row("100084", "xiangtan", 1, 8979394.0),
        Row("100044", "shenzhen", 1, 5660804.0),
        Row("100024", "wuhan", 1, 6171950.0),
        Row("100025", "wuhan", 1, 6535623.0),
        Row("10005", "zhuzhou", 1, 6191676.0),
        Row("100057", "changsha", 1, 5453821.0),
        Row("100", "yichang", 1, 8232606.0),
        Row("100033", "yichang", 1, 8230567.0),
        Row("100055", "guangzhou", 1, 7344049.0),
        Row("1", "guangzhou", 1, 5284541.562),
        Row("10006", "wuhan", 1, 9397210.0),
        Row("100054", "shenzhen", 1, 6284524.0),
        Row("100031", "xiangtan", 1, 6417154.0),
        Row("100039", "wuhan", 1, 1903639.0),
        Row("10", "wuhan", 1, 6807314.635),
        Row("100064", "changsha", 1, 5566105.0),
        Row("100065", "changsha", 1, 3167625.0),
        Row("100040", "yichang", 1, 7882517.0),
        Row("100042", "zhuzhou", 1, 25995.0),
        Row("100067", "changsha", 1, 7575768.0),
        Row("100038", "xiangtan", 1, 2845110.0),
        Row("100043", "yichang", 1, 3278738.0),
        Row("10008", "changsha", 1, 3361201.0),
        Row("100005", "changsha", 1, 3943313.0),
        Row("100006", "changsha", 1, 4453789.0),
        Row("100060", "shenzhen", 1, 7421353.0),
        Row("100045", "wuhan", 1, 9954785.0),
        Row("100046", "wuhan", 1, 426000.0),
        Row("100047", "wuhan", 1, 7841745.0),
        Row("100041", "xiangtan", 1, 3457065.0),
        Row("100071", "changsha", 1, 2201930.0),
        Row("100052", "zhuzhou", 1, 168570.0),
        Row("100050", "yichang", 1, 7768497.0),
        Row("100077", "changsha", 1, 6385639.0),
        Row("1000", "changsha", 1, 8979457.0),
        Row("100053", "zhuzhou", 1, 2652739.0),
        Row("100048", "xiangtan", 1, 9502885.0),
        Row("100049", "xiangtan", 1, 1954940.0),
        Row("100079", "changsha", 1, 5160219.0),
        Row("100075", "guangzhou", 1, 3217834.0),
        Row("100014", "changsha", 1, 7610226.0),
        Row("100015", "changsha", 1, 6665954.0),
        Row("100019", "changsha", 1, 4461270.0),
        Row("100010", "guangzhou", 1, 8543359.0),
        Row("100073", "shenzhen", 1, 583100.0),
        Row("100074", "shenzhen", 1, 276249.0),
        Row("100056", "wuhan", 1, 1754573.0),
        Row("100051", "xiangtan", 1, 7238326.0),
        Row("100017", "guangzhou", 1, 2613669.0),
        Row("100058", "wuhan", 1, 5405743.0),
        Row("100062", "zhuzhou", 1, 2365066.0),
        Row("100063", "zhuzhou", 1, 9319460.0),
        Row("100013", "shenzhen", 1, 2051894.0),
        Row("100059", "xiangtan", 1, 170094.0)
      )
    )
  }
  )

  //TC_302
  test(
    "select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  ActiveAreaId='6'" +
      " and ActiveOperatorId='100004' group by ActiveOperatorId,Activecity order by total desc"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
          "ActiveAreaId='6' and ActiveOperatorId='100004' group by ActiveOperatorId,Activecity " +
          "order by total desc"
      ),
      Seq(Row("100004", "wuhan", 1, 1442333.0))
    )
  }
  )

  //TC_303
  test(
    "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2  group by Activecity," +
      "ActiveAreaId1"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2  group by " +
          "Activecity,ActiveAreaId"
      ),
      Seq(Row("1", "shenzhen", 7, 4.8863556E7),
        Row("5", "zhuzhou", 6, 3.8359697E7),
        Row("6", "wuhan", 8, 7.662756463499999E7),
        Row("3", "changsha", 8, 1.00876754E8),
        Row("2", "guangzhou", 7, 4.1185336562E7),
        Row("7", "yichang", 6, 6.7214095E7),
        Row("4", "xiangtan", 7, 1.02219772E8)
      )
    )
  }
  )

  //TC_304
  test(
    "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  ActiveAreaId='6'" +
      " group by Activecity,ActiveAreaId order by total desc"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
          "ActiveAreaId='6' group by Activecity,ActiveAreaId order by total desc"
      ),
      Seq(Row("6", "wuhan", 8, 7.662756463499999E7))
    )
  }
  )

  //TC_305
  test(
    "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2  group by Activecity," +
      "ActiveAreaId"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2  group by " +
          "Activecity,ActiveAreaId"
      ),
      Seq(Row("1", "shenzhen", 7, 4.8863556E7),
        Row("5", "zhuzhou", 6, 3.8359697E7),
        Row("6", "wuhan", 8, 7.662756463499999E7),
        Row("3", "changsha", 8, 1.00876754E8),
        Row("2", "guangzhou", 7, 4.1185336562E7),
        Row("7", "yichang", 6, 6.7214095E7),
        Row("4", "xiangtan", 7, 1.02219772E8)
      )
    )
  }
  )

  //TC_306
  test(
    "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  ActiveOperatorId" +
      " in('100000','100004') group by Activecity,ActiveAreaId order by total desc"
  )({
    checkAnswer(
      sql(
        "select ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
          "(gamePointId+contractNumber) as total from Carbon_automation_test2 where  " +
          "ActiveOperatorId in('100000','100004') group by Activecity,ActiveAreaId order by total" +
          " desc"
      ),
      Seq(Row("7", "yichang", 1, 1602594.0), Row("6", "wuhan", 1, 1442333.0))
    )
  }
  )

  //TC_307
  test(
    "select ActiveOperatorId,ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number,sum" +
      "(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
      "ActiveOperatorId,Activecity,ActiveAreaId"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId,ActiveAreaId,Activecity,count(distinct AMSize) as AMSize_number," +
          "sum(gamePointId+contractNumber) as total from Carbon_automation_test2 group by " +
          "ActiveOperatorId,Activecity,ActiveAreaId"
      ),
      Seq(Row("100043", "7", "yichang", 1, 3278738.0),
        Row("100082", "4", "xiangtan", 1, 997163.0),
        Row("100016", "4", "xiangtan", 1, 6497165.0),
        Row("100081", "5", "zhuzhou", 1, 3336093.0),
        Row("100076", "7", "yichang", 1, 8070591.0),
        Row("100019", "3", "changsha", 1, 4461270.0),
        Row("10004", "1", "shenzhen", 1, 3079020.0),
        Row("10", "6", "wuhan", 1, 6807314.635),
        Row("100042", "5", "zhuzhou", 1, 25995.0),
        Row("100070", "6", "wuhan", 1, 4204181.0),
        Row("100038", "4", "xiangtan", 1, 2845110.0),
        Row("100004", "6", "wuhan", 1, 1442333.0),
        Row("100013", "1", "shenzhen", 1, 2051894.0),
        Row("10006", "6", "wuhan", 1, 9397210.0),
        Row("10003", "4", "xiangtan", 1, 5588789.0),
        Row("100074", "1", "shenzhen", 1, 276249.0),
        Row("100053", "5", "zhuzhou", 1, 2652739.0),
        Row("100049", "4", "xiangtan", 1, 1954940.0),
        Row("100003", "5", "zhuzhou", 1, 90470.0),
        Row("10005", "5", "zhuzhou", 1, 6191676.0),
        Row("10002", "7", "yichang", 1, 5207711.0),
        Row("100057", "3", "changsha", 1, 5453821.0),
        Row("100031", "4", "xiangtan", 1, 6417154.0),
        Row("100035", "1", "shenzhen", 1, 6285204.0),
        Row("1000", "3", "changsha", 1, 8979457.0),
        Row("100034", "3", "changsha", 1, 5799140.0),
        Row("100079", "3", "changsha", 1, 5160219.0),
        Row("100029", "3", "changsha", 1, 7776281.0),
        Row("100073", "1", "shenzhen", 1, 583100.0),
        Row("100052", "5", "zhuzhou", 1, 168570.0),
        Row("100048", "4", "xiangtan", 1, 9502885.0),
        Row("100002", "5", "zhuzhou", 1, 9456953.0),
        Row("100080", "4", "xiangtan", 1, 5153939.0),
        Row("100063", "5", "zhuzhou", 1, 9319460.0),
        Row("100059", "4", "xiangtan", 1, 170094.0),
        Row("100025", "6", "wuhan", 1, 6535623.0),
        Row("100006", "3", "changsha", 1, 4453789.0),
        Row("100009", "4", "xiangtan", 1, 2391498.0),
        Row("100067", "3", "changsha", 1, 7575768.0),
        Row("100041", "4", "xiangtan", 1, 3457065.0),
        Row("100022", "2", "guangzhou", 1, 834386.0),
        Row("100069", "7", "yichang", 1, 7918697.0),
        Row("100047", "6", "wuhan", 1, 7841745.0),
        Row("100040", "7", "yichang", 1, 7882517.0),
        Row("10001", "4", "xiangtan", 1, 5986487.0),
        Row("100017", "2", "guangzhou", 1, 2613669.0),
        Row("100058", "6", "wuhan", 1, 5405743.0),
        Row("100083", "1", "shenzhen", 1, 509421.0),
        Row("100062", "5", "zhuzhou", 1, 2365066.0),
        Row("100008", "6", "wuhan", 1, 1072199.0),
        Row("100071", "3", "changsha", 1, 2201930.0),
        Row("100024", "6", "wuhan", 1, 6171950.0),
        Row("10007", "4", "xiangtan", 1, 4157689.0),
        Row("100005", "3", "changsha", 1, 3943313.0),
        Row("100021", "3", "changsha", 1, 568695.0),
        Row("100010", "2", "guangzhou", 1, 8543359.0),
        Row("100055", "2", "guangzhou", 1, 7344049.0),
        Row("100028", "1", "shenzhen", 1, 8882961.0),
        Row("100044", "1", "shenzhen", 1, 5660804.0),
        Row("100060", "1", "shenzhen", 1, 7421353.0),
        Row("100077", "3", "changsha", 1, 6385639.0),
        Row("100023", "7", "yichang", 1, 2852440.0),
        Row("100051", "4", "xiangtan", 1, 7238326.0),
        Row("100001", "6", "wuhan", 1, 2920291.0),
        Row("100046", "6", "wuhan", 1, 426000.0),
        Row("10000", "4", "xiangtan", 1, 3787033.0),
        Row("1000000", "4", "xiangtan", 1, 9739368.0),
        Row("100018", "7", "yichang", 1, 574816.0),
        Row("100050", "7", "yichang", 1, 7768497.0),
        Row("100012", "4", "xiangtan", 1, 8454563.0),
        Row("100000", "7", "yichang", 1, 1602594.0),
        Row("1", "2", "guangzhou", 1, 5284541.562),
        Row("100032", "1", "shenzhen", 1, 6995116.0),
        Row("100007", "4", "xiangtan", 1, 337574.0),
        Row("100065", "3", "changsha", 1, 3167625.0),
        Row("100068", "4", "xiangtan", 1, 3235498.0),
        Row("100084", "4", "xiangtan", 1, 8979394.0),
        Row("100015", "3", "changsha", 1, 6665954.0),
        Row("100045", "6", "wuhan", 1, 9954785.0),
        Row("100054", "1", "shenzhen", 1, 6284524.0),
        Row("100061", "6", "wuhan", 1, 4360028.0),
        Row("100033", "7", "yichang", 1, 8230567.0),
        Row("100056", "6", "wuhan", 1, 1754573.0),
        Row("100020", "1", "shenzhen", 1, 833910.0),
        Row("100037", "3", "changsha", 1, 3312327.0),
        Row("100026", "2", "guangzhou", 1, 7488902.0),
        Row("100011", "4", "xiangtan", 1, 4816462.0),
        Row("100072", "4", "xiangtan", 1, 511576.0),
        Row("100064", "3", "changsha", 1, 5566105.0),
        Row("100078", "6", "wuhan", 1, 6429950.0),
        Row("100014", "3", "changsha", 1, 7610226.0),
        Row("100", "7", "yichang", 1, 8232606.0),
        Row("10008", "3", "changsha", 1, 3361201.0),
        Row("100066", "7", "yichang", 1, 5594321.0),
        Row("100039", "6", "wuhan", 1, 1903639.0),
        Row("100030", "2", "guangzhou", 1, 5858596.0),
        Row("100075", "2", "guangzhou", 1, 3217834.0),
        Row("100027", "5", "zhuzhou", 1, 4752675.0),
        Row("100036", "3", "changsha", 1, 8433994.0)
      )
    )
  }
  )

  //TC_308
  test(
    "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
      ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
      "where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
  )({
    checkAnswer(
      sql(
        "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
          "where t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
      ),
      Seq(Row("7RAM size", "yichang", 14, 1.15242211E8),
        Row("0RAM size", "shenzhen", 11, 9.7712571E7),
        Row("6RAM size", "guangzhou", 9, 6.7400118E7),
        Row("1RAM size", "xiangtan", 36, 1.94071005E8),
        Row("9RAM size", "shenzhen", 20, 1.1945328E8),
        Row("4RAM size", "changsha", 132, 6.55134084E8),
        Row("8RAM size", "zhuzhou", 30, 1.2824123E8),
        Row("8RAM size", "yichang", 20, 8.060151E7),
        Row("4RAM size", "guangzhou", 22, 1.61569078E8),
        Row("0RAM size", "zhuzhou", 11, 1.04026483E8),
        Row("8RAM size", "wuhan", 10, 1.442333E7),
        Row("2RAM size", "changsha", 2, 4403860.0),
        Row("8RAM size", "xiangtan", 20, 9.342275E7),
        Row("3RAM size", "yichang", 14, 1.10861758E8),
        Row("6RAM size", "zhuzhou", 9, 2.1285594E7),
        Row("0RAM size", "wuhan", 44, 1.4694625E8),
        Row("6RAM size", "wuhan", 18, 1.28445255E8),
        Row("9RAM size", "wuhan", 10, 4.204181E7),
        Row("0RAM size", "changsha", 44, 1.55468467E8),
        Row("3RAM size", "changsha", 14, 9.3323356E7),
        Row("6RAM size", "xiangtan", 18, 1.2996945E8),
        Row("4RAM size", "yichang", 88, 3.96362956E8),
        Row("7RAM size", "zhuzhou", 7, 633290.0),
        Row("0RAM size", "guangzhou", 11, 9.3976949E7),
        Row("3RAM size", "guangzhou", 14, 1.1681404E7),
        Row("9RAM size", "changsha", 20, 1.1746321E8),
        Row("3RAM size", "shenzhen", 56, 1.17094404E8),
        Row("4RAM size", "wuhan", 66, 3.7813819197E8),
        Row("6RAM size", "shenzhen", 9, 5.6566836E7),
        Row("7RAM size", "wuhan", 21, 7.9516871E7),
        Row("4RAM size", "xiangtan", 132, 5.86041984E8),
        Row("1RAM size", "guangzhou", 9, 5.2727364E7),
        Row("7RAM size", "changsha", 7, 5.3271582E7),
        Row("1RAM size", "shenzhen", 9, 7505190.0),
        Row("4RAM size", "shenzhen", 44, 2.31008206E8),
        Row("5RAM size", "wuhan", 5, 4.698605E7),
        Row("2RAM size", "xiangtan", 2, 8315378.0),
        Row("3RAM size", "zhuzhou", 42, 1.9035926E8),
        Row("5RAM size", "changsha", 10, 7.682548E7),
        Row("9RAM size", "yichang", 10, 3.278738E7),
        Row("5RAM size", "guangzhou", 10, 2.9157515E7),
        Row("3RAM size", "wuhan", 14, 7.5680402E7),
        Row("8RAM size", "guangzhou", 10, 5.284541562E7),
        Row("3RAM size", "xiangtan", 42, 1.93558036E8),
        Row("1RAM size", "zhuzhou", 9, 233955.0),
        Row("1RAM size", "yichang", 18, 1.21291542E8),
        Row("8RAM size", "shenzhen", 10, 2.051894E7),
        Row("9RAM size", "xiangtan", 40, 1.2251467E8),
        Row("6RAM size", "changsha", 18, 1.20374172E8)
      )
    )
  }
  )

  //TC_309
  test(
    "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
      ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
      "where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2.AMSize"
  )({
    checkAnswer(
      sql(
        "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
          "where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2" +
          ".AMSize"
      ),
      Seq(Row("8RAM size", "wuhan", 10, 1.442333E7))
    )
  }
  )

  //TC_310
  test(
    "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
      ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
      "where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2" +
      ".AMSize order by total desc"
  )({
    checkAnswer(
      sql(
        "select t2.AMSize,t1.Activecity,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
          "where t1.ActiveOperatorId='100004' and t1.AMSize=t2.AMSize group by t1.Activecity,t2" +
          ".AMSize order by total desc"
      ),
      Seq(Row("8RAM size", "wuhan", 10, 1.442333E7))
    )
  }
  )

  //TC_311
  test(
    "select t2.AMSize,t1.ActiveAreaId,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
      ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
      "where t1.AMSize=t2.AMSize group by t1.ActiveAreaId,t2.AMSize"
  )({
    checkAnswer(
      sql(
        "select t2.AMSize,t1.ActiveAreaId,count(t1.AMSize) as AMSize_number,sum(t1.gamePointId+t1" +
          ".contractNumber) as total from Carbon_automation_test2 t1, Carbon_automation_test2 t2 " +
          "where t1.AMSize=t2.AMSize group by t1.ActiveAreaId,t2.AMSize"
      ),
      Seq(Row("7RAM size", "3", 7, 5.3271582E7),
        Row("4RAM size", "6", 66, 3.7813819197E8),
        Row("9RAM size", "4", 40, 1.2251467E8),
        Row("3RAM size", "7", 14, 1.10861758E8),
        Row("8RAM size", "5", 30, 1.2824123E8),
        Row("1RAM size", "1", 9, 7505190.0),
        Row("4RAM size", "1", 44, 2.31008206E8),
        Row("3RAM size", "2", 14, 1.1681404E7),
        Row("6RAM size", "2", 9, 6.7400118E7),
        Row("0RAM size", "5", 11, 1.04026483E8),
        Row("5RAM size", "3", 10, 7.682548E7),
        Row("1RAM size", "7", 18, 1.21291542E8),
        Row("4RAM size", "7", 88, 3.96362956E8),
        Row("8RAM size", "6", 10, 1.442333E7),
        Row("1RAM size", "2", 9, 5.2727364E7),
        Row("4RAM size", "2", 22, 1.61569078E8),
        Row("3RAM size", "3", 14, 9.3323356E7),
        Row("8RAM size", "1", 10, 2.051894E7),
        Row("6RAM size", "3", 18, 1.20374172E8),
        Row("0RAM size", "6", 44, 1.4694625E8),
        Row("7RAM size", "5", 7, 633290.0),
        Row("0RAM size", "1", 11, 9.7712571E7),
        Row("9RAM size", "6", 10, 4.204181E7),
        Row("8RAM size", "7", 20, 8.060151E7),
        Row("4RAM size", "3", 132, 6.55134084E8),
        Row("9RAM size", "1", 20, 1.1945328E8),
        Row("3RAM size", "4", 42, 1.93558036E8),
        Row("8RAM size", "2", 10, 5.284541562E7),
        Row("6RAM size", "4", 18, 1.2996945E8),
        Row("7RAM size", "6", 21, 7.9516871E7),
        Row("0RAM size", "2", 11, 9.3976949E7),
        Row("9RAM size", "7", 10, 3.278738E7),
        Row("2RAM size", "3", 2, 4403860.0),
        Row("1RAM size", "4", 36, 1.94071005E8),
        Row("4RAM size", "4", 132, 5.86041984E8),
        Row("3RAM size", "5", 42, 1.9035926E8),
        Row("6RAM size", "5", 9, 2.1285594E7),
        Row("5RAM size", "6", 5, 4.698605E7),
        Row("7RAM size", "7", 14, 1.15242211E8),
        Row("0RAM size", "3", 44, 1.55468467E8),
        Row("2RAM size", "4", 2, 8315378.0),
        Row("1RAM size", "5", 9, 233955.0),
        Row("9RAM size", "3", 20, 1.1746321E8),
        Row("3RAM size", "6", 14, 7.5680402E7),
        Row("6RAM size", "6", 18, 1.28445255E8),
        Row("8RAM size", "4", 20, 9.342275E7),
        Row("3RAM size", "1", 56, 1.17094404E8),
        Row("6RAM size", "1", 9, 5.6566836E7),
        Row("5RAM size", "2", 10, 2.9157515E7)
      )
    )
  }
  )

  //TC_312
  test(
    "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as AMSize_count " +
      "from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if((t1.gamePointId+t1" +
      ".contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)>100,'50~10',if((t1" +
      ".gamePointId+t1.contractNumber)>100, '10~1','<1'))) as ActiveOperatorId from " +
      "Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as " +
          "AMSize_count from (select AMSize, t1.gamePointId+t1.contractNumber as imeiupdown, if(" +
          "(t1.gamePointId+t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)" +
          ">100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as " +
          "ActiveOperatorId from Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
      ),
      Seq(Row(">50", 4.75346775197E8, 10))
    )
  }
  )

  //TC_313
  test(
    "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as AMSize_count " +
      "from (select AMSize, t1.gamePointId+ t1.contractNumber as imeiupdown, if((t1.gamePointId+ " +
      "t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)>100,'50~10',if((t1" +
      ".gamePointId+t1.contractNumber)>100, '10~1','<1'))) as ActiveOperatorId from " +
      "Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
  )({
    checkAnswer(
      sql(
        "select ActiveOperatorId, sum(imeiupdown) as total, count(distinct AMSize) as " +
          "AMSize_count from (select AMSize, t1.gamePointId+ t1.contractNumber as imeiupdown, if(" +
          "(t1.gamePointId+ t1.contractNumber)>100, '>50', if((t1.gamePointId+t1.contractNumber)" +
          ">100,'50~10',if((t1.gamePointId+t1.contractNumber)>100, '10~1','<1'))) as " +
          "ActiveOperatorId from Carbon_automation_test2 t1) t2 group by ActiveOperatorId"
      ),
      Seq(Row(">50", 4.75346775197E8, 10))
    )
  }
  )

  //TC_314
  test(
    "SELECT AMSize, ActiveAreaId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from " +
      "Carbon_automation_test2) SUB_QRY WHERE AMSize BETWEEN \"0RAM size\" AND \"1RAM size\" " +
      "GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, SUM(gamepointid) AS Sum_gamepointid FROM (select * from " +
          "Carbon_automation_test2) SUB_QRY WHERE AMSize BETWEEN \"0RAM size\" AND \"1RAM size\" " +
          "GROUP BY AMSize, ActiveAreaId ORDER BY AMSize ASC, ActiveAreaId ASC"
      ),
      Seq(Row("0RAM size", "1", 2849.0),
        Row("0RAM size", "2", 79.0),
        Row("0RAM size", "3", 7663.0),
        Row("0RAM size", "5", 1341.0),
        Row("0RAM size", "6", 6082.0),
        Row("1RAM size", "1", 256.0),
        Row("1RAM size", "2", 1333.0),
        Row("1RAM size", "4", 7510.0),
        Row("1RAM size", "5", 2745.0),
        Row("1RAM size", "7", 3942.0)
      )
    )
  }
  )

  //TC_315
  test(
    "SELECT AMSize, ActiveAreaId, imei FROM (select * from Carbon_automation_test2) SUB_QRY WHERE" +
      "  Latest_DAY BETWEEN 1 AND 1"
  )({
    checkAnswer(
      sql(
        "SELECT AMSize, ActiveAreaId, imei FROM (select * from Carbon_automation_test2) SUB_QRY " +
          "WHERE  Latest_DAY BETWEEN 1 AND 1"
      ),
      Seq(Row("8RAM size", "2", "1AA1"),
        Row("4RAM size", "6", "1AA10"),
        Row("7RAM size", "7", "1AA100"),
        Row("5RAM size", "3", "1AA1000"),
        Row("1RAM size", "4", "1AA10000"),
        Row("4RAM size", "7", "1AA100000"),
        Row("4RAM size", "4", "1AA1000000"),
        Row("7RAM size", "6", "1AA100001"),
        Row("0RAM size", "5", "1AA100002"),
        Row("7RAM size", "5", "1AA100003"),
        Row("8RAM size", "6", "1AA100004"),
        Row("0RAM size", "3", "1AA100005"),
        Row("4RAM size", "3", "1AA100006"),
        Row("4RAM size", "4", "1AA100007"),
        Row("0RAM size", "6", "1AA100008"),
        Row("4RAM size", "4", "1AA100009"),
        Row("6RAM size", "4", "1AA10001"),
        Row("0RAM size", "2", "1AA100010"),
        Row("1RAM size", "4", "1AA100011"),
        Row("6RAM size", "4", "1AA100012"),
        Row("8RAM size", "1", "1AA100013"),
        Row("7RAM size", "3", "1AA100014"),
        Row("3RAM size", "3", "1AA100015"),
        Row("8RAM size", "4", "1AA100016"),
        Row("5RAM size", "2", "1AA100017"),
        Row("4RAM size", "7", "1AA100018"),
        Row("0RAM size", "3", "1AA100019"),
        Row("8RAM size", "7", "1AA10002"),
        Row("1RAM size", "1", "1AA100020"),
        Row("0RAM size", "3", "1AA100021"),
        Row("3RAM size", "2", "1AA100022"),
        Row("8RAM size", "7", "1AA100023"),
        Row("0RAM size", "6", "1AA100024"),
        Row("7RAM size", "6", "1AA100025"),
        Row("6RAM size", "2", "1AA100026"),
        Row("3RAM size", "5", "1AA100027"),
        Row("0RAM size", "1", "1AA100028"),
        Row("4RAM size", "3", "1AA100029"),
        Row("9RAM size", "4", "1AA10003"),
        Row("1RAM size", "2", "1AA100030"),
        Row("3RAM size", "4", "1AA100031"),
        Row("3RAM size", "1", "1AA100032"),
        Row("7RAM size", "7", "1AA100033"),
        Row("6RAM size", "3", "1AA100034"),
        Row("6RAM size", "1", "1AA100035"),
        Row("9RAM size", "3", "1AA100036"),
        Row("9RAM size", "3", "1AA100037"),
        Row("8RAM size", "4", "1AA100038"),
        Row("7RAM size", "6", "1AA100039"),
        Row("4RAM size", "1", "1AA10004"),
        Row("1RAM size", "7", "1AA100040"),
        Row("1RAM size", "4", "1AA100041"),
        Row("1RAM size", "5", "1AA100042"),
        Row("9RAM size", "7", "1AA100043"),
        Row("9RAM size", "1", "1AA100044"),
        Row("4RAM size", "6", "1AA100045"),
        Row("4RAM size", "6", "1AA100046"),
        Row("6RAM size", "6", "1AA100047"),
        Row("1RAM size", "4", "1AA100048"),
        Row("4RAM size", "4", "1AA100049"),
        Row("3RAM size", "5", "1AA10005"),
        Row("4RAM size", "7", "1AA100050"),
        Row("3RAM size", "4", "1AA100051"),
        Row("8RAM size", "5", "1AA100052"),
        Row("3RAM size", "5", "1AA100053"),
        Row("9RAM size", "1", "1AA100054"),
        Row("4RAM size", "2", "1AA100055"),
        Row("0RAM size", "6", "1AA100056"),
        Row("4RAM size", "3", "1AA100057"),
        Row("3RAM size", "6", "1AA100058"),
        Row("3RAM size", "4", "1AA100059"),
        Row("5RAM size", "6", "1AA10006"),
        Row("4RAM size", "1", "1AA100060"),
        Row("0RAM size", "6", "1AA100061"),
        Row("6RAM size", "5", "1AA100062"),
        Row("8RAM size", "5", "1AA100063"),
        Row("4RAM size", "3", "1AA100064"),
        Row("4RAM size", "3", "1AA100065"),
        Row("1RAM size", "7", "1AA100066"),
        Row("6RAM size", "3", "1AA100067"),
        Row("4RAM size", "4", "1AA100068"),
        Row("3RAM size", "7", "1AA100069"),
        Row("2RAM size", "4", "1AA10007"),
        Row("9RAM size", "6", "1AA100070"),
        Row("2RAM size", "3", "1AA100071"),
        Row("9RAM size", "4", "1AA100072"),
        Row("3RAM size", "1", "1AA100073"),
        Row("3RAM size", "1", "1AA100074"),
        Row("5RAM size", "2", "1AA100075"),
        Row("4RAM size", "7", "1AA100076"),
        Row("5RAM size", "3", "1AA100077"),
        Row("6RAM size", "6", "1AA100078"),
        Row("0RAM size", "3", "1AA100079"),
        Row("4RAM size", "3", "1AA10008"),
        Row("9RAM size", "4", "1AA100080"),
        Row("8RAM size", "5", "1AA100081"),
        Row("9RAM size", "4", "1AA100082"),
        Row("3RAM size", "1", "1AA100083"),
        Row("4RAM size", "4", "1AA100084")
      )
    )
  }
  )

  //TC_316
  test(
    "select series,gamepointid from Carbon_automation_test2 where gamepointid between 1407 and 1407"
  )({
    checkAnswer(
      sql(
        "select series,gamepointid from Carbon_automation_test2 where gamepointid between 1407 " +
          "and 1407"
      ),
      Seq(Row("2Series", 1407.0), Row("6Series", 1407.0))
    )
  }
  )

  //TC_327
  test(
    "SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS " +
      "Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY WHERE imei >= " +
      "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"
  )({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, COUNT(Latest_YEAR) AS Count_Latest_YEAR, SUM(gamePointId) AS " +
          "Sum_gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY WHERE imei >= " +
          "\"1AA1000000\" GROUP BY imei, deliveryCity ORDER BY imei ASC, deliveryCity ASC"
      ),
      Seq(Row("1AA1000000", "yichang", 1, 1600.0),
        Row("1AA100001", "xiangtan", 1, 505.0),
        Row("1AA100002", "changsha", 1, 1341.0),
        Row("1AA100003", "zhuzhou", 1, 2239.0),
        Row("1AA100004", "yichang", 1, 2970.0),
        Row("1AA100005", "yichang", 1, 2593.0),
        Row("1AA100006", "changsha", 1, 2572.0),
        Row("1AA100007", "changsha", 1, 1991.0),
        Row("1AA100008", "changsha", 1, 1442.0),
        Row("1AA100009", "yichang", 1, 1841.0),
        Row("1AA10001", "changsha", 1, 298.0),
        Row("1AA100010", "zhuzhou", 1, 79.0),
        Row("1AA100011", "guangzhou", 1, 202.0),
        Row("1AA100012", "xiangtan", 1, 568.0),
        Row("1AA100013", "changsha", 1, 355.0),
        Row("1AA100014", "zhuzhou", 1, 151.0),
        Row("1AA100015", "xiangtan", 1, 2863.0),
        Row("1AA100016", "changsha", 1, 1873.0),
        Row("1AA100017", "xiangtan", 1, 2205.0),
        Row("1AA100018", "yichang", 1, 441.0),
        Row("1AA100019", "zhuzhou", 1, 2194.0),
        Row("1AA10002", "wuhan", 1, 2972.0),
        Row("1AA100020", "shenzhen", 1, 256.0),
        Row("1AA100021", "changsha", 1, 1778.0),
        Row("1AA100022", "zhuzhou", 1, 1999.0),
        Row("1AA100023", "guangzhou", 1, 2194.0),
        Row("1AA100024", "changsha", 1, 2483.0),
        Row("1AA100025", "guangzhou", 1, 1724.0),
        Row("1AA100026", "yichang", 1, 1768.0),
        Row("1AA100027", "zhuzhou", 1, 2436.0),
        Row("1AA100028", "zhuzhou", 1, 2849.0),
        Row("1AA100029", "xiangtan", 1, 1691.0),
        Row("1AA10003", "xiangtan", 1, 2071.0),
        Row("1AA100030", "zhuzhou", 1, 1333.0),
        Row("1AA100031", "yichang", 1, 1080.0),
        Row("1AA100032", "shenzhen", 1, 1053.0),
        Row("1AA100033", "wuhan", 1, 760.0),
        Row("1AA100034", "guangzhou", 1, 2061.0),
        Row("1AA100035", "changsha", 1, 2142.0),
        Row("1AA100036", "changsha", 1, 2224.0),
        Row("1AA100037", "xiangtan", 1, 1015.0),
        Row("1AA100038", "shenzhen", 1, 1229.0),
        Row("1AA100039", "shenzhen", 1, 1750.0),
        Row("1AA10004", "guangzhou", 1, 1717.0),
        Row("1AA100040", "yichang", 1, 2078.0),
        Row("1AA100041", "shenzhen", 1, 2734.0),
        Row("1AA100042", "shenzhen", 1, 2745.0),
        Row("1AA100043", "guangzhou", 1, 571.0),
        Row("1AA100044", "guangzhou", 1, 1697.0),
        Row("1AA100045", "xiangtan", 1, 2553.0),
        Row("1AA100046", "guangzhou", 1, 1077.0),
        Row("1AA100047", "zhuzhou", 1, 1823.0),
        Row("1AA100048", "guangzhou", 1, 2399.0),
        Row("1AA100049", "guangzhou", 1, 2890.0),
        Row("1AA10005", "xiangtan", 1, 1608.0),
        Row("1AA100050", "yichang", 1, 29.0),
        Row("1AA100051", "guangzhou", 1, 1407.0),
        Row("1AA100052", "zhuzhou", 1, 845.0),
        Row("1AA100053", "wuhan", 1, 1655.0),
        Row("1AA100054", "shenzhen", 1, 1368.0),
        Row("1AA100055", "yichang", 1, 1728.0),
        Row("1AA100056", "wuhan", 1, 750.0),
        Row("1AA100057", "zhuzhou", 1, 2288.0),
        Row("1AA100058", "guangzhou", 1, 2635.0),
        Row("1AA100059", "shenzhen", 1, 1337.0),
        Row("1AA10006", "guangzhou", 1, 2478.0),
        Row("1AA100060", "xiangtan", 1, 538.0),
        Row("1AA100061", "changsha", 1, 1407.0),
        Row("1AA100062", "yichang", 1, 2952.0),
        Row("1AA100063", "yichang", 1, 1226.0),
        Row("1AA100064", "zhuzhou", 1, 865.0),
        Row("1AA100065", "xiangtan", 1, 901.0),
        Row("1AA100066", "zhuzhou", 1, 1864.0),
        Row("1AA100067", "wuhan", 1, 572.0),
        Row("1AA100068", "guangzhou", 1, 412.0),
        Row("1AA100069", "xiangtan", 1, 1491.0),
        Row("1AA10007", "xiangtan", 1, 1350.0),
        Row("1AA100070", "guangzhou", 1, 1567.0),
        Row("1AA100071", "guangzhou", 1, 1973.0),
        Row("1AA100072", "changsha", 1, 448.0),
        Row("1AA100073", "zhuzhou", 1, 2488.0),
        Row("1AA100074", "wuhan", 1, 907.0),
        Row("1AA100075", "shenzhen", 1, 2507.0),
        Row("1AA100076", "wuhan", 1, 732.0),
        Row("1AA100077", "yichang", 1, 2077.0),
        Row("1AA100078", "yichang", 1, 1434.0),
        Row("1AA100079", "xiangtan", 1, 1098.0),
        Row("1AA10008", "shenzhen", 1, 813.0),
        Row("1AA100080", "shenzhen", 1, 954.0),
        Row("1AA100081", "shenzhen", 1, 613.0),
        Row("1AA100082", "xiangtan", 1, 2348.0),
        Row("1AA100083", "zhuzhou", 1, 2192.0),
        Row("1AA100084", "guangzhou", 1, 2826.0)
      )
    )
  }
  )

  //TC_329
  test(
    "SELECT imei, deliveryCity, series, SUM(gamePointId) AS Sum_gamePointId, COUNT(Latest_DAY) AS" +
      " Count_Latest_DAY FROM (select * from Carbon_automation_test2) SUB_QRY WHERE imei >= " +
      "\"1AA1000000\" GROUP BY imei, deliveryCity, series ORDER BY imei ASC, deliveryCity ASC, " +
      "series ASC"
  )({
    checkAnswer(
      sql(
        "SELECT imei, deliveryCity, series, SUM(gamePointId) AS Sum_gamePointId, COUNT" +
          "(Latest_DAY) AS Count_Latest_DAY FROM (select * from Carbon_automation_test2) SUB_QRY " +
          "WHERE imei >= \"1AA1000000\" GROUP BY imei, deliveryCity, series ORDER BY imei ASC, " +
          "deliveryCity ASC, series ASC"
      ),
      Seq(Row("1AA1000000", "yichang", "7Series", 1600.0, 1),
        Row("1AA100001", "xiangtan", "0Series", 505.0, 1),
        Row("1AA100002", "changsha", "0Series", 1341.0, 1),
        Row("1AA100003", "zhuzhou", "5Series", 2239.0, 1),
        Row("1AA100004", "yichang", "4Series", 2970.0, 1),
        Row("1AA100005", "yichang", "1Series", 2593.0, 1),
        Row("1AA100006", "changsha", "6Series", 2572.0, 1),
        Row("1AA100007", "changsha", "9Series", 1991.0, 1),
        Row("1AA100008", "changsha", "8Series", 1442.0, 1),
        Row("1AA100009", "yichang", "0Series", 1841.0, 1),
        Row("1AA10001", "changsha", "2Series", 298.0, 1),
        Row("1AA100010", "zhuzhou", "3Series", 79.0, 1),
        Row("1AA100011", "guangzhou", "0Series", 202.0, 1),
        Row("1AA100012", "xiangtan", "4Series", 568.0, 1),
        Row("1AA100013", "changsha", "1Series", 355.0, 1),
        Row("1AA100014", "zhuzhou", "5Series", 151.0, 1),
        Row("1AA100015", "xiangtan", "4Series", 2863.0, 1),
        Row("1AA100016", "changsha", "3Series", 1873.0, 1),
        Row("1AA100017", "xiangtan", "9Series", 2205.0, 1),
        Row("1AA100018", "yichang", "8Series", 441.0, 1),
        Row("1AA100019", "zhuzhou", "5Series", 2194.0, 1),
        Row("1AA10002", "wuhan", "0Series", 2972.0, 1),
        Row("1AA100020", "shenzhen", "5Series", 256.0, 1),
        Row("1AA100021", "changsha", "0Series", 1778.0, 1),
        Row("1AA100022", "zhuzhou", "5Series", 1999.0, 1),
        Row("1AA100023", "guangzhou", "5Series", 2194.0, 1),
        Row("1AA100024", "changsha", "6Series", 2483.0, 1),
        Row("1AA100025", "guangzhou", "0Series", 1724.0, 1),
        Row("1AA100026", "yichang", "7Series", 1768.0, 1),
        Row("1AA100027", "zhuzhou", "0Series", 2436.0, 1),
        Row("1AA100028", "zhuzhou", "5Series", 2849.0, 1),
        Row("1AA100029", "xiangtan", "2Series", 1691.0, 1),
        Row("1AA10003", "xiangtan", "7Series", 2071.0, 1),
        Row("1AA100030", "zhuzhou", "7Series", 1333.0, 1),
        Row("1AA100031", "yichang", "7Series", 1080.0, 1),
        Row("1AA100032", "shenzhen", "1Series", 1053.0, 1),
        Row("1AA100033", "wuhan", "8Series", 760.0, 1),
        Row("1AA100034", "guangzhou", "2Series", 2061.0, 1),
        Row("1AA100035", "changsha", "5Series", 2142.0, 1),
        Row("1AA100036", "changsha", "5Series", 2224.0, 1),
        Row("1AA100037", "xiangtan", "7Series", 1015.0, 1),
        Row("1AA100038", "shenzhen", "6Series", 1229.0, 1),
        Row("1AA100039", "shenzhen", "8Series", 1750.0, 1),
        Row("1AA10004", "guangzhou", "5Series", 1717.0, 1),
        Row("1AA100040", "yichang", "8Series", 2078.0, 1),
        Row("1AA100041", "shenzhen", "5Series", 2734.0, 1),
        Row("1AA100042", "shenzhen", "3Series", 2745.0, 1),
        Row("1AA100043", "guangzhou", "9Series", 571.0, 1),
        Row("1AA100044", "guangzhou", "8Series", 1697.0, 1),
        Row("1AA100045", "xiangtan", "2Series", 2553.0, 1),
        Row("1AA100046", "guangzhou", "3Series", 1077.0, 1),
        Row("1AA100047", "zhuzhou", "9Series", 1823.0, 1),
        Row("1AA100048", "guangzhou", "3Series", 2399.0, 1),
        Row("1AA100049", "guangzhou", "0Series", 2890.0, 1),
        Row("1AA10005", "xiangtan", "8Series", 1608.0, 1),
        Row("1AA100050", "yichang", "2Series", 29.0, 1),
        Row("1AA100051", "guangzhou", "2Series", 1407.0, 1),
        Row("1AA100052", "zhuzhou", "6Series", 845.0, 1),
        Row("1AA100053", "wuhan", "2Series", 1655.0, 1),
        Row("1AA100054", "shenzhen", "7Series", 1368.0, 1),
        Row("1AA100055", "yichang", "7Series", 1728.0, 1),
        Row("1AA100056", "wuhan", "6Series", 750.0, 1),
        Row("1AA100057", "zhuzhou", "9Series", 2288.0, 1),
        Row("1AA100058", "guangzhou", "5Series", 2635.0, 1),
        Row("1AA100059", "shenzhen", "4Series", 1337.0, 1),
        Row("1AA10006", "guangzhou", "3Series", 2478.0, 1),
        Row("1AA100060", "xiangtan", "8Series", 538.0, 1),
        Row("1AA100061", "changsha", "6Series", 1407.0, 1),
        Row("1AA100062", "yichang", "9Series", 2952.0, 1),
        Row("1AA100063", "yichang", "2Series", 1226.0, 1),
        Row("1AA100064", "zhuzhou", "6Series", 865.0, 1),
        Row("1AA100065", "xiangtan", "0Series", 901.0, 1),
        Row("1AA100066", "zhuzhou", "6Series", 1864.0, 1),
        Row("1AA100067", "wuhan", "4Series", 572.0, 1),
        Row("1AA100068", "guangzhou", "8Series", 412.0, 1),
        Row("1AA100069", "xiangtan", "8Series", 1491.0, 1),
        Row("1AA10007", "xiangtan", "8Series", 1350.0, 1),
        Row("1AA100070", "guangzhou", "0Series", 1567.0, 1),
        Row("1AA100071", "guangzhou", "0Series", 1973.0, 1),
        Row("1AA100072", "changsha", "4Series", 448.0, 1),
        Row("1AA100073", "zhuzhou", "4Series", 2488.0, 1),
        Row("1AA100074", "wuhan", "6Series", 907.0, 1),
        Row("1AA100075", "shenzhen", "3Series", 2507.0, 1),
        Row("1AA100076", "wuhan", "0Series", 732.0, 1),
        Row("1AA100077", "yichang", "3Series", 2077.0, 1),
        Row("1AA100078", "yichang", "2Series", 1434.0, 1),
        Row("1AA100079", "xiangtan", "4Series", 1098.0, 1),
        Row("1AA10008", "shenzhen", "5Series", 813.0, 1),
        Row("1AA100080", "shenzhen", "9Series", 954.0, 1),
        Row("1AA100081", "shenzhen", "5Series", 613.0, 1),
        Row("1AA100082", "xiangtan", "5Series", 2348.0, 1),
        Row("1AA100083", "zhuzhou", "0Series", 2192.0, 1),
        Row("1AA100084", "guangzhou", "0Series", 2826.0, 1)
      )
    )
  }
  )

  //TC_340
  test(
    "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
      "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamepointid > " +
      "2000.0)"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT" +
          "(gamepointid > 2000.0)"
      ),
      Seq(Row(2015, 1, "1AA10", 1714.635, 10, "7Series", "1AA10", "yichang"),
        Row(2015, 1, "1AA100", 1271.0, 100, "5Series", "1AA100", "xiangtan"),
        Row(2015, 1, "1AA1000", 692.0, 1000, "5Series", "1AA1000", "wuhan"),
        Row(2015, 1, "1AA100000", 136.0, 100000, "9Series", "1AA100000", "wuhan"),
        Row(2015, 1, "1AA1000000", 1600.0, 1000000, "7Series", "1AA1000000", "yichang"),
        Row(2015, 1, "1AA100001", 505.0, 100001, "0Series", "1AA100001", "xiangtan"),
        Row(2015, 1, "1AA100002", 1341.0, 100002, "0Series", "1AA100002", "changsha"),
        Row(2015, 1, "1AA100007", 1991.0, 100007, "9Series", "1AA100007", "changsha"),
        Row(2015, 1, "1AA100008", 1442.0, 100008, "8Series", "1AA100008", "changsha"),
        Row(2015, 1, "1AA100009", 1841.0, 100009, "0Series", "1AA100009", "yichang"),
        Row(2015, 1, "1AA10001", 298.0, 10001, "2Series", "1AA10001", "changsha"),
        Row(2015, 1, "1AA100010", 79.0, 100010, "3Series", "1AA100010", "zhuzhou"),
        Row(2015, 1, "1AA100011", 202.0, 100011, "0Series", "1AA100011", "guangzhou"),
        Row(2015, 1, "1AA100012", 568.0, 100012, "4Series", "1AA100012", "xiangtan"),
        Row(2015, 1, "1AA100013", 355.0, 100013, "1Series", "1AA100013", "changsha"),
        Row(2015, 1, "1AA100014", 151.0, 100014, "5Series", "1AA100014", "zhuzhou"),
        Row(2015, 1, "1AA100016", 1873.0, 100016, "3Series", "1AA100016", "changsha"),
        Row(2015, 1, "1AA100018", 441.0, 100018, "8Series", "1AA100018", "yichang"),
        Row(2015, 1, "1AA100020", 256.0, 100020, "5Series", "1AA100020", "shenzhen"),
        Row(2015, 1, "1AA100021", 1778.0, 100021, "0Series", "1AA100021", "changsha"),
        Row(2015, 1, "1AA100022", 1999.0, 100022, "5Series", "1AA100022", "zhuzhou"),
        Row(2015, 1, "1AA100025", 1724.0, 100025, "0Series", "1AA100025", "guangzhou"),
        Row(2015, 1, "1AA100026", 1768.0, 100026, "7Series", "1AA100026", "yichang"),
        Row(2015, 1, "1AA100029", 1691.0, 100029, "2Series", "1AA100029", "xiangtan"),
        Row(2015, 1, "1AA100030", 1333.0, 100030, "7Series", "1AA100030", "zhuzhou"),
        Row(2015, 1, "1AA100031", 1080.0, 100031, "7Series", "1AA100031", "yichang"),
        Row(2015, 1, "1AA100032", 1053.0, 100032, "1Series", "1AA100032", "shenzhen"),
        Row(2015, 1, "1AA100033", 760.0, 100033, "8Series", "1AA100033", "wuhan"),
        Row(2015, 1, "1AA100037", 1015.0, 100037, "7Series", "1AA100037", "xiangtan"),
        Row(2015, 1, "1AA100038", 1229.0, 100038, "6Series", "1AA100038", "shenzhen"),
        Row(2015, 1, "1AA100039", 1750.0, 100039, "8Series", "1AA100039", "shenzhen"),
        Row(2015, 1, "1AA10004", 1717.0, 10004, "5Series", "1AA10004", "guangzhou"),
        Row(2015, 1, "1AA100043", 571.0, 100043, "9Series", "1AA100043", "guangzhou"),
        Row(2015, 1, "1AA100044", 1697.0, 100044, "8Series", "1AA100044", "guangzhou"),
        Row(2015, 1, "1AA100046", 1077.0, 100046, "3Series", "1AA100046", "guangzhou"),
        Row(2015, 1, "1AA100047", 1823.0, 100047, "9Series", "1AA100047", "zhuzhou"),
        Row(2015, 1, "1AA10005", 1608.0, 10005, "8Series", "1AA10005", "xiangtan"),
        Row(2015, 1, "1AA100050", 29.0, 100050, "2Series", "1AA100050", "yichang"),
        Row(2015, 1, "1AA100051", 1407.0, 100051, "2Series", "1AA100051", "guangzhou"),
        Row(2015, 1, "1AA100052", 845.0, 100052, "6Series", "1AA100052", "zhuzhou"),
        Row(2015, 1, "1AA100053", 1655.0, 100053, "2Series", "1AA100053", "wuhan"),
        Row(2015, 1, "1AA100054", 1368.0, 100054, "7Series", "1AA100054", "shenzhen"),
        Row(2015, 1, "1AA100055", 1728.0, 100055, "7Series", "1AA100055", "yichang"),
        Row(2015, 1, "1AA100056", 750.0, 100056, "6Series", "1AA100056", "wuhan"),
        Row(2015, 1, "1AA100059", 1337.0, 100059, "4Series", "1AA100059", "shenzhen"),
        Row(2015, 1, "1AA100060", 538.0, 100060, "8Series", "1AA100060", "xiangtan"),
        Row(2015, 1, "1AA100061", 1407.0, 100061, "6Series", "1AA100061", "changsha"),
        Row(2015, 1, "1AA100063", 1226.0, 100063, "2Series", "1AA100063", "yichang"),
        Row(2015, 1, "1AA100064", 865.0, 100064, "6Series", "1AA100064", "zhuzhou"),
        Row(2015, 1, "1AA100065", 901.0, 100065, "0Series", "1AA100065", "xiangtan"),
        Row(2015, 1, "1AA100066", 1864.0, 100066, "6Series", "1AA100066", "zhuzhou"),
        Row(2015, 1, "1AA100067", 572.0, 100067, "4Series", "1AA100067", "wuhan"),
        Row(2015, 1, "1AA100068", 412.0, 100068, "8Series", "1AA100068", "guangzhou"),
        Row(2015, 1, "1AA100069", 1491.0, 100069, "8Series", "1AA100069", "xiangtan"),
        Row(2015, 1, "1AA10007", 1350.0, 10007, "8Series", "1AA10007", "xiangtan"),
        Row(2015, 1, "1AA100070", 1567.0, 100070, "0Series", "1AA100070", "guangzhou"),
        Row(2015, 1, "1AA100071", 1973.0, 100071, "0Series", "1AA100071", "guangzhou"),
        Row(2015, 1, "1AA100072", 448.0, 100072, "4Series", "1AA100072", "changsha"),
        Row(2015, 1, "1AA100074", 907.0, 100074, "6Series", "1AA100074", "wuhan"),
        Row(2015, 1, "1AA100076", 732.0, 100076, "0Series", "1AA100076", "wuhan"),
        Row(2015, 1, "1AA100078", 1434.0, 100078, "2Series", "1AA100078", "yichang"),
        Row(2015, 1, "1AA100079", 1098.0, 100079, "4Series", "1AA100079", "xiangtan"),
        Row(2015, 1, "1AA10008", 813.0, 10008, "5Series", "1AA10008", "shenzhen"),
        Row(2015, 1, "1AA100080", 954.0, 100080, "9Series", "1AA100080", "shenzhen"),
        Row(2015, 1, "1AA100081", 613.0, 100081, "5Series", "1AA100081", "shenzhen")
      )
    )
  }
  )

  //TC_341
  test(
    "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE imei = \"1AA10000\""
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE imei = \"1AA10000\""
      ),
      Seq(Row(2175.0, 10000, 2015, "7Series", "1AA10000", "guangzhou"))
    )
  }
  )

  //TC_342
  test(
    "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
      "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamepointid = " +
      "1600)"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, latest_day, imei, gamepointid, deviceinformationid, series, imei, " +
          "deliverycity FROM (select * from Carbon_automation_test2) SUB_QRY WHERE NOT" +
          "(gamepointid = 1600)"
      ),
      Seq(Row(2015, 1, "1AA1", 2738.562, 1, "7Series", "1AA1", "yichang"),
        Row(2015, 1, "1AA10", 1714.635, 10, "7Series", "1AA10", "yichang"),
        Row(2015, 1, "1AA100", 1271.0, 100, "5Series", "1AA100", "xiangtan"),
        Row(2015, 1, "1AA1000", 692.0, 1000, "5Series", "1AA1000", "wuhan"),
        Row(2015, 1, "1AA10000", 2175.0, 10000, "7Series", "1AA10000", "guangzhou"),
        Row(2015, 1, "1AA100000", 136.0, 100000, "9Series", "1AA100000", "wuhan"),
        Row(2015, 1, "1AA100001", 505.0, 100001, "0Series", "1AA100001", "xiangtan"),
        Row(2015, 1, "1AA100002", 1341.0, 100002, "0Series", "1AA100002", "changsha"),
        Row(2015, 1, "1AA100003", 2239.0, 100003, "5Series", "1AA100003", "zhuzhou"),
        Row(2015, 1, "1AA100004", 2970.0, 100004, "4Series", "1AA100004", "yichang"),
        Row(2015, 1, "1AA100005", 2593.0, 100005, "1Series", "1AA100005", "yichang"),
        Row(2015, 1, "1AA100006", 2572.0, 100006, "6Series", "1AA100006", "changsha"),
        Row(2015, 1, "1AA100007", 1991.0, 100007, "9Series", "1AA100007", "changsha"),
        Row(2015, 1, "1AA100008", 1442.0, 100008, "8Series", "1AA100008", "changsha"),
        Row(2015, 1, "1AA100009", 1841.0, 100009, "0Series", "1AA100009", "yichang"),
        Row(2015, 1, "1AA10001", 298.0, 10001, "2Series", "1AA10001", "changsha"),
        Row(2015, 1, "1AA100010", 79.0, 100010, "3Series", "1AA100010", "zhuzhou"),
        Row(2015, 1, "1AA100011", 202.0, 100011, "0Series", "1AA100011", "guangzhou"),
        Row(2015, 1, "1AA100012", 568.0, 100012, "4Series", "1AA100012", "xiangtan"),
        Row(2015, 1, "1AA100013", 355.0, 100013, "1Series", "1AA100013", "changsha"),
        Row(2015, 1, "1AA100014", 151.0, 100014, "5Series", "1AA100014", "zhuzhou"),
        Row(2015, 1, "1AA100015", 2863.0, 100015, "4Series", "1AA100015", "xiangtan"),
        Row(2015, 1, "1AA100016", 1873.0, 100016, "3Series", "1AA100016", "changsha"),
        Row(2015, 1, "1AA100017", 2205.0, 100017, "9Series", "1AA100017", "xiangtan"),
        Row(2015, 1, "1AA100018", 441.0, 100018, "8Series", "1AA100018", "yichang"),
        Row(2015, 1, "1AA100019", 2194.0, 100019, "5Series", "1AA100019", "zhuzhou"),
        Row(2015, 1, "1AA10002", 2972.0, 10002, "0Series", "1AA10002", "wuhan"),
        Row(2015, 1, "1AA100020", 256.0, 100020, "5Series", "1AA100020", "shenzhen"),
        Row(2015, 1, "1AA100021", 1778.0, 100021, "0Series", "1AA100021", "changsha"),
        Row(2015, 1, "1AA100022", 1999.0, 100022, "5Series", "1AA100022", "zhuzhou"),
        Row(2015, 1, "1AA100023", 2194.0, 100023, "5Series", "1AA100023", "guangzhou"),
        Row(2015, 1, "1AA100024", 2483.0, 100024, "6Series", "1AA100024", "changsha"),
        Row(2015, 1, "1AA100025", 1724.0, 100025, "0Series", "1AA100025", "guangzhou"),
        Row(2015, 1, "1AA100026", 1768.0, 100026, "7Series", "1AA100026", "yichang"),
        Row(2015, 1, "1AA100027", 2436.0, 100027, "0Series", "1AA100027", "zhuzhou"),
        Row(2015, 1, "1AA100028", 2849.0, 100028, "5Series", "1AA100028", "zhuzhou"),
        Row(2015, 1, "1AA100029", 1691.0, 100029, "2Series", "1AA100029", "xiangtan"),
        Row(2015, 1, "1AA10003", 2071.0, 10003, "7Series", "1AA10003", "xiangtan"),
        Row(2015, 1, "1AA100030", 1333.0, 100030, "7Series", "1AA100030", "zhuzhou"),
        Row(2015, 1, "1AA100031", 1080.0, 100031, "7Series", "1AA100031", "yichang"),
        Row(2015, 1, "1AA100032", 1053.0, 100032, "1Series", "1AA100032", "shenzhen"),
        Row(2015, 1, "1AA100033", 760.0, 100033, "8Series", "1AA100033", "wuhan"),
        Row(2015, 1, "1AA100034", 2061.0, 100034, "2Series", "1AA100034", "guangzhou"),
        Row(2015, 1, "1AA100035", 2142.0, 100035, "5Series", "1AA100035", "changsha"),
        Row(2015, 1, "1AA100036", 2224.0, 100036, "5Series", "1AA100036", "changsha"),
        Row(2015, 1, "1AA100037", 1015.0, 100037, "7Series", "1AA100037", "xiangtan"),
        Row(2015, 1, "1AA100038", 1229.0, 100038, "6Series", "1AA100038", "shenzhen"),
        Row(2015, 1, "1AA100039", 1750.0, 100039, "8Series", "1AA100039", "shenzhen"),
        Row(2015, 1, "1AA10004", 1717.0, 10004, "5Series", "1AA10004", "guangzhou"),
        Row(2015, 1, "1AA100040", 2078.0, 100040, "8Series", "1AA100040", "yichang"),
        Row(2015, 1, "1AA100041", 2734.0, 100041, "5Series", "1AA100041", "shenzhen"),
        Row(2015, 1, "1AA100042", 2745.0, 100042, "3Series", "1AA100042", "shenzhen"),
        Row(2015, 1, "1AA100043", 571.0, 100043, "9Series", "1AA100043", "guangzhou"),
        Row(2015, 1, "1AA100044", 1697.0, 100044, "8Series", "1AA100044", "guangzhou"),
        Row(2015, 1, "1AA100045", 2553.0, 100045, "2Series", "1AA100045", "xiangtan"),
        Row(2015, 1, "1AA100046", 1077.0, 100046, "3Series", "1AA100046", "guangzhou"),
        Row(2015, 1, "1AA100047", 1823.0, 100047, "9Series", "1AA100047", "zhuzhou"),
        Row(2015, 1, "1AA100048", 2399.0, 100048, "3Series", "1AA100048", "guangzhou"),
        Row(2015, 1, "1AA100049", 2890.0, 100049, "0Series", "1AA100049", "guangzhou"),
        Row(2015, 1, "1AA10005", 1608.0, 10005, "8Series", "1AA10005", "xiangtan"),
        Row(2015, 1, "1AA100050", 29.0, 100050, "2Series", "1AA100050", "yichang"),
        Row(2015, 1, "1AA100051", 1407.0, 100051, "2Series", "1AA100051", "guangzhou"),
        Row(2015, 1, "1AA100052", 845.0, 100052, "6Series", "1AA100052", "zhuzhou"),
        Row(2015, 1, "1AA100053", 1655.0, 100053, "2Series", "1AA100053", "wuhan"),
        Row(2015, 1, "1AA100054", 1368.0, 100054, "7Series", "1AA100054", "shenzhen"),
        Row(2015, 1, "1AA100055", 1728.0, 100055, "7Series", "1AA100055", "yichang"),
        Row(2015, 1, "1AA100056", 750.0, 100056, "6Series", "1AA100056", "wuhan"),
        Row(2015, 1, "1AA100057", 2288.0, 100057, "9Series", "1AA100057", "zhuzhou"),
        Row(2015, 1, "1AA100058", 2635.0, 100058, "5Series", "1AA100058", "guangzhou"),
        Row(2015, 1, "1AA100059", 1337.0, 100059, "4Series", "1AA100059", "shenzhen"),
        Row(2015, 1, "1AA10006", 2478.0, 10006, "3Series", "1AA10006", "guangzhou"),
        Row(2015, 1, "1AA100060", 538.0, 100060, "8Series", "1AA100060", "xiangtan"),
        Row(2015, 1, "1AA100061", 1407.0, 100061, "6Series", "1AA100061", "changsha"),
        Row(2015, 1, "1AA100062", 2952.0, 100062, "9Series", "1AA100062", "yichang"),
        Row(2015, 1, "1AA100063", 1226.0, 100063, "2Series", "1AA100063", "yichang"),
        Row(2015, 1, "1AA100064", 865.0, 100064, "6Series", "1AA100064", "zhuzhou"),
        Row(2015, 1, "1AA100065", 901.0, 100065, "0Series", "1AA100065", "xiangtan"),
        Row(2015, 1, "1AA100066", 1864.0, 100066, "6Series", "1AA100066", "zhuzhou"),
        Row(2015, 1, "1AA100067", 572.0, 100067, "4Series", "1AA100067", "wuhan"),
        Row(2015, 1, "1AA100068", 412.0, 100068, "8Series", "1AA100068", "guangzhou"),
        Row(2015, 1, "1AA100069", 1491.0, 100069, "8Series", "1AA100069", "xiangtan"),
        Row(2015, 1, "1AA10007", 1350.0, 10007, "8Series", "1AA10007", "xiangtan"),
        Row(2015, 1, "1AA100070", 1567.0, 100070, "0Series", "1AA100070", "guangzhou"),
        Row(2015, 1, "1AA100071", 1973.0, 100071, "0Series", "1AA100071", "guangzhou"),
        Row(2015, 1, "1AA100072", 448.0, 100072, "4Series", "1AA100072", "changsha"),
        Row(2015, 1, "1AA100073", 2488.0, 100073, "4Series", "1AA100073", "zhuzhou"),
        Row(2015, 1, "1AA100074", 907.0, 100074, "6Series", "1AA100074", "wuhan"),
        Row(2015, 1, "1AA100075", 2507.0, 100075, "3Series", "1AA100075", "shenzhen"),
        Row(2015, 1, "1AA100076", 732.0, 100076, "0Series", "1AA100076", "wuhan"),
        Row(2015, 1, "1AA100077", 2077.0, 100077, "3Series", "1AA100077", "yichang"),
        Row(2015, 1, "1AA100078", 1434.0, 100078, "2Series", "1AA100078", "yichang"),
        Row(2015, 1, "1AA100079", 1098.0, 100079, "4Series", "1AA100079", "xiangtan"),
        Row(2015, 1, "1AA10008", 813.0, 10008, "5Series", "1AA10008", "shenzhen"),
        Row(2015, 1, "1AA100080", 954.0, 100080, "9Series", "1AA100080", "shenzhen"),
        Row(2015, 1, "1AA100081", 613.0, 100081, "5Series", "1AA100081", "shenzhen"),
        Row(2015, 1, "1AA100082", 2348.0, 100082, "5Series", "1AA100082", "xiangtan"),
        Row(2015, 1, "1AA100083", 2192.0, 100083, "0Series", "1AA100083", "zhuzhou"),
        Row(2015, 1, "1AA100084", 2826.0, 100084, "0Series", "1AA100084", "guangzhou")
      )
    )
  }
  )

  //TC_344
  test(
    "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity LIKE 'wu%'"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, deviceInformationId, Latest_YEAR, series, imei, deliveryCity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity LIKE 'wu%'"
      ),
      Seq(Row(692.0, 1000, 2015, "5Series", "1AA1000", "wuhan"),
        Row(136.0, 100000, 2015, "9Series", "1AA100000", "wuhan"),
        Row(2972.0, 10002, 2015, "0Series", "1AA10002", "wuhan"),
        Row(760.0, 100033, 2015, "8Series", "1AA100033", "wuhan"),
        Row(1655.0, 100053, 2015, "2Series", "1AA100053", "wuhan"),
        Row(750.0, 100056, 2015, "6Series", "1AA100056", "wuhan"),
        Row(572.0, 100067, 2015, "4Series", "1AA100067", "wuhan"),
        Row(907.0, 100074, 2015, "6Series", "1AA100074", "wuhan"),
        Row(732.0, 100076, 2015, "0Series", "1AA100076", "wuhan")
      )
    )
  }
  )

  //TC_345
  test(
    "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(imei LIKE '%1AA10%')"
      ),
      Seq(Row(2738.562, 1, 2015, "7Series", "1AA1", "yichang"))
    )
  }
  )

  //TC_346
  test(
    "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE gamepointid BETWEEN 1015 AND 1080"
  )({
    checkAnswer(
      sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE gamepointid BETWEEN 1015 AND 1080"
      ),
      Seq(Row(1080.0, 100031, 2015, "7Series", "1AA100031", "yichang"),
        Row(1053.0, 100032, 2015, "1Series", "1AA100032", "shenzhen"),
        Row(1015.0, 100037, 2015, "7Series", "1AA100037", "xiangtan"),
        Row(1077.0, 100046, 2015, "3Series", "1AA100046", "guangzhou")
      )
    )
  }
  )

  //TC_347
  test(
    "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deviceinformationid = 100031"
  )({
    checkAnswer(
      sql(
        "SELECT gamepointid, deviceinformationid, latest_year, series, imei, deliverycity FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deviceinformationid = 100031"
      ),
      Seq(Row(1080.0, 100031, 2015, "7Series", "1AA100031", "yichang"))
    )
  }
  )

  //TC_348
  test("SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY")({
    checkAnswer(
      sql("SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY"),
      Seq(Row(2015, 2738.562),
        Row(2015, 1714.635),
        Row(2015, 1271.0),
        Row(2015, 692.0),
        Row(2015, 2175.0),
        Row(2015, 136.0),
        Row(2015, 1600.0),
        Row(2015, 505.0),
        Row(2015, 1341.0),
        Row(2015, 2239.0),
        Row(2015, 2970.0),
        Row(2015, 2593.0),
        Row(2015, 2572.0),
        Row(2015, 1991.0),
        Row(2015, 1442.0),
        Row(2015, 1841.0),
        Row(2015, 298.0),
        Row(2015, 79.0),
        Row(2015, 202.0),
        Row(2015, 568.0),
        Row(2015, 355.0),
        Row(2015, 151.0),
        Row(2015, 2863.0),
        Row(2015, 1873.0),
        Row(2015, 2205.0),
        Row(2015, 441.0),
        Row(2015, 2194.0),
        Row(2015, 2972.0),
        Row(2015, 256.0),
        Row(2015, 1778.0),
        Row(2015, 1999.0),
        Row(2015, 2194.0),
        Row(2015, 2483.0),
        Row(2015, 1724.0),
        Row(2015, 1768.0),
        Row(2015, 2436.0),
        Row(2015, 2849.0),
        Row(2015, 1691.0),
        Row(2015, 2071.0),
        Row(2015, 1333.0),
        Row(2015, 1080.0),
        Row(2015, 1053.0),
        Row(2015, 760.0),
        Row(2015, 2061.0),
        Row(2015, 2142.0),
        Row(2015, 2224.0),
        Row(2015, 1015.0),
        Row(2015, 1229.0),
        Row(2015, 1750.0),
        Row(2015, 1717.0),
        Row(2015, 2078.0),
        Row(2015, 2734.0),
        Row(2015, 2745.0),
        Row(2015, 571.0),
        Row(2015, 1697.0),
        Row(2015, 2553.0),
        Row(2015, 1077.0),
        Row(2015, 1823.0),
        Row(2015, 2399.0),
        Row(2015, 2890.0),
        Row(2015, 1608.0),
        Row(2015, 29.0),
        Row(2015, 1407.0),
        Row(2015, 845.0),
        Row(2015, 1655.0),
        Row(2015, 1368.0),
        Row(2015, 1728.0),
        Row(2015, 750.0),
        Row(2015, 2288.0),
        Row(2015, 2635.0),
        Row(2015, 1337.0),
        Row(2015, 2478.0),
        Row(2015, 538.0),
        Row(2015, 1407.0),
        Row(2015, 2952.0),
        Row(2015, 1226.0),
        Row(2015, 865.0),
        Row(2015, 901.0),
        Row(2015, 1864.0),
        Row(2015, 572.0),
        Row(2015, 412.0),
        Row(2015, 1491.0),
        Row(2015, 1350.0),
        Row(2015, 1567.0),
        Row(2015, 1973.0),
        Row(2015, 448.0),
        Row(2015, 2488.0),
        Row(2015, 907.0),
        Row(2015, 2507.0),
        Row(2015, 732.0),
        Row(2015, 2077.0),
        Row(2015, 1434.0),
        Row(2015, 1098.0),
        Row(2015, 813.0),
        Row(2015, 954.0),
        Row(2015, 613.0),
        Row(2015, 2348.0),
        Row(2015, 2192.0),
        Row(2015, 2826.0)
      )
    )
  }
  )

  //TC_349
  test(
    "SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY WHERE " +
      "gamepointid BETWEEN 200.0 AND 300.0"
  )({
    checkAnswer(
      sql(
        "SELECT latest_year, gamepointid FROM (select * from Carbon_automation_test2) SUB_QRY " +
          "WHERE gamepointid BETWEEN 200.0 AND 300.0"
      ),
      Seq(Row(2015, 298.0), Row(2015, 202.0), Row(2015, 256.0))
    )
  }
  )

  //TC_351
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      ),
      Seq(Row(1341.0, 2015, 100002, "1AA100002", "changsha"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha")
      )
    )
  }
  )

  //TC_352
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE deliveryCity IS NOT NULL"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IS NOT NULL"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang"),
        Row(1714.635, 2015, 10, "1AA10", "yichang"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan"),
        Row(692.0, 2015, 1000, "1AA1000", "wuhan"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou"),
        Row(136.0, 2015, 100000, "1AA100000", "wuhan"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou"),
        Row(1655.0, 2015, 100053, "1AA100053", "wuhan"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou"),
        Row(572.0, 2015, 100067, "1AA100067", "wuhan"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou")
      )
    )
  }
  )

  //TC_353
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity IN (\"guangzhou\",\"changsha\"))"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity IN (\"guangzhou\"," +
          "\"changsha\"))"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang"),
        Row(1714.635, 2015, 10, "1AA10", "yichang"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan"),
        Row(692.0, 2015, 1000, "1AA1000", "wuhan"),
        Row(136.0, 2015, 100000, "1AA100000", "wuhan"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou"),
        Row(1655.0, 2015, 100053, "1AA100053", "wuhan"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou"),
        Row(572.0, 2015, 100067, "1AA100067", "wuhan"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou")
      )
    )
  }
  )

  //TC_354
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * from" +
      " Carbon_automation_test2) SUB_QRY WHERE NOT(imei BETWEEN \"1AA100\" AND \"1AA10000\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity FROM (select * " +
          "from Carbon_automation_test2) SUB_QRY WHERE NOT(imei BETWEEN \"1AA100\" AND " +
          "\"1AA10000\")"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang"),
        Row(1714.635, 2015, 10, "1AA10", "yichang"),
        Row(136.0, 2015, 100000, "1AA100000", "wuhan"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou"),
        Row(1655.0, 2015, 100053, "1AA100053", "wuhan"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou"),
        Row(572.0, 2015, 100067, "1AA100067", "wuhan"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou")
      )
    )
  }
  )

  //TC_355
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(series BETWEEN \"2Series\" AND " +
      "\"5Series\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(series BETWEEN \"2Series\" " +
          "AND \"5Series\")"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang", "7Series"),
        Row(1714.635, 2015, 10, "1AA10", "yichang", "7Series"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou", "7Series"),
        Row(136.0, 2015, 100000, "1AA100000", "wuhan", "9Series"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang", "7Series"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan", "0Series"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang", "1Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang", "0Series"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou", "0Series"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha", "1Series"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan", "9Series"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang", "8Series"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan", "0Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou", "0Series"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang", "7Series"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou", "0Series"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan", "7Series"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou", "7Series"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang", "7Series"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen", "1Series"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan", "8Series"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan", "7Series"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen", "6Series"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen", "8Series"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang", "8Series"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou", "9Series"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou", "8Series"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou", "9Series"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou", "0Series"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan", "8Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen", "7Series"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang", "7Series"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan", "6Series"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou", "9Series"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang", "9Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou", "6Series"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou", "8Series"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan", "8Series"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan", "8Series"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou", "0Series"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou", "0Series"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan", "6Series"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan", "0Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou", "0Series"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou", "0Series")
      )
    )
  }
  )

  //TC_356
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId >= 1000.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId >= 1000.0)"
      ),
      Seq(Row(692.0, 2015, 1000, "1AA1000", "wuhan", "5Series"),
        Row(136.0, 2015, 100000, "1AA100000", "wuhan", "9Series"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan", "0Series"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha", "2Series"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou", "3Series"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou", "0Series"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan", "4Series"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha", "1Series"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou", "5Series"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang", "8Series"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen", "5Series"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan", "8Series"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou", "9Series"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang", "2Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan", "6Series"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan", "8Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(572.0, 2015, 100067, "1AA100067", "wuhan", "4Series"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou", "8Series"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha", "4Series"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan", "6Series"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan", "0Series"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen", "5Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen", "5Series")
      )
    )
  }
  )

  //TC_357
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId < 500.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId < 500.0)"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang", "7Series"),
        Row(1714.635, 2015, 10, "1AA10", "yichang", "7Series"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan", "5Series"),
        Row(692.0, 2015, 1000, "1AA1000", "wuhan", "5Series"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou", "7Series"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang", "7Series"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan", "0Series"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou", "5Series"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang", "4Series"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang", "1Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang", "0Series"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan", "4Series"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan", "4Series"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha", "3Series"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan", "9Series"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou", "5Series"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan", "0Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou", "5Series"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou", "5Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou", "0Series"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang", "7Series"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou", "0Series"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou", "5Series"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan", "2Series"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan", "7Series"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou", "7Series"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang", "7Series"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen", "1Series"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan", "8Series"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou", "2Series"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha", "5Series"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha", "5Series"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan", "7Series"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen", "6Series"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen", "8Series"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou", "5Series"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang", "8Series"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen", "5Series"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen", "3Series"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou", "9Series"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou", "8Series"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan", "2Series"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou", "3Series"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou", "9Series"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou", "3Series"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou", "0Series"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou", "2Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(1655.0, 2015, 100053, "1AA100053", "wuhan", "2Series"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen", "7Series"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang", "7Series"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan", "6Series"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou", "9Series"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou", "5Series"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen", "4Series"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou", "3Series"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang", "9Series"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang", "2Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou", "6Series"),
        Row(572.0, 2015, 100067, "1AA100067", "wuhan", "4Series"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan", "8Series"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan", "8Series"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou", "0Series"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou", "0Series"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou", "4Series"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan", "6Series"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen", "3Series"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan", "0Series"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang", "3Series"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang", "2Series"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan", "4Series"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen", "5Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen", "5Series"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan", "5Series"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou", "0Series"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou", "0Series")
      )
    )
  }
  )

  //TC_358
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId <= 600.0)"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(gamePointId <= 600.0)"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang", "7Series"),
        Row(1714.635, 2015, 10, "1AA10", "yichang", "7Series"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan", "5Series"),
        Row(692.0, 2015, 1000, "1AA1000", "wuhan", "5Series"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou", "7Series"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang", "7Series"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou", "5Series"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang", "4Series"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang", "1Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang", "0Series"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan", "4Series"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha", "3Series"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan", "9Series"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou", "5Series"),
        Row(2972.0, 2015, 10002, "1AA10002", "wuhan", "0Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou", "5Series"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou", "5Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou", "0Series"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang", "7Series"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou", "0Series"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou", "5Series"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan", "2Series"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan", "7Series"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou", "7Series"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang", "7Series"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen", "1Series"),
        Row(760.0, 2015, 100033, "1AA100033", "wuhan", "8Series"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou", "2Series"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha", "5Series"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha", "5Series"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan", "7Series"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen", "6Series"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen", "8Series"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou", "5Series"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang", "8Series"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen", "5Series"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen", "3Series"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou", "8Series"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan", "2Series"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou", "3Series"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou", "9Series"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou", "3Series"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou", "0Series"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou", "2Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(1655.0, 2015, 100053, "1AA100053", "wuhan", "2Series"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen", "7Series"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang", "7Series"),
        Row(750.0, 2015, 100056, "1AA100056", "wuhan", "6Series"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou", "9Series"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou", "5Series"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen", "4Series"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou", "3Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang", "9Series"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang", "2Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou", "6Series"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan", "8Series"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan", "8Series"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou", "0Series"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou", "0Series"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou", "4Series"),
        Row(907.0, 2015, 100074, "1AA100074", "wuhan", "6Series"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen", "3Series"),
        Row(732.0, 2015, 100076, "1AA100076", "wuhan", "0Series"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang", "3Series"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang", "2Series"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan", "4Series"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen", "5Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen", "5Series"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan", "5Series"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou", "0Series"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou", "0Series")
      )
    )
  }
  )

  //TC_359
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE '%wuhan%')"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang", "7Series"),
        Row(1714.635, 2015, 10, "1AA10", "yichang", "7Series"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan", "5Series"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou", "7Series"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang", "7Series"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan", "0Series"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou", "5Series"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang", "4Series"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang", "1Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang", "0Series"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha", "2Series"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou", "3Series"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou", "0Series"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan", "4Series"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha", "1Series"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou", "5Series"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan", "4Series"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha", "3Series"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan", "9Series"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang", "8Series"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou", "5Series"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen", "5Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou", "5Series"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou", "5Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou", "0Series"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang", "7Series"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou", "0Series"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou", "5Series"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan", "2Series"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan", "7Series"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou", "7Series"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang", "7Series"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen", "1Series"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou", "2Series"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha", "5Series"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha", "5Series"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan", "7Series"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen", "6Series"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen", "8Series"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou", "5Series"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang", "8Series"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen", "5Series"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen", "3Series"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou", "9Series"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou", "8Series"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan", "2Series"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou", "3Series"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou", "9Series"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou", "3Series"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou", "0Series"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan", "8Series"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang", "2Series"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou", "2Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen", "7Series"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang", "7Series"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou", "9Series"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou", "5Series"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen", "4Series"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou", "3Series"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang", "9Series"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang", "2Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou", "6Series"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou", "8Series"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan", "8Series"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan", "8Series"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou", "0Series"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou", "0Series"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha", "4Series"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou", "4Series"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen", "3Series"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang", "3Series"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang", "2Series"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan", "4Series"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen", "5Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen", "5Series"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan", "5Series"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou", "0Series"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou", "0Series")
      )
    )
  }
  )

  //TC_360
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE NOT(deliveryCity LIKE 'wu%')"
      ),
      Seq(Row(2738.562, 2015, 1, "1AA1", "yichang", "7Series"),
        Row(1714.635, 2015, 10, "1AA10", "yichang", "7Series"),
        Row(1271.0, 2015, 100, "1AA100", "xiangtan", "5Series"),
        Row(2175.0, 2015, 10000, "1AA10000", "guangzhou", "7Series"),
        Row(1600.0, 2015, 1000000, "1AA1000000", "yichang", "7Series"),
        Row(505.0, 2015, 100001, "1AA100001", "xiangtan", "0Series"),
        Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2239.0, 2015, 100003, "1AA100003", "zhuzhou", "5Series"),
        Row(2970.0, 2015, 100004, "1AA100004", "yichang", "4Series"),
        Row(2593.0, 2015, 100005, "1AA100005", "yichang", "1Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(1841.0, 2015, 100009, "1AA100009", "yichang", "0Series"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha", "2Series"),
        Row(79.0, 2015, 100010, "1AA100010", "zhuzhou", "3Series"),
        Row(202.0, 2015, 100011, "1AA100011", "guangzhou", "0Series"),
        Row(568.0, 2015, 100012, "1AA100012", "xiangtan", "4Series"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha", "1Series"),
        Row(151.0, 2015, 100014, "1AA100014", "zhuzhou", "5Series"),
        Row(2863.0, 2015, 100015, "1AA100015", "xiangtan", "4Series"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha", "3Series"),
        Row(2205.0, 2015, 100017, "1AA100017", "xiangtan", "9Series"),
        Row(441.0, 2015, 100018, "1AA100018", "yichang", "8Series"),
        Row(2194.0, 2015, 100019, "1AA100019", "zhuzhou", "5Series"),
        Row(256.0, 2015, 100020, "1AA100020", "shenzhen", "5Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(1999.0, 2015, 100022, "1AA100022", "zhuzhou", "5Series"),
        Row(2194.0, 2015, 100023, "1AA100023", "guangzhou", "5Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(1724.0, 2015, 100025, "1AA100025", "guangzhou", "0Series"),
        Row(1768.0, 2015, 100026, "1AA100026", "yichang", "7Series"),
        Row(2436.0, 2015, 100027, "1AA100027", "zhuzhou", "0Series"),
        Row(2849.0, 2015, 100028, "1AA100028", "zhuzhou", "5Series"),
        Row(1691.0, 2015, 100029, "1AA100029", "xiangtan", "2Series"),
        Row(2071.0, 2015, 10003, "1AA10003", "xiangtan", "7Series"),
        Row(1333.0, 2015, 100030, "1AA100030", "zhuzhou", "7Series"),
        Row(1080.0, 2015, 100031, "1AA100031", "yichang", "7Series"),
        Row(1053.0, 2015, 100032, "1AA100032", "shenzhen", "1Series"),
        Row(2061.0, 2015, 100034, "1AA100034", "guangzhou", "2Series"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha", "5Series"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha", "5Series"),
        Row(1015.0, 2015, 100037, "1AA100037", "xiangtan", "7Series"),
        Row(1229.0, 2015, 100038, "1AA100038", "shenzhen", "6Series"),
        Row(1750.0, 2015, 100039, "1AA100039", "shenzhen", "8Series"),
        Row(1717.0, 2015, 10004, "1AA10004", "guangzhou", "5Series"),
        Row(2078.0, 2015, 100040, "1AA100040", "yichang", "8Series"),
        Row(2734.0, 2015, 100041, "1AA100041", "shenzhen", "5Series"),
        Row(2745.0, 2015, 100042, "1AA100042", "shenzhen", "3Series"),
        Row(571.0, 2015, 100043, "1AA100043", "guangzhou", "9Series"),
        Row(1697.0, 2015, 100044, "1AA100044", "guangzhou", "8Series"),
        Row(2553.0, 2015, 100045, "1AA100045", "xiangtan", "2Series"),
        Row(1077.0, 2015, 100046, "1AA100046", "guangzhou", "3Series"),
        Row(1823.0, 2015, 100047, "1AA100047", "zhuzhou", "9Series"),
        Row(2399.0, 2015, 100048, "1AA100048", "guangzhou", "3Series"),
        Row(2890.0, 2015, 100049, "1AA100049", "guangzhou", "0Series"),
        Row(1608.0, 2015, 10005, "1AA10005", "xiangtan", "8Series"),
        Row(29.0, 2015, 100050, "1AA100050", "yichang", "2Series"),
        Row(1407.0, 2015, 100051, "1AA100051", "guangzhou", "2Series"),
        Row(845.0, 2015, 100052, "1AA100052", "zhuzhou", "6Series"),
        Row(1368.0, 2015, 100054, "1AA100054", "shenzhen", "7Series"),
        Row(1728.0, 2015, 100055, "1AA100055", "yichang", "7Series"),
        Row(2288.0, 2015, 100057, "1AA100057", "zhuzhou", "9Series"),
        Row(2635.0, 2015, 100058, "1AA100058", "guangzhou", "5Series"),
        Row(1337.0, 2015, 100059, "1AA100059", "shenzhen", "4Series"),
        Row(2478.0, 2015, 10006, "1AA10006", "guangzhou", "3Series"),
        Row(538.0, 2015, 100060, "1AA100060", "xiangtan", "8Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(2952.0, 2015, 100062, "1AA100062", "yichang", "9Series"),
        Row(1226.0, 2015, 100063, "1AA100063", "yichang", "2Series"),
        Row(865.0, 2015, 100064, "1AA100064", "zhuzhou", "6Series"),
        Row(901.0, 2015, 100065, "1AA100065", "xiangtan", "0Series"),
        Row(1864.0, 2015, 100066, "1AA100066", "zhuzhou", "6Series"),
        Row(412.0, 2015, 100068, "1AA100068", "guangzhou", "8Series"),
        Row(1491.0, 2015, 100069, "1AA100069", "xiangtan", "8Series"),
        Row(1350.0, 2015, 10007, "1AA10007", "xiangtan", "8Series"),
        Row(1567.0, 2015, 100070, "1AA100070", "guangzhou", "0Series"),
        Row(1973.0, 2015, 100071, "1AA100071", "guangzhou", "0Series"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha", "4Series"),
        Row(2488.0, 2015, 100073, "1AA100073", "zhuzhou", "4Series"),
        Row(2507.0, 2015, 100075, "1AA100075", "shenzhen", "3Series"),
        Row(2077.0, 2015, 100077, "1AA100077", "yichang", "3Series"),
        Row(1434.0, 2015, 100078, "1AA100078", "yichang", "2Series"),
        Row(1098.0, 2015, 100079, "1AA100079", "xiangtan", "4Series"),
        Row(813.0, 2015, 10008, "1AA10008", "shenzhen", "5Series"),
        Row(954.0, 2015, 100080, "1AA100080", "shenzhen", "9Series"),
        Row(613.0, 2015, 100081, "1AA100081", "shenzhen", "5Series"),
        Row(2348.0, 2015, 100082, "1AA100082", "xiangtan", "5Series"),
        Row(2192.0, 2015, 100083, "1AA100083", "zhuzhou", "0Series"),
        Row(2826.0, 2015, 100084, "1AA100084", "guangzhou", "0Series")
      )
    )
  }
  )

  //TC_361
  test(
    "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
  )({
    checkAnswer(
      sql(
        "SELECT gamePointId, Latest_YEAR, deviceInformationId, imei, deliveryCity, series FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY WHERE deliveryCity IN (\"changsha\")"
      ),
      Seq(Row(1341.0, 2015, 100002, "1AA100002", "changsha", "0Series"),
        Row(2572.0, 2015, 100006, "1AA100006", "changsha", "6Series"),
        Row(1991.0, 2015, 100007, "1AA100007", "changsha", "9Series"),
        Row(1442.0, 2015, 100008, "1AA100008", "changsha", "8Series"),
        Row(298.0, 2015, 10001, "1AA10001", "changsha", "2Series"),
        Row(355.0, 2015, 100013, "1AA100013", "changsha", "1Series"),
        Row(1873.0, 2015, 100016, "1AA100016", "changsha", "3Series"),
        Row(1778.0, 2015, 100021, "1AA100021", "changsha", "0Series"),
        Row(2483.0, 2015, 100024, "1AA100024", "changsha", "6Series"),
        Row(2142.0, 2015, 100035, "1AA100035", "changsha", "5Series"),
        Row(2224.0, 2015, 100036, "1AA100036", "changsha", "5Series"),
        Row(1407.0, 2015, 100061, "1AA100061", "changsha", "6Series"),
        Row(448.0, 2015, 100072, "1AA100072", "changsha", "4Series")
      )
    )
  }
  )

  //TC_362
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY"
      ),
      Seq(Row(2015, 1, "1AA1", "yichang", "7Series", 2738.562),
        Row(2015, 10, "1AA10", "yichang", "7Series", 1714.635),
        Row(2015, 100, "1AA100", "xiangtan", "5Series", 1271.0),
        Row(2015, 1000, "1AA1000", "wuhan", "5Series", 692.0),
        Row(2015, 10000, "1AA10000", "guangzhou", "7Series", 2175.0),
        Row(2015, 100000, "1AA100000", "wuhan", "9Series", 136.0),
        Row(2015, 1000000, "1AA1000000", "yichang", "7Series", 1600.0),
        Row(2015, 100001, "1AA100001", "xiangtan", "0Series", 505.0),
        Row(2015, 100002, "1AA100002", "changsha", "0Series", 1341.0),
        Row(2015, 100003, "1AA100003", "zhuzhou", "5Series", 2239.0),
        Row(2015, 100004, "1AA100004", "yichang", "4Series", 2970.0),
        Row(2015, 100005, "1AA100005", "yichang", "1Series", 2593.0),
        Row(2015, 100006, "1AA100006", "changsha", "6Series", 2572.0),
        Row(2015, 100007, "1AA100007", "changsha", "9Series", 1991.0),
        Row(2015, 100008, "1AA100008", "changsha", "8Series", 1442.0),
        Row(2015, 100009, "1AA100009", "yichang", "0Series", 1841.0),
        Row(2015, 10001, "1AA10001", "changsha", "2Series", 298.0),
        Row(2015, 100010, "1AA100010", "zhuzhou", "3Series", 79.0),
        Row(2015, 100011, "1AA100011", "guangzhou", "0Series", 202.0),
        Row(2015, 100012, "1AA100012", "xiangtan", "4Series", 568.0),
        Row(2015, 100013, "1AA100013", "changsha", "1Series", 355.0),
        Row(2015, 100014, "1AA100014", "zhuzhou", "5Series", 151.0),
        Row(2015, 100015, "1AA100015", "xiangtan", "4Series", 2863.0),
        Row(2015, 100016, "1AA100016", "changsha", "3Series", 1873.0),
        Row(2015, 100017, "1AA100017", "xiangtan", "9Series", 2205.0),
        Row(2015, 100018, "1AA100018", "yichang", "8Series", 441.0),
        Row(2015, 100019, "1AA100019", "zhuzhou", "5Series", 2194.0),
        Row(2015, 10002, "1AA10002", "wuhan", "0Series", 2972.0),
        Row(2015, 100020, "1AA100020", "shenzhen", "5Series", 256.0),
        Row(2015, 100021, "1AA100021", "changsha", "0Series", 1778.0),
        Row(2015, 100022, "1AA100022", "zhuzhou", "5Series", 1999.0),
        Row(2015, 100023, "1AA100023", "guangzhou", "5Series", 2194.0),
        Row(2015, 100024, "1AA100024", "changsha", "6Series", 2483.0),
        Row(2015, 100025, "1AA100025", "guangzhou", "0Series", 1724.0),
        Row(2015, 100026, "1AA100026", "yichang", "7Series", 1768.0),
        Row(2015, 100027, "1AA100027", "zhuzhou", "0Series", 2436.0),
        Row(2015, 100028, "1AA100028", "zhuzhou", "5Series", 2849.0),
        Row(2015, 100029, "1AA100029", "xiangtan", "2Series", 1691.0),
        Row(2015, 10003, "1AA10003", "xiangtan", "7Series", 2071.0),
        Row(2015, 100030, "1AA100030", "zhuzhou", "7Series", 1333.0),
        Row(2015, 100031, "1AA100031", "yichang", "7Series", 1080.0),
        Row(2015, 100032, "1AA100032", "shenzhen", "1Series", 1053.0),
        Row(2015, 100033, "1AA100033", "wuhan", "8Series", 760.0),
        Row(2015, 100034, "1AA100034", "guangzhou", "2Series", 2061.0),
        Row(2015, 100035, "1AA100035", "changsha", "5Series", 2142.0),
        Row(2015, 100036, "1AA100036", "changsha", "5Series", 2224.0),
        Row(2015, 100037, "1AA100037", "xiangtan", "7Series", 1015.0),
        Row(2015, 100038, "1AA100038", "shenzhen", "6Series", 1229.0),
        Row(2015, 100039, "1AA100039", "shenzhen", "8Series", 1750.0),
        Row(2015, 10004, "1AA10004", "guangzhou", "5Series", 1717.0),
        Row(2015, 100040, "1AA100040", "yichang", "8Series", 2078.0),
        Row(2015, 100041, "1AA100041", "shenzhen", "5Series", 2734.0),
        Row(2015, 100042, "1AA100042", "shenzhen", "3Series", 2745.0),
        Row(2015, 100043, "1AA100043", "guangzhou", "9Series", 571.0),
        Row(2015, 100044, "1AA100044", "guangzhou", "8Series", 1697.0),
        Row(2015, 100045, "1AA100045", "xiangtan", "2Series", 2553.0),
        Row(2015, 100046, "1AA100046", "guangzhou", "3Series", 1077.0),
        Row(2015, 100047, "1AA100047", "zhuzhou", "9Series", 1823.0),
        Row(2015, 100048, "1AA100048", "guangzhou", "3Series", 2399.0),
        Row(2015, 100049, "1AA100049", "guangzhou", "0Series", 2890.0),
        Row(2015, 10005, "1AA10005", "xiangtan", "8Series", 1608.0),
        Row(2015, 100050, "1AA100050", "yichang", "2Series", 29.0),
        Row(2015, 100051, "1AA100051", "guangzhou", "2Series", 1407.0),
        Row(2015, 100052, "1AA100052", "zhuzhou", "6Series", 845.0),
        Row(2015, 100053, "1AA100053", "wuhan", "2Series", 1655.0),
        Row(2015, 100054, "1AA100054", "shenzhen", "7Series", 1368.0),
        Row(2015, 100055, "1AA100055", "yichang", "7Series", 1728.0),
        Row(2015, 100056, "1AA100056", "wuhan", "6Series", 750.0),
        Row(2015, 100057, "1AA100057", "zhuzhou", "9Series", 2288.0),
        Row(2015, 100058, "1AA100058", "guangzhou", "5Series", 2635.0),
        Row(2015, 100059, "1AA100059", "shenzhen", "4Series", 1337.0),
        Row(2015, 10006, "1AA10006", "guangzhou", "3Series", 2478.0),
        Row(2015, 100060, "1AA100060", "xiangtan", "8Series", 538.0),
        Row(2015, 100061, "1AA100061", "changsha", "6Series", 1407.0),
        Row(2015, 100062, "1AA100062", "yichang", "9Series", 2952.0),
        Row(2015, 100063, "1AA100063", "yichang", "2Series", 1226.0),
        Row(2015, 100064, "1AA100064", "zhuzhou", "6Series", 865.0),
        Row(2015, 100065, "1AA100065", "xiangtan", "0Series", 901.0),
        Row(2015, 100066, "1AA100066", "zhuzhou", "6Series", 1864.0),
        Row(2015, 100067, "1AA100067", "wuhan", "4Series", 572.0),
        Row(2015, 100068, "1AA100068", "guangzhou", "8Series", 412.0),
        Row(2015, 100069, "1AA100069", "xiangtan", "8Series", 1491.0),
        Row(2015, 10007, "1AA10007", "xiangtan", "8Series", 1350.0),
        Row(2015, 100070, "1AA100070", "guangzhou", "0Series", 1567.0),
        Row(2015, 100071, "1AA100071", "guangzhou", "0Series", 1973.0),
        Row(2015, 100072, "1AA100072", "changsha", "4Series", 448.0),
        Row(2015, 100073, "1AA100073", "zhuzhou", "4Series", 2488.0),
        Row(2015, 100074, "1AA100074", "wuhan", "6Series", 907.0),
        Row(2015, 100075, "1AA100075", "shenzhen", "3Series", 2507.0),
        Row(2015, 100076, "1AA100076", "wuhan", "0Series", 732.0),
        Row(2015, 100077, "1AA100077", "yichang", "3Series", 2077.0),
        Row(2015, 100078, "1AA100078", "yichang", "2Series", 1434.0),
        Row(2015, 100079, "1AA100079", "xiangtan", "4Series", 1098.0),
        Row(2015, 10008, "1AA10008", "shenzhen", "5Series", 813.0),
        Row(2015, 100080, "1AA100080", "shenzhen", "9Series", 954.0),
        Row(2015, 100081, "1AA100081", "shenzhen", "5Series", 613.0),
        Row(2015, 100082, "1AA100082", "xiangtan", "5Series", 2348.0),
        Row(2015, 100083, "1AA100083", "zhuzhou", "0Series", 2192.0),
        Row(2015, 100084, "1AA100084", "guangzhou", "0Series", 2826.0)
      )
    )
  }
  )

  //TC_363
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId ASC"
      ),
      Seq(Row(2015, 1, "1AA1", "yichang", "7Series", 2738.562),
        Row(2015, 10, "1AA10", "yichang", "7Series", 1714.635),
        Row(2015, 100, "1AA100", "xiangtan", "5Series", 1271.0),
        Row(2015, 1000, "1AA1000", "wuhan", "5Series", 692.0),
        Row(2015, 10000, "1AA10000", "guangzhou", "7Series", 2175.0),
        Row(2015, 10001, "1AA10001", "changsha", "2Series", 298.0),
        Row(2015, 10002, "1AA10002", "wuhan", "0Series", 2972.0),
        Row(2015, 10003, "1AA10003", "xiangtan", "7Series", 2071.0),
        Row(2015, 10004, "1AA10004", "guangzhou", "5Series", 1717.0),
        Row(2015, 10005, "1AA10005", "xiangtan", "8Series", 1608.0),
        Row(2015, 10006, "1AA10006", "guangzhou", "3Series", 2478.0),
        Row(2015, 10007, "1AA10007", "xiangtan", "8Series", 1350.0),
        Row(2015, 10008, "1AA10008", "shenzhen", "5Series", 813.0),
        Row(2015, 100000, "1AA100000", "wuhan", "9Series", 136.0),
        Row(2015, 100001, "1AA100001", "xiangtan", "0Series", 505.0),
        Row(2015, 100002, "1AA100002", "changsha", "0Series", 1341.0),
        Row(2015, 100003, "1AA100003", "zhuzhou", "5Series", 2239.0),
        Row(2015, 100004, "1AA100004", "yichang", "4Series", 2970.0),
        Row(2015, 100005, "1AA100005", "yichang", "1Series", 2593.0),
        Row(2015, 100006, "1AA100006", "changsha", "6Series", 2572.0),
        Row(2015, 100007, "1AA100007", "changsha", "9Series", 1991.0),
        Row(2015, 100008, "1AA100008", "changsha", "8Series", 1442.0),
        Row(2015, 100009, "1AA100009", "yichang", "0Series", 1841.0),
        Row(2015, 100010, "1AA100010", "zhuzhou", "3Series", 79.0),
        Row(2015, 100011, "1AA100011", "guangzhou", "0Series", 202.0),
        Row(2015, 100012, "1AA100012", "xiangtan", "4Series", 568.0),
        Row(2015, 100013, "1AA100013", "changsha", "1Series", 355.0),
        Row(2015, 100014, "1AA100014", "zhuzhou", "5Series", 151.0),
        Row(2015, 100015, "1AA100015", "xiangtan", "4Series", 2863.0),
        Row(2015, 100016, "1AA100016", "changsha", "3Series", 1873.0),
        Row(2015, 100017, "1AA100017", "xiangtan", "9Series", 2205.0),
        Row(2015, 100018, "1AA100018", "yichang", "8Series", 441.0),
        Row(2015, 100019, "1AA100019", "zhuzhou", "5Series", 2194.0),
        Row(2015, 100020, "1AA100020", "shenzhen", "5Series", 256.0),
        Row(2015, 100021, "1AA100021", "changsha", "0Series", 1778.0),
        Row(2015, 100022, "1AA100022", "zhuzhou", "5Series", 1999.0),
        Row(2015, 100023, "1AA100023", "guangzhou", "5Series", 2194.0),
        Row(2015, 100024, "1AA100024", "changsha", "6Series", 2483.0),
        Row(2015, 100025, "1AA100025", "guangzhou", "0Series", 1724.0),
        Row(2015, 100026, "1AA100026", "yichang", "7Series", 1768.0),
        Row(2015, 100027, "1AA100027", "zhuzhou", "0Series", 2436.0),
        Row(2015, 100028, "1AA100028", "zhuzhou", "5Series", 2849.0),
        Row(2015, 100029, "1AA100029", "xiangtan", "2Series", 1691.0),
        Row(2015, 100030, "1AA100030", "zhuzhou", "7Series", 1333.0),
        Row(2015, 100031, "1AA100031", "yichang", "7Series", 1080.0),
        Row(2015, 100032, "1AA100032", "shenzhen", "1Series", 1053.0),
        Row(2015, 100033, "1AA100033", "wuhan", "8Series", 760.0),
        Row(2015, 100034, "1AA100034", "guangzhou", "2Series", 2061.0),
        Row(2015, 100035, "1AA100035", "changsha", "5Series", 2142.0),
        Row(2015, 100036, "1AA100036", "changsha", "5Series", 2224.0),
        Row(2015, 100037, "1AA100037", "xiangtan", "7Series", 1015.0),
        Row(2015, 100038, "1AA100038", "shenzhen", "6Series", 1229.0),
        Row(2015, 100039, "1AA100039", "shenzhen", "8Series", 1750.0),
        Row(2015, 100040, "1AA100040", "yichang", "8Series", 2078.0),
        Row(2015, 100041, "1AA100041", "shenzhen", "5Series", 2734.0),
        Row(2015, 100042, "1AA100042", "shenzhen", "3Series", 2745.0),
        Row(2015, 100043, "1AA100043", "guangzhou", "9Series", 571.0),
        Row(2015, 100044, "1AA100044", "guangzhou", "8Series", 1697.0),
        Row(2015, 100045, "1AA100045", "xiangtan", "2Series", 2553.0),
        Row(2015, 100046, "1AA100046", "guangzhou", "3Series", 1077.0),
        Row(2015, 100047, "1AA100047", "zhuzhou", "9Series", 1823.0),
        Row(2015, 100048, "1AA100048", "guangzhou", "3Series", 2399.0),
        Row(2015, 100049, "1AA100049", "guangzhou", "0Series", 2890.0),
        Row(2015, 100050, "1AA100050", "yichang", "2Series", 29.0),
        Row(2015, 100051, "1AA100051", "guangzhou", "2Series", 1407.0),
        Row(2015, 100052, "1AA100052", "zhuzhou", "6Series", 845.0),
        Row(2015, 100053, "1AA100053", "wuhan", "2Series", 1655.0),
        Row(2015, 100054, "1AA100054", "shenzhen", "7Series", 1368.0),
        Row(2015, 100055, "1AA100055", "yichang", "7Series", 1728.0),
        Row(2015, 100056, "1AA100056", "wuhan", "6Series", 750.0),
        Row(2015, 100057, "1AA100057", "zhuzhou", "9Series", 2288.0),
        Row(2015, 100058, "1AA100058", "guangzhou", "5Series", 2635.0),
        Row(2015, 100059, "1AA100059", "shenzhen", "4Series", 1337.0),
        Row(2015, 100060, "1AA100060", "xiangtan", "8Series", 538.0),
        Row(2015, 100061, "1AA100061", "changsha", "6Series", 1407.0),
        Row(2015, 100062, "1AA100062", "yichang", "9Series", 2952.0),
        Row(2015, 100063, "1AA100063", "yichang", "2Series", 1226.0),
        Row(2015, 100064, "1AA100064", "zhuzhou", "6Series", 865.0),
        Row(2015, 100065, "1AA100065", "xiangtan", "0Series", 901.0),
        Row(2015, 100066, "1AA100066", "zhuzhou", "6Series", 1864.0),
        Row(2015, 100067, "1AA100067", "wuhan", "4Series", 572.0),
        Row(2015, 100068, "1AA100068", "guangzhou", "8Series", 412.0),
        Row(2015, 100069, "1AA100069", "xiangtan", "8Series", 1491.0),
        Row(2015, 100070, "1AA100070", "guangzhou", "0Series", 1567.0),
        Row(2015, 100071, "1AA100071", "guangzhou", "0Series", 1973.0),
        Row(2015, 100072, "1AA100072", "changsha", "4Series", 448.0),
        Row(2015, 100073, "1AA100073", "zhuzhou", "4Series", 2488.0),
        Row(2015, 100074, "1AA100074", "wuhan", "6Series", 907.0),
        Row(2015, 100075, "1AA100075", "shenzhen", "3Series", 2507.0),
        Row(2015, 100076, "1AA100076", "wuhan", "0Series", 732.0),
        Row(2015, 100077, "1AA100077", "yichang", "3Series", 2077.0),
        Row(2015, 100078, "1AA100078", "yichang", "2Series", 1434.0),
        Row(2015, 100079, "1AA100079", "xiangtan", "4Series", 1098.0),
        Row(2015, 100080, "1AA100080", "shenzhen", "9Series", 954.0),
        Row(2015, 100081, "1AA100081", "shenzhen", "5Series", 613.0),
        Row(2015, 100082, "1AA100082", "xiangtan", "5Series", 2348.0),
        Row(2015, 100083, "1AA100083", "zhuzhou", "0Series", 2192.0),
        Row(2015, 100084, "1AA100084", "guangzhou", "0Series", 2826.0),
        Row(2015, 1000000, "1AA1000000", "yichang", "7Series", 1600.0)
      )
    )
  }
  )

  //TC_364
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
      "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId DESC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM " +
          "(select * from Carbon_automation_test2) SUB_QRY ORDER BY deviceInformationId DESC"
      ),
      Seq(Row(2015, 1000000, "1AA1000000", "yichang", "7Series", 1600.0),
        Row(2015, 100084, "1AA100084", "guangzhou", "0Series", 2826.0),
        Row(2015, 100083, "1AA100083", "zhuzhou", "0Series", 2192.0),
        Row(2015, 100082, "1AA100082", "xiangtan", "5Series", 2348.0),
        Row(2015, 100081, "1AA100081", "shenzhen", "5Series", 613.0),
        Row(2015, 100080, "1AA100080", "shenzhen", "9Series", 954.0),
        Row(2015, 100079, "1AA100079", "xiangtan", "4Series", 1098.0),
        Row(2015, 100078, "1AA100078", "yichang", "2Series", 1434.0),
        Row(2015, 100077, "1AA100077", "yichang", "3Series", 2077.0),
        Row(2015, 100076, "1AA100076", "wuhan", "0Series", 732.0),
        Row(2015, 100075, "1AA100075", "shenzhen", "3Series", 2507.0),
        Row(2015, 100074, "1AA100074", "wuhan", "6Series", 907.0),
        Row(2015, 100073, "1AA100073", "zhuzhou", "4Series", 2488.0),
        Row(2015, 100072, "1AA100072", "changsha", "4Series", 448.0),
        Row(2015, 100071, "1AA100071", "guangzhou", "0Series", 1973.0),
        Row(2015, 100070, "1AA100070", "guangzhou", "0Series", 1567.0),
        Row(2015, 100069, "1AA100069", "xiangtan", "8Series", 1491.0),
        Row(2015, 100068, "1AA100068", "guangzhou", "8Series", 412.0),
        Row(2015, 100067, "1AA100067", "wuhan", "4Series", 572.0),
        Row(2015, 100066, "1AA100066", "zhuzhou", "6Series", 1864.0),
        Row(2015, 100065, "1AA100065", "xiangtan", "0Series", 901.0),
        Row(2015, 100064, "1AA100064", "zhuzhou", "6Series", 865.0),
        Row(2015, 100063, "1AA100063", "yichang", "2Series", 1226.0),
        Row(2015, 100062, "1AA100062", "yichang", "9Series", 2952.0),
        Row(2015, 100061, "1AA100061", "changsha", "6Series", 1407.0),
        Row(2015, 100060, "1AA100060", "xiangtan", "8Series", 538.0),
        Row(2015, 100059, "1AA100059", "shenzhen", "4Series", 1337.0),
        Row(2015, 100058, "1AA100058", "guangzhou", "5Series", 2635.0),
        Row(2015, 100057, "1AA100057", "zhuzhou", "9Series", 2288.0),
        Row(2015, 100056, "1AA100056", "wuhan", "6Series", 750.0),
        Row(2015, 100055, "1AA100055", "yichang", "7Series", 1728.0),
        Row(2015, 100054, "1AA100054", "shenzhen", "7Series", 1368.0),
        Row(2015, 100053, "1AA100053", "wuhan", "2Series", 1655.0),
        Row(2015, 100052, "1AA100052", "zhuzhou", "6Series", 845.0),
        Row(2015, 100051, "1AA100051", "guangzhou", "2Series", 1407.0),
        Row(2015, 100050, "1AA100050", "yichang", "2Series", 29.0),
        Row(2015, 100049, "1AA100049", "guangzhou", "0Series", 2890.0),
        Row(2015, 100048, "1AA100048", "guangzhou", "3Series", 2399.0),
        Row(2015, 100047, "1AA100047", "zhuzhou", "9Series", 1823.0),
        Row(2015, 100046, "1AA100046", "guangzhou", "3Series", 1077.0),
        Row(2015, 100045, "1AA100045", "xiangtan", "2Series", 2553.0),
        Row(2015, 100044, "1AA100044", "guangzhou", "8Series", 1697.0),
        Row(2015, 100043, "1AA100043", "guangzhou", "9Series", 571.0),
        Row(2015, 100042, "1AA100042", "shenzhen", "3Series", 2745.0),
        Row(2015, 100041, "1AA100041", "shenzhen", "5Series", 2734.0),
        Row(2015, 100040, "1AA100040", "yichang", "8Series", 2078.0),
        Row(2015, 100039, "1AA100039", "shenzhen", "8Series", 1750.0),
        Row(2015, 100038, "1AA100038", "shenzhen", "6Series", 1229.0),
        Row(2015, 100037, "1AA100037", "xiangtan", "7Series", 1015.0),
        Row(2015, 100036, "1AA100036", "changsha", "5Series", 2224.0),
        Row(2015, 100035, "1AA100035", "changsha", "5Series", 2142.0),
        Row(2015, 100034, "1AA100034", "guangzhou", "2Series", 2061.0),
        Row(2015, 100033, "1AA100033", "wuhan", "8Series", 760.0),
        Row(2015, 100032, "1AA100032", "shenzhen", "1Series", 1053.0),
        Row(2015, 100031, "1AA100031", "yichang", "7Series", 1080.0),
        Row(2015, 100030, "1AA100030", "zhuzhou", "7Series", 1333.0),
        Row(2015, 100029, "1AA100029", "xiangtan", "2Series", 1691.0),
        Row(2015, 100028, "1AA100028", "zhuzhou", "5Series", 2849.0),
        Row(2015, 100027, "1AA100027", "zhuzhou", "0Series", 2436.0),
        Row(2015, 100026, "1AA100026", "yichang", "7Series", 1768.0),
        Row(2015, 100025, "1AA100025", "guangzhou", "0Series", 1724.0),
        Row(2015, 100024, "1AA100024", "changsha", "6Series", 2483.0),
        Row(2015, 100023, "1AA100023", "guangzhou", "5Series", 2194.0),
        Row(2015, 100022, "1AA100022", "zhuzhou", "5Series", 1999.0),
        Row(2015, 100021, "1AA100021", "changsha", "0Series", 1778.0),
        Row(2015, 100020, "1AA100020", "shenzhen", "5Series", 256.0),
        Row(2015, 100019, "1AA100019", "zhuzhou", "5Series", 2194.0),
        Row(2015, 100018, "1AA100018", "yichang", "8Series", 441.0),
        Row(2015, 100017, "1AA100017", "xiangtan", "9Series", 2205.0),
        Row(2015, 100016, "1AA100016", "changsha", "3Series", 1873.0),
        Row(2015, 100015, "1AA100015", "xiangtan", "4Series", 2863.0),
        Row(2015, 100014, "1AA100014", "zhuzhou", "5Series", 151.0),
        Row(2015, 100013, "1AA100013", "changsha", "1Series", 355.0),
        Row(2015, 100012, "1AA100012", "xiangtan", "4Series", 568.0),
        Row(2015, 100011, "1AA100011", "guangzhou", "0Series", 202.0),
        Row(2015, 100010, "1AA100010", "zhuzhou", "3Series", 79.0),
        Row(2015, 100009, "1AA100009", "yichang", "0Series", 1841.0),
        Row(2015, 100008, "1AA100008", "changsha", "8Series", 1442.0),
        Row(2015, 100007, "1AA100007", "changsha", "9Series", 1991.0),
        Row(2015, 100006, "1AA100006", "changsha", "6Series", 2572.0),
        Row(2015, 100005, "1AA100005", "yichang", "1Series", 2593.0),
        Row(2015, 100004, "1AA100004", "yichang", "4Series", 2970.0),
        Row(2015, 100003, "1AA100003", "zhuzhou", "5Series", 2239.0),
        Row(2015, 100002, "1AA100002", "changsha", "0Series", 1341.0),
        Row(2015, 100001, "1AA100001", "xiangtan", "0Series", 505.0),
        Row(2015, 100000, "1AA100000", "wuhan", "9Series", 136.0),
        Row(2015, 10008, "1AA10008", "shenzhen", "5Series", 813.0),
        Row(2015, 10007, "1AA10007", "xiangtan", "8Series", 1350.0),
        Row(2015, 10006, "1AA10006", "guangzhou", "3Series", 2478.0),
        Row(2015, 10005, "1AA10005", "xiangtan", "8Series", 1608.0),
        Row(2015, 10004, "1AA10004", "guangzhou", "5Series", 1717.0),
        Row(2015, 10003, "1AA10003", "xiangtan", "7Series", 2071.0),
        Row(2015, 10002, "1AA10002", "wuhan", "0Series", 2972.0),
        Row(2015, 10001, "1AA10001", "changsha", "2Series", 298.0),
        Row(2015, 10000, "1AA10000", "guangzhou", "7Series", 2175.0),
        Row(2015, 1000, "1AA1000", "wuhan", "5Series", 692.0),
        Row(2015, 100, "1AA100", "xiangtan", "5Series", 1271.0),
        Row(2015, 10, "1AA10", "yichang", "7Series", 1714.635),
        Row(2015, 1, "1AA1", "yichang", "7Series", 2738.562)
      )
    )
  }
  )

  //TC_369
  test(
    "SELECT series, gamePointId, deviceInformationId, Latest_YEAR, imei, deliveryCity FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY imei ASC, deliveryCity ASC, series ASC"
  )({
    checkAnswer(
      sql(
        "SELECT series, gamePointId, deviceInformationId, Latest_YEAR, imei, deliveryCity FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY imei ASC, deliveryCity ASC, series ASC"
      ),
      Seq(Row("7Series", 2738.562, 1, 2015, "1AA1", "yichang"),
        Row("7Series", 1714.635, 10, 2015, "1AA10", "yichang"),
        Row("5Series", 1271.0, 100, 2015, "1AA100", "xiangtan"),
        Row("5Series", 692.0, 1000, 2015, "1AA1000", "wuhan"),
        Row("7Series", 2175.0, 10000, 2015, "1AA10000", "guangzhou"),
        Row("9Series", 136.0, 100000, 2015, "1AA100000", "wuhan"),
        Row("7Series", 1600.0, 1000000, 2015, "1AA1000000", "yichang"),
        Row("0Series", 505.0, 100001, 2015, "1AA100001", "xiangtan"),
        Row("0Series", 1341.0, 100002, 2015, "1AA100002", "changsha"),
        Row("5Series", 2239.0, 100003, 2015, "1AA100003", "zhuzhou"),
        Row("4Series", 2970.0, 100004, 2015, "1AA100004", "yichang"),
        Row("1Series", 2593.0, 100005, 2015, "1AA100005", "yichang"),
        Row("6Series", 2572.0, 100006, 2015, "1AA100006", "changsha"),
        Row("9Series", 1991.0, 100007, 2015, "1AA100007", "changsha"),
        Row("8Series", 1442.0, 100008, 2015, "1AA100008", "changsha"),
        Row("0Series", 1841.0, 100009, 2015, "1AA100009", "yichang"),
        Row("2Series", 298.0, 10001, 2015, "1AA10001", "changsha"),
        Row("3Series", 79.0, 100010, 2015, "1AA100010", "zhuzhou"),
        Row("0Series", 202.0, 100011, 2015, "1AA100011", "guangzhou"),
        Row("4Series", 568.0, 100012, 2015, "1AA100012", "xiangtan"),
        Row("1Series", 355.0, 100013, 2015, "1AA100013", "changsha"),
        Row("5Series", 151.0, 100014, 2015, "1AA100014", "zhuzhou"),
        Row("4Series", 2863.0, 100015, 2015, "1AA100015", "xiangtan"),
        Row("3Series", 1873.0, 100016, 2015, "1AA100016", "changsha"),
        Row("9Series", 2205.0, 100017, 2015, "1AA100017", "xiangtan"),
        Row("8Series", 441.0, 100018, 2015, "1AA100018", "yichang"),
        Row("5Series", 2194.0, 100019, 2015, "1AA100019", "zhuzhou"),
        Row("0Series", 2972.0, 10002, 2015, "1AA10002", "wuhan"),
        Row("5Series", 256.0, 100020, 2015, "1AA100020", "shenzhen"),
        Row("0Series", 1778.0, 100021, 2015, "1AA100021", "changsha"),
        Row("5Series", 1999.0, 100022, 2015, "1AA100022", "zhuzhou"),
        Row("5Series", 2194.0, 100023, 2015, "1AA100023", "guangzhou"),
        Row("6Series", 2483.0, 100024, 2015, "1AA100024", "changsha"),
        Row("0Series", 1724.0, 100025, 2015, "1AA100025", "guangzhou"),
        Row("7Series", 1768.0, 100026, 2015, "1AA100026", "yichang"),
        Row("0Series", 2436.0, 100027, 2015, "1AA100027", "zhuzhou"),
        Row("5Series", 2849.0, 100028, 2015, "1AA100028", "zhuzhou"),
        Row("2Series", 1691.0, 100029, 2015, "1AA100029", "xiangtan"),
        Row("7Series", 2071.0, 10003, 2015, "1AA10003", "xiangtan"),
        Row("7Series", 1333.0, 100030, 2015, "1AA100030", "zhuzhou"),
        Row("7Series", 1080.0, 100031, 2015, "1AA100031", "yichang"),
        Row("1Series", 1053.0, 100032, 2015, "1AA100032", "shenzhen"),
        Row("8Series", 760.0, 100033, 2015, "1AA100033", "wuhan"),
        Row("2Series", 2061.0, 100034, 2015, "1AA100034", "guangzhou"),
        Row("5Series", 2142.0, 100035, 2015, "1AA100035", "changsha"),
        Row("5Series", 2224.0, 100036, 2015, "1AA100036", "changsha"),
        Row("7Series", 1015.0, 100037, 2015, "1AA100037", "xiangtan"),
        Row("6Series", 1229.0, 100038, 2015, "1AA100038", "shenzhen"),
        Row("8Series", 1750.0, 100039, 2015, "1AA100039", "shenzhen"),
        Row("5Series", 1717.0, 10004, 2015, "1AA10004", "guangzhou"),
        Row("8Series", 2078.0, 100040, 2015, "1AA100040", "yichang"),
        Row("5Series", 2734.0, 100041, 2015, "1AA100041", "shenzhen"),
        Row("3Series", 2745.0, 100042, 2015, "1AA100042", "shenzhen"),
        Row("9Series", 571.0, 100043, 2015, "1AA100043", "guangzhou"),
        Row("8Series", 1697.0, 100044, 2015, "1AA100044", "guangzhou"),
        Row("2Series", 2553.0, 100045, 2015, "1AA100045", "xiangtan"),
        Row("3Series", 1077.0, 100046, 2015, "1AA100046", "guangzhou"),
        Row("9Series", 1823.0, 100047, 2015, "1AA100047", "zhuzhou"),
        Row("3Series", 2399.0, 100048, 2015, "1AA100048", "guangzhou"),
        Row("0Series", 2890.0, 100049, 2015, "1AA100049", "guangzhou"),
        Row("8Series", 1608.0, 10005, 2015, "1AA10005", "xiangtan"),
        Row("2Series", 29.0, 100050, 2015, "1AA100050", "yichang"),
        Row("2Series", 1407.0, 100051, 2015, "1AA100051", "guangzhou"),
        Row("6Series", 845.0, 100052, 2015, "1AA100052", "zhuzhou"),
        Row("2Series", 1655.0, 100053, 2015, "1AA100053", "wuhan"),
        Row("7Series", 1368.0, 100054, 2015, "1AA100054", "shenzhen"),
        Row("7Series", 1728.0, 100055, 2015, "1AA100055", "yichang"),
        Row("6Series", 750.0, 100056, 2015, "1AA100056", "wuhan"),
        Row("9Series", 2288.0, 100057, 2015, "1AA100057", "zhuzhou"),
        Row("5Series", 2635.0, 100058, 2015, "1AA100058", "guangzhou"),
        Row("4Series", 1337.0, 100059, 2015, "1AA100059", "shenzhen"),
        Row("3Series", 2478.0, 10006, 2015, "1AA10006", "guangzhou"),
        Row("8Series", 538.0, 100060, 2015, "1AA100060", "xiangtan"),
        Row("6Series", 1407.0, 100061, 2015, "1AA100061", "changsha"),
        Row("9Series", 2952.0, 100062, 2015, "1AA100062", "yichang"),
        Row("2Series", 1226.0, 100063, 2015, "1AA100063", "yichang"),
        Row("6Series", 865.0, 100064, 2015, "1AA100064", "zhuzhou"),
        Row("0Series", 901.0, 100065, 2015, "1AA100065", "xiangtan"),
        Row("6Series", 1864.0, 100066, 2015, "1AA100066", "zhuzhou"),
        Row("4Series", 572.0, 100067, 2015, "1AA100067", "wuhan"),
        Row("8Series", 412.0, 100068, 2015, "1AA100068", "guangzhou"),
        Row("8Series", 1491.0, 100069, 2015, "1AA100069", "xiangtan"),
        Row("8Series", 1350.0, 10007, 2015, "1AA10007", "xiangtan"),
        Row("0Series", 1567.0, 100070, 2015, "1AA100070", "guangzhou"),
        Row("0Series", 1973.0, 100071, 2015, "1AA100071", "guangzhou"),
        Row("4Series", 448.0, 100072, 2015, "1AA100072", "changsha"),
        Row("4Series", 2488.0, 100073, 2015, "1AA100073", "zhuzhou"),
        Row("6Series", 907.0, 100074, 2015, "1AA100074", "wuhan"),
        Row("3Series", 2507.0, 100075, 2015, "1AA100075", "shenzhen"),
        Row("0Series", 732.0, 100076, 2015, "1AA100076", "wuhan"),
        Row("3Series", 2077.0, 100077, 2015, "1AA100077", "yichang"),
        Row("2Series", 1434.0, 100078, 2015, "1AA100078", "yichang"),
        Row("4Series", 1098.0, 100079, 2015, "1AA100079", "xiangtan"),
        Row("5Series", 813.0, 10008, 2015, "1AA10008", "shenzhen"),
        Row("9Series", 954.0, 100080, 2015, "1AA100080", "shenzhen"),
        Row("5Series", 613.0, 100081, 2015, "1AA100081", "shenzhen"),
        Row("5Series", 2348.0, 100082, 2015, "1AA100082", "xiangtan"),
        Row("0Series", 2192.0, 100083, 2015, "1AA100083", "zhuzhou"),
        Row("0Series", 2826.0, 100084, 2015, "1AA100084", "guangzhou")
      )
    )
  }
  )

  //TC_372
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY gamePointId ASC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY gamePointId ASC"
      ),
      Seq(Row(2015, 100050, "1AA100050", "yichang", "2Series", 29.0),
        Row(2015, 100010, "1AA100010", "zhuzhou", "3Series", 79.0),
        Row(2015, 100000, "1AA100000", "wuhan", "9Series", 136.0),
        Row(2015, 100014, "1AA100014", "zhuzhou", "5Series", 151.0),
        Row(2015, 100011, "1AA100011", "guangzhou", "0Series", 202.0),
        Row(2015, 100020, "1AA100020", "shenzhen", "5Series", 256.0),
        Row(2015, 10001, "1AA10001", "changsha", "2Series", 298.0),
        Row(2015, 100013, "1AA100013", "changsha", "1Series", 355.0),
        Row(2015, 100068, "1AA100068", "guangzhou", "8Series", 412.0),
        Row(2015, 100018, "1AA100018", "yichang", "8Series", 441.0),
        Row(2015, 100072, "1AA100072", "changsha", "4Series", 448.0),
        Row(2015, 100001, "1AA100001", "xiangtan", "0Series", 505.0),
        Row(2015, 100060, "1AA100060", "xiangtan", "8Series", 538.0),
        Row(2015, 100012, "1AA100012", "xiangtan", "4Series", 568.0),
        Row(2015, 100043, "1AA100043", "guangzhou", "9Series", 571.0),
        Row(2015, 100067, "1AA100067", "wuhan", "4Series", 572.0),
        Row(2015, 100081, "1AA100081", "shenzhen", "5Series", 613.0),
        Row(2015, 1000, "1AA1000", "wuhan", "5Series", 692.0),
        Row(2015, 100076, "1AA100076", "wuhan", "0Series", 732.0),
        Row(2015, 100056, "1AA100056", "wuhan", "6Series", 750.0),
        Row(2015, 100033, "1AA100033", "wuhan", "8Series", 760.0),
        Row(2015, 10008, "1AA10008", "shenzhen", "5Series", 813.0),
        Row(2015, 100052, "1AA100052", "zhuzhou", "6Series", 845.0),
        Row(2015, 100064, "1AA100064", "zhuzhou", "6Series", 865.0),
        Row(2015, 100065, "1AA100065", "xiangtan", "0Series", 901.0),
        Row(2015, 100074, "1AA100074", "wuhan", "6Series", 907.0),
        Row(2015, 100080, "1AA100080", "shenzhen", "9Series", 954.0),
        Row(2015, 100037, "1AA100037", "xiangtan", "7Series", 1015.0),
        Row(2015, 100032, "1AA100032", "shenzhen", "1Series", 1053.0),
        Row(2015, 100046, "1AA100046", "guangzhou", "3Series", 1077.0),
        Row(2015, 100031, "1AA100031", "yichang", "7Series", 1080.0),
        Row(2015, 100079, "1AA100079", "xiangtan", "4Series", 1098.0),
        Row(2015, 100063, "1AA100063", "yichang", "2Series", 1226.0),
        Row(2015, 100038, "1AA100038", "shenzhen", "6Series", 1229.0),
        Row(2015, 100, "1AA100", "xiangtan", "5Series", 1271.0),
        Row(2015, 100030, "1AA100030", "zhuzhou", "7Series", 1333.0),
        Row(2015, 100059, "1AA100059", "shenzhen", "4Series", 1337.0),
        Row(2015, 100002, "1AA100002", "changsha", "0Series", 1341.0),
        Row(2015, 10007, "1AA10007", "xiangtan", "8Series", 1350.0),
        Row(2015, 100054, "1AA100054", "shenzhen", "7Series", 1368.0),
        Row(2015, 100051, "1AA100051", "guangzhou", "2Series", 1407.0),
        Row(2015, 100061, "1AA100061", "changsha", "6Series", 1407.0),
        Row(2015, 100078, "1AA100078", "yichang", "2Series", 1434.0),
        Row(2015, 100008, "1AA100008", "changsha", "8Series", 1442.0),
        Row(2015, 100069, "1AA100069", "xiangtan", "8Series", 1491.0),
        Row(2015, 100070, "1AA100070", "guangzhou", "0Series", 1567.0),
        Row(2015, 1000000, "1AA1000000", "yichang", "7Series", 1600.0),
        Row(2015, 10005, "1AA10005", "xiangtan", "8Series", 1608.0),
        Row(2015, 100053, "1AA100053", "wuhan", "2Series", 1655.0),
        Row(2015, 100029, "1AA100029", "xiangtan", "2Series", 1691.0),
        Row(2015, 100044, "1AA100044", "guangzhou", "8Series", 1697.0),
        Row(2015, 10, "1AA10", "yichang", "7Series", 1714.635),
        Row(2015, 10004, "1AA10004", "guangzhou", "5Series", 1717.0),
        Row(2015, 100025, "1AA100025", "guangzhou", "0Series", 1724.0),
        Row(2015, 100055, "1AA100055", "yichang", "7Series", 1728.0),
        Row(2015, 100039, "1AA100039", "shenzhen", "8Series", 1750.0),
        Row(2015, 100026, "1AA100026", "yichang", "7Series", 1768.0),
        Row(2015, 100021, "1AA100021", "changsha", "0Series", 1778.0),
        Row(2015, 100047, "1AA100047", "zhuzhou", "9Series", 1823.0),
        Row(2015, 100009, "1AA100009", "yichang", "0Series", 1841.0),
        Row(2015, 100066, "1AA100066", "zhuzhou", "6Series", 1864.0),
        Row(2015, 100016, "1AA100016", "changsha", "3Series", 1873.0),
        Row(2015, 100071, "1AA100071", "guangzhou", "0Series", 1973.0),
        Row(2015, 100007, "1AA100007", "changsha", "9Series", 1991.0),
        Row(2015, 100022, "1AA100022", "zhuzhou", "5Series", 1999.0),
        Row(2015, 100034, "1AA100034", "guangzhou", "2Series", 2061.0),
        Row(2015, 10003, "1AA10003", "xiangtan", "7Series", 2071.0),
        Row(2015, 100077, "1AA100077", "yichang", "3Series", 2077.0),
        Row(2015, 100040, "1AA100040", "yichang", "8Series", 2078.0),
        Row(2015, 100035, "1AA100035", "changsha", "5Series", 2142.0),
        Row(2015, 10000, "1AA10000", "guangzhou", "7Series", 2175.0),
        Row(2015, 100083, "1AA100083", "zhuzhou", "0Series", 2192.0),
        Row(2015, 100019, "1AA100019", "zhuzhou", "5Series", 2194.0),
        Row(2015, 100023, "1AA100023", "guangzhou", "5Series", 2194.0),
        Row(2015, 100017, "1AA100017", "xiangtan", "9Series", 2205.0),
        Row(2015, 100036, "1AA100036", "changsha", "5Series", 2224.0),
        Row(2015, 100003, "1AA100003", "zhuzhou", "5Series", 2239.0),
        Row(2015, 100057, "1AA100057", "zhuzhou", "9Series", 2288.0),
        Row(2015, 100082, "1AA100082", "xiangtan", "5Series", 2348.0),
        Row(2015, 100048, "1AA100048", "guangzhou", "3Series", 2399.0),
        Row(2015, 100027, "1AA100027", "zhuzhou", "0Series", 2436.0),
        Row(2015, 10006, "1AA10006", "guangzhou", "3Series", 2478.0),
        Row(2015, 100024, "1AA100024", "changsha", "6Series", 2483.0),
        Row(2015, 100073, "1AA100073", "zhuzhou", "4Series", 2488.0),
        Row(2015, 100075, "1AA100075", "shenzhen", "3Series", 2507.0),
        Row(2015, 100045, "1AA100045", "xiangtan", "2Series", 2553.0),
        Row(2015, 100006, "1AA100006", "changsha", "6Series", 2572.0),
        Row(2015, 100005, "1AA100005", "yichang", "1Series", 2593.0),
        Row(2015, 100058, "1AA100058", "guangzhou", "5Series", 2635.0),
        Row(2015, 100041, "1AA100041", "shenzhen", "5Series", 2734.0),
        Row(2015, 1, "1AA1", "yichang", "7Series", 2738.562),
        Row(2015, 100042, "1AA100042", "shenzhen", "3Series", 2745.0),
        Row(2015, 100084, "1AA100084", "guangzhou", "0Series", 2826.0),
        Row(2015, 100028, "1AA100028", "zhuzhou", "5Series", 2849.0),
        Row(2015, 100015, "1AA100015", "xiangtan", "4Series", 2863.0),
        Row(2015, 100049, "1AA100049", "guangzhou", "0Series", 2890.0),
        Row(2015, 100062, "1AA100062", "yichang", "9Series", 2952.0),
        Row(2015, 100004, "1AA100004", "yichang", "4Series", 2970.0),
        Row(2015, 10002, "1AA10002", "wuhan", "0Series", 2972.0)
      )
    )
  }
  )

  //TC_373
  test(
    "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY gamePointId DESC"
  )({
    checkAnswer(
      sql(
        "SELECT Latest_YEAR, deviceInformationId, imei, deliveryCity, series, gamePointId FROM (select * from Carbon_automation_test2) SUB_QRY ORDER BY gamePointId DESC"
      ),
      Seq(Row(2015, 10002, "1AA10002", "wuhan", "0Series", 2972.0),
        Row(2015, 100004, "1AA100004", "yichang", "4Series", 2970.0),
        Row(2015, 100062, "1AA100062", "yichang", "9Series", 2952.0),
        Row(2015, 100049, "1AA100049", "guangzhou", "0Series", 2890.0),
        Row(2015, 100015, "1AA100015", "xiangtan", "4Series", 2863.0),
        Row(2015, 100028, "1AA100028", "zhuzhou", "5Series", 2849.0),
        Row(2015, 100084, "1AA100084", "guangzhou", "0Series", 2826.0),
        Row(2015, 100042, "1AA100042", "shenzhen", "3Series", 2745.0),
        Row(2015, 1, "1AA1", "yichang", "7Series", 2738.562),
        Row(2015, 100041, "1AA100041", "shenzhen", "5Series", 2734.0),
        Row(2015, 100058, "1AA100058", "guangzhou", "5Series", 2635.0),
        Row(2015, 100005, "1AA100005", "yichang", "1Series", 2593.0),
        Row(2015, 100006, "1AA100006", "changsha", "6Series", 2572.0),
        Row(2015, 100045, "1AA100045", "xiangtan", "2Series", 2553.0),
        Row(2015, 100075, "1AA100075", "shenzhen", "3Series", 2507.0),
        Row(2015, 100073, "1AA100073", "zhuzhou", "4Series", 2488.0),
        Row(2015, 100024, "1AA100024", "changsha", "6Series", 2483.0),
        Row(2015, 10006, "1AA10006", "guangzhou", "3Series", 2478.0),
        Row(2015, 100027, "1AA100027", "zhuzhou", "0Series", 2436.0),
        Row(2015, 100048, "1AA100048", "guangzhou", "3Series", 2399.0),
        Row(2015, 100082, "1AA100082", "xiangtan", "5Series", 2348.0),
        Row(2015, 100057, "1AA100057", "zhuzhou", "9Series", 2288.0),
        Row(2015, 100003, "1AA100003", "zhuzhou", "5Series", 2239.0),
        Row(2015, 100036, "1AA100036", "changsha", "5Series", 2224.0),
        Row(2015, 100017, "1AA100017", "xiangtan", "9Series", 2205.0),
        Row(2015, 100019, "1AA100019", "zhuzhou", "5Series", 2194.0),
        Row(2015, 100023, "1AA100023", "guangzhou", "5Series", 2194.0),
        Row(2015, 100083, "1AA100083", "zhuzhou", "0Series", 2192.0),
        Row(2015, 10000, "1AA10000", "guangzhou", "7Series", 2175.0),
        Row(2015, 100035, "1AA100035", "changsha", "5Series", 2142.0),
        Row(2015, 100040, "1AA100040", "yichang", "8Series", 2078.0),
        Row(2015, 100077, "1AA100077", "yichang", "3Series", 2077.0),
        Row(2015, 10003, "1AA10003", "xiangtan", "7Series", 2071.0),
        Row(2015, 100034, "1AA100034", "guangzhou", "2Series", 2061.0),
        Row(2015, 100022, "1AA100022", "zhuzhou", "5Series", 1999.0),
        Row(2015, 100007, "1AA100007", "changsha", "9Series", 1991.0),
        Row(2015, 100071, "1AA100071", "guangzhou", "0Series", 1973.0),
        Row(2015, 100016, "1AA100016", "changsha", "3Series", 1873.0),
        Row(2015, 100066, "1AA100066", "zhuzhou", "6Series", 1864.0),
        Row(2015, 100009, "1AA100009", "yichang", "0Series", 1841.0),
        Row(2015, 100047, "1AA100047", "zhuzhou", "9Series", 1823.0),
        Row(2015, 100021, "1AA100021", "changsha", "0Series", 1778.0),
        Row(2015, 100026, "1AA100026", "yichang", "7Series", 1768.0),
        Row(2015, 100039, "1AA100039", "shenzhen", "8Series", 1750.0),
        Row(2015, 100055, "1AA100055", "yichang", "7Series", 1728.0),
        Row(2015, 100025, "1AA100025", "guangzhou", "0Series", 1724.0),
        Row(2015, 10004, "1AA10004", "guangzhou", "5Series", 1717.0),
        Row(2015, 10, "1AA10", "yichang", "7Series", 1714.635),
        Row(2015, 100044, "1AA100044", "guangzhou", "8Series", 1697.0),
        Row(2015, 100029, "1AA100029", "xiangtan", "2Series", 1691.0),
        Row(2015, 100053, "1AA100053", "wuhan", "2Series", 1655.0),
        Row(2015, 10005, "1AA10005", "xiangtan", "8Series", 1608.0),
        Row(2015, 1000000, "1AA1000000", "yichang", "7Series", 1600.0),
        Row(2015, 100070, "1AA100070", "guangzhou", "0Series", 1567.0),
        Row(2015, 100069, "1AA100069", "xiangtan", "8Series", 1491.0),
        Row(2015, 100008, "1AA100008", "changsha", "8Series", 1442.0),
        Row(2015, 100078, "1AA100078", "yichang", "2Series", 1434.0),
        Row(2015, 100051, "1AA100051", "guangzhou", "2Series", 1407.0),
        Row(2015, 100061, "1AA100061", "changsha", "6Series", 1407.0),
        Row(2015, 100054, "1AA100054", "shenzhen", "7Series", 1368.0),
        Row(2015, 10007, "1AA10007", "xiangtan", "8Series", 1350.0),
        Row(2015, 100002, "1AA100002", "changsha", "0Series", 1341.0),
        Row(2015, 100059, "1AA100059", "shenzhen", "4Series", 1337.0),
        Row(2015, 100030, "1AA100030", "zhuzhou", "7Series", 1333.0),
        Row(2015, 100, "1AA100", "xiangtan", "5Series", 1271.0),
        Row(2015, 100038, "1AA100038", "shenzhen", "6Series", 1229.0),
        Row(2015, 100063, "1AA100063", "yichang", "2Series", 1226.0),
        Row(2015, 100079, "1AA100079", "xiangtan", "4Series", 1098.0),
        Row(2015, 100031, "1AA100031", "yichang", "7Series", 1080.0),
        Row(2015, 100046, "1AA100046", "guangzhou", "3Series", 1077.0),
        Row(2015, 100032, "1AA100032", "shenzhen", "1Series", 1053.0),
        Row(2015, 100037, "1AA100037", "xiangtan", "7Series", 1015.0),
        Row(2015, 100080, "1AA100080", "shenzhen", "9Series", 954.0),
        Row(2015, 100074, "1AA100074", "wuhan", "6Series", 907.0),
        Row(2015, 100065, "1AA100065", "xiangtan", "0Series", 901.0),
        Row(2015, 100064, "1AA100064", "zhuzhou", "6Series", 865.0),
        Row(2015, 100052, "1AA100052", "zhuzhou", "6Series", 845.0),
        Row(2015, 10008, "1AA10008", "shenzhen", "5Series", 813.0),
        Row(2015, 100033, "1AA100033", "wuhan", "8Series", 760.0),
        Row(2015, 100056, "1AA100056", "wuhan", "6Series", 750.0),
        Row(2015, 100076, "1AA100076", "wuhan", "0Series", 732.0),
        Row(2015, 1000, "1AA1000", "wuhan", "5Series", 692.0),
        Row(2015, 100081, "1AA100081", "shenzhen", "5Series", 613.0),
        Row(2015, 100067, "1AA100067", "wuhan", "4Series", 572.0),
        Row(2015, 100043, "1AA100043", "guangzhou", "9Series", 571.0),
        Row(2015, 100012, "1AA100012", "xiangtan", "4Series", 568.0),
        Row(2015, 100060, "1AA100060", "xiangtan", "8Series", 538.0),
        Row(2015, 100001, "1AA100001", "xiangtan", "0Series", 505.0),
        Row(2015, 100072, "1AA100072", "changsha", "4Series", 448.0),
        Row(2015, 100018, "1AA100018", "yichang", "8Series", 441.0),
        Row(2015, 100068, "1AA100068", "guangzhou", "8Series", 412.0),
        Row(2015, 100013, "1AA100013", "changsha", "1Series", 355.0),
        Row(2015, 10001, "1AA10001", "changsha", "2Series", 298.0),
        Row(2015, 100020, "1AA100020", "shenzhen", "5Series", 256.0),
        Row(2015, 100011, "1AA100011", "guangzhou", "0Series", 202.0),
        Row(2015, 100014, "1AA100014", "zhuzhou", "5Series", 151.0),
        Row(2015, 100000, "1AA100000", "wuhan", "9Series", 136.0),
        Row(2015, 100010, "1AA100010", "zhuzhou", "3Series", 79.0),
        Row(2015, 100050, "1AA100050", "yichang", "2Series", 29.0)
      )
    )
  }
  )

  //TC_390
  test(
    "select Latest_DAY,imei,gamepointid  from Carbon_automation_test2 where ( Latest_DAY+1) == 2 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,imei,gamepointid  from Carbon_automation_test2 where ( Latest_DAY+1) == 2 order by imei limit 5"
      ),
      Seq(Row(1, "1AA1", 2738.562),
        Row(1, "1AA10", 1714.635),
        Row(1, "1AA100", 1271.0),
        Row(1, "1AA1000", 692.0),
        Row(1, "1AA10000", 2175.0)
      )
    )
  }
  )

  //TC_391
  test(
    "select gamepointid,imei from Carbon_automation_test2 where ( gamePointId+1) == 80 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select gamepointid,imei from Carbon_automation_test2 where ( gamePointId+1) == 80 order by imei limit 5"
      ),
      Seq(Row(79.0, "1AA100010"))
    )
  }
  )

  //TC_392
  test(
    "select deviceInformationId,imei from Carbon_automation_test2 where ( deviceInformationId+1) == 100084 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select deviceInformationId,imei from Carbon_automation_test2 where ( deviceInformationId+1) == 100084 order by imei limit 5"
      ),
      Seq(Row(100083, "1AA100083"))
    )
  }
  )

  //TC_393
  test(
    "select channelsId,imei from Carbon_automation_test2 where ( channelsId+1) == 5 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select channelsId,imei from Carbon_automation_test2 where ( channelsId+1) == 5 order by imei limit 5"
      ),
      Seq(Row("4", "1AA1"),
        Row("4", "1AA10"),
        Row("4", "1AA100018"),
        Row("4", "1AA100027"),
        Row("4", "1AA10003")
      )
    )
  }
  )

  //TC_394
  test(
    "select contractNumber,imei from Carbon_automation_test2 where (contractNumber+1) == 507230.0 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select contractNumber,imei from Carbon_automation_test2 where (contractNumber+1) == 507230.0 order by imei limit 5"
      ),
      Seq(Row(507229.0, "1AA100083"))
    )
  }
  )

  //TC_395
  test(
    "select  Latest_YEAR,imei from Carbon_automation_test2 where ( Latest_YEAR+1) == 2016 order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select  Latest_YEAR,imei from Carbon_automation_test2 where ( Latest_YEAR+1) == 2016 order by imei limit 5"
      ),
      Seq(Row(2015, "1AA1"),
        Row(2015, "1AA10"),
        Row(2015, "1AA100"),
        Row(2015, "1AA1000"),
        Row(2015, "1AA10000")
      )
    )
  }
  )

  //TC_396
  test(
    "select Latest_province,imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_province,imei from Carbon_automation_test2 where UPPER(Latest_province) == 'GUANGDONG PROVINCE' order by imei limit 5"
      ),
      Seq(Row("Guangdong Province", "1AA1"),
        Row("Guangdong Province", "1AA1000000"),
        Row("Guangdong Province", "1AA100001"),
        Row("Guangdong Province", "1AA100002"),
        Row("Guangdong Province", "1AA100005")
      )
    )
  }
  )

  //TC_397
  test(
    "select Latest_DAY,imei from Carbon_automation_test2 where UPPER(Latest_DAY) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,imei from Carbon_automation_test2 where UPPER(Latest_DAY) == '1' order by imei limit 5"
      ),
      Seq(Row(1, "1AA1"), Row(1, "1AA10"), Row(1, "1AA100"), Row(1, "1AA1000"), Row(1, "1AA10000"))
    )
  }
  )

  //TC_398
  test(
    "select Latest_DAY,imei from Carbon_automation_test2 where LOWER(Latest_DAY) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select Latest_DAY,imei from Carbon_automation_test2 where LOWER(Latest_DAY) == '1' order by imei limit 5"
      ),
      Seq(Row(1, "1AA1"), Row(1, "1AA10"), Row(1, "1AA100"), Row(1, "1AA1000"), Row(1, "1AA10000"))
    )
  }
  )

  //TC_399
  test(
    "select deviceInformationId,imei from Carbon_automation_test2 where UPPER(deviceInformationId) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select deviceInformationId,imei from Carbon_automation_test2 where UPPER(deviceInformationId) == '1' order by imei limit 5"
      ),
      Seq(Row(1, "1AA1"))
    )
  }
  )

  //TC_400
  test(
    "select deviceInformationId,imei from Carbon_automation_test2 where LOWER(deviceInformationId) == '1' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select deviceInformationId,imei from Carbon_automation_test2 where LOWER(deviceInformationId) == '1' order by imei limit 5"
      ),
      Seq(Row(1, "1AA1"))
    )
  }
  )


  //TC_401
  test(
    "select channelsId,imei from Carbon_automation_test2 where UPPER(channelsId) == '4' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select channelsId,imei from Carbon_automation_test2 where UPPER(channelsId) == '4' order by imei limit 5"
      ),
      Seq(Row("4", "1AA1"),
        Row("4", "1AA10"),
        Row("4", "1AA100018"),
        Row("4", "1AA100027"),
        Row("4", "1AA10003")
      )
    )
  }
  )

  //TC_402
  test(
    "select channelsId,imei from Carbon_automation_test2 where LOWER(channelsId) == '4' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select channelsId,imei from Carbon_automation_test2 where LOWER(channelsId) == '4' order by imei limit 5"
      ),
      Seq(Row("4", "1AA1"),
        Row("4", "1AA10"),
        Row("4", "1AA100018"),
        Row("4", "1AA100027"),
        Row("4", "1AA10003")
      )
    )
  }
  )

  //TC_403
  test(
    "select gamePointId,imei from Carbon_automation_test2 where UPPER(gamePointId) == '136.0' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select gamePointId,imei from Carbon_automation_test2 where UPPER(gamePointId) == '136.0' order by imei limit 5"
      ),
      Seq(Row(136.0, "1AA100000"))
    )
  }
  )

  //TC_404
  test(
    "select gamePointId,imei from Carbon_automation_test2 where LOWER(gamePointId) == '136.0' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select gamePointId,imei from Carbon_automation_test2 where LOWER(gamePointId) == '136.0' order by imei limit 5"
      ),
      Seq(Row(136.0, "1AA100000"))
    )
  }
  )

  //TC_405
  test(
    "select imei from Carbon_automation_test2 where UPPER(imei) == '1AA100083' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where UPPER(imei) == '1AA100083' order by imei limit 5"
      ),
      Seq(Row("1AA100083"))
    )
  }
  )

  //TC_406
  test(
    "select imei from Carbon_automation_test2 where LOWER(imei) == '1aa100083' order by imei limit 5"
  )({
    checkAnswer(
      sql(
        "select imei from Carbon_automation_test2 where LOWER(imei) == '1aa100083' order by imei limit 5"
      ),
      Seq(Row("1AA100083"))
    )
  }
  )

  //TC_407
  test("select MAC,imei from Carbon_automation_test2 where UPPER(MAC)='MAC' order by imei limit 10")(
  {
    checkAnswer(
      sql(
        "select MAC,imei from Carbon_automation_test2 where UPPER(MAC)='MAC' order by imei limit 10"
      ),
      Seq(Row("MAC", "1AA1"),
        Row("MAC", "1AA10"),
        Row("MAC", "1AA100"),
        Row("MAC", "1AA1000"),
        Row("MAC", "1AA10000"),
        Row("MAC", "1AA100000"),
        Row("MAC", "1AA1000000"),
        Row("MAC", "1AA100001"),
        Row("MAC", "1AA100002"),
        Row("MAC", "1AA100003")
      )
    )
  }
  )

  //TC_426
  test("select  gamePointId from Carbon_automation_test2 where deviceInformationId is NOT null")({
    checkAnswer(
      sql("select  gamePointId from Carbon_automation_test2 where deviceInformationId is NOT null"),
      Seq(Row(2738.562),
        Row(1714.635),
        Row(1271.0),
        Row(692.0),
        Row(2175.0),
        Row(136.0),
        Row(1600.0),
        Row(505.0),
        Row(1341.0),
        Row(2239.0),
        Row(2970.0),
        Row(2593.0),
        Row(2572.0),
        Row(1991.0),
        Row(1442.0),
        Row(1841.0),
        Row(298.0),
        Row(79.0),
        Row(202.0),
        Row(568.0),
        Row(355.0),
        Row(151.0),
        Row(2863.0),
        Row(1873.0),
        Row(2205.0),
        Row(441.0),
        Row(2194.0),
        Row(2972.0),
        Row(256.0),
        Row(1778.0),
        Row(1999.0),
        Row(2194.0),
        Row(2483.0),
        Row(1724.0),
        Row(1768.0),
        Row(2436.0),
        Row(2849.0),
        Row(1691.0),
        Row(2071.0),
        Row(1333.0),
        Row(1080.0),
        Row(1053.0),
        Row(760.0),
        Row(2061.0),
        Row(2142.0),
        Row(2224.0),
        Row(1015.0),
        Row(1229.0),
        Row(1750.0),
        Row(1717.0),
        Row(2078.0),
        Row(2734.0),
        Row(2745.0),
        Row(571.0),
        Row(1697.0),
        Row(2553.0),
        Row(1077.0),
        Row(1823.0),
        Row(2399.0),
        Row(2890.0),
        Row(1608.0),
        Row(29.0),
        Row(1407.0),
        Row(845.0),
        Row(1655.0),
        Row(1368.0),
        Row(1728.0),
        Row(750.0),
        Row(2288.0),
        Row(2635.0),
        Row(1337.0),
        Row(2478.0),
        Row(538.0),
        Row(1407.0),
        Row(2952.0),
        Row(1226.0),
        Row(865.0),
        Row(901.0),
        Row(1864.0),
        Row(572.0),
        Row(412.0),
        Row(1491.0),
        Row(1350.0),
        Row(1567.0),
        Row(1973.0),
        Row(448.0),
        Row(2488.0),
        Row(907.0),
        Row(2507.0),
        Row(732.0),
        Row(2077.0),
        Row(1434.0),
        Row(1098.0),
        Row(813.0),
        Row(954.0),
        Row(613.0),
        Row(2348.0),
        Row(2192.0),
        Row(2826.0)
      )
    )
  }
  )

  //TC_427
  test("select contractnumber from Carbon_automation_test2 where AMSIZE is NOT null")({
    checkAnswer(
      sql("select contractnumber from Carbon_automation_test2 where AMSIZE is NOT null"),
      Seq(Row(5281803.0),
        Row(6805600.0),
        Row(8231335.0),
        Row(8978765.0),
        Row(3784858.0),
        Row(1602458.0),
        Row(9737768.0),
        Row(2919786.0),
        Row(9455612.0),
        Row(88231.0),
        Row(1439363.0),
        Row(3940720.0),
        Row(4451217.0),
        Row(335583.0),
        Row(1070757.0),
        Row(2389657.0),
        Row(5986189.0),
        Row(8543280.0),
        Row(4816260.0),
        Row(8453995.0),
        Row(2051539.0),
        Row(7610075.0),
        Row(6663091.0),
        Row(6495292.0),
        Row(2611464.0),
        Row(574375.0),
        Row(4459076.0),
        Row(5204739.0),
        Row(833654.0),
        Row(566917.0),
        Row(832387.0),
        Row(2850246.0),
        Row(6169467.0),
        Row(6533899.0),
        Row(7487134.0),
        Row(4750239.0),
        Row(8880112.0),
        Row(7774590.0),
        Row(5586718.0),
        Row(5857263.0),
        Row(6416074.0),
        Row(6994063.0),
        Row(8229807.0),
        Row(5797079.0),
        Row(6283062.0),
        Row(8431770.0),
        Row(3311312.0),
        Row(2843881.0),
        Row(1901889.0),
        Row(3077303.0),
        Row(7880439.0),
        Row(3454331.0),
        Row(23250.0),
        Row(3278167.0),
        Row(5659107.0),
        Row(9952232.0),
        Row(424923.0),
        Row(7839922.0),
        Row(9500486.0),
        Row(1952050.0),
        Row(6190068.0),
        Row(7768468.0),
        Row(7236919.0),
        Row(167725.0),
        Row(2651084.0),
        Row(6283156.0),
        Row(7342321.0),
        Row(1753823.0),
        Row(5451533.0),
        Row(5403108.0),
        Row(168757.0),
        Row(9394732.0),
        Row(7420815.0),
        Row(4358621.0),
        Row(2362114.0),
        Row(9318234.0),
        Row(5565240.0),
        Row(3166724.0),
        Row(5592457.0),
        Row(7575196.0),
        Row(3235086.0),
        Row(7917206.0),
        Row(4156339.0),
        Row(4202614.0),
        Row(2199957.0),
        Row(511128.0),
        Row(580612.0),
        Row(275342.0),
        Row(3215327.0),
        Row(8069859.0),
        Row(6383562.0),
        Row(6428516.0),
        Row(5159121.0),
        Row(3360388.0),
        Row(5152985.0),
        Row(3335480.0),
        Row(994815.0),
        Row(507229.0),
        Row(8976568.0)
      )
    )
  }
  )

  //TC_428
  test("select gamePointId from Carbon_automation_test2 where LATEST_YEAR is NOT null")({
    checkAnswer(
      sql("select gamePointId from Carbon_automation_test2 where LATEST_YEAR is NOT null"),
      Seq(Row(2738.562),
        Row(1714.635),
        Row(1271.0),
        Row(692.0),
        Row(2175.0),
        Row(136.0),
        Row(1600.0),
        Row(505.0),
        Row(1341.0),
        Row(2239.0),
        Row(2970.0),
        Row(2593.0),
        Row(2572.0),
        Row(1991.0),
        Row(1442.0),
        Row(1841.0),
        Row(298.0),
        Row(79.0),
        Row(202.0),
        Row(568.0),
        Row(355.0),
        Row(151.0),
        Row(2863.0),
        Row(1873.0),
        Row(2205.0),
        Row(441.0),
        Row(2194.0),
        Row(2972.0),
        Row(256.0),
        Row(1778.0),
        Row(1999.0),
        Row(2194.0),
        Row(2483.0),
        Row(1724.0),
        Row(1768.0),
        Row(2436.0),
        Row(2849.0),
        Row(1691.0),
        Row(2071.0),
        Row(1333.0),
        Row(1080.0),
        Row(1053.0),
        Row(760.0),
        Row(2061.0),
        Row(2142.0),
        Row(2224.0),
        Row(1015.0),
        Row(1229.0),
        Row(1750.0),
        Row(1717.0),
        Row(2078.0),
        Row(2734.0),
        Row(2745.0),
        Row(571.0),
        Row(1697.0),
        Row(2553.0),
        Row(1077.0),
        Row(1823.0),
        Row(2399.0),
        Row(2890.0),
        Row(1608.0),
        Row(29.0),
        Row(1407.0),
        Row(845.0),
        Row(1655.0),
        Row(1368.0),
        Row(1728.0),
        Row(750.0),
        Row(2288.0),
        Row(2635.0),
        Row(1337.0),
        Row(2478.0),
        Row(538.0),
        Row(1407.0),
        Row(2952.0),
        Row(1226.0),
        Row(865.0),
        Row(901.0),
        Row(1864.0),
        Row(572.0),
        Row(412.0),
        Row(1491.0),
        Row(1350.0),
        Row(1567.0),
        Row(1973.0),
        Row(448.0),
        Row(2488.0),
        Row(907.0),
        Row(2507.0),
        Row(732.0),
        Row(2077.0),
        Row(1434.0),
        Row(1098.0),
        Row(813.0),
        Row(954.0),
        Row(613.0),
        Row(2348.0),
        Row(2192.0),
        Row(2826.0)
      )
    )
  }
  )


  //TC_436
  test("SELECT count(DISTINCT gamePointId) FROM  Carbon_automation_test2 where imei is NOT null")({
    checkAnswer(
      sql("SELECT count(DISTINCT gamePointId) FROM  Carbon_automation_test2 where imei is NOT null"),
      Seq(Row(97))
    )
  }
  )

  //TC_438
  test("SELECT avg(contractNumber) FROM  Carbon_automation_test2  where imei is NOT null")({
    checkAnswer(
      sql("SELECT avg(contractNumber) FROM  Carbon_automation_test2  where imei is NOT null"),
      Seq(Row(4799897.878787879))
    )
  }
  )

  //TC_440
  test("SELECT max(gamePointId) FROM Carbon_automation_test2  where contractNumber is NOT null")({
    checkAnswer(
      sql("SELECT max(gamePointId) FROM Carbon_automation_test2  where contractNumber is NOT null"),
      Seq(Row(2972.0))
    )
  }
  )

  //TC_442
  test(
    "select variance(deviceInformationId), var_pop(imei)  from Carbon_automation_test2 where activeareaid>3"
  )({
    checkAnswer(
      sql(
        "select variance(deviceInformationId), var_pop(imei)  from Carbon_automation_test2 where activeareaid>3"
      ),
      Seq(Row(1.477644655616972E10, null))
    )
  }
  )

  //TC_443
  test(
    "select variance(contractNumber), var_pop(contractNumber)  from Carbon_automation_test2 where deliveryareaid>5"
  )({
    checkAnswer(
      sql(
        "select variance(contractNumber), var_pop(contractNumber)  from Carbon_automation_test2 where deliveryareaid>5"
      ),
      Seq(Row(8.508651970169495E12, 8.508651970169495E12))
    )
  }
  )

  //TC_444
  test(
    "select variance(AMSize), var_pop(channelsid)  from Carbon_automation_test2 where channelsid>2"
  )({
    checkAnswer(
      sql(
        "select variance(AMSize), var_pop(channelsid)  from Carbon_automation_test2 where channelsid>2"
      ),
      Seq(Row(null, 2.148423005565863))
    )
  }
  )

  //TC_446
  test(
    "select variance(deviceInformationId), var_pop(deviceInformationId)  from Carbon_automation_test2 where activeareaid>3"
  )({
    checkAnswer(
      sql(
        "select variance(deviceInformationId), var_pop(deviceInformationId)  from Carbon_automation_test2 where activeareaid>3"
      ),
      Seq(Row(1.477644655616972E10, 1.477644655616972E10))
    )
  }
  )


  //TC_450
  test("select var_samp(gamepointId) from Carbon_automation_test2")({
    checkAnswer(
      sql("select var_samp(gamepointId) from Carbon_automation_test2"),
      Seq(Row(661469.3525424678))
    )
  }
  )


  //TC_453
  test("select stddev_pop(gamePointId)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(gamePointId)from Carbon_automation_test2"),
      Seq(Row(809.1896217395077))
    )
  }
  )


  //TC_455
  test("select stddev_pop(contractNumber)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(contractNumber)from Carbon_automation_test2"),
      Seq(Row(2857306.2389803873))
    )
  }
  )

  //TC_456
  test("select stddev_samp(contractNumber)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_samp(contractNumber)from Carbon_automation_test2"),
      Seq(Row(2871847.3315618755))
    )
  }
  )


  //TC_460
  test("select stddev_samp(contractnumber)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_samp(contractnumber)from Carbon_automation_test2"),
      Seq(Row(2871847.3315618755))
    )
  }
  )

  //TC_461
  test("select covar_pop(gamePointId, Latest_MONTH) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_pop(gamePointId, Latest_MONTH) from Carbon_automation_test2"),
      Seq(Row(0.0))
    )
  }
  )

  //TC_462
  test("select covar_pop(gamePointId, contractNumber) from Carbon_automation_test21")({
    checkAnswer(
      sql("select covar_pop(gamePointId, contractNumber) from Carbon_automation_test2"),
      Seq(Row(-1.3381444931641521E7))
    )
  }
  )

  //TC_463
  test("select covar_pop(gamePointId, Latest_DAY) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_pop(gamePointId, Latest_DAY) from Carbon_automation_test2"),
      Seq(Row(0.0))
    )
  }
  )

  //TC_465
  test("select covar_samp(gamePointId, Latest_MONTH) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId, Latest_MONTH) from Carbon_automation_test2"),
      Seq(Row(0.0))
    )
  }
  )

  //TC_466
  test("select covar_samp(gamePointId, contractNumber) from Carbon_automation_test21")({
    checkAnswer(
      sql("select covar_samp(gamePointId, contractNumber) from Carbon_automation_test2"),
      Seq(Row(-1.3517990288086843E7))
    )
  }
  )

  //TC_467
  test("select covar_samp(gamePointId, Latest_DAY) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId, Latest_DAY) from Carbon_automation_test2"),
      Seq(Row(0.0))
    )
  }
  )

  //TC_468
  test("select covar_samp(gamePointId, Latest_YEAR) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId, Latest_YEAR) from Carbon_automation_test2"),
      Seq(Row(0.0))
    )
  }
  )

  //TC_469
  test("select covar_samp(gamePointId, deviceInformationId) from Carbon_automation_test2")({
    checkAnswer(
      sql("select covar_samp(gamePointId, deviceInformationId) from Carbon_automation_test2"),
      Seq(Row(-1068492.4347517013))
    )
  }
  )

  //TC_470
  test("select corr(gamePointId, deviceInformationId) from Carbon_automation_test2")({
    checkAnswer(
      sql("select corr(gamePointId, deviceInformationId) from Carbon_automation_test2"),
      Seq(Row(-0.013546512302823152))
    )
  }
  )


  //TC_478
  test("select cast(gamepointid as int) as a from Carbon_automation_test2 limit 10")({
    checkAnswer(
      sql("select cast(gamepointid as int) as a from Carbon_automation_test2 limit 10"),
      Seq(Row(2738),
        Row(1714),
        Row(1271),
        Row(692),
        Row(2175),
        Row(136),
        Row(1600),
        Row(505),
        Row(1341),
        Row(2239)
      )
    )
  }
  )


  //TC_496
  test("select stddev_pop(deviceInformationId)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_pop(deviceInformationId)from Carbon_automation_test2"),
      Seq(Row(96490.49465950707))
    )
  }
  )

  //TC_497
  test("select stddev_samp(deviceInformationId)from Carbon_automation_test2")({
    checkAnswer(
      sql("select stddev_samp(deviceInformationId)from Carbon_automation_test2"),
      Seq(Row(96981.54360516652))
    )
  }
  )

}
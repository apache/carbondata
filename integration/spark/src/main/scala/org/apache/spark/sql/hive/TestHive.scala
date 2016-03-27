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

package org.apache.spark.sql.hive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.SqlParser
import org.apache.spark.sql.catalyst.SqlParser
import java.util.Arrays

object TestHive {

  case class Record(key: Int, value: String)

  def main(args: Array[String]) {

    var sparkConf = new SparkConf().setAppName("HiveFromSpark").setMaster("local").set("spark.sql.dialect", "hiveql")
    sparkConf.set("carbon.storelocation", "E:/Spark_Olap/data_load/load_test/olap_store")
    sparkConf.set("molap.kettle.home", "E:/Spark_Olap/data_load/load_test/molapplugins")
    sparkConf.set("molap.is.columnar.storage", "true")
    sparkConf.set("spark.sql.huawei.register.dialect", "org.apache.spark.sql.CarbonSqlDDLParser")
    sparkConf.set("spark.sql.huawei.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
    sparkConf.set("spark.sql.huawei.initFunction", "org.apache.spark.sql.CarbonEnv")
    sparkConf.set("spark.sql.huawei.acl.enable", "false")
    sparkConf.set("molap.tempstore.location", "E:/Spark_Olap/data_load/load_test/temp")

    val sc = new SparkContext(sparkConf)
    val olap = new HiveContext(sc)


    olap.sql("CREATE CUBE MySchema.MyCube DIMENSIONS (column1 STRING, column2 INTEGER, datacol3 STRING) MEASURES (column4 INTEGER, column5 INTEGER) OPTIONS (AGGREGATION [column4 = 'count', column5 = 'avg'], PARTITIONER [CLASS = 'com.suprith.MyDataPartitionerImpl', COLUMNS= (column2), PARTITION_COUNT=1] )")
    olap.sql("CREATE CUBE MyCsvCube FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_header1.csv'")
    olap.sql("CREATE CUBE MyCsvCube1 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/fact.csv', 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/dim.csv' RELATION (FACT.empid = empid)")
    olap.sql("CREATE CUBE MyCsvCube2 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_header1.csv' INCLUDE (imei, MAC, AMSize, bomCode, gamePointId)")
    olap.sql("CREATE CUBE test25 DIMENSIONS (imei, deviceInformationId, MAC) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' EXCLUDE (imei)");
    olap.sql("CREATE CUBE test29 DIMENSIONS (imei, deviceInformationId) MEASURES (contractNumber, gamepointid) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv'");
    olap.sql("CREATE CUBE test41 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/chetan_fact.csv', 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/chetan_dim.csv' RELATION (fact.contractNumber =contractNumber)");
    //olap.sql("CREATE TABLE IF NOT EXISTS hivetable (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    //olap.sql("create table vmall(imei string,deviceInformationId int,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber double, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId Double, gamePointDescription string, imei_count int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    olap.sql("CREATE CUBE mytableSchema.newTable DIMENSIONS (imei, deviceInformationId, MAC) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' EXCLUDE (imei)");
    //olap.sql("CREATE CUBE myCubeFrmHiveTable FROM 'hivetable'")
    olap.sql("CREATE CUBE myschema.test22 MEASURES (gamepointid as col1, contractNumber as col2) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' EXCLUDE (LATEST_DAY)");
    //olap.sql("CREATE CUBE cube_include DIMENSIONS (AMSize String) MEASURES (Latest_Day Integer) WITH vmall RELATION (fact.AMSize = AMSize) INCLUDE (imei, AMSize, deviceinformationid)OPTIONS (AGGREGATION [Latest_Day = SUM],PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl',COLUMNS = (AMSize), PARTITION_COUNT=1])");
    //olap.sql("CREATE CUBE cube_include1 DIMENSIONS (value String) MEASURES (key Integer) WITH hivetable RELATION (fact.value = value) INCLUDE (value) OPTIONS (AGGREGATION [key = SUM], PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl', COLUMNS = (value), PARTITION_COUNT=1])");
    olap.sql("CREATE CUBE myschema.include6 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' INCLUDE (imei, deviceInformationId),'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' RELATION (fact.gamepointid = gamepointid) EXCLUDE (MAC, bomcode, imei, deviceInformationId)");
    olap.sql("CREATE CUBE myschema.include7 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' INCLUDE (imei, deviceInformationId),'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' RELATION (fact.contractNumber = contractNumber) EXCLUDE (MAC, bomcode, imei, deviceInformationId)");
    olap.sql("CREATE CUBE MySchema.test8 DIMENSIONS (column1 string,column2 integer) MEASURES (column4 integer) OPTIONS (AGGREGATION [column4 = 'count'])")

    //olap.sql("CREATE CUBE MyCsvCube FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_header_not_present.csv'")
    //olap.sql("CREATE CUBE MyCsvCube1 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/fact.csv', 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/dim_not_present.csv' RELATION (FACT.empid = empid)")
    //olap.sql("CREATE CUBE test25 DIMENSIONS (imei, deviceInformationId, MAC) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' EXCLUDE (imei)");
    //olap.sql("CREATE CUBE test25 MEASURES (imei, deviceInformationId, MAC) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv'");
    //olap.sql("CREATE CUBE test41 FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/chetan_fact.csv', 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/chetan_dim.csv' RELATION (fact.contractNumber =contractNumber2)");
    //olap.sql("create table vmall(imei string,deviceInformationId int,MAC string,deviceColor string, device_backColor string,modelId string, marketName string, AMSize string, ROMSize string, CUPAudit string, CPIClocked string, series string, productionDate string, bomCode string, internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, contractNumber double, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointId Double, gamePointDescription string, imei_count int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
    //olap.sql("CREATE CUBE myschema.test22 MEASURES (gamepointid as col1, contractNumber as col2) FROM 'hdfs://10.19.92.151:54310/opt/huawei/data/suprith/100_hive.csv' EXCLUDE (LATEST_DAY)");
    //olap.sql("CREATE CUBE cube_include DIMENSIONS (AMSize String AS suprith) MEASURES (Latest_Day Integer) WITH vmall RELATION (fact.AMSize = AMSize) INCLUDE (imei, AMSize, deviceinformationid)OPTIONS (AGGREGATION [Latest_Day = SUM],PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl',COLUMNS = (AMSize), PARTITION_COUNT=1])");
    //olap.sql("CREATE CUBE cube_include1 DIMENSIONS (value String) MEASURES (key Integer) WITH hivetable RELATION (fact.value = value) INCLUDE (value) OPTIONS (AGGREGATION [key = SUM], PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl', COLUMNS = (value), PARTITION_COUNT=1])");

    //olap.sql("CREATE CUBE test181 DIMENSIONS (column1, column2, datacol3) MEASURES (gamepointid, contractNumber) FROM 'E:/Spark_Olap/data_load/load_test/input/krishna/100_header1.csv' EXCLUDE (LATEST_DAY, deviceInformationId)")

    //olap.sql("create cube my.babu_2 dimensions(imei String,deviceInformationId Integer,MAC String,deviceColor String,device_backColor String,modelId String,marketName String,AMSize String,ROMSize String,CUPAudit String,CPIClocked String,series String,productionDate Timestamp,bomCode String,internalModels String, deliveryTime String, channelsId String, channelsName String , deliveryAreaId String, deliveryCountry String, deliveryProvince String, deliveryCity String,deliveryDistrict String, deliveryStreet String, oxSingleNumber String, ActiveCheckTime String, ActiveAreaId String, ActiveCountry String, ActiveProvince String, Activecity String, ActiveDistrict String, ActiveStreet String, ActiveOperatorId String, Active_releaseId String, Active_EMUIVersion String, Active_operaSysVersion String, Active_BacVerNumber String, Active_BacFlashVer String, Active_webUIVersion String, Active_webUITypeCarrVer String,Active_webTypeDataVerNumber String, Active_operatorsVersion String, Active_phonePADPartitionedVersions String, Latest_YEAR Integer, Latest_MONTH Integer, Latest_DAY Integer, Latest_HOUR String, Latest_areaId String, Latest_country String, Latest_province String, Latest_city String, Latest_district String, Latest_street String, Latest_releaseId String, Latest_EMUIVersion String, Latest_operaSysVersion String, Latest_BacVerNumber String, Latest_BacFlashVer String, Latest_webUIVersion String, Latest_webUITypeCarrVer String, Latest_webTypeDataVerNumber String, Latest_operatorsVersion String, Latest_phonePADPartitionedVersions String, Latest_operatorId String, gamePointDescription String) measures(gamePointId Numeric,contractNumber Numeric) OPTIONS (PARTITIONER [CLASS = 'com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl', columns= (imei), PARTITION_COUNT=1])")
    //olap.sql("CREATE CUBE MySchema.test DIMENSIONS (column1 STRING, column2 INTEGER, datacol3 STRING) MEASURES (column4 INTEGER, column5 INTEGER) OPTIONS (AGGREGATION [column4 = count, column5 = avg], PARTITIONER [CLASS = 'com.suprith.MyDataPartitionerImpl', COLUMNS= (column2), PARTITION_COUNT=1] )")

    //olap.sql("CREATE CUBE MyCsvCube1 FROM 'E:/Spark_Olap/data_load/load_test/input/fact.csv', 'E:/Spark_Olap/data_load/load_test/input/dim.csv' RELATION (FACT.empid = empid)")
    //olap.sql("CREATE CUBE MyCsvCube FROM 'E:/Spark_Olap/data_load/load_test/input/krishna/100_header1.csv'")
    //olap.sql("CREATE CUBE MyCsvCube1 FROM 'E:/Spark_Olap/data_load/load_test/input/krishna/100_header1.csv' INCLUDE (imei, MAC, AMSize, bomCode, gamePointId)")

    //olap.sql("CREATE CUBE MySchema.test2 DIMENSIONS (column1 String as col1, column2 String, datacol7 Timestamp as col1) MEASURES (column4 NUMERIC as col4, column5 Integer)")
    //olap.sql("CREATE CUBE MySchema.test8 DIMENSIONS (column1 string,column2 integer) MEASURES (column4 string) OPTIONS (AGGREGATION [column4 = count])" )
    //olap.sql("CREATE CUBE MySchema.MyCube DIMENSIONS (column1 STRING, column2 INTEGER, datacol3 STRING) MEASURES (column4 INTEGER, column5 INTEGER) OPTIONS (AGGREGATION [column4 = count, column5 = avg] PARTITIONER [CLASS = 'com.suprith.MyDataPartitionerImpl' COLUMNS= (column2) PARTITION_COUNT=1] )")
    //olap.sql("DROP CUBE MySchema.test")

    //olap.addAggregatesToCube("default", "Carbon_automation1", "agg_2_Vmall_FACT", Seq("contractNumber", "MAC"));
    //olap.addAggregatesToCube("default","Carbon_automation1","agg_3_Vmall_FACT",Seq("imei","series","modelId","deviceInformationId","gamepointid"))

    //olap.loadSchema("E:/Spark_Olap/data_load/load_test/vishalschema_1.xml",false,false,Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("imei").toArray, 1, null))
    //olap.partitionData("default", "Carbon_automation1", "E:/Spark_Olap/data_load/load_test/input/100_default_date_3.csv", "E:/Spark_Olap/data_load/load_test/output/", ",", "", "imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription")
    //olap.loadData("default", "Carbon_automation1", "E:/Spark_Olap/data_load/load_test/output", null)

    //olap.sql("LOAD DATA LOCAL INPATH 'E:/Spark_Olap/data_load/output' INTO CUBE default.upgrade_behavior_cube").collect

    //olap.sql("select count(imei_count) from upgrade_behavior_cube").show
    //olap.sql("select series, sum(gamepointid) from Carbon_automation1 group by series").show(100)

    //lap.sql("select series,sum(gamepointid) from Carbon_automation1 group by series").show

    //    val olap = new CarbonContext(sc, "D:/TestData/Vmall/store/")
    //    olap.setConf("molap.kettle.home", "D:/TestData/molapplugins")
    //    olap.setConf("molap.is.columnar.storage", "true")
    //    olap.loadSchema("D:/TestData/Vmall/schemal.xml",false,false, Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("imei").toArray, 1, null))
    //    olap.partitionData("upgrade_schema1", "upgrade_cube1", "D:/TestData/Vmall/input", "D:/TestData/Vmall/output", "\t", "", "imei\tevent_time\tevent_year\tevent_month\tevent_day\tevent_hour\tarea_id\tcountry\tprovince\tcity\tdistrict\tsite\tsite_desc\tproduct\tproduct_desc\tversion_id\tversion_name\tversion_number\tpkgsize_byte\tpublish_time\tpublish_year\tpublish_month\tpublish_day\tpublish_hour\tevent_id\terror_code")
    //
    //    olap.loadSchema("D:/VivoDemo/Vmall/upgrade_schema.xml",false,false, Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("imei").toArray, 2, null))
    //    MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "false");
    //    olap.loadData("default", "upgrade_behavior_cube", "D:/VivoDemo/Vmall/output")
    //    olap.sql("select count(imei) from upgrade_behavior_cube").collect().foreach(println)
    //    olap.sql("select count(imei) from upgrade_behavior_cube").collect().foreach(println)
    //    olap.sql("select event_year from upgrade_behavior_cube").collect().foreach(println)
    //    olap.sql("desc upgrade_behavior_cube").show(100)
    //    olap.sql("select count(distinct event_year) from upgrade_behavior_cube").show()
    //    olap.sql("select imei,count(imei) from upgrade_behavior_cube").show()
    //    olap.sql("SHOW tables").show()
    //    olap.sql("SHOW tables detail").show()
    //    olap.sql("select country, district from upgrade_behavior_cube").show()
    //    olap.sql("select imei,imei_count from upgrade_behavior_cube group by imei,imei_count").collect.foreach(println);
    //    olap.sql("select event_year from upgrade_behavior_cube").collect.foreach(println);
    //    olap.sql("select * from upgrade_behavior_cube").show(50)
    //    olap.sql("describe upgrade_behavior_cube").show(100)
    //
    //    System.setProperty("HADOOP_USER_NAME", "root")
    //    val olap = new CarbonContext(sc, "D:/TestData/context/")
    //    val olap = new CarbonContext(sc, "hdfs://10.67.180.124:54310/opt/datasight/vmalldemo2/store")
    //    val olap = new CarbonContext(sc, "hdfs://master:54310/opt/naresh/itr2/test1")
    //    olap.setConf("molap.kettle.home", "D:/TestData/molapplugins")
    //    olap.setConf("molap.is.columnar.storage", "true")

    //    MolapProperties.getInstance().addProperty("spark.molap.use.agg", "false");
    //    olap.loadSchema("D:/TestData/SmartCare_withoutAgg.xml",false,false, Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("MSISDN").toArray, 2, null))
    //    olap.partitionData("default", "ODM", "D:/TestData/input", "D:/TestData/output")
    //    olap.partitionData("default", "ODM", "hdfs://master:54310/opt/puneet/smartpccwebsite/partitioneddata/1000rows/0", "D:/TestData/output1")
    //    olap.loadData("default", "ODM", "D:/TestData/output","hdfs://master:54310/ravi/dimensions")
    //    olap.sql("select *from ODM limit 10").collect().foreach(println)
    //    olap.sql("show cubes").collect().foreach(println)
    //    olap.sql("CREATE CUBE PCC.Vivo DIMENSIONS(MSISDN String,TAC Integer,TerModelName String,RAT_TYPE Integer,RAT_NAME String) MEASURES(THROUGHPUT_UL Integer,THROUGHPUT_DL Integer) MAPPED BY(fact_table COLS[MSISDN=MSISDN,THROUGHPUT_UL=MEAN_THROUGHPUT_UL,THROUGHPUT_DL=MEAN_THROUGHPUT_DL], tac_dim_table RELATION[fact_table.TAC=tac_dim_table.TAC] COLS[TAC=TAC,TerModelName=TerModelName], rat_dim_table RELATION[fact_table.RAT_TYPE=tac_dim_table.RAT] COLS[RAT_TYPE=RAT,RAT_NAME=RAT_NAME])options(CARDINALITY[MSISDN=5000000,TAC=1000,TerModelName=1000,RAT_TYPE=10,RAT_NAME=10] AGGREGATION[THROUGHPUT_UL=sum,THROUGHPUT_DL=sum] PARTITIONER[CLASS='com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' LEVELS{MSISDN} PARTITION_COUNT=2])")
    //    olap.sql("CREATE CUBE PCC.Vivo DIMENSIONS(MSISDN Integer,APN String,IP_PROTOCOL_NUMBER String,HOST_NAME String,PROCEDURE_TYPE Integer,MCC_MNC Integer,CGI String,TAC Integer,RAT_TYPE Integer) MEASURES(THROUGHPUT_UL Integer,THROUGHPUT_DL Integer) MAPPED BY(cubename COLS[MSISDN=MSISDN,APN=APN,IP_PROTOCOL_NUMBER=IP_PROTOCOL_NUMBER,HOST_NAME=HOST_NAME,PROCEDURE_TYPE=PROCEDURE_TYPE,MCC_MNC=MCC_MNC,CGI=CGI,RAT_TYPE=RAT_TYPE,THROUGHPUT_UL=MEAN_THROUGHPUT_UL,THROUGHPUT_DL=MEAN_THROUGHPUT_DL])options(CARDINALITY[MSISDN=5000000,APN=20,IP_PROTOCOL_NUMBER=10,HOST_NAME=110000,PROCEDURE_TYPE=20,MCC_MNC=15,CGI=50000,TAC=110000,RAT_TYPE=10] AGGREGATION[THROUGHPUT_UL=sum,THROUGHPUT_DL=sum] PARTITIONER[CLASS='com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' LEVELS{MSISDN} PARTITION_COUNT=2])")
    //    olap.sql("select MSISDN from ODM").collect().foreach(println)
    //    olap.loadSchema("C:/Users/n00902756/Documents/HQ124/upgrade_schema.xml",false,false, Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("imei").toArray, 2, null))
    //    olap.partitionData("upgrade_schema1" , "upgrade_cube1", "C:/Users/n00902756/Documents/HQ124/sample.csv", "hdfs://linux-124:54310/opt/datasight/vmalldemo3/part", "\t", "", "imei\tevent_time\tevent_year\tevent_month\tevent_day\tevent_hour\tarea_id\tcountry\tprovince\tcity\tdistrict\tsite\tsite_desc\tproduct\tproduct_desc\tversion_id\tversion_name\tversion_number\tpkgsize_byte\tpublish_time\tpublish_year\tpublish_month\tpublish_day\tpublish_hour\tevent_id\terror_code")
    //    olap.loadSchema("C:/Users/n00902756/Documents/HQ124/upgrade_schema.xml",false,false, Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("imei").toArray, 2, null))
    //    olap.partitionData("upgrade_schema1" , "upgrade_cube1", "C:/Users/n00902756/Documents/HQ124/sample.csv", "hdfs://10.67.180.124:54310/opt/datasight/vmalldemo3/part", "\t")
    //    olap.sql("show cubes").show()
    //    olap.sql("describe ODM").collect().foreach(println)
    //
    //    sparkConf = sparkConf.set(ConfVars.METASTOREWAREHOUSE.toString(), "F:/TRPSVN/Spark/spark/examples/warehoue")
    //    val sc = new SparkContext(sparkConf)
    //    System.setProperty("HADOOP_USER_NAME", "root")
    //
    //    // A local hive context creates an instance of the Hive Metastore in process, storing the
    //    // the warehouse data in the current directory.  This location can be overridden by
    //    // specifying a second parameter to the constructor.
    //    val hiveContext = new HiveContext(sc)
    //    import hiveContext._
    //
    //    println(getConf("spark.sql.dialect", "hiveql"))
    //
    //    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    //    hiveContext.sql("CREATE TABLE IF NOT EXISTS src4 (key INT, value STRING)")
    //
    //    hiveContext.sql("LOAD DATA LOCAL INPATH '/opt/ravi/resources/kv1.txt' INTO TABLE src")
    //    hiveContext.sql("LOAD DATA LOCAL INPATH '/opt/ravi/resources/kv2.txt' INTO TABLE src4")
    //
    //    println(HiveQl.parseSql("select case a when b then c end from x"))

    //    val sqlParser = {
    //    val fallback = new MolapSqlParser
    //    new SparkSQLParser(fallback(_))
    //    }

    //    val olap = new CarbonContext(sc, "hdfs://master:54310/1daymeta")
    //    val olap = new CarbonContext(sc, "hdfs://10.67.174.246:9000/sampledata")
    //    val olap = new CarbonContext(sc, "D:/ravif")
    //    val olap = new CarbonContext(sc, "hdfs://master:54310/partmeta8")
    //    olap.setConf("molap.kettle.home", "G:/mavenlib/molapplugins")
    //    olap.setConf("molap.is.columnar.storage", "true")
    //    olap.setConf("molap.kettle.home", "F:/TRPSVN/Molap/Molap-Data-Processor_vishal/molapplugins/molapplugins")
    //    olap.setConf("molap.kettle.home", "/opt/spark-1.0.0-rc3/jars/molapplugins")
    //    olap.loadSchema("G:/mavenlib/PCC_Slim_de_captions.xml",false,false,Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl",Seq("Msisdn").toArray, 1,null))
    //    olap.loadSchema("H:/ReceiveFile/PCC_Vivo_cube.xml",true,false)
    //    olap.loadAggregation("PCC", "ODM", Seq("agg_4_SDR_PCC_USER_ALL_INFO_1DAY_16085","agg_5_SDR_PCC_USER_ALL_INFO_1DAY_16085"))
    //    olap.loadSchema("hdfs://master:54310/ravi/PCC_Java.xml")
    //    olap.hSql("create cube rav dimensions(d1 string, d2 Integer) measures(m1 Integer) mapped by (csv1 cols[d2=c2,m1=c1] ,csv3 relation[csv1.c1=csv3.c3] cols[d1=c1]) options(cardinality[d1=100] aggregation[m1=COUNT])").collect
    //    olap.sql("create cube indra1 dimensions(d1 String,d2 String)"+
    //        " measures(m1 Integer) mapped by (kv1 cols[d1=c2,m1=c1,d2=c1])"+
    //        " options(cardinality[d1=1000,d2=1000] aggregation[m1=SUM] PARTITIONER[CLASS='com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' LEVELS{d2} PARTITION_COUNT=10])").collect
    //    olap.sql("create cube rav3.indra dimensions(d1 String)"+
    //        " measures(m1 Integer) mapped by ('key.gv' cols[d1=c2,m1=c1])"+
    //        " options(cardinality[d1=1000] aggregation[m1=SUM] HIERARCHIES[h1 levels{d1}, h2 type=Time levels{d2,d3}] PARTITIONER[CLASS='com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' LEVELS{Location} PARTITION_COUNT=1])").collect
    //    olap.sql("create cube samplecube dimensions (d1 Integer, d2 String) measures (m_price Integer) mapped by (fact_142673430 COLS[m_price=attribute_user_accountattribute_price], login_dim RELATION[fact.imei=login_dim.imei] COLS[d1=login_type,d2=login_location]) options(cardinality[login_type=10,login_location=10] aggregation[m_price=SUM])")
    //    olap.sql("SELECT (CASE WHEN (`odm`.`L4_DW_THROUGHPUT` < 4838752132) THEN 'Low BW' ELSE 'High BW' END) AS `none_calculation_4750314120000000_nk`,\n  SUM(`odm`.`L4_DW_THROUGHPUT`) AS `sum_l4_dw_throughput_ok`\nFROM `PCC`.`ODM` `odm`\nGROUP BY (CASE WHEN (`odm`.`L4_DW_THROUGHPUT` < 4838752132) THEN 'Low BW' ELSE 'High BW' END)").collect foreach(println)
    //    olap.sql("SELECT `odm`.`IsWorkDay` AS `none_isworkday_nk`,\n  `odm`.`ProtocolCategory` AS `none_protocolcategory_nk`,  SUM(`odm`.`L4_DW_THROUGHPUT`) AS `sum_l4_dw_throughput_ok`\nFROM `PCC`.`ODM` `odm` WHERE ((`odm`.`IsWorkDay` = '1') AND (`odm`.`ProtocolCategory` = 'CATEGORY_2' OR `odm`.`ProtocolCategory` = 'CATEGORY_3')) \nGROUP BY `odm`.`IsWorkDay`,\n  `odm`.`ProtocolCategory`").collect foreach(println)
    //    olap.sql("SELECT `odm`.`RAT` AS `none_rat_nk`\nFROM `PCC`.`ODM` `odm`\nGROUP BY `odm`.`RAT`").collect foreach(println)
    //    olap.sql("SELECT `odm`.`IsWorkDay` AS `none_isworkday_nk`,\n  `odm`.`ProtocolCategory` AS `none_protocolcategory_nk`,\n  `odm`.`Province` AS `none_province_nk`,\n  `odm`.`TerBrandName` AS `none_terbrandname_nk`,\n  `odm`.`hour_of_day` AS `none_hour_of_day_nk`,\n  SUM(`odm`.`L4_DW_PACKETS`) AS `sum_l4_dw_packets_ok`\nFROM `PCC`.`ODM` `odm`\nWHERE ((`odm`.`IsWorkDay` = '1') AND (`odm`.`ProtocolCategory` = 'CATEGORY_2' OR `odm`.`ProtocolCategory` = 'CATEGORY_3') AND (`odm`.`Province` = 'PROVINCE_NAME_1' OR `odm`.`Province` = 'PROVINCE_NAME_2') AND (`odm`.`TerBrandName` = 'HUAWEI' OR `odm`.`TerBrandName` = 'SAMSUNG') AND (`odm`.`hour_of_day` = '10' OR `odm`.`hour_of_day` = '20'))\nGROUP BY `odm`.`IsWorkDay`,\n  `odm`.`ProtocolCategory`,\n  `odm`.`Province`,\n  `odm`.`TerBrandName`,\n  `odm`.`hour_of_day`").collect foreach(println)
    //    olap.sql("SELECT `odm`.`RAT` AS `none_rat_nk`,\n  COUNT(DISTINCT `odm`.`TerModelName`) AS `ctd_termodelname_ok`,\n  SUM(`odm`.`L4_DW_THROUGHPUT`) AS `sum_l4_dw_throughput_ok`\nFROM `PCC`.`ODM` `odm`\nGROUP BY `odm`.`RAT`").collect foreach(println)
    //    olap.partitionData("default", "PCC_Vivo_bill2", "hdfs://master:54310/ravi/vivo", "D:/ravif/part2",",","ARRIVAL_TIME_STAMP,LOCATION_CODE,TRANSACTION_ID,MEASURING_PROBE,MEASURING_PROBE_TYPE,INTERFACE_TYPE,PROCEDURES_COUNT,SESSION_ID,ATTEMPTS,START_TIME,PROCEDURE_DURATION,PROCEDURE_TYPE,EVENT_TYPE,END_CAUSE,MAX_ATTEMPTS,PROCEDURE_AVG_DURATION,PROCEDURE_MAX_DURATION,IMSI,MSISDN,APN,REFERENCE_GROUP,URI,MS_IP_ADDRESS,PCF_IP_ADDRESS,PDSN_IP_ADDRESS,SESSION_VIEW,USER_PLANE,MEAN_THROUGHPUT_UL,MEAN_THROUGHPUT_DL,PEAK_THROUGHPUT_UL,PEAK_THROUGHPUT_DL,TOTAL_UL_FRAMES,TOTAL_DL_FRAMES,TOTAL_DATA_UL,TOTAL_DATA_DL,USER_NAME,SERVICE_SRC_IP,SERVICE_DST_IP,COMPRESSION_TYPE,SERVICE_PROTOCOL,PCF_DOMAIN,DNS_SERVER,AAA_SERVER,HOME_AGENT,FOREIGN_AGENT,IP_PROTOCOL_NUMBER,SERVICE_PORT,GGSN_ID,MMS_TIME_OF_EXPIRY_ABS,MMS_TIME_OF_EXPIRY_REL,MMS_MESSAGE_CLASS,USERNAME_DOMAIN,TCP_TERMINATION,AIRLINK_RECORD_TYPE,SECTOR_ID,SID_NID,ACCESS_NETWORK_SID,ACCESS_NETWORK_NID,CDMA_CELL_ID,RELEASE_INDICATOR,HOST_NAME,TCP_SYN_COUNT,RTT,TCP_FIN_RATIO,TCP_RESET_RATIO,TCP_RETRANS_FRAMES_RATIO_DL,TCP_RETRANS_BYTES_RATIO_DL,TCP_AVG_WINDOW_SIZE_DL,TCP_MAX_WINDOW_SIZE_DL,TCP_RETRANS_FRAMES_RATIO_UL,TCP_RETRANS_BYTES_RATIO_UL,TCP_AVG_WINDOW_SIZE_UL,TCP_MAX_WINDOW_SIZE_UL,HTTP_GET_COUNT,ECN_ECHO_DL,ECN_ECHO_UL,HTTP_LATENCY,TCP_MSS_DL,TCP_MSS_UL,USER_AGENT,SGSN_ADDRESS_FOR_USER_TRAFFIC,GGSN_ADDRESS_FOR_USER_TRAFFIC,MEID,ESN,MFR_CODE,SERVICE,CALLED_STATION_ID,NAS_PORT_TYPE,ACCOUNT_STATUS_TYPE,INTERIM_UPDATE,ACCOUNT_DELAY_TIME,ACCOUNT_SESSION_ID,ACCOUNT_AUTHENTIC,ACCOUNT_SESSION_TIME,ACCOUNT_MULTI_SESSION_ID,ACCOUNT_LINK_COUNT,PHYLAYER_SIGNALING,MCC_MNC,MCC,MNC,IMEI,IMEI_SV,RAT_TYPE,CGI,CELL_ID,SAC,RAC,LAC,TAC,TIME_ZONE,INTERNET_LATENCY,RAN_LATENCY,OUT_OF_ORDER_PACKETS_UL,OUT_OF_ORDER_PACKETS_DL,MISSING_PACKETS_UL,MISSING_PACKETS_DL,DUPLICATE_ACK_UL,DUPLICATE_ACK_DL,DUPLICATE_ACK_COUNT_DL,DUPLICATE_ACK_COUNT_UL,OUT_OF_ORDER_PACKETS_RATIO_UL,OUT_OF_ORDER_PACKETS_RATIO_DL,MISSING_PACKETS_RATIO_UL,MISSING_PACKETS_RATIO_DL,DUPLICATE_ACK_RATIO_UL,DUPLICATE_ACK_RATIO_DL,DUPLICATE_ACK_COUNT_RATIO_DL,DUPLICATE_ACK_COUNT_RATIO_UL,CDMA2000_VER,TUNNEL_ID_FOR_DATA_UPLINK,TUNNEL_ID_FOR_DATA_DOWNLINK,TUNNEL_ID_FOR_GTP_VERSION_0,GTP_VERSION,SERVICE_SOURCE_PORT,HTTP_CONTENT_LENGTH,SESSION_DURATION,SGSN_ADDRESS_FOR_SIGNALING,GGSN_ADDRESS_FOR_SIGNALING,USERNAME,FAILED_COMMAND,FAILURE_END_CAUSE,FILES_TRANSFERRED,FROM_EMAIL_ADDRESS,TO_EMAIL_ADDRESS,EMAIL_COUNT,END_CAUSE_1XX,END_CAUSE_2XX,END_CAUSE_3XX,END_CAUSE_4XX,END_CAUSE_5XX,END_CAUSE_1XX_COUNT,END_CAUSE_2XX_COUNT,END_CAUSE_3XX_COUNT,END_CAUSE_4XX_COUNT,END_CAUSE_5XX_COUNT,FIRST_END_CAUSE,REFERER,FAILED_RESPONSE,MMS_VERSION,MMS_TRANSACTION_ID,MMS_MESSAGE_ID,MMS_FROM,MMS_MESSAGE_SIZE,MMS_STATUS,HTTP_SUCCESS_RATIO,HTTP_FAILURE_RATIO,HTTP_SESSION_COUNT,TOTAL_DL_TCP_FRAMES,TOTAL_UL_TCP_FRAMES,TOTAL_DL_TCP_BYTES,TOTAL_UL_TCP_BYTES,MMS_MESSAGE_TYPE,MMS_TO,QVIP_SOURCE_PROFILE_NAME,QVIP_SRC_GRP_RES_PROF_NAMES,QVIP_DEST_PROFILE_NAME,QVIP_DEST_GRP_RES_PROF_NAMES,QVIP_SOURCE_PROFILE_KEY,QVIP_DESTINATION_PROFILE_KEY,SYSTEM_SOURCE_PROFILE_NAME,SYSTEM_SRC_GRP_RES_PROF_NAMES,SYSTEM_DEST_PROF_NAME")
    //    olap.setConf("molap.tempstore.location","D:/ff/ff/rr")
    //    olap.loadData("default", "PCC_Vivo_bill2", "H:/ReceiveFile/dd1")
    //    olap.sql("select MSISDN,LAC,Count(MEAN_THROUGHPUT_DL_min) from PCC_Vivo_bill2 group by MSISDN,LAC").collect foreach(println)
    //       Partitioner("com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl", Seq("d1").toArray, 10, null)
    //    olap.loadData("PCC", "ODM", "hdfs://master:54310/ravi/part4", "hdfs://master:54310/ravi/dimensions")
    //    println(new CarbonSqlDDLParser().apply("create cube rav dimensions(d1 string, d2 Integer) measures(m1 int) mapped by (csv1 cols[d2=c2,m1=c1] ,csv3 relation[csv1.c1=csv3.c3] cols[d1=c1]) options(cardinality[d1=100] aggregation[m1=COUNT])"))
    //                 MolapProperties.getInstance().addProperty("molap.is.columnar.storage", "true");
    //             MolapProperties.getInstance().addProperty("molap.dimension.split.value.in.columnar", "1");
    //             MolapProperties.getInstance().addProperty("molap.is.fullyfilled.bits", "true");
    //             MolapProperties.getInstance().addProperty("is.int.based.indexer", "true");
    //             MolapProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true");
    //             MolapProperties.getInstance().addProperty("high.cardinality.value", "100000");
    //             MolapProperties.getInstance().addProperty("is.compressed.keyblock", "false");
    //             MolapProperties.getInstance().addProperty("molap.leaf.node.size", "500");
    //
    //             //MolapProperties.getInstance().addProperty("spark.molap.use.agg", "false");
    //    olap.sql("load data local INPATH 'D:/csv' into cube indra4 FIELDS terminated by ','").collect
    //    olap.partitionData("default", "indra1", "D:/csv/0", "D:/ravif/part1")
    //    olap.loadData("PCC", "ODM", "D:/ravif/part","hdfs://master:54310/ravi/dimensions")
    //    olap.mSql("create cube demo.cube2 dimensions(Location String, Protocol String) measures(traffic Integer) mapped by (fact_table cols[Location=location,Protocol=protocol,traffic=traffic]) options(cardinality[Location=100, Protocol=100] aggregation[traffic=SUM] PARTITIONER[CLASS='com.huawei.datasight.molap.partition.api.impl.SampleDataPartitionerImpl' LEVELS{Location} COUNT=1])")
    //    olap.loadData("PCC", "ODM", "/hadoopmnt/smartpcc/1day", "/opt/naresh/smartpcc/dimensions")
    //    olap.flattenRDD(olap.sql("show schemas")).collect.foreach(println)
    //    olap.sql("show schemas like 'P'").foreach(println)
    //    olap.sql("SELECT Tac AS `nk`, sum(L4_DW_PACKETS) as `k` FROM PCC.ODM group by Tac ORDER BY `k` limit 1").collect foreach(println)
    //    olap.sql("SELECT count(*) FROM PCC.ODM ").collect.foreach(println)
    //    olap.sql("SELECT * FROM PCC.ODM limit 10 ").collect.foreach(println)
    //    olap.sql("select d1 as n from rav3.indra indra join (SELECT d1 as a, sum(m1) as b FROM rav3.indra group by d1 order by  b desc limit 10) t on (indra.d1 = t.a) where indra.d1='val_466' and indra.d1='val_466' ").collect.foreach(println)
    //    olap.sql("SELECT  d1,count(distinct m1) FROM indra4 group by d1").collect.foreach(println)
    //    olap.sql("SELECT  Tac,Msisdn FROM PCC.ODM  where Tac in (10000002,10000101,10000226)").collect.foreach(println)
    //    olap.sql("SELECT  Year FROM PCC.ODM").collect.foreach(println)
    //    val rdd = olap.sparkContext.textFile("D:/csv/kv1.csv")
    //    val rdd1 = rdd.map{f=>
    //      val d = f.split(",")
    //        Record(d(0).toInt,d(1))
    //    }
    //    import olap._
    //    rdd1.registerTempTable("dd")
    //    olap.sql("select a.d1,b.value from indra a join dd b on (a.d1=b.value)").collect.foreach(println)
    //    println(olap.sql("SELECT `odm`.`Tac` AS `none_tac_nk`,\n  COUNT(DISTINCT `odm`.`MSISDN_COUNT`) AS `ctd_msisdn_count_ok`,\n  SUM(`odm`.`L4_DW_THROUGHPUT`) AS `sum_l4_dw_throughput_ok`\nFROM `PCC`.`ODM` `odm`\n  JOIN (\n  SELECT `odm`.`Tac` AS `none_tac_nk`,\n    COUNT(1) AS `xtableau_join_flag`,\n    COUNT(DISTINCT `odm`.`MSISDN_COUNT`) AS `x__alias__0`\n  FROM `PCC`.`ODM` `odm`\n  GROUP BY `odm`.`Tac`\n  ORDER BY `x__alias__0` DESC\n  LIMIT 10\n) t0 ON (`odm`.`Tac` = `t0`.`none_tac_nk`)\nGROUP BY `odm`.`Tac`"))//.collect.foreach(println)
    //    val tab1 = hiveContext.sql("select * from src")
    //    val tab2 = hiveContext.sql("select * from src3")
    //    val tab = tab1.join(tab2,Inner,Some("src.key".attr === "src3.key".attr)).collect//take(3)
    //    val tab = sql("select case key when '286' then 'ravi' else key end from src").collect//take(3)
    //    val tab = sql("select src.key from src inner join src4 on src.key=src4.key").collect//take(3)
    //    tab.registerAsTable("src1");
    //    println("((((((((((((((((((((((((((")
    //    tab.foreach(println)
    //    println("((((((((((((((((((((((((((")
    //
    //    // Queries are expressed in HiveQL
    //    //hiveContext.sql("SELECT key, value FROM  src WHERE key < 10 GROUP BY value").collect.foreach(println)
    //
    //    // Aggregation queries are also supported.
    //    val count = sql("SELECT COUNT(*) FROM src").collect().head.getLong(0)
    //    println(s"COUNT(*): $count")
    //
    //    // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    //    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    //    val rddFromSql = hiveContext.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    //
    //    println("Result of RDD.map:")
    //    val rddAsStrings = rddFromSql.map {
    //      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    //    }
    //
    //   // You can also register RDDs as temporary tables within a HiveContext.
    //    val rdd = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    //    rdd.registerTempTable("records")
    //
    //    // Queries can then join RDD data with data stored in Hive.
    //    println("Result of SELECT *:")
    //    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").collect().foreach(println)
  }

}

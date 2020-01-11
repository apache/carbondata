
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonV3DataFormatConstants}
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Test Class for DataLoadingV3TestCase to verify all scenerios
 */

class DataLoadingV3TestCase extends QueryTest with BeforeAndAfterAll {
         

  //Check query reponse for select * query with no filters
  test("V3_01_Query_01_001", Include) {
     sql(s"""CREATE TABLE 3lakh_uniqdata (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES('table_blocksize'='128','include_dictionary'='BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,CUST_ID')""").collect
   sql(
     s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""".stripMargin).collect
    checkAnswer(s"""select count(*) from 3lakh_uniqdata""",
      Seq(Row(300635)), "DataLoadingV3TestCase_V3_01_Query_01_001")

  }


  //Check query reponse where table is having > 10 columns as dimensions and all the columns are selected in the query
  test("V3_01_Query_01_002", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from 3lakh_uniqdata)c""",
      Seq(Row(300635)), "DataLoadingV3TestCase_V3_01_Query_01_002")

  }


  //Check query reponse when filter is having eq condition on 1st column and data is selected within a page
  test("V3_01_Query_01_003", Include) {

    checkAnswer(s"""select CUST_ID from 3lakh_uniqdata where cust_id = 35000""",
      Seq(Row(35000)), "DataLoadingV3TestCase_V3_01_Query_01_003")

  }


  //Check query reponse when filter is having in condition on 1st column and data is selected within a page
  test("V3_01_Query_01_004", Include) {

    checkAnswer(s"""select CUST_ID from 3lakh_uniqdata where cust_id in (30000, 35000 ,37000)""",
      Seq(Row(30000),Row(35000),Row(37000)), "DataLoadingV3TestCase_V3_01_Query_01_004")

  }


  //Check query reponse when filter is having range condition on 1st column and data is selected within a page
  test("V3_01_Query_01_005", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where cust_id between 59000 and 60000)c""",
      Seq(Row(1001)), "DataLoadingV3TestCase_V3_01_Query_01_005")

  }


  //Check query reponse when filter is having range condition on 1st coluumn and data is selected within a pages - values just in the boundary of the page upper llimit - without offheap sort and vector reader
  test("V3_01_Query_01_009", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where cust_id between 59000 and 61000)c""",
      Seq(Row(2001)), "DataLoadingV3TestCase_V3_01_Query_01_009")

  }


  //Check query reponse when filter is having in condition 1st column and data is selected across multiple pages - with no offheap sort and vector reader
  test("V3_01_Query_01_010", Include) {

    checkAnswer(s"""select CUST_ID from 3lakh_uniqdata where cust_id in (30000, 35000 ,37000, 69000,101000,133000,165000,197000,229000,261000,293000, 329622)""",
      Seq(Row(133000),Row(165000),Row(197000),Row(30000),Row(229000),Row(261000),Row(35000),Row(37000),Row(293000),Row(329622),Row(69000),Row(101000)), "DataLoadingV3TestCase_V3_01_Query_01_010")

  }


  //Check query reponse when filter is having not between condition 1st column and data is selected across all pages - with no offheap sort and vector reader
  test("V3_01_Query_01_011", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where cust_id not between 29001 and 329621)c""",
      Seq(Row(3)), "DataLoadingV3TestCase_V3_01_Query_01_011")

  }


  //Check query reponse when filter is applied on on the 2nd column and data is selected across all pages  -with no offheap sort and vector reader
  test("V3_01_Query_01_012", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where cust_name like 'CUST_NAME_2%')c""",
      Seq(Row(110000)), "DataLoadingV3TestCase_V3_01_Query_01_012")

  }


  //Check query reponse when filter is having not like condition set on the 2nd columns and data is selected across all pages
  test("V3_01_Query_01_013", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where cust_name not like 'CUST_NAME_2%')c""",
      Seq(Row(190635)), "DataLoadingV3TestCase_V3_01_Query_01_013")

  }


  //Check query reponse when filter is having > operator set on the 10th columns and data is selected within a  page
  test("V3_01_Query_01_014", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata where Double_COLUMN1 > 42000)b""",
      Seq(Row(300624)), "DataLoadingV3TestCase_V3_01_Query_01_014")

  }


  //Check query reponse when filter is having like operator set on the 3rd columns and data is selected across all pages - with no offheap sort and vector reader
  test("V3_01_Query_01_015", Include) {

    checkAnswer(s"""select count(*) from (select ACTIVE_EMUI_VERSION from 3lakh_uniqdata where ACTIVE_EMUI_VERSION like 'ACTIVE_EMUI_VERSION_20%')c""",
      Seq(Row(11000)), "DataLoadingV3TestCase_V3_01_Query_01_015")

  }


  //Check query reponse when filter condtion is put on all collumns connected through and operator and data is selected across from 1  page
  test("V3_01_Query_01_016", Include) {

    checkAnswer(s"""select count(*) from (select * from 3lakh_uniqdata where CUST_ID = 29000 and CUST_NAME = 'CUST_NAME_20000' and ACTIVE_EMUI_VERSION = 'ACTIVE_EMUI_VERSION_20000' and  DOB = '04-10-2010 01:00' and DOJ = '04-10-2012 02:00' and BIGINT_COLUMN1 = 1.23372E+11 and BIGINT_COLUMN2 = -2.23E+11 and DECIMAL_COLUMN1 =  12345698901	 and DECIMAL_COLUMN2 = 22345698901	 and Double_COLUMN1 = 11234567490	 and Double_COLUMN2 = -11234567490 	and  INTEGER_COLUMN1 = 20001)c""",
      Seq(Row(0)), "DataLoadingV3TestCase_V3_01_Query_01_016")

  }


  //Check query reponse when filter condtion is put on all collumns connected through and and grouping operator and data is selected across from 1  page
  test("V3_01_Query_01_018", Include) {

    checkAnswer(s"""select count(*) from (select * from 3lakh_uniqdata where CUST_ID = 29000 and CUST_NAME = 'CUST_NAME_20000' and (ACTIVE_EMUI_VERSION = 'ACTIVE_EMUI_VERSION_20001' or DOB = '04-10-2010 01:00') and DOJ = '04-10-2012 02:00' and BIGINT_COLUMN1 = 1.23372E+11 and BIGINT_COLUMN2 = -2.23E+11 and DECIMAL_COLUMN1 =  12345698901 and DECIMAL_COLUMN2 = 22345698901 or Double_COLUMN1 = 11234567490 and ( Double_COLUMN2 = -11234567490 or  INTEGER_COLUMN1 = 20003))c""",
      Seq(Row(300623)), "DataLoadingV3TestCase_V3_01_Query_01_018")

  }


  //Check query reponse when filter condtion is 1st column and connected through OR condition and data is selected across multiple pages
  test("V3_01_Query_01_019", Include) {

    checkAnswer(s"""select CUST_NAME from 3lakh_uniqdata where CUST_ID = 29000 or CUST_ID = 60000 or CUST_ID = 100000 or CUST_ID = 130000""",
      Seq(Row("CUST_NAME_121000"),Row("CUST_NAME_20000"),Row("CUST_NAME_51000"),Row("CUST_NAME_91000")), "DataLoadingV3TestCase_V3_01_Query_01_019")

  }


  //Check query reponse when filter condtion is put on all collumns connected through and/or operator and range is used and data is selected across multiple   pages
  test("V3_01_Query_01_020", Include) {

    checkAnswer(s"""select count(*) from (select * from 3lakh_uniqdata where (CUST_ID >= 29000 and CUST_ID <= 60000) and CUST_NAME like 'CUST_NAME_20%' and ACTIVE_EMUI_VERSION = 'ACTIVE_EMUI_VERSION_20000' and  DOB = '04-10-2010 01:00' and DOJ = '04-10-2012 02:00' and BIGINT_COLUMN1 = 1.23372E+11 and BIGINT_COLUMN2 = -2.23E+11 and DECIMAL_COLUMN1 =  12345698901 or DECIMAL_COLUMN2 = 22345698901 and Double_COLUMN1 = 11234567490 and (Double_COLUMN2 = -11234567490 or  INTEGER_COLUMN1 = 20001))c""",
      Seq(Row(1)), "DataLoadingV3TestCase_V3_01_Query_01_020")

  }


  //Check query reponse when 1st column select ed nd filter is applied and data is selected from 1 page
  test("V3_01_Query_01_022", Include) {

    checkAnswer(s"""select CUST_ID from 3lakh_uniqdata order by CUST_ID limit 10""",
      Seq(Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null),Row(null)), "DataLoadingV3TestCase_V3_01_Query_01_022")

  }


  //Check query reponse when 2nd column select ed nd filter is applied and data is selected from 1 page
  test("V3_01_Query_01_023", Include) {

    checkAnswer(s"""select count(*) from (select CUST_NAME from 3lakh_uniqdata limit 30000)c""",
      Seq(Row(30000)), "DataLoadingV3TestCase_V3_01_Query_01_023")

  }


  //Check query reponse when 4th column select ed nd filter is applied and data is selected from 1 page
  test("V3_01_Query_01_024", Include) {

    checkAnswer(s"""select count(*) from (select DOB from 3lakh_uniqdata limit 30000)c""",
      Seq(Row(30000)), "DataLoadingV3TestCase_V3_01_Query_01_024")

  }


  //Check query reponse when 1st column select ed nd filter is applied and data is selected from 2 page
  test("V3_01_Query_01_025", Include) {

    checkAnswer(s"""select count(*) from (select CUST_ID from 3lakh_uniqdata limit 60000)c""",
      Seq(Row(60000)), "DataLoadingV3TestCase_V3_01_Query_01_025")

  }


  //Check query reponse when 2nd column select ed nd filter is applied and data is selected from 2 page
  test("V3_01_Query_01_026", Include) {

    checkAnswer(s"""select count(*) from (select CUST_NAME from 3lakh_uniqdata limit 60000)c""",
      Seq(Row(60000)), "DataLoadingV3TestCase_V3_01_Query_01_026")

  }


  //Check query reponse when 4th column selected nd filter is applied and data is selected from 2 page
  test("V3_01_Query_01_027", Include) {

    checkAnswer(s"""select count(*) from (select DOB from 3lakh_uniqdata limit 60000)c""",
      Seq(Row(60000)), "DataLoadingV3TestCase_V3_01_Query_01_027")

  }


  //Check query reponse when 2nd column select ed nd with order by and data is selected from 1 page
  test("V3_01_Query_01_028", Include) {

    checkAnswer(s"""select cust_id from 3lakh_uniqdata order by CUST_NAME desc limit 10""",
      Seq(Row(108999),Row(108998),Row(108997),Row(108996),Row(108995),Row(108994),Row(108993),Row(108992),Row(108991),Row(108990)), "DataLoadingV3TestCase_V3_01_Query_01_028")

  }


  //Check query reponse when temp table is used and multiple pages are scanned
  test("V3_01_Query_01_029", Include) {

    checkAnswer(s"""select count(*) from ( select a.cust_id from 3lakh_uniqdata a where a.cust_id in (select c.cust_id from 3lakh_uniqdata c where c.cust_name  like  'CUST_NAME_2000%') and a.cust_id between 29000 and 60000)d""",
      Seq(Row(10)), "DataLoadingV3TestCase_V3_01_Query_01_029")

  }


  //Check query reponse when aggregate table is used and multiple pages are scanned
  test("V3_01_Query_01_030", Include) {

    checkAnswer(s"""select substring(CUST_NAME,1,11),count(*) from 3lakh_uniqdata group by substring(CUST_NAME,1,11) having count(*) > 1""",
      Seq(Row("CUST_NAME_4",10000),Row("CUST_NAME_1",100000),Row("CUST_NAME_8",10000),Row("CUST_NAME_6",10000),Row("CUST_NAME_2", 110000),
        Row("CUST_NAME_5",10000),Row("CUST_NAME_7",10000),Row("CUST_NAME_9",10000),Row("",11),Row("CUST_NAME_3",30623)), "DataLoadingV3TestCase_V3_01_Query_01_030")

  }


  //Check query reponse when aggregate table is used along with filter condition and multiple pages are scanned
  test("V3_01_Query_01_031", Include) {

    checkAnswer(s"""select substring(CUST_NAME,1,11),count(*) from 3lakh_uniqdata where  cust_id between 59000 and 160000 group by substring(CUST_NAME,1,11) having count(*) > 1""",
      Seq(Row("CUST_NAME_1",51001),Row("CUST_NAME_8",10000),Row("CUST_NAME_6",10000),Row("CUST_NAME_5",10000),
        Row("CUST_NAME_7",10000),Row("CUST_NAME_9",10000)), "DataLoadingV3TestCase_V3_01_Query_01_031")

  }


  //Check join query when the table is having v3 format
  test("V3_01_Query_01_032", Include) {
     sql(s"""CREATE TABLE 3lakh_uniqdata2 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES('table_blocksize'='128','include_dictionary'='BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata2 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select a.cust_id, b.cust_name from 3lakh_uniqdata a, 3lakh_uniqdata2 b where a.cust_id = b.cust_id and a.cust_name = b.cust_name and a.cust_id in (29000, 59000, 69000,15000,250000, 310000)""",
      Seq(Row(29000,"CUST_NAME_20000"),Row(250000,"CUST_NAME_241000"),Row(310000,"CUST_NAME_301000"),
        Row(59000,"CUST_NAME_50000"),Row(69000,"CUST_NAME_60000")), "DataLoadingV3TestCase_V3_01_Query_01_032")

  }


  //Check query when table is having single column so that the records count per blocklet is > 120000, where query scan is done on single page
  test("V3_01_Param_01_005", Include) {
     sql(s"""CREATE TABLE 3lakh_uniqdata1 (CUST_NAME String) STORED AS carbondata TBLPROPERTIES('table_blocksize'='128')""").collect
   sql(s"""insert into 3lakh_uniqdata1 select cust_name from 3lakh_uniqdata""").collect
    checkAnswer(s"""select count(*) from (select CUST_NAME from 3lakh_uniqdata where cust_name  like  'CUST_NAME_2000%')c""",
      Seq(Row(110)), "DataLoadingV3TestCase_V3_01_Param_01_005")

  }


  //Check query when table is having single column so that the records count per blocklet is > 120000, where query scan is done across the pages in the blocklet
  test("V3_01_Load_01_006", Include) {

    checkAnswer(s"""select count(*) from (select CUST_NAME from 3lakh_uniqdata where cust_name  like  'CUST_NAME_20%')c""",
      Seq(Row(11000)), "DataLoadingV3TestCase_V3_01_Load_01_006")

  }


  //Check impact on load and query reading when larger value (1 lakh length) present in the column
  ignore("V3_01_Stress_01_001", Include) {
     sql(s"""create table t_carbn1c (name string) STORED AS carbondata TBLPROPERTIES('table_blocksize'='128','include_dictionary'='name')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/1lakh.csv' into table t_carbn1c OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='name')""").collect
    checkAnswer(s"""select count(*) from t_carbn1c""",
      Seq(Row(1)), "DataLoadingV3TestCase_V3_01_Stress_01_001")

  }


  //Check impact on load and query reading when larger value (1 lakh length) present in the column when the column is measure
  ignore("V3_01_Stress_01_007", Include) {

    checkAnswer(s"""select substring(name,1,10) from t_carbn1c""",
      Seq(Row("hellohowar")), "DataLoadingV3TestCase_V3_01_Stress_01_007")

  }


  //Check vertical compaction on V3 format, for minor compaction 1st level
  test("V3_01_Compaction_01_001", Include) {
     sql(s"""CREATE TABLE 3lakh_uniqdata3 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10), DECIMAL_COLUMN2 decimal(36,10),Double_COLUMN1 double, Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED AS carbondata TBLPROPERTIES('table_blocksize'='128','include_dictionary'='BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1,CUST_ID')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
   sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from (select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from 3lakh_uniqdata)c""",
      Seq(Row(300635)), "DataLoadingV3TestCase_V3_01_Compaction_01_001")

  }


  //Check vertical compaction on V3 format, for minor compaction 2nd level
  test("V3_01_Compaction_01_002", Include) {
     sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from (select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from 3lakh_uniqdata)c""",
      Seq(Row(300635)), "DataLoadingV3TestCase_V3_01_Compaction_01_002")

  }


  //Check vertical compaction on V3 format, for major compaction
  test("V3_01_Compaction_01_003", Include) {
     sql(s"""LOAD DATA INPATH '$resourcesPath/Data/3Lakh.csv' into table 3lakh_uniqdata3 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1')""").collect
    checkAnswer(s"""select count(*) from (select CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN1,Double_COLUMN2,INTEGER_COLUMN1 from 3lakh_uniqdata)c""",
      Seq(Row(300635)), "DataLoadingV3TestCase_V3_01_Compaction_01_003")
  }

  val prop = CarbonProperties.getInstance()
  val p1 = prop.getProperty("carbon.blockletgroup.size.in.mb", CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_DEFAULT_VALUE)
  val p2 = prop.getProperty("enable.offheap.sort", CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT)
  val p3 = prop.getProperty("carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
  val p4 = prop.getProperty("carbon.data.file.version", CarbonCommonConstants.CARBON_DATA_FILE_DEFAULT_VERSION)
  val p5 = prop.getProperty("carbon.enable.auto.load.merge", CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE)
  val p6 = prop.getProperty("carbon.compaction.level.threshold", CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)

  override protected def beforeAll() {
    // Adding new properties
    prop.addProperty("carbon.blockletgroup.size.in.mb", "16")
    prop.addProperty("enable.offheap.sort", "false")
    prop.addProperty("carbon.enable.vector.reader", "false")
    prop.addProperty("carbon.data.file.version", "V3")
    prop.addProperty("carbon.enable.auto.load.merge", "false")
    prop.addProperty("carbon.compaction.level.threshold", "(2,2)")
    sql(s"""drop table if exists 3lakh_uniqdata""").collect
    sql(s"""drop table if exists 3lakh_uniqdata2""").collect
    sql(s"""drop table if exists t_carbn1c""").collect
    sql(s"""drop table if exists 3lakh_uniqdata1""").collect
  }

  override def afterAll: Unit = {
    sql(s"""drop table if exists 3lakh_uniqdata""").collect
    sql(s"""drop table if exists 3lakh_uniqdata2""").collect
    sql(s"""drop table if exists t_carbn1c""").collect
    sql(s"""drop table if exists 3lakh_uniqdata1""").collect
    //Reverting to old
    prop.addProperty("carbon.blockletgroup.size.in.mb", p1)
    prop.addProperty("enable.offheap.sort", p2)
    prop.addProperty("carbon.enable.vector.reader", p3)
    prop.addProperty("carbon.data.file.version", p4)
    prop.addProperty("carbon.enable.auto.load.merge", p5)
    prop.addProperty("carbon.compaction.level.threshold", p6)
  }
}
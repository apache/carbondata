
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

/**
 * Test Class for ShowLoadsTestCase to verify all scenerios
 */

class ShowLoadsTestCase extends QueryTest with BeforeAndAfterAll {
         

 //Verify failure/success/Partial status in show segments.
 test("AR-DataSightCarbon-Maintenance-DataLoadManagement001_TOR_001-PTS-005-TC-01_196", Include) {
    sql(
      s"""drop TABLE if exists ShowSegment_196""".stripMargin).collect
  sql(s"""CREATE TABLE ShowSegment_196 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table ShowSegment_196 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table ShowSegment_196 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""show segments for table ShowSegment_196""").collect()
    sql(s"""drop table ShowSegment_196""").collect
 }


 //Verify show segment commands with database name.
 test("AR-DataSightCarbon-Maintenance-DataLoadManagement001_TOR_001-PTS-002-TC-01_196", Include) {
    sql(s"""drop TABLE if exists Database_ShowSegment_196""").collect
  sql(s"""CREATE TABLE Database_ShowSegment_196 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/InsertData/join1.csv' into table Database_ShowSegment_196 OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ,BIGINT_COLUMN1,BIGINT_COLUMN2,DECIMAL_COLUMN1,Double_COLUMN1,DECIMAL_COLUMN2,Double_COLUMN2,INTEGER_COLUMN1')""").collect
  sql(s"""show segments for table default.Database_ShowSegment_196""").collect()
    sql(s"""drop table Database_ShowSegment_196""").collect
 }


 //Show Segments failing if table name not in same case
 test("PTS-TOR_AR-DataSight_Carbon-LCM_002_001-001-TC-008_830", Include) {
    sql(s"""drop TABLE if exists Case_ShowSegment_196""").collect
  sql(s"""CREATE TABLE Case_ShowSegment_196 (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string,DOB timestamp, DOJ timestamp, BIGINT_COLUMN1 bigint,BIGINT_COLUMN2 bigint,DECIMAL_COLUMN1 decimal(30,10),Double_COLUMN1 double,DECIMAL_COLUMN2 decimal(36,10), Double_COLUMN2 double,INTEGER_COLUMN1 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('table_blocksize'='1')""").collect
   sql(s"""show segments for table CASE_ShowSegment_196""").collect

    sql(s"""drop table Case_ShowSegment_196""").collect
 }

}
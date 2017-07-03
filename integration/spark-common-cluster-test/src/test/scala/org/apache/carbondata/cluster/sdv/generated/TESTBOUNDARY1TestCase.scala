
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
 * Test Class for testboundary1 to verify all scenerios
 */

class TESTBOUNDARY1TestCase extends QueryTest with BeforeAndAfterAll {
         

//BVA_SPL_DATA_CreateTable2_Drop
test("BVA_SPL_DATA_CreateTable2_Drop", Include) {
  sql(s"""drop table if exists  test_boundary1""").collect

  sql(s"""drop table if exists  test_boundary1_hive""").collect

}
       

//BVA_SPL_DATA_CreateTable2
test("BVA_SPL_DATA_CreateTable2", Include) {
  sql(s"""create table Test_Boundary1 (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,30),c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table Test_Boundary1_hive (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,30),c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//BVA_SPL_DATA_DataLoad2
test("BVA_SPL_DATA_DataLoad2", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/Test_Data1.csv' INTO table Test_Boundary1 OPTIONS('DELIMITER'=',','QUOTECHAR'='','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/Test_Data1.csv' INTO table Test_Boundary1_hive """).collect

}
       

//BVA_SPL_DATA_CreateTable1_Drop
test("BVA_SPL_DATA_CreateTable1_Drop", Include) {
  sql(s"""drop table if exists  test_boundary""").collect

  sql(s"""drop table if exists  test_boundary_hive""").collect

}
       

//BVA_SPL_DATA_CreateTable1
test("BVA_SPL_DATA_CreateTable1", Include) {
  sql(s"""create table Test_Boundary (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,30),c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table Test_Boundary_hive (c1_int int,c2_Bigint Bigint,c3_Decimal Decimal(38,30),c4_double double,c5_string string,c6_Timestamp Timestamp,c7_Datatype_Desc string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//BVA_SPL_DATA_DataLoad1
test("BVA_SPL_DATA_DataLoad1", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/Test_Data1.csv' INTO table Test_Boundary OPTIONS('DELIMITER'=',','QUOTECHAR'='','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/Test_Data1.csv' INTO table Test_Boundary_hive """).collect

}
       

//BVA_SPL_DATA_INT_001
test("BVA_SPL_DATA_INT_001", Include) {
  sql(s"""select c1_int from Test_Boundary where c1_int in (2.147483647E9,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_INT_002
test("BVA_SPL_DATA_INT_002", Include) {
  sql(s"""select c1_int from Test_Boundary where c1_int in (-2.147483647E9,2345.0,-1234.0)""").collect
}
       

//BVA_SPL_DATA_INT_003
test("BVA_SPL_DATA_INT_003", Include) {
  sql(s"""select c1_int from Test_Boundary where c1_int in (0,-1234.0)""").collect
}
       

//BVA_SPL_DATA_INT_004
test("BVA_SPL_DATA_INT_004", Include) {
  sql(s"""select c1_int from Test_Boundary where c1_int not in (2.147483647E9,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_INT_005
test("BVA_SPL_DATA_INT_005", Include) {
  sql(s"""select c1_int from Test_Boundary where c1_int in (2.147483647E9,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_INT_006
test("BVA_SPL_DATA_INT_006", Include) {
  sql(s"""select c1_int+0.100 from Test_Boundary where c1_int < 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_007
test("BVA_SPL_DATA_INT_007", Include) {
  checkAnswer(s"""select c1_int+0.9 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int+0.9 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_008
test("BVA_SPL_DATA_INT_008", Include) {
  checkAnswer(s"""select c1_int+0.9 from Test_Boundary where c1_int >= 2.147483647E9 """,
    s"""select c1_int+0.9 from Test_Boundary_hive where c1_int >= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_009
test("BVA_SPL_DATA_INT_009", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int <= 2.147483647E9 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int <= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_010
test("BVA_SPL_DATA_INT_010", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int <> 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int <> 4567""")
}
       

//BVA_SPL_DATA_INT_011
test("BVA_SPL_DATA_INT_011", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567""")
}
       

//BVA_SPL_DATA_INT_012
test("BVA_SPL_DATA_INT_012", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_013
test("BVA_SPL_DATA_INT_013", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 or c1_int <> 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 or c1_int <> 4567""")
}
       

//BVA_SPL_DATA_INT_014
test("BVA_SPL_DATA_INT_014", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 or c1_int = 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 or c1_int = 4567""")
}
       

//BVA_SPL_DATA_INT_015
test("BVA_SPL_DATA_INT_015", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 or c1_int = 4567 group by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 or c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_016
test("BVA_SPL_DATA_INT_016", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int <> 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int <> 4567""")
}
       

//BVA_SPL_DATA_INT_017
test("BVA_SPL_DATA_INT_017", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567""",
    s"""select c1_int from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567""")
}
       

//BVA_SPL_DATA_INT_018
test("BVA_SPL_DATA_INT_018", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_019
test("BVA_SPL_DATA_INT_019", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int is null""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int is null""")
}
       

//BVA_SPL_DATA_INT_020
test("BVA_SPL_DATA_INT_020", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int is not null""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int is not null""")
}
       

//BVA_SPL_DATA_INT_021
test("BVA_SPL_DATA_INT_021", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where  c1_int =2345 and c1_int <> 4567""",
    s"""select c1_int from Test_Boundary_hive where  c1_int =2345 and c1_int <> 4567""")
}
       

//BVA_SPL_DATA_INT_022
test("BVA_SPL_DATA_INT_022", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where  c1_int =2345 and c1_int = 4567""",
    s"""select c1_int from Test_Boundary_hive where  c1_int =2345 and c1_int = 4567""")
}
       

//BVA_SPL_DATA_INT_023
test("BVA_SPL_DATA_INT_023", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary where c1_int =2345 and c1_int = 4567 group by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive where c1_int =2345 and c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_024
test("BVA_SPL_DATA_INT_024", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where  c1_int =2345 or c1_int <> 4567""",
    s"""select c1_int from Test_Boundary_hive where  c1_int =2345 or c1_int <> 4567""")
}
       

//BVA_SPL_DATA_INT_025
test("BVA_SPL_DATA_INT_025", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where  c1_int =2345 or c1_int = 4567""",
    s"""select c1_int from Test_Boundary_hive where  c1_int =2345 or c1_int = 4567""")
}
       

//BVA_SPL_DATA_INT_026
test("BVA_SPL_DATA_INT_026", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary where c1_int =2345 or c1_int = 4567 group by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive where c1_int =2345 or c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_027
test("BVA_SPL_DATA_INT_027", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int between -2.147483648E9 and 2.147483647E9) e """,
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int between -2.147483648E9 and 2.147483647E9) e """)
}
       

//BVA_SPL_DATA_INT_028
test("BVA_SPL_DATA_INT_028", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int not between -2.147483648E9 and 0) e""",
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int not between -2.147483648E9 and 0) e""")
}
       

//BVA_SPL_DATA_INT_029
test("BVA_SPL_DATA_INT_029", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int not between 0 and 2.147483647E9) e""",
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int not between 0 and 2.147483647E9) e""")
}
       

//BVA_SPL_DATA_INT_030
test("BVA_SPL_DATA_INT_030", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int between -2.147483648E9 and 2.147483647E9 """,
    s"""select c1_int from Test_Boundary_hive where c1_int between -2.147483648E9 and 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_031
test("BVA_SPL_DATA_INT_031", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int  between -2.147483648E9 and 0 """,
    s"""select c1_int from Test_Boundary_hive where c1_int  between -2.147483648E9 and 0 """)
}
       

//BVA_SPL_DATA_INT_032
test("BVA_SPL_DATA_INT_032", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int  between 0 and 2.147483647E9""",
    s"""select c1_int from Test_Boundary_hive where c1_int  between 0 and 2.147483647E9""")
}
       

//BVA_SPL_DATA_INT_033
test("BVA_SPL_DATA_INT_033", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int between -2.147483648E9 and 2.147483647E9) e """,
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int between -2.147483648E9 and 2.147483647E9) e """)
}
       

//BVA_SPL_DATA_INT_034
test("BVA_SPL_DATA_INT_034", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int  between -2.147483648E9 and 0) e""",
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int  between -2.147483648E9 and 0) e""")
}
       

//BVA_SPL_DATA_INT_035
test("BVA_SPL_DATA_INT_035", Include) {
  checkAnswer(s"""select c1_int from (select c1_int from Test_Boundary where c1_int  between 0 and 2.147483647E9) e""",
    s"""select c1_int from (select c1_int from Test_Boundary_hive where c1_int  between 0 and 2.147483647E9) e""")
}
       

//BVA_SPL_DATA_INT_036
test("BVA_SPL_DATA_INT_036", Include) {
  checkAnswer(s"""select count(*) from Test_Boundary""",
    s"""select count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_037
test("BVA_SPL_DATA_INT_037", Include) {
  checkAnswer(s"""select distinct count(*) from Test_Boundary""",
    s"""select distinct count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_038
test("BVA_SPL_DATA_INT_038", Include) {
  checkAnswer(s"""select distinct count(c1_int) from Test_Boundary""",
    s"""select distinct count(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_039
test("BVA_SPL_DATA_INT_039", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int not between -2.147483648E9 and 2.147483647E9 """,
    s"""select c1_int from Test_Boundary_hive where c1_int not between -2.147483648E9 and 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_040
test("BVA_SPL_DATA_INT_040", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int not between -2.147483648E9 and 0 """,
    s"""select c1_int from Test_Boundary_hive where c1_int not between -2.147483648E9 and 0 """)
}
       

//BVA_SPL_DATA_INT_041
test("BVA_SPL_DATA_INT_041", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int not between 0 and 2.147483647E9""",
    s"""select c1_int from Test_Boundary_hive where c1_int not between 0 and 2.147483647E9""")
}
       

//BVA_SPL_DATA_INT_042
test("BVA_SPL_DATA_INT_042", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_043
test("BVA_SPL_DATA_INT_043", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_044
test("BVA_SPL_DATA_INT_044", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int < 2.147483647E9 and c1_int >3.147483647E9""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int < 2.147483647E9 and c1_int >3.147483647E9""")
}
       

//BVA_SPL_DATA_INT_045
test("BVA_SPL_DATA_INT_045", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int < 2.147483647E9 and c1_int >3.147483647E9""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int < 2.147483647E9 and c1_int >3.147483647E9""")
}
       

//BVA_SPL_DATA_INT_046
test("BVA_SPL_DATA_INT_046", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int +0.1000= 2.147483647E9 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int +0.1000= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_047
test("BVA_SPL_DATA_INT_047", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c1_int = Test_Boundary1.c1_int WHERE Test_Boundary.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c1_int ORDER BY Test_Boundary.c1_int ASC""",
    s"""SELECT Test_Boundary_hive.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c1_int = Test_Boundary1_hive.c1_int WHERE Test_Boundary_hive.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c1_int ORDER BY Test_Boundary_hive.c1_int ASC""")
}
       

//BVA_SPL_DATA_INT_048
test("BVA_SPL_DATA_INT_048", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c1_int = Test_Boundary1.c1_int WHERE Test_Boundary.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c1_int ORDER BY Test_Boundary.c1_int ASC""",
    s"""SELECT Test_Boundary_hive.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c1_int = Test_Boundary1_hive.c1_int WHERE Test_Boundary_hive.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c1_int ORDER BY Test_Boundary_hive.c1_int ASC""")
}
       

//BVA_SPL_DATA_INT_049
test("BVA_SPL_DATA_INT_049", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c1_int = Test_Boundary1.c1_int WHERE Test_Boundary.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c1_int ORDER BY Test_Boundary.c1_int ASC""",
    s"""SELECT Test_Boundary_hive.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c1_int = Test_Boundary1_hive.c1_int WHERE Test_Boundary_hive.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c1_int ORDER BY Test_Boundary_hive.c1_int ASC""")
}
       

//BVA_SPL_DATA_INT_050
test("BVA_SPL_DATA_INT_050", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c1_int = Test_Boundary1.c1_int WHERE Test_Boundary.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c1_int ORDER BY Test_Boundary.c1_int ASC""",
    s"""SELECT Test_Boundary_hive.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c1_int = Test_Boundary1_hive.c1_int WHERE Test_Boundary_hive.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c1_int ORDER BY Test_Boundary_hive.c1_int ASC""")
}
       

//BVA_SPL_DATA_INT_051
test("BVA_SPL_DATA_INT_051", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c1_int = Test_Boundary1.c1_int WHERE Test_Boundary.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c1_int ORDER BY Test_Boundary.c1_int ASC""",
    s"""SELECT Test_Boundary_hive.c1_int AS c1_int FROM ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c1_int FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c1_int = Test_Boundary1_hive.c1_int WHERE Test_Boundary_hive.c1_int <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c1_int ORDER BY Test_Boundary_hive.c1_int ASC""")
}
       

//BVA_SPL_DATA_INT_052
test("BVA_SPL_DATA_INT_052", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary group by c1_int having max(c1_int) >5000""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive group by c1_int having max(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_053
test("BVA_SPL_DATA_INT_053", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary group by c1_int having max(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive group by c1_int having max(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_054
test("BVA_SPL_DATA_INT_054", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary group by c1_int having max(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive group by c1_int having max(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_055
test("BVA_SPL_DATA_INT_055", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary group by c1_int having max(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive group by c1_int having max(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_056
test("BVA_SPL_DATA_INT_056", Include) {
  checkAnswer(s"""select c1_int,max(c1_int) from Test_Boundary group by c1_int having max(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,max(c1_int) from Test_Boundary_hive group by c1_int having max(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_060
test("BVA_SPL_DATA_INT_060", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int""")
}
       

//BVA_SPL_DATA_INT_062
test("BVA_SPL_DATA_INT_062", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary group by c1_int having count(c1_int) >5000""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive group by c1_int having count(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_063
test("BVA_SPL_DATA_INT_063", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary group by c1_int having count(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive group by c1_int having count(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_064
test("BVA_SPL_DATA_INT_064", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary group by c1_int having count(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive group by c1_int having count(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_065
test("BVA_SPL_DATA_INT_065", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary group by c1_int having count(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive group by c1_int having count(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_066
test("BVA_SPL_DATA_INT_066", Include) {
  checkAnswer(s"""select c1_int,count(c1_int) from Test_Boundary group by c1_int having count(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,count(c1_int) from Test_Boundary_hive group by c1_int having count(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_071
test("BVA_SPL_DATA_INT_071", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >5000""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_072
test("BVA_SPL_DATA_INT_072", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_073
test("BVA_SPL_DATA_INT_073", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_074
test("BVA_SPL_DATA_INT_074", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_075
test("BVA_SPL_DATA_INT_075", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_080
test("BVA_SPL_DATA_INT_080", Include) {
  checkAnswer(s"""select c1_int,sum(c1_int) from Test_Boundary group by c1_int having sum(c1_int) >5000""",
    s"""select c1_int,sum(c1_int) from Test_Boundary_hive group by c1_int having sum(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_081
test("BVA_SPL_DATA_INT_081", Include) {
  checkAnswer(s"""select c1_int,sum(c1_int) from Test_Boundary group by c1_int having sum(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,sum(c1_int) from Test_Boundary_hive group by c1_int having sum(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_082
test("BVA_SPL_DATA_INT_082", Include) {
  checkAnswer(s"""select c1_int,sum(c1_int) from Test_Boundary group by c1_int having sum(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,sum(c1_int) from Test_Boundary_hive group by c1_int having sum(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_083
test("BVA_SPL_DATA_INT_083", Include) {
  checkAnswer(s"""select c1_int,sum(c1_int) from Test_Boundary group by c1_int having sum(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,sum(c1_int) from Test_Boundary_hive group by c1_int having sum(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_084
test("BVA_SPL_DATA_INT_084", Include) {
  checkAnswer(s"""select c1_int,sum(c1_int) from Test_Boundary group by c1_int having sum(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,sum(c1_int) from Test_Boundary_hive group by c1_int having sum(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_089
test("BVA_SPL_DATA_INT_089", Include) {
  checkAnswer(s"""select c1_int,avg(c1_int) from Test_Boundary group by c1_int having avg(c1_int) >5000""",
    s"""select c1_int,avg(c1_int) from Test_Boundary_hive group by c1_int having avg(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_090
test("BVA_SPL_DATA_INT_090", Include) {
  checkAnswer(s"""select c1_int,avg(c1_int) from Test_Boundary group by c1_int having avg(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,avg(c1_int) from Test_Boundary_hive group by c1_int having avg(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_091
test("BVA_SPL_DATA_INT_091", Include) {
  checkAnswer(s"""select c1_int,avg(c1_int) from Test_Boundary group by c1_int having avg(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,avg(c1_int) from Test_Boundary_hive group by c1_int having avg(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_092
test("BVA_SPL_DATA_INT_092", Include) {
  checkAnswer(s"""select c1_int,avg(c1_int) from Test_Boundary group by c1_int having avg(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,avg(c1_int) from Test_Boundary_hive group by c1_int having avg(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_093
test("BVA_SPL_DATA_INT_093", Include) {
  checkAnswer(s"""select c1_int,avg(c1_int) from Test_Boundary group by c1_int having avg(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,avg(c1_int) from Test_Boundary_hive group by c1_int having avg(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_097
test("BVA_SPL_DATA_INT_097", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) >5000""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_098
test("BVA_SPL_DATA_INT_098", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_099
test("BVA_SPL_DATA_INT_099", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_100
test("BVA_SPL_DATA_INT_100", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_101
test("BVA_SPL_DATA_INT_101", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_105
test("BVA_SPL_DATA_INT_105", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive where c1_int =-2.147483648E9 or c1_int =2345 and c1_int = 4567 group by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_107
test("BVA_SPL_DATA_INT_107", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having count(c1_int) >5000""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having count(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_108
test("BVA_SPL_DATA_INT_108", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having count(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having count(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_109
test("BVA_SPL_DATA_INT_109", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having count(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having count(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_110
test("BVA_SPL_DATA_INT_110", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having count(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having count(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_111
test("BVA_SPL_DATA_INT_111", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having count(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,count(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having count(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_116
test("BVA_SPL_DATA_INT_116", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >5000""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_117
test("BVA_SPL_DATA_INT_117", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_118
test("BVA_SPL_DATA_INT_118", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_119
test("BVA_SPL_DATA_INT_119", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_120
test("BVA_SPL_DATA_INT_120", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_125
test("BVA_SPL_DATA_INT_125", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having sum(c1_int) >5000""",
    s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having sum(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_126
test("BVA_SPL_DATA_INT_126", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_127
test("BVA_SPL_DATA_INT_127", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_128
test("BVA_SPL_DATA_INT_128", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having sum(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_129
test("BVA_SPL_DATA_INT_129", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having sum(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,sum(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having sum(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_134
test("BVA_SPL_DATA_INT_134", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having avg(c1_int) >5000""",
    s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having avg(c1_int) >5000""")
}
       

//BVA_SPL_DATA_INT_135
test("BVA_SPL_DATA_INT_135", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_136
test("BVA_SPL_DATA_INT_136", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_137
test("BVA_SPL_DATA_INT_137", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having avg(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_138
test("BVA_SPL_DATA_INT_138", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having avg(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,avg(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having avg(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_142
test("BVA_SPL_DATA_INT_142", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_143
test("BVA_SPL_DATA_INT_143", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having max(c1_int) <-2.147483646E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,max(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having max(c1_int) <-2.147483646E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_147
test("BVA_SPL_DATA_INT_147", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >2.147483646E9  order by c1_int""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >2.147483646E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_148
test("BVA_SPL_DATA_INT_148", Include) {
  checkAnswer(s"""select c1_int,min(c1_int) from Test_Boundary group by c1_int having min(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,min(c1_int) from Test_Boundary_hive group by c1_int having min(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_149
test("BVA_SPL_DATA_INT_149", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_INT_150
test("BVA_SPL_DATA_INT_150", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int""")
}
       

//BVA_SPL_DATA_INT_151
test("BVA_SPL_DATA_INT_151", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc asc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc asc""")
}
       

//BVA_SPL_DATA_INT_152
test("BVA_SPL_DATA_INT_152", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int asc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int asc""")
}
       

//BVA_SPL_DATA_INT_153
test("BVA_SPL_DATA_INT_153", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc desc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc desc""")
}
       

//BVA_SPL_DATA_INT_154
test("BVA_SPL_DATA_INT_154", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int desc""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int desc""")
}
       

//BVA_SPL_DATA_INT_155
test("BVA_SPL_DATA_INT_155", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc limit 5""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483646E9  order by c1_int,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_INT_156
test("BVA_SPL_DATA_INT_156", Include) {
  checkAnswer(s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int limit 5""",
    s"""select c1_int,c7_datatype_desc,min(c1_int) from Test_Boundary_hive group by c1_int,c7_datatype_desc having min(c1_int) >2.147483648E9  order by c1_int limit 5""")
}
       

//BVA_SPL_DATA_INT_166
test("BVA_SPL_DATA_INT_166", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int between -9223372036854775808 and 9223372036854775807 """,
    s"""select c1_int from Test_Boundary_hive where c1_int between -9223372036854775808 and 9223372036854775807 """)
}
       

//BVA_SPL_DATA_INT_167
test("BVA_SPL_DATA_INT_167", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int not between -9223372036854775808 and 0 """,
    s"""select c1_int from Test_Boundary_hive where c1_int not between -9223372036854775808 and 0 """)
}
       

//BVA_SPL_DATA_INT_168
test("BVA_SPL_DATA_INT_168", Include) {
  checkAnswer(s"""select c1_int from Test_Boundary where c1_int not between 0 and 9223372036854775807""",
    s"""select c1_int from Test_Boundary_hive where c1_int not between 0 and 9223372036854775807""")
}
       

//BVA_SPL_DATA_INT_169
test("BVA_SPL_DATA_INT_169", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int is null""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int is null""")
}
       

//BVA_SPL_DATA_INT_170
test("BVA_SPL_DATA_INT_170", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int is not null""",
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int is not null""")
}
       

//BVA_SPL_DATA_INT_171
test("BVA_SPL_DATA_INT_171", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int not like 123 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int not like 123 """)
}
       

//BVA_SPL_DATA_INT_172
test("BVA_SPL_DATA_INT_172", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int like 123 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int like 123 """)
}
       

//BVA_SPL_DATA_INT_173
test("BVA_SPL_DATA_INT_173", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int rlike 123 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int rlike 123 """)
}
       

//BVA_SPL_DATA_INT_174
test("BVA_SPL_DATA_INT_174", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int regexp 123 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int regexp 123 """)
}
       

//BVA_SPL_DATA_INT_175
test("BVA_SPL_DATA_INT_175", Include) {
  checkAnswer(s"""select c1_int+0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int+0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_176
test("BVA_SPL_DATA_INT_176", Include) {
  checkAnswer(s"""select c1_int+0.00100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int+0.00100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_177
test("BVA_SPL_DATA_INT_177", Include) {
  sql(s"""select c1_int+23 from Test_Boundary where c1_int < 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_178
test("BVA_SPL_DATA_INT_178", Include) {
  sql(s"""select c1_int+50 from Test_Boundary where c1_int <= 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_179
test("BVA_SPL_DATA_INT_179", Include) {
  checkAnswer(s"""select c1_int+0.50 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int+0.50 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_180
test("BVA_SPL_DATA_INT_180", Include) {
  sql(s"""select c1_int+75 from Test_Boundary where c1_int >= 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_181
test("BVA_SPL_DATA_INT_181", Include) {
  checkAnswer(s"""select c1_int-0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int-0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_182
test("BVA_SPL_DATA_INT_182", Include) {
  checkAnswer(s"""select c1_int-0.00100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int-0.00100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_183
test("BVA_SPL_DATA_INT_183", Include) {
  sql(s"""select c1_int-23 from Test_Boundary where c1_int < 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_184
test("BVA_SPL_DATA_INT_184", Include) {
  sql(s"""select c1_int-50 from Test_Boundary where c1_int <= 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_185
test("BVA_SPL_DATA_INT_185", Include) {
  checkAnswer(s"""select c1_int-0.50 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int-0.50 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_186
test("BVA_SPL_DATA_INT_186", Include) {
  checkAnswer(s"""select c1_int-75 from Test_Boundary where c1_int >= 2.147483647E9 """,
    s"""select c1_int-75 from Test_Boundary_hive where c1_int >= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_187
test("BVA_SPL_DATA_INT_187", Include) {
  checkAnswer(s"""select c1_int*0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int*0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_188
test("BVA_SPL_DATA_INT_188", Include) {
  checkAnswer(s"""select c1_int*0.00100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int*0.00100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_189
test("BVA_SPL_DATA_INT_189", Include) {
  sql(s"""select c1_int*23 from Test_Boundary where c1_int < 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_190
test("BVA_SPL_DATA_INT_190", Include) {
  sql(s"""select c1_int*50 from Test_Boundary where c1_int <= 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_191
test("BVA_SPL_DATA_INT_191", Include) {
  checkAnswer(s"""select c1_int*0.50 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int*0.50 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_192
test("BVA_SPL_DATA_INT_192", Include) {
  sql(s"""select c1_int*75 from Test_Boundary where c1_int >= 2.147483647E9 """).collect
}
       

//BVA_SPL_DATA_INT_193
test("BVA_SPL_DATA_INT_193", Include) {
  checkAnswer(s"""select c1_int/0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int/0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_194
test("BVA_SPL_DATA_INT_194", Include) {
  checkAnswer(s"""select c1_int/0.00100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int/0.00100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_195
test("BVA_SPL_DATA_INT_195", Include) {
  checkAnswer(s"""select c1_int/23 from Test_Boundary where c1_int < 2.147483647E9 """,
    s"""select c1_int/23 from Test_Boundary_hive where c1_int < 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_196
test("BVA_SPL_DATA_INT_196", Include) {
  checkAnswer(s"""select c1_int/50 from Test_Boundary where c1_int <= 2.147483647E9 """,
    s"""select c1_int/50 from Test_Boundary_hive where c1_int <= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_197
test("BVA_SPL_DATA_INT_197", Include) {
  checkAnswer(s"""select c1_int/0.50 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int/0.50 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_198
test("BVA_SPL_DATA_INT_198", Include) {
  checkAnswer(s"""select c1_int/75 from Test_Boundary where c1_int >= 2.147483647E9 """,
    s"""select c1_int/75 from Test_Boundary_hive where c1_int >= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_199
test("BVA_SPL_DATA_INT_199", Include) {
  checkAnswer(s"""select c1_int%0.100 from Test_Boundary where c1_int <> 2.147483647E9 """,
    s"""select c1_int%0.100 from Test_Boundary_hive where c1_int <> 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_200
test("BVA_SPL_DATA_INT_200", Include) {
  checkAnswer(s"""select c1_int%0.00100 from Test_Boundary where c1_int = 2.147483647E9 """,
    s"""select c1_int%0.00100 from Test_Boundary_hive where c1_int = 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_201
test("BVA_SPL_DATA_INT_201", Include) {
  checkAnswer(s"""select c1_int%23 from Test_Boundary where c1_int < 2.147483647E9 """,
    s"""select c1_int%23 from Test_Boundary_hive where c1_int < 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_202
test("BVA_SPL_DATA_INT_202", Include) {
  checkAnswer(s"""select c1_int%50 from Test_Boundary where c1_int <= 2.147483647E9 """,
    s"""select c1_int%50 from Test_Boundary_hive where c1_int <= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_203
test("BVA_SPL_DATA_INT_203", Include) {
  checkAnswer(s"""select c1_int%0.50 from Test_Boundary where c1_int > 2.147483647E9 """,
    s"""select c1_int%0.50 from Test_Boundary_hive where c1_int > 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_204
test("BVA_SPL_DATA_INT_204", Include) {
  checkAnswer(s"""select c1_int%75 from Test_Boundary where c1_int >= 2.147483647E9 """,
    s"""select c1_int%75 from Test_Boundary_hive where c1_int >= 2.147483647E9 """)
}
       

//BVA_SPL_DATA_INT_205
test("BVA_SPL_DATA_INT_205", Include) {
  checkAnswer(s"""select round(c1_int,1)  from Test_Boundary""",
    s"""select round(c1_int,1)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_206
test("BVA_SPL_DATA_INT_206", Include) {
  checkAnswer(s"""select round(c1_int,1)  from Test_Boundary""",
    s"""select round(c1_int,1)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_209
test("BVA_SPL_DATA_INT_209", Include) {
  checkAnswer(s"""select floor(c1_int)  from Test_Boundary """,
    s"""select floor(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_210
test("BVA_SPL_DATA_INT_210", Include) {
  checkAnswer(s"""select ceil(c1_int)  from Test_Boundary""",
    s"""select ceil(c1_int)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_211
test("BVA_SPL_DATA_INT_211", Include) {
  sql(s"""select rand(5)  from Test_Boundary """).collect
}
       

//BVA_SPL_DATA_INT_212
test("BVA_SPL_DATA_INT_212", Include) {
  checkAnswer(s"""select exp(c1_int) from Test_Boundary""",
    s"""select exp(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_213
test("BVA_SPL_DATA_INT_213", Include) {
  checkAnswer(s"""select ln(c1_int) from Test_Boundary""",
    s"""select ln(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_214
test("BVA_SPL_DATA_INT_214", Include) {
  checkAnswer(s"""select log10(c1_int) from Test_Boundary""",
    s"""select log10(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_215
test("BVA_SPL_DATA_INT_215", Include) {
  checkAnswer(s"""select log2(c1_int) from Test_Boundary""",
    s"""select log2(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_216
test("BVA_SPL_DATA_INT_216", Include) {
  checkAnswer(s"""select log(c1_int) from Test_Boundary""",
    s"""select log(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_217
test("BVA_SPL_DATA_INT_217", Include) {
  checkAnswer(s"""select log(c1_int) from Test_Boundary""",
    s"""select log(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_218
test("BVA_SPL_DATA_INT_218", Include) {
  checkAnswer(s"""select pow(c1_int,c1_int) from Test_Boundary""",
    s"""select pow(c1_int,c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_219
test("BVA_SPL_DATA_INT_219", Include) {
  checkAnswer(s"""select sqrt(c1_int) from Test_Boundary""",
    s"""select sqrt(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_220
test("BVA_SPL_DATA_INT_220", Include) {
  checkAnswer(s"""select pmod(c1_int,1) from Test_Boundary""",
    s"""select pmod(c1_int,1) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_221
test("BVA_SPL_DATA_INT_221", Include) {
  checkAnswer(s"""select  sin(c1_int)  from Test_Boundary """,
    s"""select  sin(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_222
test("BVA_SPL_DATA_INT_222", Include) {
  checkAnswer(s"""select  asin(c1_int)  from Test_Boundary """,
    s"""select  asin(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_223
test("BVA_SPL_DATA_INT_223", Include) {
  checkAnswer(s"""select cos(c1_int)  from Test_Boundary """,
    s"""select cos(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_224
test("BVA_SPL_DATA_INT_224", Include) {
  checkAnswer(s"""select acos(c1_int)  from Test_Boundary """,
    s"""select acos(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_225
test("BVA_SPL_DATA_INT_225", Include) {
  checkAnswer(s"""select tan(c1_int)  from Test_Boundary """,
    s"""select tan(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_226
test("BVA_SPL_DATA_INT_226", Include) {
  checkAnswer(s"""select atan(c1_int)  from Test_Boundary """,
    s"""select atan(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_227
test("BVA_SPL_DATA_INT_227", Include) {
  checkAnswer(s"""select degrees(c1_int)  from Test_Boundary """,
    s"""select degrees(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_228
test("BVA_SPL_DATA_INT_228", Include) {
  checkAnswer(s"""select radians(c1_int)  from Test_Boundary """,
    s"""select radians(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_229
test("BVA_SPL_DATA_INT_229", Include) {
  checkAnswer(s"""select positive(c1_int)  from Test_Boundary """,
    s"""select positive(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_230
test("BVA_SPL_DATA_INT_230", Include) {
  sql(s"""select negative(c1_int)  from Test_Boundary """).collect
}
       

//BVA_SPL_DATA_INT_231
test("BVA_SPL_DATA_INT_231", Include) {
  checkAnswer(s"""select sign(c1_int)  from Test_Boundary """,
    s"""select sign(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_232
test("BVA_SPL_DATA_INT_232", Include) {
  checkAnswer(s"""select exp(c1_int)  from Test_Boundary """,
    s"""select exp(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_234
test("BVA_SPL_DATA_INT_234", Include) {
  checkAnswer(s"""select factorial(c1_int)  from Test_Boundary """,
    s"""select factorial(c1_int)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_INT_235
test("BVA_SPL_DATA_INT_235", Include) {
  checkAnswer(s"""select cbrt(c1_int) from Test_Boundary""",
    s"""select cbrt(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_236
test("BVA_SPL_DATA_INT_236", Include) {
  sql(s"""select shiftleft(c1_int,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_237
test("BVA_SPL_DATA_INT_237", Include) {
  sql(s"""select shiftleft(c1_int,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_238
test("BVA_SPL_DATA_INT_238", Include) {
  checkAnswer(s"""select shiftright(c1_int,2) from Test_Boundary""",
    s"""select shiftright(c1_int,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_239
test("BVA_SPL_DATA_INT_239", Include) {
  checkAnswer(s"""select shiftright(c1_int,2) from Test_Boundary""",
    s"""select shiftright(c1_int,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_240
test("BVA_SPL_DATA_INT_240", Include) {
  sql(s"""select shiftrightunsigned(c1_int,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_241
test("BVA_SPL_DATA_INT_241", Include) {
  sql(s"""select shiftrightunsigned(c1_int,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_242
test("BVA_SPL_DATA_INT_242", Include) {
  checkAnswer(s"""select greatest(1,2,3,4,5) from Test_Boundary""",
    s"""select greatest(1,2,3,4,5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_243
test("BVA_SPL_DATA_INT_243", Include) {
  checkAnswer(s"""select least(1,2,3,4,5) from Test_Boundary""",
    s"""select least(1,2,3,4,5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_244
test("BVA_SPL_DATA_INT_244", Include) {
  checkAnswer(s"""select cast(c1_int as double) from Test_Boundary""",
    s"""select cast(c1_int as double) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_245
test("BVA_SPL_DATA_INT_245", Include) {
  checkAnswer(s"""select if(c1_int<5000,'t','f') from Test_Boundary""",
    s"""select if(c1_int<5000,'t','f') from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_246
test("BVA_SPL_DATA_INT_246", Include) {
  checkAnswer(s"""select isnull(c1_int) from Test_Boundary""",
    s"""select isnull(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_247
test("BVA_SPL_DATA_INT_247", Include) {
  checkAnswer(s"""select isnotnull(c1_int) from Test_Boundary""",
    s"""select isnotnull(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_248
test("BVA_SPL_DATA_INT_248", Include) {
  checkAnswer(s"""select nvl(c1_int,10) from Test_Boundary""",
    s"""select nvl(c1_int,10) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_249
test("BVA_SPL_DATA_INT_249", Include) {
  checkAnswer(s"""select nvl(c1_int,0) from Test_Boundary""",
    s"""select nvl(c1_int,0) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_250
test("BVA_SPL_DATA_INT_250", Include) {
  checkAnswer(s"""select nvl(c1_int,null) from Test_Boundary""",
    s"""select nvl(c1_int,null) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_251
test("BVA_SPL_DATA_INT_251", Include) {
  checkAnswer(s"""select coalesce(c1_int,null,null,null,756) from Test_Boundary""",
    s"""select coalesce(c1_int,null,null,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_252
test("BVA_SPL_DATA_INT_252", Include) {
  checkAnswer(s"""select coalesce(c1_int,1,null,null,756) from Test_Boundary""",
    s"""select coalesce(c1_int,1,null,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_253
test("BVA_SPL_DATA_INT_253", Include) {
  checkAnswer(s"""select coalesce(c1_int,345,null,756) from Test_Boundary""",
    s"""select coalesce(c1_int,345,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_254
test("BVA_SPL_DATA_INT_254", Include) {
  checkAnswer(s"""select coalesce(c1_int,345,0.1,456,756) from Test_Boundary""",
    s"""select coalesce(c1_int,345,0.1,456,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_255
test("BVA_SPL_DATA_INT_255", Include) {
  checkAnswer(s"""select coalesce(c1_int,756,null,null,null) from Test_Boundary""",
    s"""select coalesce(c1_int,756,null,null,null) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_256
test("BVA_SPL_DATA_INT_256", Include) {
  checkAnswer(s"""select case c1_int when 2345 then true else false end from Test_boundary""",
    s"""select case c1_int when 2345 then true else false end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_257
test("BVA_SPL_DATA_INT_257", Include) {
  checkAnswer(s"""select case c1_int when 2345 then true end from Test_boundary""",
    s"""select case c1_int when 2345 then true end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_258
test("BVA_SPL_DATA_INT_258", Include) {
  checkAnswer(s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_259
test("BVA_SPL_DATA_INT_259", Include) {
  checkAnswer(s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_260
test("BVA_SPL_DATA_INT_260", Include) {
  checkAnswer(s"""select case when c1_int <2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case when c1_int <2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_261
test("BVA_SPL_DATA_INT_261", Include) {
  checkAnswer(s"""select case c1_int when 2345 then true else false end from Test_boundary""",
    s"""select case c1_int when 2345 then true else false end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_262
test("BVA_SPL_DATA_INT_262", Include) {
  checkAnswer(s"""select case c1_int when 2345 then true end from Test_boundary""",
    s"""select case c1_int when 2345 then true end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_263
test("BVA_SPL_DATA_INT_263", Include) {
  checkAnswer(s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_264
test("BVA_SPL_DATA_INT_264", Include) {
  checkAnswer(s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case c1_int when 2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_265
test("BVA_SPL_DATA_INT_265", Include) {
  checkAnswer(s"""select case when c1_int <2345 then 1000 else c1_int end from Test_boundary""",
    s"""select case when c1_int <2345 then 1000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_266
test("BVA_SPL_DATA_INT_266", Include) {
  checkAnswer(s"""select case when c1_int <2345 then 1000 when c1_int >2535353535 then 1000000000 else c1_int end from Test_boundary""",
    s"""select case when c1_int <2345 then 1000 when c1_int >2535353535 then 1000000000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_267
test("BVA_SPL_DATA_INT_267", Include) {
  checkAnswer(s"""select case when c1_int <2345 then 1000 when c1_int is null then 1000000000 else c1_int end from Test_boundary""",
    s"""select case when c1_int <2345 then 1000 when c1_int is null then 1000000000 else c1_int end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_INT_268
test("BVA_SPL_DATA_INT_268", Include) {
  checkAnswer(s"""select distinct count(*) from Test_Boundary""",
    s"""select distinct count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_269
test("BVA_SPL_DATA_INT_269", Include) {
  checkAnswer(s"""select distinct count(c1_int) from Test_Boundary""",
    s"""select distinct count(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_270
test("BVA_SPL_DATA_INT_270", Include) {
  checkAnswer(s"""select max(c1_int) from Test_Boundary""",
    s"""select max(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_271
test("BVA_SPL_DATA_INT_271", Include) {
  checkAnswer(s"""select  count(distinct (c1_int)) from Test_Boundary""",
    s"""select  count(distinct (c1_int)) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_272
test("BVA_SPL_DATA_INT_272", Include) {
  checkAnswer(s"""select distinct sum(c1_int) from Test_Boundary""",
    s"""select distinct sum(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_273
test("BVA_SPL_DATA_INT_273", Include) {
  checkAnswer(s"""select  sum(distinct c1_int) from Test_Boundary""",
    s"""select  sum(distinct c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_274
test("BVA_SPL_DATA_INT_274", Include) {
  checkAnswer(s"""select distinct avg(c1_int) from Test_Boundary""",
    s"""select distinct avg(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_275
test("BVA_SPL_DATA_INT_275", Include) {
  checkAnswer(s"""select  avg( c1_int) from Test_Boundary""",
    s"""select  avg( c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_276
test("BVA_SPL_DATA_INT_276", Include) {
  checkAnswer(s"""select min(c1_int) from Test_Boundary""",
    s"""select min(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_277
test("BVA_SPL_DATA_INT_277", Include) {
  checkAnswer(s"""select distinct min(c1_int) from Test_Boundary""",
    s"""select distinct min(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_278
test("BVA_SPL_DATA_INT_278", Include) {
  checkAnswer(s"""select max(c1_int) from Test_Boundary""",
    s"""select max(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_279
test("BVA_SPL_DATA_INT_279", Include) {
  checkAnswer(s"""select variance(c1_int) from Test_Boundary""",
    s"""select variance(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_280
test("BVA_SPL_DATA_INT_280", Include) {
  checkAnswer(s"""select var_samp(c1_int) from Test_Boundary""",
    s"""select var_samp(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_281
test("BVA_SPL_DATA_INT_281", Include) {
  checkAnswer(s"""select stddev_pop(c1_int) from Test_Boundary""",
    s"""select stddev_pop(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_282
test("BVA_SPL_DATA_INT_282", Include) {
  checkAnswer(s"""select stddev_samp(c1_int) from Test_Boundary""",
    s"""select stddev_samp(c1_int) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_283
test("BVA_SPL_DATA_INT_283", Include) {
  sql(s"""select covar_pop(c1_int,c1_int) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_284
test("BVA_SPL_DATA_INT_284", Include) {
  sql(s"""select covar_samp(c1_int,c1_int) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_285
test("BVA_SPL_DATA_INT_285", Include) {
  checkAnswer(s"""select corr(c1_int,1) from Test_Boundary""",
    s"""select corr(c1_int,1) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_286
test("BVA_SPL_DATA_INT_286", Include) {
  checkAnswer(s"""select percentile(c1_int,0.5) from Test_Boundary""",
    s"""select percentile(c1_int,0.5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_INT_287
test("BVA_SPL_DATA_INT_287", Include) {
  sql(s"""select histogram_numeric(c1_int,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_288
test("BVA_SPL_DATA_INT_288", Include) {
  sql(s"""select collect_set(c1_int) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_289
test("BVA_SPL_DATA_INT_289", Include) {
  sql(s"""select collect_list(c1_int) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_INT_290
test("BVA_SPL_DATA_INT_290", Include) {
  checkAnswer(s"""select cast(c1_int as double) from Test_Boundary""",
    s"""select cast(c1_int as double) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_001
test("BVA_SPL_DATA_BIGINT_001", Include) {
  sql(s"""select c2_bigint from Test_Boundary where c2_bigint in (9223372036854775807,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_BIGINT_002
test("BVA_SPL_DATA_BIGINT_002", Include) {
  sql(s"""select c2_bigint from Test_Boundary where c2_bigint in (-9223372036854775808,2345.0,-1234.0)""").collect
}
       

//BVA_SPL_DATA_BIGINT_003
test("BVA_SPL_DATA_BIGINT_003", Include) {
  sql(s"""select c2_bigint from Test_Boundary where c2_bigint in (0,-1234.0)""").collect
}
       

//BVA_SPL_DATA_BIGINT_004
test("BVA_SPL_DATA_BIGINT_004", Include) {
  sql(s"""select c2_bigint from Test_Boundary where c2_bigint not in (9223372036854775807,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_BIGINT_005
test("BVA_SPL_DATA_BIGINT_005", Include) {
  sql(s"""select c2_bigint from Test_Boundary where c2_bigint in (9223372036854775807,2345.0,1234.0)""").collect
}
       

//BVA_SPL_DATA_BIGINT_006
test("BVA_SPL_DATA_BIGINT_006", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_007
test("BVA_SPL_DATA_BIGINT_007", Include) {
  checkAnswer(s"""select c2_bigint+0.9 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint+0.9 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_008
test("BVA_SPL_DATA_BIGINT_008", Include) {
  checkAnswer(s"""select c2_bigint+0.9 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint+0.9 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_009
test("BVA_SPL_DATA_BIGINT_009", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_010
test("BVA_SPL_DATA_BIGINT_010", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint <> 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint <> 4567""")
}
       

//BVA_SPL_DATA_BIGINT_011
test("BVA_SPL_DATA_BIGINT_011", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567""")
}
       

//BVA_SPL_DATA_BIGINT_012
test("BVA_SPL_DATA_BIGINT_012", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_013
test("BVA_SPL_DATA_BIGINT_013", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint <> 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint <> 4567""")
}
       

//BVA_SPL_DATA_BIGINT_014
test("BVA_SPL_DATA_BIGINT_014", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint = 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint = 4567""")
}
       

//BVA_SPL_DATA_BIGINT_015
test("BVA_SPL_DATA_BIGINT_015", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 or c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_016
test("BVA_SPL_DATA_BIGINT_016", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint <> 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint <> 4567""")
}
       

//BVA_SPL_DATA_BIGINT_017
test("BVA_SPL_DATA_BIGINT_017", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567""")
}
       

//BVA_SPL_DATA_BIGINT_018
test("BVA_SPL_DATA_BIGINT_018", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_019
test("BVA_SPL_DATA_BIGINT_019", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint is null""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint is null""")
}
       

//BVA_SPL_DATA_BIGINT_020
test("BVA_SPL_DATA_BIGINT_020", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint is not null""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint is not null""")
}
       

//BVA_SPL_DATA_BIGINT_021
test("BVA_SPL_DATA_BIGINT_021", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where  c2_bigint =2345 and c2_bigint <> 4567""",
    s"""select c2_bigint from Test_Boundary_hive where  c2_bigint =2345 and c2_bigint <> 4567""")
}
       

//BVA_SPL_DATA_BIGINT_022
test("BVA_SPL_DATA_BIGINT_022", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where  c2_bigint =2345 and c2_bigint = 4567""",
    s"""select c2_bigint from Test_Boundary_hive where  c2_bigint =2345 and c2_bigint = 4567""")
}
       

//BVA_SPL_DATA_BIGINT_023
test("BVA_SPL_DATA_BIGINT_023", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary where c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive where c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_024
test("BVA_SPL_DATA_BIGINT_024", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where  c2_bigint =2345 or c2_bigint <> 4567""",
    s"""select c2_bigint from Test_Boundary_hive where  c2_bigint =2345 or c2_bigint <> 4567""")
}
       

//BVA_SPL_DATA_BIGINT_025
test("BVA_SPL_DATA_BIGINT_025", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where  c2_bigint =2345 or c2_bigint = 4567""",
    s"""select c2_bigint from Test_Boundary_hive where  c2_bigint =2345 or c2_bigint = 4567""")
}
       

//BVA_SPL_DATA_BIGINT_026
test("BVA_SPL_DATA_BIGINT_026", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary where c2_bigint =2345 or c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive where c2_bigint =2345 or c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_027
test("BVA_SPL_DATA_BIGINT_027", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint between -9223372036854775808 and 9223372036854775807) e """,
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint between -9223372036854775808 and 9223372036854775807) e """)
}
       

//BVA_SPL_DATA_BIGINT_028
test("BVA_SPL_DATA_BIGINT_028", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint not between -9223372036854775808 and 0) e""",
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint not between -9223372036854775808 and 0) e""")
}
       

//BVA_SPL_DATA_BIGINT_029
test("BVA_SPL_DATA_BIGINT_029", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint not between 0 and 9223372036854775807) e""",
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint not between 0 and 9223372036854775807) e""")
}
       

//BVA_SPL_DATA_BIGINT_030
test("BVA_SPL_DATA_BIGINT_030", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint between -9223372036854775808 and 9223372036854775807 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint between -9223372036854775808 and 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_031
test("BVA_SPL_DATA_BIGINT_031", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint  between -9223372036854775808 and 0 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint  between -9223372036854775808 and 0 """)
}
       

//BVA_SPL_DATA_BIGINT_032
test("BVA_SPL_DATA_BIGINT_032", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint  between 0 and 9223372036854775807""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint  between 0 and 9223372036854775807""")
}
       

//BVA_SPL_DATA_BIGINT_033
test("BVA_SPL_DATA_BIGINT_033", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint between -9223372036854775808 and 9223372036854775807) e """,
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint between -9223372036854775808 and 9223372036854775807) e """)
}
       

//BVA_SPL_DATA_BIGINT_034
test("BVA_SPL_DATA_BIGINT_034", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint  between -9223372036854775808 and 0) e""",
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint  between -9223372036854775808 and 0) e""")
}
       

//BVA_SPL_DATA_BIGINT_035
test("BVA_SPL_DATA_BIGINT_035", Include) {
  checkAnswer(s"""select c2_bigint from (select c2_bigint from Test_Boundary where c2_bigint  between 0 and 9223372036854775807) e""",
    s"""select c2_bigint from (select c2_bigint from Test_Boundary_hive where c2_bigint  between 0 and 9223372036854775807) e""")
}
       

//BVA_SPL_DATA_BIGINT_036
test("BVA_SPL_DATA_BIGINT_036", Include) {
  checkAnswer(s"""select count(*) from Test_Boundary""",
    s"""select count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_037
test("BVA_SPL_DATA_BIGINT_037", Include) {
  checkAnswer(s"""select distinct count(*) from Test_Boundary""",
    s"""select distinct count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_038
test("BVA_SPL_DATA_BIGINT_038", Include) {
  checkAnswer(s"""select distinct count(c2_bigint) from Test_Boundary""",
    s"""select distinct count(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_039
test("BVA_SPL_DATA_BIGINT_039", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint not between -9223372036854775808 and 9223372036854775807 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint not between -9223372036854775808 and 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_040
test("BVA_SPL_DATA_BIGINT_040", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint not between -9223372036854775808 and 0 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint not between -9223372036854775808 and 0 """)
}
       

//BVA_SPL_DATA_BIGINT_041
test("BVA_SPL_DATA_BIGINT_041", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint not between 0 and 9223372036854775807""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint not between 0 and 9223372036854775807""")
}
       

//BVA_SPL_DATA_BIGINT_042
test("BVA_SPL_DATA_BIGINT_042", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_043
test("BVA_SPL_DATA_BIGINT_043", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_044
test("BVA_SPL_DATA_BIGINT_044", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint < 9223372036854775807 and c2_bigint >3.147483647E9""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint < 9223372036854775807 and c2_bigint >3.147483647E9""")
}
       

//BVA_SPL_DATA_BIGINT_045
test("BVA_SPL_DATA_BIGINT_045", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint < 9223372036854775807 and c2_bigint >3.147483647E9""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint < 9223372036854775807 and c2_bigint >3.147483647E9""")
}
       

//BVA_SPL_DATA_BIGINT_046
test("BVA_SPL_DATA_BIGINT_046", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint +0.1000= 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint +0.1000= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_047
test("BVA_SPL_DATA_BIGINT_047", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c2_bigint = Test_Boundary1.c2_bigint WHERE Test_Boundary.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c2_bigint ORDER BY Test_Boundary.c2_bigint ASC""",
    s"""SELECT Test_Boundary_hive.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c2_bigint = Test_Boundary1_hive.c2_bigint WHERE Test_Boundary_hive.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c2_bigint ORDER BY Test_Boundary_hive.c2_bigint ASC""")
}
       

//BVA_SPL_DATA_BIGINT_048
test("BVA_SPL_DATA_BIGINT_048", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c2_bigint = Test_Boundary1.c2_bigint WHERE Test_Boundary.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c2_bigint ORDER BY Test_Boundary.c2_bigint ASC""",
    s"""SELECT Test_Boundary_hive.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c2_bigint = Test_Boundary1_hive.c2_bigint WHERE Test_Boundary_hive.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c2_bigint ORDER BY Test_Boundary_hive.c2_bigint ASC""")
}
       

//BVA_SPL_DATA_BIGINT_049
test("BVA_SPL_DATA_BIGINT_049", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c2_bigint = Test_Boundary1.c2_bigint WHERE Test_Boundary.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c2_bigint ORDER BY Test_Boundary.c2_bigint ASC""",
    s"""SELECT Test_Boundary_hive.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c2_bigint = Test_Boundary1_hive.c2_bigint WHERE Test_Boundary_hive.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c2_bigint ORDER BY Test_Boundary_hive.c2_bigint ASC""")
}
       

//BVA_SPL_DATA_BIGINT_050
test("BVA_SPL_DATA_BIGINT_050", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c2_bigint = Test_Boundary1.c2_bigint WHERE Test_Boundary.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c2_bigint ORDER BY Test_Boundary.c2_bigint ASC""",
    s"""SELECT Test_Boundary_hive.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c2_bigint = Test_Boundary1_hive.c2_bigint WHERE Test_Boundary_hive.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c2_bigint ORDER BY Test_Boundary_hive.c2_bigint ASC""")
}
       

//BVA_SPL_DATA_BIGINT_051
test("BVA_SPL_DATA_BIGINT_051", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c2_bigint = Test_Boundary1.c2_bigint WHERE Test_Boundary.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c2_bigint ORDER BY Test_Boundary.c2_bigint ASC""",
    s"""SELECT Test_Boundary_hive.c2_bigint AS c2_bigint FROM ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c2_bigint FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c2_bigint = Test_Boundary1_hive.c2_bigint WHERE Test_Boundary_hive.c2_bigint <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c2_bigint ORDER BY Test_Boundary_hive.c2_bigint ASC""")
}
       

//BVA_SPL_DATA_BIGINT_052
test("BVA_SPL_DATA_BIGINT_052", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary group by c2_bigint having max(c2_bigint) >5000""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive group by c2_bigint having max(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_053
test("BVA_SPL_DATA_BIGINT_053", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary group by c2_bigint having max(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive group by c2_bigint having max(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_054
test("BVA_SPL_DATA_BIGINT_054", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary group by c2_bigint having max(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive group by c2_bigint having max(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_055
test("BVA_SPL_DATA_BIGINT_055", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary group by c2_bigint having max(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive group by c2_bigint having max(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_056
test("BVA_SPL_DATA_BIGINT_056", Include) {
  checkAnswer(s"""select c2_bigint,max(c2_bigint) from Test_Boundary group by c2_bigint having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,max(c2_bigint) from Test_Boundary_hive group by c2_bigint having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_060
test("BVA_SPL_DATA_BIGINT_060", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_062
test("BVA_SPL_DATA_BIGINT_062", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary group by c2_bigint having count(c2_bigint) >5000""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive group by c2_bigint having count(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_063
test("BVA_SPL_DATA_BIGINT_063", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary group by c2_bigint having count(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive group by c2_bigint having count(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_064
test("BVA_SPL_DATA_BIGINT_064", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary group by c2_bigint having count(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive group by c2_bigint having count(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_065
test("BVA_SPL_DATA_BIGINT_065", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary group by c2_bigint having count(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive group by c2_bigint having count(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_066
test("BVA_SPL_DATA_BIGINT_066", Include) {
  checkAnswer(s"""select c2_bigint,count(c2_bigint) from Test_Boundary group by c2_bigint having count(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,count(c2_bigint) from Test_Boundary_hive group by c2_bigint having count(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_071
test("BVA_SPL_DATA_BIGINT_071", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >5000""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_072
test("BVA_SPL_DATA_BIGINT_072", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_073
test("BVA_SPL_DATA_BIGINT_073", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_074
test("BVA_SPL_DATA_BIGINT_074", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_075
test("BVA_SPL_DATA_BIGINT_075", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_080
test("BVA_SPL_DATA_BIGINT_080", Include) {
  checkAnswer(s"""select c2_bigint,sum(c2_bigint) from Test_Boundary group by c2_bigint having sum(c2_bigint) >5000""",
    s"""select c2_bigint,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint having sum(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_081
test("BVA_SPL_DATA_BIGINT_081", Include) {
  checkAnswer(s"""select c2_bigint,sum(c2_bigint) from Test_Boundary group by c2_bigint having sum(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint having sum(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_082
test("BVA_SPL_DATA_BIGINT_082", Include) {
  checkAnswer(s"""select c2_bigint,sum(c2_bigint) from Test_Boundary group by c2_bigint having sum(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint having sum(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_083
test("BVA_SPL_DATA_BIGINT_083", Include) {
  checkAnswer(s"""select c2_bigint,sum(c2_bigint) from Test_Boundary group by c2_bigint having sum(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint having sum(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_084
test("BVA_SPL_DATA_BIGINT_084", Include) {
  checkAnswer(s"""select c2_bigint,sum(c2_bigint) from Test_Boundary group by c2_bigint having sum(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint having sum(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_089
test("BVA_SPL_DATA_BIGINT_089", Include) {
  checkAnswer(s"""select c2_bigint,avg(c2_bigint) from Test_Boundary group by c2_bigint having avg(c2_bigint) >5000""",
    s"""select c2_bigint,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint having avg(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_090
test("BVA_SPL_DATA_BIGINT_090", Include) {
  checkAnswer(s"""select c2_bigint,avg(c2_bigint) from Test_Boundary group by c2_bigint having avg(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint having avg(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_091
test("BVA_SPL_DATA_BIGINT_091", Include) {
  checkAnswer(s"""select c2_bigint,avg(c2_bigint) from Test_Boundary group by c2_bigint having avg(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint having avg(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_092
test("BVA_SPL_DATA_BIGINT_092", Include) {
  checkAnswer(s"""select c2_bigint,avg(c2_bigint) from Test_Boundary group by c2_bigint having avg(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint having avg(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_093
test("BVA_SPL_DATA_BIGINT_093", Include) {
  checkAnswer(s"""select c2_bigint,avg(c2_bigint) from Test_Boundary group by c2_bigint having avg(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint having avg(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_097
test("BVA_SPL_DATA_BIGINT_097", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) >5000""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_098
test("BVA_SPL_DATA_BIGINT_098", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_099
test("BVA_SPL_DATA_BIGINT_099", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_100
test("BVA_SPL_DATA_BIGINT_100", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_101
test("BVA_SPL_DATA_BIGINT_101", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_105
test("BVA_SPL_DATA_BIGINT_105", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive where c2_bigint =-9223372036854775808 or c2_bigint =2345 and c2_bigint = 4567 group by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_107
test("BVA_SPL_DATA_BIGINT_107", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having count(c2_bigint) >5000""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having count(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_108
test("BVA_SPL_DATA_BIGINT_108", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_109
test("BVA_SPL_DATA_BIGINT_109", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_110
test("BVA_SPL_DATA_BIGINT_110", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having count(c2_bigint) >2.147483648E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_111
test("BVA_SPL_DATA_BIGINT_111", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having count(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,count(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having count(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_116
test("BVA_SPL_DATA_BIGINT_116", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >5000""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_117
test("BVA_SPL_DATA_BIGINT_117", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_118
test("BVA_SPL_DATA_BIGINT_118", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_119
test("BVA_SPL_DATA_BIGINT_119", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_120
test("BVA_SPL_DATA_BIGINT_120", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_125
test("BVA_SPL_DATA_BIGINT_125", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >5000""",
    s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_126
test("BVA_SPL_DATA_BIGINT_126", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_127
test("BVA_SPL_DATA_BIGINT_127", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_128
test("BVA_SPL_DATA_BIGINT_128", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having sum(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_129
test("BVA_SPL_DATA_BIGINT_129", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having sum(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,sum(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having sum(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_134
test("BVA_SPL_DATA_BIGINT_134", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >5000""",
    s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >5000""")
}
       

//BVA_SPL_DATA_BIGINT_135
test("BVA_SPL_DATA_BIGINT_135", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_136
test("BVA_SPL_DATA_BIGINT_136", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_137
test("BVA_SPL_DATA_BIGINT_137", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having avg(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_138
test("BVA_SPL_DATA_BIGINT_138", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having avg(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,avg(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having avg(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_142
test("BVA_SPL_DATA_BIGINT_142", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_143
test("BVA_SPL_DATA_BIGINT_143", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""",
    s"""select c2_bigint,c7_datatype_desc,max(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having max(c2_bigint) <-2.147483646E9  order by c2_bigint limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_147
test("BVA_SPL_DATA_BIGINT_147", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >2.147483646E9  order by c2_bigint""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >2.147483646E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_148
test("BVA_SPL_DATA_BIGINT_148", Include) {
  checkAnswer(s"""select c2_bigint,min(c2_bigint) from Test_Boundary group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint""",
    s"""select c2_bigint,min(c2_bigint) from Test_Boundary_hive group by c2_bigint having min(c2_bigint) >2.147483648E9  order by c2_bigint""")
}
       

//BVA_SPL_DATA_BIGINT_149
test("BVA_SPL_DATA_BIGINT_149", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_150
test("BVA_SPL_DATA_BIGINT_150", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc""")
}
       

//BVA_SPL_DATA_BIGINT_151
test("BVA_SPL_DATA_BIGINT_151", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc asc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc asc""")
}
       

//BVA_SPL_DATA_BIGINT_152
test("BVA_SPL_DATA_BIGINT_152", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc asc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc asc""")
}
       

//BVA_SPL_DATA_BIGINT_153
test("BVA_SPL_DATA_BIGINT_153", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc desc""")
}
       

//BVA_SPL_DATA_BIGINT_154
test("BVA_SPL_DATA_BIGINT_154", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc desc""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc desc""")
}
       

//BVA_SPL_DATA_BIGINT_155
test("BVA_SPL_DATA_BIGINT_155", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483646E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_156
test("BVA_SPL_DATA_BIGINT_156", Include) {
  checkAnswer(s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""",
    s"""select c2_bigint,c7_datatype_desc,min(c2_bigint) from Test_Boundary_hive group by c2_bigint,c7_datatype_desc having min(c2_bigint) >2.147483648E9  order by c2_bigint,c7_datatype_desc limit 5""")
}
       

//BVA_SPL_DATA_BIGINT_166
test("BVA_SPL_DATA_BIGINT_166", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint between -9223372036854775808 and 9223372036854775807 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint between -9223372036854775808 and 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_167
test("BVA_SPL_DATA_BIGINT_167", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint not between -9223372036854775808 and 0 """,
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint not between -9223372036854775808 and 0 """)
}
       

//BVA_SPL_DATA_BIGINT_168
test("BVA_SPL_DATA_BIGINT_168", Include) {
  checkAnswer(s"""select c2_bigint from Test_Boundary where c2_bigint not between 0 and 9223372036854775807""",
    s"""select c2_bigint from Test_Boundary_hive where c2_bigint not between 0 and 9223372036854775807""")
}
       

//BVA_SPL_DATA_BIGINT_169
test("BVA_SPL_DATA_BIGINT_169", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint is null""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint is null""")
}
       

//BVA_SPL_DATA_BIGINT_170
test("BVA_SPL_DATA_BIGINT_170", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint is not null""",
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint is not null""")
}
       

//BVA_SPL_DATA_BIGINT_171
test("BVA_SPL_DATA_BIGINT_171", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint not like 123 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint not like 123 """)
}
       

//BVA_SPL_DATA_BIGINT_172
test("BVA_SPL_DATA_BIGINT_172", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint like 123 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint like 123 """)
}
       

//BVA_SPL_DATA_BIGINT_173
test("BVA_SPL_DATA_BIGINT_173", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint rlike 123 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint rlike 123 """)
}
       

//BVA_SPL_DATA_BIGINT_174
test("BVA_SPL_DATA_BIGINT_174", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint regexp 123 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint regexp 123 """)
}
       

//BVA_SPL_DATA_BIGINT_175
test("BVA_SPL_DATA_BIGINT_175", Include) {
  checkAnswer(s"""select c2_bigint+0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint+0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_176
test("BVA_SPL_DATA_BIGINT_176", Include) {
  checkAnswer(s"""select c2_bigint+0.00100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint+0.00100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_177
test("BVA_SPL_DATA_BIGINT_177", Include) {
  checkAnswer(s"""select c2_bigint+23 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint+23 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_178
test("BVA_SPL_DATA_BIGINT_178", Include) {
  checkAnswer(s"""select c2_bigint+50 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint+50 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_179
test("BVA_SPL_DATA_BIGINT_179", Include) {
  checkAnswer(s"""select c2_bigint+0.50 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint+0.50 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_180
test("BVA_SPL_DATA_BIGINT_180", Include) {
  checkAnswer(s"""select c2_bigint+75 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint+75 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_181
test("BVA_SPL_DATA_BIGINT_181", Include) {
  checkAnswer(s"""select c2_bigint-0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint-0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_182
test("BVA_SPL_DATA_BIGINT_182", Include) {
  checkAnswer(s"""select c2_bigint-0.00100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint-0.00100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_183
test("BVA_SPL_DATA_BIGINT_183", Include) {
  checkAnswer(s"""select c2_bigint-23 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint-23 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_184
test("BVA_SPL_DATA_BIGINT_184", Include) {
  checkAnswer(s"""select c2_bigint-50 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint-50 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_185
test("BVA_SPL_DATA_BIGINT_185", Include) {
  checkAnswer(s"""select c2_bigint-0.50 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint-0.50 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_186
test("BVA_SPL_DATA_BIGINT_186", Include) {
  checkAnswer(s"""select c2_bigint-75 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint-75 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_187
test("BVA_SPL_DATA_BIGINT_187", Include) {
  checkAnswer(s"""select c2_bigint*0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint*0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_188
test("BVA_SPL_DATA_BIGINT_188", Include) {
  checkAnswer(s"""select c2_bigint*0.00100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint*0.00100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_189
test("BVA_SPL_DATA_BIGINT_189", Include) {
  checkAnswer(s"""select c2_bigint*23 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint*23 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_190
test("BVA_SPL_DATA_BIGINT_190", Include) {
  checkAnswer(s"""select c2_bigint*50 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint*50 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_191
test("BVA_SPL_DATA_BIGINT_191", Include) {
  checkAnswer(s"""select c2_bigint*0.50 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint*0.50 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_192
test("BVA_SPL_DATA_BIGINT_192", Include) {
  checkAnswer(s"""select c2_bigint*75 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint*75 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_193
test("BVA_SPL_DATA_BIGINT_193", Include) {
  checkAnswer(s"""select c2_bigint/0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint/0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_194
test("BVA_SPL_DATA_BIGINT_194", Include) {
  checkAnswer(s"""select c2_bigint/0.00100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint/0.00100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_195
test("BVA_SPL_DATA_BIGINT_195", Include) {
  checkAnswer(s"""select c2_bigint/23 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint/23 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_196
test("BVA_SPL_DATA_BIGINT_196", Include) {
  checkAnswer(s"""select c2_bigint/50 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint/50 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_197
test("BVA_SPL_DATA_BIGINT_197", Include) {
  checkAnswer(s"""select c2_bigint/0.50 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint/0.50 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_198
test("BVA_SPL_DATA_BIGINT_198", Include) {
  checkAnswer(s"""select c2_bigint/75 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint/75 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_199
test("BVA_SPL_DATA_BIGINT_199", Include) {
  checkAnswer(s"""select c2_bigint%0.100 from Test_Boundary where c2_bigint <> 9223372036854775807 """,
    s"""select c2_bigint%0.100 from Test_Boundary_hive where c2_bigint <> 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_200
test("BVA_SPL_DATA_BIGINT_200", Include) {
  checkAnswer(s"""select c2_bigint%0.00100 from Test_Boundary where c2_bigint = 9223372036854775807 """,
    s"""select c2_bigint%0.00100 from Test_Boundary_hive where c2_bigint = 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_201
test("BVA_SPL_DATA_BIGINT_201", Include) {
  checkAnswer(s"""select c2_bigint%23 from Test_Boundary where c2_bigint < 9223372036854775807 """,
    s"""select c2_bigint%23 from Test_Boundary_hive where c2_bigint < 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_202
test("BVA_SPL_DATA_BIGINT_202", Include) {
  checkAnswer(s"""select c2_bigint%50 from Test_Boundary where c2_bigint <= 9223372036854775807 """,
    s"""select c2_bigint%50 from Test_Boundary_hive where c2_bigint <= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_203
test("BVA_SPL_DATA_BIGINT_203", Include) {
  checkAnswer(s"""select c2_bigint%0.50 from Test_Boundary where c2_bigint > 9223372036854775807 """,
    s"""select c2_bigint%0.50 from Test_Boundary_hive where c2_bigint > 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_204
test("BVA_SPL_DATA_BIGINT_204", Include) {
  checkAnswer(s"""select c2_bigint%75 from Test_Boundary where c2_bigint >= 9223372036854775807 """,
    s"""select c2_bigint%75 from Test_Boundary_hive where c2_bigint >= 9223372036854775807 """)
}
       

//BVA_SPL_DATA_BIGINT_205
test("BVA_SPL_DATA_BIGINT_205", Include) {
  checkAnswer(s"""select round(c2_bigint,1)  from Test_Boundary""",
    s"""select round(c2_bigint,1)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_206
test("BVA_SPL_DATA_BIGINT_206", Include) {
  checkAnswer(s"""select round(c2_bigint,1)  from Test_Boundary""",
    s"""select round(c2_bigint,1)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_209
test("BVA_SPL_DATA_BIGINT_209", Include) {
  checkAnswer(s"""select floor(c2_bigint)  from Test_Boundary """,
    s"""select floor(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_210
test("BVA_SPL_DATA_BIGINT_210", Include) {
  checkAnswer(s"""select ceil(c2_bigint)  from Test_Boundary""",
    s"""select ceil(c2_bigint)  from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_211
test("BVA_SPL_DATA_BIGINT_211", Include) {
  sql(s"""select rand(5)  from Test_Boundary """).collect
}
       

//BVA_SPL_DATA_BIGINT_212
test("BVA_SPL_DATA_BIGINT_212", Include) {
  checkAnswer(s"""select exp(c2_bigint) from Test_Boundary""",
    s"""select exp(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_213
test("BVA_SPL_DATA_BIGINT_213", Include) {
  checkAnswer(s"""select ln(c2_bigint) from Test_Boundary""",
    s"""select ln(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_220
test("BVA_SPL_DATA_BIGINT_220", Include) {
  checkAnswer(s"""select pmod(c2_bigint,1) from Test_Boundary""",
    s"""select pmod(c2_bigint,1) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_221
test("BVA_SPL_DATA_BIGINT_221", Include) {
  checkAnswer(s"""select  sin(c2_bigint)  from Test_Boundary """,
    s"""select  sin(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_222
test("BVA_SPL_DATA_BIGINT_222", Include) {
  checkAnswer(s"""select  asin(c2_bigint)  from Test_Boundary """,
    s"""select  asin(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_223
test("BVA_SPL_DATA_BIGINT_223", Include) {
  checkAnswer(s"""select cos(c2_bigint)  from Test_Boundary """,
    s"""select cos(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_224
test("BVA_SPL_DATA_BIGINT_224", Include) {
  checkAnswer(s"""select acos(c2_bigint)  from Test_Boundary """,
    s"""select acos(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_225
test("BVA_SPL_DATA_BIGINT_225", Include) {
  checkAnswer(s"""select tan(c2_bigint)  from Test_Boundary """,
    s"""select tan(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_226
test("BVA_SPL_DATA_BIGINT_226", Include) {
  checkAnswer(s"""select atan(c2_bigint)  from Test_Boundary """,
    s"""select atan(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_227
test("BVA_SPL_DATA_BIGINT_227", Include) {
  checkAnswer(s"""select degrees(c2_bigint)  from Test_Boundary """,
    s"""select degrees(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_228
test("BVA_SPL_DATA_BIGINT_228", Include) {
  checkAnswer(s"""select radians(c2_bigint)  from Test_Boundary """,
    s"""select radians(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_229
test("BVA_SPL_DATA_BIGINT_229", Include) {
  checkAnswer(s"""select positive(c2_bigint)  from Test_Boundary """,
    s"""select positive(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_230
test("BVA_SPL_DATA_BIGINT_230", Include) {
  checkAnswer(s"""select negative(c2_bigint)  from Test_Boundary """,
    s"""select negative(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_231
test("BVA_SPL_DATA_BIGINT_231", Include) {
  checkAnswer(s"""select sign(c2_bigint)  from Test_Boundary """,
    s"""select sign(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_232
test("BVA_SPL_DATA_BIGINT_232", Include) {
  checkAnswer(s"""select exp(c2_bigint)  from Test_Boundary """,
    s"""select exp(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_234
test("BVA_SPL_DATA_BIGINT_234", Include) {
  checkAnswer(s"""select factorial(c2_bigint)  from Test_Boundary """,
    s"""select factorial(c2_bigint)  from Test_Boundary_hive """)
}
       

//BVA_SPL_DATA_BIGINT_236
test("BVA_SPL_DATA_BIGINT_236", Include) {
  checkAnswer(s"""select shiftleft(c2_bigint,2) from Test_Boundary""",
    s"""select shiftleft(c2_bigint,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_237
test("BVA_SPL_DATA_BIGINT_237", Include) {
  checkAnswer(s"""select shiftleft(c2_bigint,2) from Test_Boundary""",
    s"""select shiftleft(c2_bigint,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_238
test("BVA_SPL_DATA_BIGINT_238", Include) {
  checkAnswer(s"""select shiftright(c2_bigint,2) from Test_Boundary""",
    s"""select shiftright(c2_bigint,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_240
test("BVA_SPL_DATA_BIGINT_240", Include) {
  checkAnswer(s"""select shiftrightunsigned(c2_bigint,2) from Test_Boundary""",
    s"""select shiftrightunsigned(c2_bigint,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_241
test("BVA_SPL_DATA_BIGINT_241", Include) {
  checkAnswer(s"""select shiftrightunsigned(c2_bigint,2) from Test_Boundary""",
    s"""select shiftrightunsigned(c2_bigint,2) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_242
test("BVA_SPL_DATA_BIGINT_242", Include) {
  checkAnswer(s"""select greatest(1,2,3,4,5) from Test_Boundary""",
    s"""select greatest(1,2,3,4,5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_243
test("BVA_SPL_DATA_BIGINT_243", Include) {
  checkAnswer(s"""select least(1,2,3,4,5) from Test_Boundary""",
    s"""select least(1,2,3,4,5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_245
test("BVA_SPL_DATA_BIGINT_245", Include) {
  checkAnswer(s"""select if(c2_bigint<5000,'t','f') from Test_Boundary""",
    s"""select if(c2_bigint<5000,'t','f') from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_246
test("BVA_SPL_DATA_BIGINT_246", Include) {
  checkAnswer(s"""select isnull(c2_bigint) from Test_Boundary""",
    s"""select isnull(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_247
test("BVA_SPL_DATA_BIGINT_247", Include) {
  checkAnswer(s"""select isnotnull(c2_bigint) from Test_Boundary""",
    s"""select isnotnull(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_248
test("BVA_SPL_DATA_BIGINT_248", Include) {
  checkAnswer(s"""select nvl(c2_bigint,10) from Test_Boundary""",
    s"""select nvl(c2_bigint,10) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_249
test("BVA_SPL_DATA_BIGINT_249", Include) {
  checkAnswer(s"""select nvl(c2_bigint,0) from Test_Boundary""",
    s"""select nvl(c2_bigint,0) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_250
test("BVA_SPL_DATA_BIGINT_250", Include) {
  checkAnswer(s"""select nvl(c2_bigint,null) from Test_Boundary""",
    s"""select nvl(c2_bigint,null) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_251
test("BVA_SPL_DATA_BIGINT_251", Include) {
  checkAnswer(s"""select coalesce(c2_bigint,null,null,null,756) from Test_Boundary""",
    s"""select coalesce(c2_bigint,null,null,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_252
test("BVA_SPL_DATA_BIGINT_252", Include) {
  checkAnswer(s"""select coalesce(c2_bigint,1,null,null,756) from Test_Boundary""",
    s"""select coalesce(c2_bigint,1,null,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_253
test("BVA_SPL_DATA_BIGINT_253", Include) {
  checkAnswer(s"""select coalesce(c2_bigint,345,null,756) from Test_Boundary""",
    s"""select coalesce(c2_bigint,345,null,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_254
test("BVA_SPL_DATA_BIGINT_254", Include) {
  checkAnswer(s"""select coalesce(c2_bigint,345,0.1,456,756) from Test_Boundary""",
    s"""select coalesce(c2_bigint,345,0.1,456,756) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_255
test("BVA_SPL_DATA_BIGINT_255", Include) {
  checkAnswer(s"""select coalesce(c2_bigint,756,null,null,null) from Test_Boundary""",
    s"""select coalesce(c2_bigint,756,null,null,null) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_256
test("BVA_SPL_DATA_BIGINT_256", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then true else false end from Test_boundary""",
    s"""select case c2_bigint when 2345 then true else false end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_257
test("BVA_SPL_DATA_BIGINT_257", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then true end from Test_boundary""",
    s"""select case c2_bigint when 2345 then true end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_258
test("BVA_SPL_DATA_BIGINT_258", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_259
test("BVA_SPL_DATA_BIGINT_259", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_260
test("BVA_SPL_DATA_BIGINT_260", Include) {
  checkAnswer(s"""select case when c2_bigint <2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case when c2_bigint <2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_261
test("BVA_SPL_DATA_BIGINT_261", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then true else false end from Test_boundary""",
    s"""select case c2_bigint when 2345 then true else false end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_262
test("BVA_SPL_DATA_BIGINT_262", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then true end from Test_boundary""",
    s"""select case c2_bigint when 2345 then true end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_263
test("BVA_SPL_DATA_BIGINT_263", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_264
test("BVA_SPL_DATA_BIGINT_264", Include) {
  checkAnswer(s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case c2_bigint when 2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_265
test("BVA_SPL_DATA_BIGINT_265", Include) {
  checkAnswer(s"""select case when c2_bigint <2345 then 1000 else c2_bigint end from Test_boundary""",
    s"""select case when c2_bigint <2345 then 1000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_266
test("BVA_SPL_DATA_BIGINT_266", Include) {
  checkAnswer(s"""select case when c2_bigint <2345 then 1000 when c2_bigint >2535353535 then 1000000000 else c2_bigint end from Test_boundary""",
    s"""select case when c2_bigint <2345 then 1000 when c2_bigint >2535353535 then 1000000000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_267
test("BVA_SPL_DATA_BIGINT_267", Include) {
  checkAnswer(s"""select case when c2_bigint <2345 then 1000 when c2_bigint is null then 1000000000 else c2_bigint end from Test_boundary""",
    s"""select case when c2_bigint <2345 then 1000 when c2_bigint is null then 1000000000 else c2_bigint end from Test_boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_268
test("BVA_SPL_DATA_BIGINT_268", Include) {
  checkAnswer(s"""select distinct count(*) from Test_Boundary""",
    s"""select distinct count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_269
test("BVA_SPL_DATA_BIGINT_269", Include) {
  checkAnswer(s"""select distinct count(c2_bigint) from Test_Boundary""",
    s"""select distinct count(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_270
test("BVA_SPL_DATA_BIGINT_270", Include) {
  checkAnswer(s"""select max(c2_bigint) from Test_Boundary""",
    s"""select max(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_271
test("BVA_SPL_DATA_BIGINT_271", Include) {
  checkAnswer(s"""select  count(distinct (c2_bigint)) from Test_Boundary""",
    s"""select  count(distinct (c2_bigint)) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_272
test("BVA_SPL_DATA_BIGINT_272", Include) {
  checkAnswer(s"""select distinct sum(c2_bigint) from Test_Boundary""",
    s"""select distinct sum(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_273
test("BVA_SPL_DATA_BIGINT_273", Include) {
  checkAnswer(s"""select  sum(distinct c2_bigint) from Test_Boundary""",
    s"""select  sum(distinct c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_274
test("BVA_SPL_DATA_BIGINT_274", Include) {
  sql(s"""select distinct avg(c2_bigint) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_BIGINT_275
test("BVA_SPL_DATA_BIGINT_275", Include) {
  sql(s"""select  avg( c2_bigint) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_BIGINT_276
test("BVA_SPL_DATA_BIGINT_276", Include) {
  checkAnswer(s"""select min(c2_bigint) from Test_Boundary""",
    s"""select min(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_277
test("BVA_SPL_DATA_BIGINT_277", Include) {
  checkAnswer(s"""select distinct min(c2_bigint) from Test_Boundary""",
    s"""select distinct min(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_278
test("BVA_SPL_DATA_BIGINT_278", Include) {
  checkAnswer(s"""select max(c2_bigint) from Test_Boundary""",
    s"""select max(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_279
test("BVA_SPL_DATA_BIGINT_279", Include) {
  checkAnswer(s"""select variance(c2_bigint) from Test_Boundary""",
    s"""select variance(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_280
test("BVA_SPL_DATA_BIGINT_280", Include) {
  checkAnswer(s"""select var_samp(c2_bigint) from Test_Boundary""",
    s"""select var_samp(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_281
test("BVA_SPL_DATA_BIGINT_281", Include) {
  checkAnswer(s"""select stddev_pop(c2_bigint) from Test_Boundary""",
    s"""select stddev_pop(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_282
test("BVA_SPL_DATA_BIGINT_282", Include) {
  checkAnswer(s"""select stddev_samp(c2_bigint) from Test_Boundary""",
    s"""select stddev_samp(c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_283
test("BVA_SPL_DATA_BIGINT_283", Include) {
  checkAnswer(s"""select covar_pop(c2_bigint,c2_bigint) from Test_Boundary""",
    s"""select covar_pop(c2_bigint,c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_284
test("BVA_SPL_DATA_BIGINT_284", Include) {
  checkAnswer(s"""select covar_samp(c2_bigint,c2_bigint) from Test_Boundary""",
    s"""select covar_samp(c2_bigint,c2_bigint) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_285
test("BVA_SPL_DATA_BIGINT_285", Include) {
  checkAnswer(s"""select corr(c2_bigint,1) from Test_Boundary""",
    s"""select corr(c2_bigint,1) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_286
test("BVA_SPL_DATA_BIGINT_286", Include) {
  checkAnswer(s"""select percentile(c2_bigint,0.5) from Test_Boundary""",
    s"""select percentile(c2_bigint,0.5) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_BIGINT_287
test("BVA_SPL_DATA_BIGINT_287", Include) {
  sql(s"""select histogram_numeric(c2_bigint,2) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_BIGINT_288
test("BVA_SPL_DATA_BIGINT_288", Include) {
  sql(s"""select collect_set(c2_bigint) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_BIGINT_289
test("BVA_SPL_DATA_BIGINT_289", Include) {
  sql(s"""select collect_list(c2_bigint) from Test_Boundary""").collect
}
       

//BVA_SPL_DATA_BIGINT_290
test("BVA_SPL_DATA_BIGINT_290", Include) {
  checkAnswer(s"""select cast(c2_bigint as double) from Test_Boundary""",
    s"""select cast(c2_bigint as double) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_DECIMAL_001
test("BVA_SPL_DATA_DECIMAL_001", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal in (0.12345678900987654321123456789012345638,0.12345678900987654321123456789009876544 ,1234.0)""").collect
}
       

//BVA_SPL_DATA_DECIMAL_002
test("BVA_SPL_DATA_DECIMAL_002", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal in (-0.12345678900987654321123456789009876538,0.12345678900987654321123456789009876544 ,-1234.0)""").collect
}
       

//BVA_SPL_DATA_DECIMAL_003
test("BVA_SPL_DATA_DECIMAL_003", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal in (0,-1234.0)""").collect
}
       

//BVA_SPL_DATA_DECIMAL_004
test("BVA_SPL_DATA_DECIMAL_004", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal not in (0.12345678900987654321123456789012345638,0.12345678900987654321123456789009876544 ,1234.0)""").collect
}
       

//BVA_SPL_DATA_DECIMAL_005
test("BVA_SPL_DATA_DECIMAL_005", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal in (0.12345678900987654321123456789012345638,0.12345678900987654321123456789009876544 ,1234.0)""").collect
}
       

//BVA_SPL_DATA_DECIMAL_006
test("BVA_SPL_DATA_DECIMAL_006", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
}
       

//BVA_SPL_DATA_DECIMAL_007
test("BVA_SPL_DATA_DECIMAL_007", Include) {
  sql(s"""select c3_decimal+0.9 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_008
test("BVA_SPL_DATA_DECIMAL_008", Include) {
  checkAnswer(s"""select c3_decimal+0.9 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
    s"""select c3_decimal+0.9 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
}
       

//BVA_SPL_DATA_DECIMAL_009
test("BVA_SPL_DATA_DECIMAL_009", Include) {
  sql(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_010
test("BVA_SPL_DATA_DECIMAL_010", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal <> 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal <> 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_011
test("BVA_SPL_DATA_DECIMAL_011", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_012
test("BVA_SPL_DATA_DECIMAL_012", Include) {
  checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""",
    s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""")
}
       

//BVA_SPL_DATA_DECIMAL_013
test("BVA_SPL_DATA_DECIMAL_013", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal <> 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal <> 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_014
test("BVA_SPL_DATA_DECIMAL_014", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal = 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal = 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_015
test("BVA_SPL_DATA_DECIMAL_015", Include) {
  checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal = 4567 group by c3_decimal""",
    s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 or c3_decimal = 4567 group by c3_decimal""")
}
       

//BVA_SPL_DATA_DECIMAL_016
test("BVA_SPL_DATA_DECIMAL_016", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal <> 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal <> 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_017
test("BVA_SPL_DATA_DECIMAL_017", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567""",
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_018
test("BVA_SPL_DATA_DECIMAL_018", Include) {
  checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""",
    s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =-0.12345678900987654321123456789009876538 or c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""")
}
       

//BVA_SPL_DATA_DECIMAL_019
test("BVA_SPL_DATA_DECIMAL_019", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal is null""",
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal is null""")
}
       

//BVA_SPL_DATA_DECIMAL_020
test("BVA_SPL_DATA_DECIMAL_020", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal is not null""",
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal is not null""")
}
       

//BVA_SPL_DATA_DECIMAL_021
test("BVA_SPL_DATA_DECIMAL_021", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where  c3_decimal =2345 and c3_decimal <> 4567""",
    s"""select c3_decimal from Test_Boundary_hive where  c3_decimal =2345 and c3_decimal <> 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_022
test("BVA_SPL_DATA_DECIMAL_022", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where  c3_decimal =2345 and c3_decimal = 4567""",
    s"""select c3_decimal from Test_Boundary_hive where  c3_decimal =2345 and c3_decimal = 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_023
test("BVA_SPL_DATA_DECIMAL_023", Include) {
  checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""",
    s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =2345 and c3_decimal = 4567 group by c3_decimal""")
}
       

//BVA_SPL_DATA_DECIMAL_024
test("BVA_SPL_DATA_DECIMAL_024", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where  c3_decimal =2345 or c3_decimal <> 4567""",
    s"""select c3_decimal from Test_Boundary_hive where  c3_decimal =2345 or c3_decimal <> 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_025
test("BVA_SPL_DATA_DECIMAL_025", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where  c3_decimal =2345 or c3_decimal = 4567""",
    s"""select c3_decimal from Test_Boundary_hive where  c3_decimal =2345 or c3_decimal = 4567""")
}
       

//BVA_SPL_DATA_DECIMAL_026
test("BVA_SPL_DATA_DECIMAL_026", Include) {
  checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =2345 or c3_decimal = 4567 group by c3_decimal""",
    s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =2345 or c3_decimal = 4567 group by c3_decimal""")
}
       

//BVA_SPL_DATA_DECIMAL_027
test("BVA_SPL_DATA_DECIMAL_027", Include) {
  sql(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal between -0.12345678900987654321123456789009876538 and 0.12345678900987654321123456789012345638) e """).collect
}
       

//BVA_SPL_DATA_DECIMAL_028
test("BVA_SPL_DATA_DECIMAL_028", Include) {
  checkAnswer(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal not between -0.12345678900987654321123456789009876538 and 0) e""",
    s"""select c3_decimal from (select c3_decimal from Test_Boundary_hive where c3_decimal not between -0.12345678900987654321123456789009876538 and 0) e""")
}
       

//BVA_SPL_DATA_DECIMAL_029
test("BVA_SPL_DATA_DECIMAL_029", Include) {
  sql(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal not between 0 and 0.12345678900987654321123456789012345638) e""").collect
}
       

//BVA_SPL_DATA_DECIMAL_030
test("BVA_SPL_DATA_DECIMAL_030", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal between -0.12345678900987654321123456789009876538 and 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_031
test("BVA_SPL_DATA_DECIMAL_031", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal  between -0.12345678900987654321123456789009876538 and 0 """,
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal  between -0.12345678900987654321123456789009876538 and 0 """)
}
       

//BVA_SPL_DATA_DECIMAL_032
test("BVA_SPL_DATA_DECIMAL_032", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal  between 0 and 0.12345678900987654321123456789012345638""").collect
}
       

//BVA_SPL_DATA_DECIMAL_033
test("BVA_SPL_DATA_DECIMAL_033", Include) {
  sql(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal between -0.12345678900987654321123456789009876538 and 0.12345678900987654321123456789012345638) e """).collect
}
       

//BVA_SPL_DATA_DECIMAL_034
test("BVA_SPL_DATA_DECIMAL_034", Include) {
  checkAnswer(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal  between -0.12345678900987654321123456789009876538 and 0) e""",
    s"""select c3_decimal from (select c3_decimal from Test_Boundary_hive where c3_decimal  between -0.12345678900987654321123456789009876538 and 0) e""")
}
       

//BVA_SPL_DATA_DECIMAL_035
test("BVA_SPL_DATA_DECIMAL_035", Include) {
  sql(s"""select c3_decimal from (select c3_decimal from Test_Boundary where c3_decimal  between 0 and 0.12345678900987654321123456789012345638) e""").collect
}
       

//BVA_SPL_DATA_DECIMAL_036
test("BVA_SPL_DATA_DECIMAL_036", Include) {
  checkAnswer(s"""select count(*) from Test_Boundary""",
    s"""select count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_DECIMAL_037
test("BVA_SPL_DATA_DECIMAL_037", Include) {
  checkAnswer(s"""select distinct count(*) from Test_Boundary""",
    s"""select distinct count(*) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_DECIMAL_038
test("BVA_SPL_DATA_DECIMAL_038", Include) {
  checkAnswer(s"""select distinct count(c3_decimal) from Test_Boundary""",
    s"""select distinct count(c3_decimal) from Test_Boundary_hive""")
}
       

//BVA_SPL_DATA_DECIMAL_039
test("BVA_SPL_DATA_DECIMAL_039", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal not between -0.12345678900987654321123456789009876538 and 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_040
test("BVA_SPL_DATA_DECIMAL_040", Include) {
  checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal not between -0.12345678900987654321123456789009876538 and 0 """,
    s"""select c3_decimal from Test_Boundary_hive where c3_decimal not between -0.12345678900987654321123456789009876538 and 0 """)
}
       

//BVA_SPL_DATA_DECIMAL_041
test("BVA_SPL_DATA_DECIMAL_041", Include) {
  sql(s"""select c3_decimal from Test_Boundary where c3_decimal not between 0 and 0.12345678900987654321123456789012345638""").collect
}
       

//BVA_SPL_DATA_DECIMAL_042
test("BVA_SPL_DATA_DECIMAL_042", Include) {
  sql(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_043
test("BVA_SPL_DATA_DECIMAL_043", Include) {
  sql(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """).collect
}
       

//BVA_SPL_DATA_DECIMAL_044
test("BVA_SPL_DATA_DECIMAL_044", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 and c3_decimal >3.147483647E9""",
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 and c3_decimal >3.147483647E9""")
}
       

//BVA_SPL_DATA_DECIMAL_045
test("BVA_SPL_DATA_DECIMAL_045", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 and c3_decimal >3.147483647E9""",
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 and c3_decimal >3.147483647E9""")
}
       

//BVA_SPL_DATA_DECIMAL_046
test("BVA_SPL_DATA_DECIMAL_046", Include) {
  checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal +0.1000= 0.12345678900987654321123456789012345638 """,
    s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal +0.1000= 0.12345678900987654321123456789012345638 """)
}
       

//BVA_SPL_DATA_DECIMAL_047
test("BVA_SPL_DATA_DECIMAL_047", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c3_decimal = Test_Boundary1.c3_decimal WHERE Test_Boundary.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c3_decimal ORDER BY Test_Boundary.c3_decimal ASC""",
    s"""SELECT Test_Boundary_hive.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c3_decimal = Test_Boundary1_hive.c3_decimal WHERE Test_Boundary_hive.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c3_decimal ORDER BY Test_Boundary_hive.c3_decimal ASC""")
}
       

//BVA_SPL_DATA_DECIMAL_048
test("BVA_SPL_DATA_DECIMAL_048", Include) {
  checkAnswer(s"""SELECT Test_Boundary.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c3_decimal = Test_Boundary1.c3_decimal WHERE Test_Boundary.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c3_decimal ORDER BY Test_Boundary.c3_decimal ASC""",
    s"""SELECT Test_Boundary_hive.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary1_hive) SUB_QRY ) Test_Boundary1_hive ON Test_Boundary_hive.c3_decimal = Test_Boundary1_hive.c3_decimal WHERE Test_Boundary_hive.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c3_decimal ORDER BY Test_Boundary_hive.c3_decimal ASC""")
}

  //BVA_SPL_DATA_DECIMAL_049
  test("BVA_SPL_DATA_DECIMAL_049", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c3_decimal = Test_Boundary1.c3_decimal WHERE Test_Boundary.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c3_decimal ORDER BY Test_Boundary.c3_decimal ASC""",
      s"""SELECT Test_Boundary_hive.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c3_decimal = Test_Boundary_hive1.c3_decimal WHERE Test_Boundary_hive.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c3_decimal ORDER BY Test_Boundary_hive.c3_decimal ASC""")
  }


  //BVA_SPL_DATA_DECIMAL_050
  test("BVA_SPL_DATA_DECIMAL_050", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c3_decimal = Test_Boundary1.c3_decimal WHERE Test_Boundary.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c3_decimal ORDER BY Test_Boundary.c3_decimal ASC""",
      s"""SELECT Test_Boundary_hive.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c3_decimal = Test_Boundary_hive1.c3_decimal WHERE Test_Boundary_hive.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c3_decimal ORDER BY Test_Boundary_hive.c3_decimal ASC""")
  }


  //BVA_SPL_DATA_DECIMAL_051
  test("BVA_SPL_DATA_DECIMAL_051", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c3_decimal = Test_Boundary1.c3_decimal WHERE Test_Boundary.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary.c3_decimal ORDER BY Test_Boundary.c3_decimal ASC""",
      s"""SELECT Test_Boundary_hive.c3_decimal AS c3_decimal FROM ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c3_decimal FROM (select c1_int,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c3_decimal = Test_Boundary_hive1.c3_decimal WHERE Test_Boundary_hive.c3_decimal <>12345678900987654321123456789012345678 GROUP BY Test_Boundary_hive.c3_decimal ORDER BY Test_Boundary_hive.c3_decimal ASC""")
  }


  //BVA_SPL_DATA_DECIMAL_052
  test("BVA_SPL_DATA_DECIMAL_052", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary group by c3_decimal having max(c3_decimal) >5000""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive group by c3_decimal having max(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_053
  test("BVA_SPL_DATA_DECIMAL_053", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary group by c3_decimal having max(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive group by c3_decimal having max(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_054
  test("BVA_SPL_DATA_DECIMAL_054", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary group by c3_decimal having max(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive group by c3_decimal having max(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_055
  test("BVA_SPL_DATA_DECIMAL_055", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary group by c3_decimal having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive group by c3_decimal having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_056
  test("BVA_SPL_DATA_DECIMAL_056", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary group by c3_decimal having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive group by c3_decimal having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_060
  test("BVA_SPL_DATA_DECIMAL_060", Include) {
    sql(s"""select c3_decimal,count(c3_decimal) from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 and c1_int = 4567 group by c3_decimal""").collect
  }


  //BVA_SPL_DATA_DECIMAL_062
  test("BVA_SPL_DATA_DECIMAL_062", Include) {
    checkAnswer(s"""select c3_decimal,count(c3_decimal) from Test_Boundary group by c3_decimal having count(c3_decimal) >5000""",
      s"""select c3_decimal,count(c3_decimal) from Test_Boundary_hive group by c3_decimal having count(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_063
  test("BVA_SPL_DATA_DECIMAL_063", Include) {
    checkAnswer(s"""select c3_decimal,count(c3_decimal) from Test_Boundary group by c3_decimal having count(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,count(c3_decimal) from Test_Boundary_hive group by c3_decimal having count(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_064
  test("BVA_SPL_DATA_DECIMAL_064", Include) {
    checkAnswer(s"""select c3_decimal,count(c3_decimal) from Test_Boundary group by c3_decimal having count(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,count(c3_decimal) from Test_Boundary_hive group by c3_decimal having count(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_065
  test("BVA_SPL_DATA_DECIMAL_065", Include) {
    checkAnswer(s"""select c3_decimal,count(c3_decimal) from Test_Boundary group by c3_decimal having count(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,count(c3_decimal) from Test_Boundary_hive group by c3_decimal having count(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_066
  test("BVA_SPL_DATA_DECIMAL_066", Include) {
    checkAnswer(s"""select c3_decimal,count(c3_decimal) from Test_Boundary group by c3_decimal having count(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,count(c3_decimal) from Test_Boundary_hive group by c3_decimal having count(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_071
  test("BVA_SPL_DATA_DECIMAL_071", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >5000""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_072
  test("BVA_SPL_DATA_DECIMAL_072", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_073
  test("BVA_SPL_DATA_DECIMAL_073", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_074
  test("BVA_SPL_DATA_DECIMAL_074", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_075
  test("BVA_SPL_DATA_DECIMAL_075", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_080
  test("BVA_SPL_DATA_DECIMAL_080", Include) {
    checkAnswer(s"""select c3_decimal,sum(c3_decimal) from Test_Boundary group by c3_decimal having sum(c3_decimal) >5000""",
      s"""select c3_decimal,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal having sum(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_081
  test("BVA_SPL_DATA_DECIMAL_081", Include) {
    checkAnswer(s"""select c3_decimal,sum(c3_decimal) from Test_Boundary group by c3_decimal having sum(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal having sum(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_082
  test("BVA_SPL_DATA_DECIMAL_082", Include) {
    checkAnswer(s"""select c3_decimal,sum(c3_decimal) from Test_Boundary group by c3_decimal having sum(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal having sum(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_083
  test("BVA_SPL_DATA_DECIMAL_083", Include) {
    checkAnswer(s"""select c3_decimal,sum(c3_decimal) from Test_Boundary group by c3_decimal having sum(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal having sum(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_084
  test("BVA_SPL_DATA_DECIMAL_084", Include) {
    checkAnswer(s"""select c3_decimal,sum(c3_decimal) from Test_Boundary group by c3_decimal having sum(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal having sum(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_089
  test("BVA_SPL_DATA_DECIMAL_089", Include) {
    checkAnswer(s"""select c3_decimal,avg(c3_decimal) from Test_Boundary group by c3_decimal having avg(c3_decimal) >5000""",
      s"""select c3_decimal,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal having avg(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_090
  test("BVA_SPL_DATA_DECIMAL_090", Include) {
    checkAnswer(s"""select c3_decimal,avg(c3_decimal) from Test_Boundary group by c3_decimal having avg(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal having avg(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_091
  test("BVA_SPL_DATA_DECIMAL_091", Include) {
    checkAnswer(s"""select c3_decimal,avg(c3_decimal) from Test_Boundary group by c3_decimal having avg(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal having avg(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_092
  test("BVA_SPL_DATA_DECIMAL_092", Include) {
    checkAnswer(s"""select c3_decimal,avg(c3_decimal) from Test_Boundary group by c3_decimal having avg(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal having avg(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_093
  test("BVA_SPL_DATA_DECIMAL_093", Include) {
    checkAnswer(s"""select c3_decimal,avg(c3_decimal) from Test_Boundary group by c3_decimal having avg(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal having avg(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_097
  test("BVA_SPL_DATA_DECIMAL_097", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) >5000""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_098
  test("BVA_SPL_DATA_DECIMAL_098", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_099
  test("BVA_SPL_DATA_DECIMAL_099", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_100
  test("BVA_SPL_DATA_DECIMAL_100", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_101
  test("BVA_SPL_DATA_DECIMAL_101", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_105
  test("BVA_SPL_DATA_DECIMAL_105", Include) {
    sql(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 and c1_int = 4567 group by c3_decimal,c7_datatype_desc""").collect
  }


  //BVA_SPL_DATA_DECIMAL_107
  test("BVA_SPL_DATA_DECIMAL_107", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having count(c3_decimal) >5000""",
      s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having count(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_108
  test("BVA_SPL_DATA_DECIMAL_108", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_109
  test("BVA_SPL_DATA_DECIMAL_109", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_110
  test("BVA_SPL_DATA_DECIMAL_110", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having count(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_111
  test("BVA_SPL_DATA_DECIMAL_111", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having count(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,count(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having count(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_116
  test("BVA_SPL_DATA_DECIMAL_116", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >5000""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_117
  test("BVA_SPL_DATA_DECIMAL_117", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_118
  test("BVA_SPL_DATA_DECIMAL_118", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_119
  test("BVA_SPL_DATA_DECIMAL_119", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_120
  test("BVA_SPL_DATA_DECIMAL_120", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_125
  test("BVA_SPL_DATA_DECIMAL_125", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >5000""",
      s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_126
  test("BVA_SPL_DATA_DECIMAL_126", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_127
  test("BVA_SPL_DATA_DECIMAL_127", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_128
  test("BVA_SPL_DATA_DECIMAL_128", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having sum(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_129
  test("BVA_SPL_DATA_DECIMAL_129", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having sum(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,sum(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having sum(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_134
  test("BVA_SPL_DATA_DECIMAL_134", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >5000""",
      s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >5000""")
  }


  //BVA_SPL_DATA_DECIMAL_135
  test("BVA_SPL_DATA_DECIMAL_135", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_136
  test("BVA_SPL_DATA_DECIMAL_136", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_137
  test("BVA_SPL_DATA_DECIMAL_137", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having avg(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_138
  test("BVA_SPL_DATA_DECIMAL_138", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having avg(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,avg(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having avg(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_142
  test("BVA_SPL_DATA_DECIMAL_142", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_143
  test("BVA_SPL_DATA_DECIMAL_143", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,max(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having max(c3_decimal) <-2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_147
  test("BVA_SPL_DATA_DECIMAL_147", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_148
  test("BVA_SPL_DATA_DECIMAL_148", Include) {
    checkAnswer(s"""select c3_decimal,min(c3_decimal) from Test_Boundary group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,min(c3_decimal) from Test_Boundary_hive group by c3_decimal having min(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_149
  test("BVA_SPL_DATA_DECIMAL_149", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_150
  test("BVA_SPL_DATA_DECIMAL_150", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_151
  test("BVA_SPL_DATA_DECIMAL_151", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal asc""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal asc""")
  }


  //BVA_SPL_DATA_DECIMAL_152
  test("BVA_SPL_DATA_DECIMAL_152", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal asc""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal asc""")
  }


  //BVA_SPL_DATA_DECIMAL_153
  test("BVA_SPL_DATA_DECIMAL_153", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal desc""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal desc""")
  }


  //BVA_SPL_DATA_DECIMAL_154
  test("BVA_SPL_DATA_DECIMAL_154", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal desc""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal desc""")
  }


  //BVA_SPL_DATA_DECIMAL_155
  test("BVA_SPL_DATA_DECIMAL_155", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483646E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_156
  test("BVA_SPL_DATA_DECIMAL_156", Include) {
    checkAnswer(s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""",
      s"""select c3_decimal,c7_datatype_desc,min(c3_decimal) from Test_Boundary_hive group by c3_decimal,c7_datatype_desc having min(c3_decimal) >2.147483648E9  order by c3_decimal limit 5""")
  }


  //BVA_SPL_DATA_DECIMAL_166
  test("BVA_SPL_DATA_DECIMAL_166", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal between -0.12345678900987654321123456789009876538 and 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_167
  test("BVA_SPL_DATA_DECIMAL_167", Include) {
    checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal not between -0.12345678900987654321123456789009876538 and 0 """,
      s"""select c3_decimal from Test_Boundary_hive where c3_decimal not between -0.12345678900987654321123456789009876538 and 0 """)
  }


  //BVA_SPL_DATA_DECIMAL_168
  test("BVA_SPL_DATA_DECIMAL_168", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal not between 0 and 0.12345678900987654321123456789012345638""").collect
  }


  //BVA_SPL_DATA_DECIMAL_169
  test("BVA_SPL_DATA_DECIMAL_169", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal is null""",
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal is null""")
  }


  //BVA_SPL_DATA_DECIMAL_170
  test("BVA_SPL_DATA_DECIMAL_170", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal is not null""",
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal is not null""")
  }


  //BVA_SPL_DATA_DECIMAL_171
  test("BVA_SPL_DATA_DECIMAL_171", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal not like 123 """,
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal not like 123 """)
  }


  //BVA_SPL_DATA_DECIMAL_172
  test("BVA_SPL_DATA_DECIMAL_172", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal like 123 """,
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal like 123 """)
  }


  //BVA_SPL_DATA_DECIMAL_173
  test("BVA_SPL_DATA_DECIMAL_173", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal rlike 123 """,
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal rlike 123 """)
  }


  //BVA_SPL_DATA_DECIMAL_174
  test("BVA_SPL_DATA_DECIMAL_174", Include) {
    checkAnswer(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal regexp 123 """,
      s"""select c3_decimal+0.100 from Test_Boundary_hive where c3_decimal regexp 123 """)
  }


  //BVA_SPL_DATA_DECIMAL_175
  test("BVA_SPL_DATA_DECIMAL_175", Include) {
    sql(s"""select c3_decimal+0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_176
  test("BVA_SPL_DATA_DECIMAL_176", Include) {
    sql(s"""select c3_decimal+0.00100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_177
  test("BVA_SPL_DATA_DECIMAL_177", Include) {
    checkAnswer(s"""select c3_decimal+23 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal+23 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_178
  test("BVA_SPL_DATA_DECIMAL_178", Include) {
    sql(s"""select c3_decimal+50 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_179
  test("BVA_SPL_DATA_DECIMAL_179", Include) {
    sql(s"""select c3_decimal+0.50 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_180
  test("BVA_SPL_DATA_DECIMAL_180", Include) {
    checkAnswer(s"""select c3_decimal+75 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal+75 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_181
  test("BVA_SPL_DATA_DECIMAL_181", Include) {
    sql(s"""select c3_decimal-0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_182
  test("BVA_SPL_DATA_DECIMAL_182", Include) {
    sql(s"""select c3_decimal-0.00100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_183
  test("BVA_SPL_DATA_DECIMAL_183", Include) {
    checkAnswer(s"""select c3_decimal-23 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal-23 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_184
  test("BVA_SPL_DATA_DECIMAL_184", Include) {
    sql(s"""select c3_decimal-50 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_185
  test("BVA_SPL_DATA_DECIMAL_185", Include) {
    sql(s"""select c3_decimal-0.50 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_186
  test("BVA_SPL_DATA_DECIMAL_186", Include) {
    checkAnswer(s"""select c3_decimal-75 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal-75 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_187
  test("BVA_SPL_DATA_DECIMAL_187", Include) {
    checkAnswer(s"""select c3_decimal*0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal*0.100 from Test_Boundary_hive where c3_decimal <> 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_188
  test("BVA_SPL_DATA_DECIMAL_188", Include) {
    sql(s"""select c3_decimal*0.00100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_189
  test("BVA_SPL_DATA_DECIMAL_189", Include) {
    checkAnswer(s"""select c3_decimal*23 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal*23 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_190
  test("BVA_SPL_DATA_DECIMAL_190", Include) {
    sql(s"""select c3_decimal*50 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_191
  test("BVA_SPL_DATA_DECIMAL_191", Include) {
    sql(s"""select c3_decimal*0.50 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_192
  test("BVA_SPL_DATA_DECIMAL_192", Include) {
    checkAnswer(s"""select c3_decimal*75 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal*75 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_193
  test("BVA_SPL_DATA_DECIMAL_193", Include) {
    sql(s"""select c3_decimal/0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_194
  test("BVA_SPL_DATA_DECIMAL_194", Include) {
    sql(s"""select c3_decimal/0.00100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_195
  test("BVA_SPL_DATA_DECIMAL_195", Include) {
    checkAnswer(s"""select c3_decimal/23 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal/23 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_196
  test("BVA_SPL_DATA_DECIMAL_196", Include) {
    sql(s"""select c3_decimal/50 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_197
  test("BVA_SPL_DATA_DECIMAL_197", Include) {
    sql(s"""select c3_decimal/0.50 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_198
  test("BVA_SPL_DATA_DECIMAL_198", Include) {
    checkAnswer(s"""select c3_decimal/75 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal/75 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_199
  test("BVA_SPL_DATA_DECIMAL_199", Include) {
    sql(s"""select c3_decimal%0.100 from Test_Boundary where c3_decimal <> 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_200
  test("BVA_SPL_DATA_DECIMAL_200", Include) {
    sql(s"""select c3_decimal%0.00100 from Test_Boundary where c3_decimal = 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_201
  test("BVA_SPL_DATA_DECIMAL_201", Include) {
    checkAnswer(s"""select c3_decimal%23 from Test_Boundary where c3_decimal < 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal%23 from Test_Boundary_hive where c3_decimal < 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_202
  test("BVA_SPL_DATA_DECIMAL_202", Include) {
    sql(s"""select c3_decimal%50 from Test_Boundary where c3_decimal <= 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_203
  test("BVA_SPL_DATA_DECIMAL_203", Include) {
    sql(s"""select c3_decimal%0.50 from Test_Boundary where c3_decimal > 0.12345678900987654321123456789012345638 """).collect
  }


  //BVA_SPL_DATA_DECIMAL_204
  test("BVA_SPL_DATA_DECIMAL_204", Include) {
    checkAnswer(s"""select c3_decimal%75 from Test_Boundary where c3_decimal >= 0.12345678900987654321123456789012345638 """,
      s"""select c3_decimal%75 from Test_Boundary_hive where c3_decimal >= 0.12345678900987654321123456789012345638 """)
  }


  //BVA_SPL_DATA_DECIMAL_205
  test("BVA_SPL_DATA_DECIMAL_205", Include) {
    checkAnswer(s"""select round(c3_decimal,1)  from Test_Boundary""",
      s"""select round(c3_decimal,1)  from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_206
  test("BVA_SPL_DATA_DECIMAL_206", Include) {
    checkAnswer(s"""select round(c3_decimal,1)  from Test_Boundary""",
      s"""select round(c3_decimal,1)  from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_209
  test("BVA_SPL_DATA_DECIMAL_209", Include) {
    checkAnswer(s"""select floor(c3_decimal)  from Test_Boundary """,
      s"""select floor(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_210
  test("BVA_SPL_DATA_DECIMAL_210", Include) {
    checkAnswer(s"""select ceil(c3_decimal)  from Test_Boundary""",
      s"""select ceil(c3_decimal)  from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_211
  test("BVA_SPL_DATA_DECIMAL_211", Include) {
    sql(s"""select rand(5)  from Test_Boundary """).collect
  }


  //BVA_SPL_DATA_DECIMAL_212
  test("BVA_SPL_DATA_DECIMAL_212", Include) {
    checkAnswer(s"""select exp(c3_decimal) from Test_Boundary""",
      s"""select exp(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_213
  test("BVA_SPL_DATA_DECIMAL_213", Include) {
    checkAnswer(s"""select ln(c3_decimal) from Test_Boundary""",
      s"""select ln(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_220
  test("BVA_SPL_DATA_DECIMAL_220", Include) {
    checkAnswer(s"""select pmod(c3_decimal,1) from Test_Boundary""",
      s"""select pmod(c3_decimal,1) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_221
  test("BVA_SPL_DATA_DECIMAL_221", Include) {
    checkAnswer(s"""select  sin(c3_decimal)  from Test_Boundary """,
      s"""select  sin(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_222
  test("BVA_SPL_DATA_DECIMAL_222", Include) {
    checkAnswer(s"""select  asin(c3_decimal)  from Test_Boundary """,
      s"""select  asin(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_223
  test("BVA_SPL_DATA_DECIMAL_223", Include) {
    checkAnswer(s"""select cos(c3_decimal)  from Test_Boundary """,
      s"""select cos(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_224
  test("BVA_SPL_DATA_DECIMAL_224", Include) {
    checkAnswer(s"""select acos(c3_decimal)  from Test_Boundary """,
      s"""select acos(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_225
  test("BVA_SPL_DATA_DECIMAL_225", Include) {
    checkAnswer(s"""select tan(c3_decimal)  from Test_Boundary """,
      s"""select tan(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_226
  test("BVA_SPL_DATA_DECIMAL_226", Include) {
    checkAnswer(s"""select atan(c3_decimal)  from Test_Boundary """,
      s"""select atan(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_227
  test("BVA_SPL_DATA_DECIMAL_227", Include) {
    checkAnswer(s"""select degrees(c3_decimal)  from Test_Boundary """,
      s"""select degrees(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_228
  test("BVA_SPL_DATA_DECIMAL_228", Include) {
    checkAnswer(s"""select radians(c3_decimal)  from Test_Boundary """,
      s"""select radians(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_229
  test("BVA_SPL_DATA_DECIMAL_229", Include) {
    checkAnswer(s"""select positive(c3_decimal)  from Test_Boundary """,
      s"""select positive(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_230
  test("BVA_SPL_DATA_DECIMAL_230", Include) {
    checkAnswer(s"""select negative(c3_decimal)  from Test_Boundary """,
      s"""select negative(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_231
  test("BVA_SPL_DATA_DECIMAL_231", Include) {
    checkAnswer(s"""select sign(c3_decimal)  from Test_Boundary """,
      s"""select sign(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_232
  test("BVA_SPL_DATA_DECIMAL_232", Include) {
    checkAnswer(s"""select exp(c3_decimal)  from Test_Boundary """,
      s"""select exp(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_234
  test("BVA_SPL_DATA_DECIMAL_234", Include) {
    checkAnswer(s"""select factorial(c3_decimal)  from Test_Boundary """,
      s"""select factorial(c3_decimal)  from Test_Boundary_hive """)
  }


  //BVA_SPL_DATA_DECIMAL_236
  test("BVA_SPL_DATA_DECIMAL_236", Include) {
    checkAnswer(s"""select shiftleft(c3_decimal,2) from Test_Boundary""",
      s"""select shiftleft(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_237
  test("BVA_SPL_DATA_DECIMAL_237", Include) {
    checkAnswer(s"""select shiftleft(c3_decimal,2) from Test_Boundary""",
      s"""select shiftleft(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_238
  test("BVA_SPL_DATA_DECIMAL_238", Include) {
    checkAnswer(s"""select shiftright(c3_decimal,2) from Test_Boundary""",
      s"""select shiftright(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_239
  test("BVA_SPL_DATA_DECIMAL_239", Include) {
    checkAnswer(s"""select shiftright(c3_decimal,2) from Test_Boundary""",
      s"""select shiftright(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_240
  test("BVA_SPL_DATA_DECIMAL_240", Include) {
    checkAnswer(s"""select shiftrightunsigned(c3_decimal,2) from Test_Boundary""",
      s"""select shiftrightunsigned(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_241
  test("BVA_SPL_DATA_DECIMAL_241", Include) {
    checkAnswer(s"""select shiftrightunsigned(c3_decimal,2) from Test_Boundary""",
      s"""select shiftrightunsigned(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_242
  test("BVA_SPL_DATA_DECIMAL_242", Include) {
    checkAnswer(s"""select greatest(1,2,3,4,5) from Test_Boundary""",
      s"""select greatest(1,2,3,4,5) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_243
  test("BVA_SPL_DATA_DECIMAL_243", Include) {
    checkAnswer(s"""select least(1,2,3,4,5) from Test_Boundary""",
      s"""select least(1,2,3,4,5) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_245
  test("BVA_SPL_DATA_DECIMAL_245", Include) {
    checkAnswer(s"""select if(c3_decimal<5000,'t','f') from Test_Boundary""",
      s"""select if(c3_decimal<5000,'t','f') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_246
  test("BVA_SPL_DATA_DECIMAL_246", Include) {
    checkAnswer(s"""select isnull(c3_decimal) from Test_Boundary""",
      s"""select isnull(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_247
  test("BVA_SPL_DATA_DECIMAL_247", Include) {
    checkAnswer(s"""select isnotnull(c3_decimal) from Test_Boundary""",
      s"""select isnotnull(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_248
  test("BVA_SPL_DATA_DECIMAL_248", Include) {
    checkAnswer(s"""select nvl(c3_decimal,10) from Test_Boundary""",
      s"""select nvl(c3_decimal,10) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_249
  test("BVA_SPL_DATA_DECIMAL_249", Include) {
    checkAnswer(s"""select nvl(c3_decimal,0) from Test_Boundary""",
      s"""select nvl(c3_decimal,0) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_250
  test("BVA_SPL_DATA_DECIMAL_250", Include) {
    checkAnswer(s"""select nvl(c3_decimal,null) from Test_Boundary""",
      s"""select nvl(c3_decimal,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_251
  test("BVA_SPL_DATA_DECIMAL_251", Include) {
    checkAnswer(s"""select coalesce(c3_decimal,null,null,null,756) from Test_Boundary""",
      s"""select coalesce(c3_decimal,null,null,null,756) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_252
  test("BVA_SPL_DATA_DECIMAL_252", Include) {
    checkAnswer(s"""select coalesce(c3_decimal,1,null,null,756) from Test_Boundary""",
      s"""select coalesce(c3_decimal,1,null,null,756) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_253
  test("BVA_SPL_DATA_DECIMAL_253", Include) {
    checkAnswer(s"""select coalesce(c3_decimal,345,null,756) from Test_Boundary""",
      s"""select coalesce(c3_decimal,345,null,756) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_254
  test("BVA_SPL_DATA_DECIMAL_254", Include) {
    checkAnswer(s"""select coalesce(c3_decimal,345,0.1,456,756) from Test_Boundary""",
      s"""select coalesce(c3_decimal,345,0.1,456,756) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_255
  test("BVA_SPL_DATA_DECIMAL_255", Include) {
    checkAnswer(s"""select coalesce(c3_decimal,756,null,null,null) from Test_Boundary""",
      s"""select coalesce(c3_decimal,756,null,null,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_256
  test("BVA_SPL_DATA_DECIMAL_256", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then true else false end from Test_boundary""",
      s"""select case c3_decimal when 2345 then true else false end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_257
  test("BVA_SPL_DATA_DECIMAL_257", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then true end from Test_boundary""",
      s"""select case c3_decimal when 2345 then true end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_258
  test("BVA_SPL_DATA_DECIMAL_258", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_259
  test("BVA_SPL_DATA_DECIMAL_259", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_260
  test("BVA_SPL_DATA_DECIMAL_260", Include) {
    checkAnswer(s"""select case when c3_decimal <2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case when c3_decimal <2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_261
  test("BVA_SPL_DATA_DECIMAL_261", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then true else false end from Test_boundary""",
      s"""select case c3_decimal when 2345 then true else false end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_262
  test("BVA_SPL_DATA_DECIMAL_262", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then true end from Test_boundary""",
      s"""select case c3_decimal when 2345 then true end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_263
  test("BVA_SPL_DATA_DECIMAL_263", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_264
  test("BVA_SPL_DATA_DECIMAL_264", Include) {
    checkAnswer(s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case c3_decimal when 2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_265
  test("BVA_SPL_DATA_DECIMAL_265", Include) {
    checkAnswer(s"""select case when c3_decimal <2345 then 1000 else c3_decimal end from Test_boundary""",
      s"""select case when c3_decimal <2345 then 1000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_266
  test("BVA_SPL_DATA_DECIMAL_266", Include) {
    checkAnswer(s"""select case when c3_decimal <2345 then 1000 when c3_decimal >2535353535 then 1000000000 else c3_decimal end from Test_boundary""",
      s"""select case when c3_decimal <2345 then 1000 when c3_decimal >2535353535 then 1000000000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_267
  test("BVA_SPL_DATA_DECIMAL_267", Include) {
    checkAnswer(s"""select case when c3_decimal <2345 then 1000 when c3_decimal is null then 1000000000 else c3_decimal end from Test_boundary""",
      s"""select case when c3_decimal <2345 then 1000 when c3_decimal is null then 1000000000 else c3_decimal end from Test_boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_268
  test("BVA_SPL_DATA_DECIMAL_268", Include) {
    checkAnswer(s"""select distinct count(*) from Test_Boundary""",
      s"""select distinct count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_269
  test("BVA_SPL_DATA_DECIMAL_269", Include) {
    checkAnswer(s"""select distinct count(c3_decimal) from Test_Boundary""",
      s"""select distinct count(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_270
  test("BVA_SPL_DATA_DECIMAL_270", Include) {
    checkAnswer(s"""select max(c3_decimal) from Test_Boundary""",
      s"""select max(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_271
  test("BVA_SPL_DATA_DECIMAL_271", Include) {
    checkAnswer(s"""select  count(distinct (c3_decimal)) from Test_Boundary""",
      s"""select  count(distinct (c3_decimal)) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_272
  test("BVA_SPL_DATA_DECIMAL_272", Include) {
    checkAnswer(s"""select distinct sum(c3_decimal) from Test_Boundary""",
      s"""select distinct sum(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_273
  test("BVA_SPL_DATA_DECIMAL_273", Include) {
    checkAnswer(s"""select  sum(distinct c3_decimal) from Test_Boundary""",
      s"""select  sum(distinct c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_274
  test("BVA_SPL_DATA_DECIMAL_274", Include) {
    checkAnswer(s"""select distinct avg(c3_decimal) from Test_Boundary""",
      s"""select distinct avg(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_275
  test("BVA_SPL_DATA_DECIMAL_275", Include) {
    checkAnswer(s"""select  avg( c3_decimal) from Test_Boundary""",
      s"""select  avg( c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_276
  test("BVA_SPL_DATA_DECIMAL_276", Include) {
    checkAnswer(s"""select min(c3_decimal) from Test_Boundary""",
      s"""select min(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_277
  test("BVA_SPL_DATA_DECIMAL_277", Include) {
    checkAnswer(s"""select distinct min(c3_decimal) from Test_Boundary""",
      s"""select distinct min(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_278
  test("BVA_SPL_DATA_DECIMAL_278", Include) {
    checkAnswer(s"""select max(c3_decimal) from Test_Boundary""",
      s"""select max(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_279
  test("BVA_SPL_DATA_DECIMAL_279", Include) {
    sql(s"""select variance(c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_280
  test("BVA_SPL_DATA_DECIMAL_280", Include) {
    sql(s"""select var_samp(c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_281
  test("BVA_SPL_DATA_DECIMAL_281", Include) {
    checkAnswer(s"""select stddev_pop(c3_decimal) from Test_Boundary""",
      s"""select stddev_pop(c3_decimal) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_282
  test("BVA_SPL_DATA_DECIMAL_282", Include) {
    sql(s"""select stddev_samp(c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_283
  test("BVA_SPL_DATA_DECIMAL_283", Include) {
    sql(s"""select covar_pop(c3_decimal,c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_284
  test("BVA_SPL_DATA_DECIMAL_284", Include) {
    sql(s"""select covar_samp(c3_decimal,c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_285
  test("BVA_SPL_DATA_DECIMAL_285", Include) {
    checkAnswer(s"""select corr(c3_decimal,1) from Test_Boundary""",
      s"""select corr(c3_decimal,1) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_287
  test("BVA_SPL_DATA_DECIMAL_287", Include) {
    checkAnswer(s"""select histogram_numeric(c3_decimal,2) from Test_Boundary""",
      s"""select histogram_numeric(c3_decimal,2) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_288
  test("BVA_SPL_DATA_DECIMAL_288", Include) {
    sql(s"""select collect_set(c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_289
  test("BVA_SPL_DATA_DECIMAL_289", Include) {
    sql(s"""select collect_list(c3_decimal) from Test_Boundary""").collect
  }


  //BVA_SPL_DATA_DECIMAL_290
  test("BVA_SPL_DATA_DECIMAL_290", Include) {
    checkAnswer(s"""select cast(c3_decimal as double) from Test_Boundary""",
      s"""select cast(c3_decimal as double) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_DECIMAL_291
  test("BVA_SPL_DATA_DECIMAL_291", Include) {
    checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or  c3_decimal <> 0E-38""",
      s"""select c3_decimal from Test_Boundary_hive where c3_decimal =0.12345678900987654321123456789009876538 or  c3_decimal <> 0E-38""")
  }


  //BVA_SPL_DATA_DECIMAL_292
  test("BVA_SPL_DATA_DECIMAL_292", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or  c3_decimal <> 0E-38 and c1_int = 4567""").collect
  }


  //BVA_SPL_DATA_DECIMAL_293
  test("BVA_SPL_DATA_DECIMAL_293", Include) {
    checkAnswer(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or  c3_decimal <> 0E-38 group by c3_decimal""",
      s"""select c3_decimal,max(c3_decimal) from Test_Boundary_hive where c3_decimal =0.12345678900987654321123456789009876538 or  c3_decimal <> 0E-38 group by c3_decimal""")
  }


  //BVA_SPL_DATA_DECIMAL_294
  test("BVA_SPL_DATA_DECIMAL_294", Include) {
    checkAnswer(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 or c1_int <> 4567""",
      s"""select c3_decimal from Test_Boundary_hive where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 or c1_int <> 4567""")
  }


  //BVA_SPL_DATA_DECIMAL_295
  test("BVA_SPL_DATA_DECIMAL_295", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 or c1_int = 4567""").collect
  }


  //BVA_SPL_DATA_DECIMAL_296
  test("BVA_SPL_DATA_DECIMAL_296", Include) {
    sql(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal = 0E-38 or c1_int = 4567 group by c3_decimal""").collect
  }


  //BVA_SPL_DATA_DECIMAL_297
  test("BVA_SPL_DATA_DECIMAL_297", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 and c1_int <> 4567""").collect
  }


  //BVA_SPL_DATA_DECIMAL_298
  test("BVA_SPL_DATA_DECIMAL_298", Include) {
    sql(s"""select c3_decimal from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 and c1_int = 4567""").collect
  }


  //BVA_SPL_DATA_DECIMAL_299
  test("BVA_SPL_DATA_DECIMAL_299", Include) {
    sql(s"""select c3_decimal,max(c3_decimal) from Test_Boundary where c3_decimal =0.12345678900987654321123456789009876538 or c3_decimal =0E-38 and c1_int = 4567 group by c3_decimal""").collect
  }


  //BVA_SPL_DATA_TIMESTAMP_001
  test("BVA_SPL_DATA_TIMESTAMP_001", Include) {
    sql(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp in ('2017-07-01 12:07:28','2018-04-07 14:02:28','2019-07-05 13:07:30')""").collect
  }


  //BVA_SPL_DATA_TIMESTAMP_003
  test("BVA_SPL_DATA_TIMESTAMP_003", Include) {
    sql(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not in ('2017-07-01 12:07:28','2018-04-07 14:02:28','2019-07-05 13:07:30')""").collect
  }


  //BVA_SPL_DATA_TIMESTAMP_004
  test("BVA_SPL_DATA_TIMESTAMP_004", Include) {
    sql(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp in ('2017-07-01 12:07:28','2018-04-07 14:02:28','2019-07-05 13:07:30')""").collect
  }


  //BVA_SPL_DATA_TIMESTAMP_005
  test("BVA_SPL_DATA_TIMESTAMP_005", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp < '2017-07-01 12:07:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp < '2017-07-01 12:07:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_006
  test("BVA_SPL_DATA_TIMESTAMP_006", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp > '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp > '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_007
  test("BVA_SPL_DATA_TIMESTAMP_007", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp >='1999-01-06 10:05:29' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp >='1999-01-06 10:05:29' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_008
  test("BVA_SPL_DATA_TIMESTAMP_008", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp <= '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp <= '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_009
  test("BVA_SPL_DATA_TIMESTAMP_009", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '1999-01-06 10:05:29'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_010
  test("BVA_SPL_DATA_TIMESTAMP_010", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp = '2018-04-07 14:02:28' and c6_Timestamp =  '2019-07-05 13:07:30' group by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp = '2018-04-07 14:02:28' and c6_Timestamp =  '2019-07-05 13:07:30' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_011
  test("BVA_SPL_DATA_TIMESTAMP_011", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp = '2019-07-05 13:07:30' or c6_Timestamp <>'1999-01-06 10:05:29' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp = '2019-07-05 13:07:30' or c6_Timestamp <>'1999-01-06 10:05:29' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_012
  test("BVA_SPL_DATA_TIMESTAMP_012", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp =' 2019-07-05 13:07:30' or c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp =' 2019-07-05 13:07:30' or c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_013
  test("BVA_SPL_DATA_TIMESTAMP_013", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_014
  test("BVA_SPL_DATA_TIMESTAMP_014", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_015
  test("BVA_SPL_DATA_TIMESTAMP_015", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_016
  test("BVA_SPL_DATA_TIMESTAMP_016", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp is null""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp is null""")
  }


  //BVA_SPL_DATA_TIMESTAMP_017
  test("BVA_SPL_DATA_TIMESTAMP_017", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp is not null""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp is not null""")
  }


  //BVA_SPL_DATA_TIMESTAMP_018
  test("BVA_SPL_DATA_TIMESTAMP_018", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where  c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where  c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp <> '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_019
  test("BVA_SPL_DATA_TIMESTAMP_019", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where  c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where  c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_020
  test("BVA_SPL_DATA_TIMESTAMP_020", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_021
  test("BVA_SPL_DATA_TIMESTAMP_021", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where  c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp <> '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where  c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp <> '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_022
  test("BVA_SPL_DATA_TIMESTAMP_022", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where  c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp = '2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where  c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp = '2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_023
  test("BVA_SPL_DATA_TIMESTAMP_023", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2018-04-07 14:02:28' or c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_024
  test("BVA_SPL_DATA_TIMESTAMP_024", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28') e """,
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28') e """)
  }


  //BVA_SPL_DATA_TIMESTAMP_025
  test("BVA_SPL_DATA_TIMESTAMP_025", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp not between '2017-07-01 12:07:28' and '0') e""",
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '2017-07-01 12:07:28' and '0') e""")
  }


  //BVA_SPL_DATA_TIMESTAMP_026
  test("BVA_SPL_DATA_TIMESTAMP_026", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp not between '0' and '2018-04-07 14:02:28') e""",
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '0' and '2018-04-07 14:02:28') e""")
  }


  //BVA_SPL_DATA_TIMESTAMP_027
  test("BVA_SPL_DATA_TIMESTAMP_027", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_028
  test("BVA_SPL_DATA_TIMESTAMP_028", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp  between '2017-07-01 12:07:28' and '0'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp  between '2017-07-01 12:07:28' and '0'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_029
  test("BVA_SPL_DATA_TIMESTAMP_029", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp  between '0' and '2018-04-07 14:02:28'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp  between '0' and '2018-04-07 14:02:28'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_030
  test("BVA_SPL_DATA_TIMESTAMP_030", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28') e """,
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp between '2017-07-01 12:07:28' and '2018-04-07 14:02:28') e """)
  }


  //BVA_SPL_DATA_TIMESTAMP_031
  test("BVA_SPL_DATA_TIMESTAMP_031", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp  between '2017-07-01 12:07:28' and '0') e""",
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp  between '2017-07-01 12:07:28' and '0') e""")
  }


  //BVA_SPL_DATA_TIMESTAMP_032
  test("BVA_SPL_DATA_TIMESTAMP_032", Include) {
    checkAnswer(s"""select * from (select c6_Timestamp from Test_Boundary where c6_Timestamp  between '0' and '2018-04-07 14:02:28') e""",
      s"""select * from (select c6_Timestamp from Test_Boundary_hive where c6_Timestamp  between '0' and '2018-04-07 14:02:28') e""")
  }


  //BVA_SPL_DATA_TIMESTAMP_033
  test("BVA_SPL_DATA_TIMESTAMP_033", Include) {
    checkAnswer(s"""select count(*) from Test_Boundary""",
      s"""select count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_034
  test("BVA_SPL_DATA_TIMESTAMP_034", Include) {
    checkAnswer(s"""select distinct count(*) from Test_Boundary""",
      s"""select distinct count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_035
  test("BVA_SPL_DATA_TIMESTAMP_035", Include) {
    checkAnswer(s"""select distinct count(c6_Timestamp) from Test_Boundary""",
      s"""select distinct count(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_036
  test("BVA_SPL_DATA_TIMESTAMP_036", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not between '2017-07-01 12:07:28' and '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '2017-07-01 12:07:28' and '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_037
  test("BVA_SPL_DATA_TIMESTAMP_037", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not between '2017-07-01 12:07:28' and '0' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '2017-07-01 12:07:28' and '0' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_038
  test("BVA_SPL_DATA_TIMESTAMP_038", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not between '0' and '2018-04-07 14:02:28'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '0' and '2018-04-07 14:02:28'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_039
  test("BVA_SPL_DATA_TIMESTAMP_039", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp = '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp = '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_040
  test("BVA_SPL_DATA_TIMESTAMP_040", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp <> '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp <> '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_041
  test("BVA_SPL_DATA_TIMESTAMP_041", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp < '2018-04-07 14:02:28' and c6_Timestamp >'2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp < '2018-04-07 14:02:28' and c6_Timestamp >'2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_042
  test("BVA_SPL_DATA_TIMESTAMP_042", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp < '2018-04-07 14:02:28' and c6_Timestamp >'2019-07-05 13:07:30'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp < '2018-04-07 14:02:28' and c6_Timestamp >'2019-07-05 13:07:30'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_043
  test("BVA_SPL_DATA_TIMESTAMP_043", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp != '2018-04-07 14:02:28' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp != '2018-04-07 14:02:28' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_044
  test("BVA_SPL_DATA_TIMESTAMP_044", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c6_Timestamp = Test_Boundary1.c6_Timestamp WHERE Test_Boundary.c6_Timestamp <'2018-04-07 14:02:28' GROUP BY Test_Boundary.c6_Timestamp ORDER BY Test_Boundary.c6_Timestamp ASC""",
      s"""SELECT Test_Boundary_hive.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c6_Timestamp = Test_Boundary_hive1.c6_Timestamp WHERE Test_Boundary_hive.c6_Timestamp <'2018-04-07 14:02:28' GROUP BY Test_Boundary_hive.c6_Timestamp ORDER BY Test_Boundary_hive.c6_Timestamp ASC""")
  }


  //BVA_SPL_DATA_TIMESTAMP_045
  test("BVA_SPL_DATA_TIMESTAMP_045", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c6_Timestamp = Test_Boundary1.c6_Timestamp WHERE Test_Boundary.c6_Timestamp >'2018-04-07 14:02:28' GROUP BY Test_Boundary.c6_Timestamp ORDER BY Test_Boundary.c6_Timestamp ASC""",
      s"""SELECT Test_Boundary_hive.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c6_Timestamp = Test_Boundary_hive1.c6_Timestamp WHERE Test_Boundary_hive.c6_Timestamp >'2018-04-07 14:02:28' GROUP BY Test_Boundary_hive.c6_Timestamp ORDER BY Test_Boundary_hive.c6_Timestamp ASC""")
  }


  //BVA_SPL_DATA_TIMESTAMP_046
  test("BVA_SPL_DATA_TIMESTAMP_046", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c6_Timestamp = Test_Boundary1.c6_Timestamp WHERE Test_Boundary.c6_Timestamp <= '2018-04-07 14:02:28' GROUP BY Test_Boundary.c6_Timestamp ORDER BY Test_Boundary.c6_Timestamp ASC""",
      s"""SELECT Test_Boundary_hive.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c6_Timestamp = Test_Boundary_hive1.c6_Timestamp WHERE Test_Boundary_hive.c6_Timestamp <= '2018-04-07 14:02:28' GROUP BY Test_Boundary_hive.c6_Timestamp ORDER BY Test_Boundary_hive.c6_Timestamp ASC""")
  }


  //BVA_SPL_DATA_TIMESTAMP_047
  test("BVA_SPL_DATA_TIMESTAMP_047", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c6_Timestamp = Test_Boundary1.c6_Timestamp WHERE Test_Boundary.c6_Timestamp <> '2018-04-07 14:02:28' GROUP BY Test_Boundary.c6_Timestamp ORDER BY Test_Boundary.c6_Timestamp ASC""",
      s"""SELECT Test_Boundary_hive.c6_Timestamp AS c6_Timestamp FROM ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c6_Timestamp FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c6_Timestamp = Test_Boundary_hive1.c6_Timestamp WHERE Test_Boundary_hive.c6_Timestamp <> '2018-04-07 14:02:28' GROUP BY Test_Boundary_hive.c6_Timestamp ORDER BY Test_Boundary_hive.c6_Timestamp ASC""")
  }


  //BVA_SPL_DATA_TIMESTAMP_048
  test("BVA_SPL_DATA_TIMESTAMP_048", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp having max(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having max(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_049
  test("BVA_SPL_DATA_TIMESTAMP_049", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp having max(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having max(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_050
  test("BVA_SPL_DATA_TIMESTAMP_050", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_051
  test("BVA_SPL_DATA_TIMESTAMP_051", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_052
  test("BVA_SPL_DATA_TIMESTAMP_052", Include) {
    checkAnswer(s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_056
  test("BVA_SPL_DATA_TIMESTAMP_056", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_058
  test("BVA_SPL_DATA_TIMESTAMP_058", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp having count(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having count(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_059
  test("BVA_SPL_DATA_TIMESTAMP_059", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp having count(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having count(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_060
  test("BVA_SPL_DATA_TIMESTAMP_060", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_061
  test("BVA_SPL_DATA_TIMESTAMP_061", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_062
  test("BVA_SPL_DATA_TIMESTAMP_062", Include) {
    checkAnswer(s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp having count(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having count(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_067
  test("BVA_SPL_DATA_TIMESTAMP_067", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_068
  test("BVA_SPL_DATA_TIMESTAMP_068", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_069
  test("BVA_SPL_DATA_TIMESTAMP_069", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_070
  test("BVA_SPL_DATA_TIMESTAMP_070", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_071
  test("BVA_SPL_DATA_TIMESTAMP_071", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_076
  test("BVA_SPL_DATA_TIMESTAMP_076", Include) {
    checkAnswer(s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp having sum(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having sum(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_077
  test("BVA_SPL_DATA_TIMESTAMP_077", Include) {
    checkAnswer(s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp having sum(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having sum(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_078
  test("BVA_SPL_DATA_TIMESTAMP_078", Include) {
    checkAnswer(s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_079
  test("BVA_SPL_DATA_TIMESTAMP_079", Include) {
    checkAnswer(s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_080
  test("BVA_SPL_DATA_TIMESTAMP_080", Include) {
    checkAnswer(s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp having sum(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having sum(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_085
  test("BVA_SPL_DATA_TIMESTAMP_085", Include) {
    checkAnswer(s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp having avg(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having avg(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_086
  test("BVA_SPL_DATA_TIMESTAMP_086", Include) {
    checkAnswer(s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp having avg(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having avg(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_087
  test("BVA_SPL_DATA_TIMESTAMP_087", Include) {
    checkAnswer(s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_088
  test("BVA_SPL_DATA_TIMESTAMP_088", Include) {
    checkAnswer(s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_089
  test("BVA_SPL_DATA_TIMESTAMP_089", Include) {
    checkAnswer(s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp having avg(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having avg(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_093
  test("BVA_SPL_DATA_TIMESTAMP_093", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_094
  test("BVA_SPL_DATA_TIMESTAMP_094", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_095
  test("BVA_SPL_DATA_TIMESTAMP_095", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_096
  test("BVA_SPL_DATA_TIMESTAMP_096", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_097
  test("BVA_SPL_DATA_TIMESTAMP_097", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_101
  test("BVA_SPL_DATA_TIMESTAMP_101", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp,c7_datatype_desc""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive where c6_Timestamp ='2017-07-01 12:07:28' or c6_Timestamp ='2018-04-07 14:02:28' and c6_Timestamp = '1999-01-06 10:05:29' group by c6_Timestamp,c7_datatype_desc""")
  }


  //BVA_SPL_DATA_TIMESTAMP_103
  test("BVA_SPL_DATA_TIMESTAMP_103", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_104
  test("BVA_SPL_DATA_TIMESTAMP_104", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_105
  test("BVA_SPL_DATA_TIMESTAMP_105", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_106
  test("BVA_SPL_DATA_TIMESTAMP_106", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_107
  test("BVA_SPL_DATA_TIMESTAMP_107", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,count(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having count(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_112
  test("BVA_SPL_DATA_TIMESTAMP_112", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_113
  test("BVA_SPL_DATA_TIMESTAMP_113", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_114
  test("BVA_SPL_DATA_TIMESTAMP_114", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_115
  test("BVA_SPL_DATA_TIMESTAMP_115", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_116
  test("BVA_SPL_DATA_TIMESTAMP_116", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_121
  test("BVA_SPL_DATA_TIMESTAMP_121", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_122
  test("BVA_SPL_DATA_TIMESTAMP_122", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_123
  test("BVA_SPL_DATA_TIMESTAMP_123", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_124
  test("BVA_SPL_DATA_TIMESTAMP_124", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_125
  test("BVA_SPL_DATA_TIMESTAMP_125", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,sum(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having sum(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_130
  test("BVA_SPL_DATA_TIMESTAMP_130", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'1999-01-06 10:05:29'""",
      s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'1999-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_131
  test("BVA_SPL_DATA_TIMESTAMP_131", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'1999-01-06 10:05:29'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_132
  test("BVA_SPL_DATA_TIMESTAMP_132", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_133
  test("BVA_SPL_DATA_TIMESTAMP_133", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_134
  test("BVA_SPL_DATA_TIMESTAMP_134", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,avg(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having avg(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_138
  test("BVA_SPL_DATA_TIMESTAMP_138", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) >'2019-07-05 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_139
  test("BVA_SPL_DATA_TIMESTAMP_139", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,max(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having max(c6_Timestamp) <'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_143
  test("BVA_SPL_DATA_TIMESTAMP_143", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_144
  test("BVA_SPL_DATA_TIMESTAMP_144", Include) {
    checkAnswer(s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_145
  test("BVA_SPL_DATA_TIMESTAMP_145", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_146
  test("BVA_SPL_DATA_TIMESTAMP_146", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp""")
  }


  //BVA_SPL_DATA_TIMESTAMP_147
  test("BVA_SPL_DATA_TIMESTAMP_147", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp asc""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp asc""")
  }


  //BVA_SPL_DATA_TIMESTAMP_148
  test("BVA_SPL_DATA_TIMESTAMP_148", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp asc""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp asc""")
  }


  //BVA_SPL_DATA_TIMESTAMP_149
  test("BVA_SPL_DATA_TIMESTAMP_149", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp desc""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp desc""")
  }


  //BVA_SPL_DATA_TIMESTAMP_150
  test("BVA_SPL_DATA_TIMESTAMP_150", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp desc""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp desc""")
  }


  //BVA_SPL_DATA_TIMESTAMP_151
  test("BVA_SPL_DATA_TIMESTAMP_151", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2017-07-01 12:07:28'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_152
  test("BVA_SPL_DATA_TIMESTAMP_152", Include) {
    checkAnswer(s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp limit 5""",
      s"""select c6_Timestamp,c7_datatype_desc,min(c6_Timestamp) from Test_Boundary_hive group by c6_Timestamp,c7_datatype_desc having min(c6_Timestamp) >'2018-04-07 13:07:30'  order by c6_Timestamp limit 5""")
  }


  //BVA_SPL_DATA_TIMESTAMP_162
  test("BVA_SPL_DATA_TIMESTAMP_162", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp between '1999-01-06 10:05:29' and '2036-01-06 10:05:29' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp between '1999-01-06 10:05:29' and '2036-01-06 10:05:29' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_163
  test("BVA_SPL_DATA_TIMESTAMP_163", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not between '1999-01-06 10:05:29' and '0'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '1999-01-06 10:05:29' and '0'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_164
  test("BVA_SPL_DATA_TIMESTAMP_164", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not between '0' and '2036-01-06 10:05:29'""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not between '0' and '2036-01-06 10:05:29'""")
  }


  //BVA_SPL_DATA_TIMESTAMP_165
  test("BVA_SPL_DATA_TIMESTAMP_165", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp is null""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp is null""")
  }


  //BVA_SPL_DATA_TIMESTAMP_166
  test("BVA_SPL_DATA_TIMESTAMP_166", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp is not null""",
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp is not null""")
  }


  //BVA_SPL_DATA_TIMESTAMP_167
  test("BVA_SPL_DATA_TIMESTAMP_167", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp not like '2018-04-07' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp not like '2018-04-07' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_168
  test("BVA_SPL_DATA_TIMESTAMP_168", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp like  '2017-07-01%' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp like  '2017-07-01%' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_169
  test("BVA_SPL_DATA_TIMESTAMP_169", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp rlike  '2017-07-01%' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp rlike  '2017-07-01%' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_170
  test("BVA_SPL_DATA_TIMESTAMP_170", Include) {
    checkAnswer(s"""select c6_Timestamp from Test_Boundary where c6_Timestamp regexp  '2017-07-01' """,
      s"""select c6_Timestamp from Test_Boundary_hive where c6_Timestamp regexp  '2017-07-01' """)
  }


  //BVA_SPL_DATA_TIMESTAMP_171
  test("BVA_SPL_DATA_TIMESTAMP_171", Include) {
    checkAnswer(s"""select if(c6_Timestamp<'2014-08-12 10:01:05','t','f') from Test_Boundary""",
      s"""select if(c6_Timestamp<'2014-08-12 10:01:05','t','f') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_172
  test("BVA_SPL_DATA_TIMESTAMP_172", Include) {
    checkAnswer(s"""select isnull(c6_Timestamp) from Test_Boundary""",
      s"""select isnull(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_173
  test("BVA_SPL_DATA_TIMESTAMP_173", Include) {
    checkAnswer(s"""select isnotnull(c6_Timestamp) from Test_Boundary""",
      s"""select isnotnull(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_174
  test("BVA_SPL_DATA_TIMESTAMP_174", Include) {
    checkAnswer(s"""select nvl(c6_Timestamp,'10') from Test_Boundary""",
      s"""select nvl(c6_Timestamp,'10') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_175
  test("BVA_SPL_DATA_TIMESTAMP_175", Include) {
    checkAnswer(s"""select nvl(c6_Timestamp,'0') from Test_Boundary""",
      s"""select nvl(c6_Timestamp,'0') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_176
  test("BVA_SPL_DATA_TIMESTAMP_176", Include) {
    checkAnswer(s"""select nvl(c6_Timestamp,null) from Test_Boundary""",
      s"""select nvl(c6_Timestamp,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_177
  test("BVA_SPL_DATA_TIMESTAMP_177", Include) {
    checkAnswer(s"""select coalesce(c6_Timestamp,null,null,null,'756') from Test_Boundary""",
      s"""select coalesce(c6_Timestamp,null,null,null,'756') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_178
  test("BVA_SPL_DATA_TIMESTAMP_178", Include) {
    checkAnswer(s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30',null,null,'2019-07-05 13:07:30') from Test_Boundary""",
      s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30',null,null,'2019-07-05 13:07:30') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_179
  test("BVA_SPL_DATA_TIMESTAMP_179", Include) {
    checkAnswer(s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30',null,'2019-07-05 13:07:30') from Test_Boundary""",
      s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30',null,'2019-07-05 13:07:30') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_180
  test("BVA_SPL_DATA_TIMESTAMP_180", Include) {
    checkAnswer(s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30','2019-07-05 13:07:30','2013-09-26 00:00:00') from Test_Boundary""",
      s"""select coalesce(c6_Timestamp,'2019-07-05 13:07:30','2019-07-05 13:07:30','2013-09-26 00:00:00') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_181
  test("BVA_SPL_DATA_TIMESTAMP_181", Include) {
    checkAnswer(s"""select coalesce(c6_Timestamp,'756',null,null,null) from Test_Boundary""",
      s"""select coalesce(c6_Timestamp,'756',null,null,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_182
  test("BVA_SPL_DATA_TIMESTAMP_182", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then true else false end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then true else false end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_183
  test("BVA_SPL_DATA_TIMESTAMP_183", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2000-04-07 14:02:28' then '2000-04-07' else '2000-04-07' end from Test_Boundary""",
      s"""select case c6_Timestamp when '2000-04-07 14:02:28' then '2000-04-07' else '2000-04-07' end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_184
  test("BVA_SPL_DATA_TIMESTAMP_184", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else 1001 end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else 1001 end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_185
  test("BVA_SPL_DATA_TIMESTAMP_185", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else 10252  end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else 10252  end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_186
  test("BVA_SPL_DATA_TIMESTAMP_186", Include) {
    checkAnswer(s"""select case when c6_Timestamp <'2018-04-07' then 1000 else '' end from Test_Boundary""",
      s"""select case when c6_Timestamp <'2018-04-07' then 1000 else '' end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_187
  test("BVA_SPL_DATA_TIMESTAMP_187", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then true else false end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then true else false end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_188
  test("BVA_SPL_DATA_TIMESTAMP_188", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else '2018-04-07' end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then 1000 else '2018-04-07' end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_189
  test("BVA_SPL_DATA_TIMESTAMP_189", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07 14:02:28' then  '2018-04-07' else c6_Timestamp end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07 14:02:28' then  '2018-04-07' else c6_Timestamp end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_190
  test("BVA_SPL_DATA_TIMESTAMP_190", Include) {
    checkAnswer(s"""select case c6_Timestamp when '2018-04-07' then '2013-04-07 14:02:28' else c6_Timestamp end from Test_Boundary""",
      s"""select case c6_Timestamp when '2018-04-07' then '2013-04-07 14:02:28' else c6_Timestamp end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_191
  test("BVA_SPL_DATA_TIMESTAMP_191", Include) {
    checkAnswer(s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary""",
      s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_192
  test("BVA_SPL_DATA_TIMESTAMP_192", Include) {
    checkAnswer(s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then 1000 when c6_Timestamp >'2017-07-01 12:07:28' then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary""",
      s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then 1000 when c6_Timestamp >'2017-07-01 12:07:28' then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_193
  test("BVA_SPL_DATA_TIMESTAMP_193", Include) {
    checkAnswer(s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then 1000  when c6_Timestamp is null then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary""",
      s"""select case when c6_Timestamp <'2018-04-07 14:02:28' then 1000  when c6_Timestamp is null then '2018-04-07 14:02:28' else c6_Timestamp end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_194
  test("BVA_SPL_DATA_TIMESTAMP_194", Include) {
    checkAnswer(s"""select distinct count(*) from Test_Boundary""",
      s"""select distinct count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_195
  test("BVA_SPL_DATA_TIMESTAMP_195", Include) {
    checkAnswer(s"""select distinct count(c6_Timestamp) from Test_Boundary""",
      s"""select distinct count(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_196
  test("BVA_SPL_DATA_TIMESTAMP_196", Include) {
    checkAnswer(s"""select max(c6_Timestamp) from Test_Boundary""",
      s"""select max(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_197
  test("BVA_SPL_DATA_TIMESTAMP_197", Include) {
    checkAnswer(s"""select  count(distinct c6_Timestamp ) from Test_Boundary""",
      s"""select  count(distinct c6_Timestamp ) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_198
  test("BVA_SPL_DATA_TIMESTAMP_198", Include) {
    checkAnswer(s"""select distinct sum(c6_Timestamp) from Test_Boundary""",
      s"""select distinct sum(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_199
  test("BVA_SPL_DATA_TIMESTAMP_199", Include) {
    checkAnswer(s"""select  distinct c6_Timestamp from Test_Boundary""",
      s"""select  distinct c6_Timestamp from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_200
  test("BVA_SPL_DATA_TIMESTAMP_200", Include) {
    checkAnswer(s"""select distinct avg(c6_Timestamp) from Test_Boundary""",
      s"""select distinct avg(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_201
  test("BVA_SPL_DATA_TIMESTAMP_201", Include) {
    checkAnswer(s"""select  avg(c6_Timestamp) from Test_Boundary""",
      s"""select  avg(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_202
  test("BVA_SPL_DATA_TIMESTAMP_202", Include) {
    checkAnswer(s"""select min(c6_Timestamp) from Test_Boundary""",
      s"""select min(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_203
  test("BVA_SPL_DATA_TIMESTAMP_203", Include) {
    checkAnswer(s"""select distinct min(c6_Timestamp) from Test_Boundary""",
      s"""select distinct min(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_204
  test("BVA_SPL_DATA_TIMESTAMP_204", Include) {
    checkAnswer(s"""select max(c6_Timestamp) from Test_Boundary""",
      s"""select max(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_213
  test("BVA_SPL_DATA_TIMESTAMP_213", Include) {
    checkAnswer(s"""select collect_set(c6_Timestamp) from Test_Boundary""",
      s"""select collect_set(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_TIMESTAMP_214
  test("BVA_SPL_DATA_TIMESTAMP_214", Include) {
    checkAnswer(s"""select collect_list(c6_Timestamp) from Test_Boundary""",
      s"""select collect_list(c6_Timestamp) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_001
  test("BVA_SPL_DATA_STRING_001", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string in ('b','c','d')""",
      s"""select c5_string from Test_Boundary_hive where c5_string in ('b','c','d')""")
  }


  //BVA_SPL_DATA_STRING_002
  test("BVA_SPL_DATA_STRING_002", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string in ('b','c','d')""",
      s"""select c5_string from Test_Boundary_hive where c5_string in ('b','c','d')""")
  }


  //BVA_SPL_DATA_STRING_003
  test("BVA_SPL_DATA_STRING_003", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string in ('b','c','d')""",
      s"""select c5_string from Test_Boundary_hive where c5_string in ('b','c','d')""")
  }


  //BVA_SPL_DATA_STRING_004
  test("BVA_SPL_DATA_STRING_004", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string not in ('b','c','d')""",
      s"""select c5_string from Test_Boundary_hive where c5_string not in ('b','c','d')""")
  }


  //BVA_SPL_DATA_STRING_005
  test("BVA_SPL_DATA_STRING_005", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string in ('b','c','d')""",
      s"""select c5_string from Test_Boundary_hive where c5_string in ('b','c','d')""")
  }


  //BVA_SPL_DATA_STRING_006
  test("BVA_SPL_DATA_STRING_006", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='b' or c5_string ='c' and c5_string <> 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='b' or c5_string ='c' and c5_string <> 'd'""")
  }


  //BVA_SPL_DATA_STRING_007
  test("BVA_SPL_DATA_STRING_007", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='b' or c5_string ='c' and c5_string = 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='b' or c5_string ='c' and c5_string = 'd'""")
  }


  //BVA_SPL_DATA_STRING_008
  test("BVA_SPL_DATA_STRING_008", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary where c5_string ='a' or c5_string ='b' and c5_string ='d' group by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive where c5_string ='a' or c5_string ='b' and c5_string ='d' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_009
  test("BVA_SPL_DATA_STRING_009", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='a' or c5_string ='c' or c5_string <> 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='a' or c5_string ='c' or c5_string <> 'd'""")
  }


  //BVA_SPL_DATA_STRING_010
  test("BVA_SPL_DATA_STRING_010", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='a' or c5_string ='c' or c5_string = 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='a' or c5_string ='c' or c5_string = 'd'""")
  }


  //BVA_SPL_DATA_STRING_011
  test("BVA_SPL_DATA_STRING_011", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary where c5_string ='a' or c5_string ='c' or c5_string = 'd' group by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive where c5_string ='a' or c5_string ='c' or c5_string = 'd' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_012
  test("BVA_SPL_DATA_STRING_012", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='a' or c5_string ='c' and c5_string <> 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='a' or c5_string ='c' and c5_string <> 'd'""")
  }


  //BVA_SPL_DATA_STRING_013
  test("BVA_SPL_DATA_STRING_013", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string ='a' or c5_string ='c' and c5_string = 'd'""",
      s"""select c5_string from Test_Boundary_hive where c5_string ='a' or c5_string ='c' and c5_string = 'd'""")
  }


  //BVA_SPL_DATA_STRING_014
  test("BVA_SPL_DATA_STRING_014", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_015
  test("BVA_SPL_DATA_STRING_015", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string is null""",
      s"""select c5_string from Test_Boundary_hive where c5_string is null""")
  }


  //BVA_SPL_DATA_STRING_016
  test("BVA_SPL_DATA_STRING_016", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string is not null""",
      s"""select c5_string from Test_Boundary_hive where c5_string is not null""")
  }


  //BVA_SPL_DATA_STRING_017
  test("BVA_SPL_DATA_STRING_017", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where  c5_string ='c' and c5_string <> 'd'""",
      s"""select c5_string from Test_Boundary_hive where  c5_string ='c' and c5_string <> 'd'""")
  }


  //BVA_SPL_DATA_STRING_018
  test("BVA_SPL_DATA_STRING_018", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where  c5_string ='c' and c5_string = 'd'""",
      s"""select c5_string from Test_Boundary_hive where  c5_string ='c' and c5_string = 'd'""")
  }


  //BVA_SPL_DATA_STRING_019
  test("BVA_SPL_DATA_STRING_019", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary where c5_string ='c' and c5_string = 'd' group by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive where c5_string ='c' and c5_string = 'd' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_020
  test("BVA_SPL_DATA_STRING_020", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where  c5_string ='c' or c5_string <> 'd'""",
      s"""select c5_string from Test_Boundary_hive where  c5_string ='c' or c5_string <> 'd'""")
  }


  //BVA_SPL_DATA_STRING_021
  test("BVA_SPL_DATA_STRING_021", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where  c5_string ='c' or c5_string = 'd'""",
      s"""select c5_string from Test_Boundary_hive where  c5_string ='c' or c5_string = 'd'""")
  }


  //BVA_SPL_DATA_STRING_022
  test("BVA_SPL_DATA_STRING_022", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary where c5_string ='c' or c5_string = 'd' group by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive where c5_string ='c' or c5_string = 'd' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_023
  test("BVA_SPL_DATA_STRING_023", Include) {
    checkAnswer(s"""select count(*) from Test_Boundary""",
      s"""select count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_024
  test("BVA_SPL_DATA_STRING_024", Include) {
    checkAnswer(s"""select distinct count(*) from Test_Boundary""",
      s"""select distinct count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_025
  test("BVA_SPL_DATA_STRING_025", Include) {
    checkAnswer(s"""select distinct count(c5_string) from Test_Boundary""",
      s"""select distinct count(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_026
  test("BVA_SPL_DATA_STRING_026", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string = 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string = 'b' """)
  }


  //BVA_SPL_DATA_STRING_027
  test("BVA_SPL_DATA_STRING_027", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string <> 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string <> 'b' """)
  }


  //BVA_SPL_DATA_STRING_028
  test("BVA_SPL_DATA_STRING_028", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c5_string = Test_Boundary1.c5_string WHERE Test_Boundary.c5_string < 'b' GROUP BY Test_Boundary.c5_string ORDER BY Test_Boundary.c5_string ASC""",
      s"""SELECT Test_Boundary_hive.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c5_string = Test_Boundary_hive1.c5_string WHERE Test_Boundary_hive.c5_string < 'b' GROUP BY Test_Boundary_hive.c5_string ORDER BY Test_Boundary_hive.c5_string ASC""")
  }


  //BVA_SPL_DATA_STRING_029
  test("BVA_SPL_DATA_STRING_029", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c5_string = Test_Boundary1.c5_string WHERE Test_Boundary.c5_string >'b' GROUP BY Test_Boundary.c5_string ORDER BY Test_Boundary.c5_string ASC""",
      s"""SELECT Test_Boundary_hive.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c5_string = Test_Boundary_hive1.c5_string WHERE Test_Boundary_hive.c5_string >'b' GROUP BY Test_Boundary_hive.c5_string ORDER BY Test_Boundary_hive.c5_string ASC""")
  }


  //BVA_SPL_DATA_STRING_030
  test("BVA_SPL_DATA_STRING_030", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c5_string = Test_Boundary1.c5_string WHERE Test_Boundary.c5_string <>'b' GROUP BY Test_Boundary.c5_string ORDER BY Test_Boundary.c5_string ASC""",
      s"""SELECT Test_Boundary_hive.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c5_string = Test_Boundary_hive1.c5_string WHERE Test_Boundary_hive.c5_string <>'b' GROUP BY Test_Boundary_hive.c5_string ORDER BY Test_Boundary_hive.c5_string ASC""")
  }


  //BVA_SPL_DATA_STRING_031
  test("BVA_SPL_DATA_STRING_031", Include) {
    checkAnswer(s"""SELECT Test_Boundary.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary full JOIN ( SELECT c5_string FROM (select * from Test_Boundary) SUB_QRY ) Test_Boundary1 ON Test_Boundary.c5_string = Test_Boundary1.c5_string WHERE Test_Boundary.c5_string != 'b' GROUP BY Test_Boundary.c5_string ORDER BY Test_Boundary.c5_string ASC""",
      s"""SELECT Test_Boundary_hive.c5_string AS c5_string FROM ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive full JOIN ( SELECT c5_string FROM (select * from Test_Boundary_hive) SUB_QRY ) Test_Boundary_hive1 ON Test_Boundary_hive.c5_string = Test_Boundary_hive1.c5_string WHERE Test_Boundary_hive.c5_string != 'b' GROUP BY Test_Boundary_hive.c5_string ORDER BY Test_Boundary_hive.c5_string ASC""")
  }


  //BVA_SPL_DATA_STRING_032
  test("BVA_SPL_DATA_STRING_032", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary group by c5_string having max(c5_string) >'d'""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive group by c5_string having max(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_033
  test("BVA_SPL_DATA_STRING_033", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary group by c5_string having max(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive group by c5_string having max(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_034
  test("BVA_SPL_DATA_STRING_034", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary group by c5_string having max(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive group by c5_string having max(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_035
  test("BVA_SPL_DATA_STRING_035", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary group by c5_string having max(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive group by c5_string having max(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_036
  test("BVA_SPL_DATA_STRING_036", Include) {
    checkAnswer(s"""select c5_string,max(c5_string) from Test_Boundary group by c5_string having max(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,max(c5_string) from Test_Boundary_hive group by c5_string having max(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_040
  test("BVA_SPL_DATA_STRING_040", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string""")
  }


  //BVA_SPL_DATA_STRING_042
  test("BVA_SPL_DATA_STRING_042", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary group by c5_string having count(c5_string) >'d'""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive group by c5_string having count(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_043
  test("BVA_SPL_DATA_STRING_043", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary group by c5_string having count(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive group by c5_string having count(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_044
  test("BVA_SPL_DATA_STRING_044", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary group by c5_string having count(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive group by c5_string having count(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_045
  test("BVA_SPL_DATA_STRING_045", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary group by c5_string having count(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive group by c5_string having count(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_046
  test("BVA_SPL_DATA_STRING_046", Include) {
    checkAnswer(s"""select c5_string,count(c5_string) from Test_Boundary group by c5_string having count(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,count(c5_string) from Test_Boundary_hive group by c5_string having count(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_051
  test("BVA_SPL_DATA_STRING_051", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'d'""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_052
  test("BVA_SPL_DATA_STRING_052", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_053
  test("BVA_SPL_DATA_STRING_053", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_054
  test("BVA_SPL_DATA_STRING_054", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_055
  test("BVA_SPL_DATA_STRING_055", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_060
  test("BVA_SPL_DATA_STRING_060", Include) {
    checkAnswer(s"""select c5_string,sum(c5_string) from Test_Boundary group by c5_string having sum(c5_string) >'d'""",
      s"""select c5_string,sum(c5_string) from Test_Boundary_hive group by c5_string having sum(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_061
  test("BVA_SPL_DATA_STRING_061", Include) {
    checkAnswer(s"""select c5_string,sum(c5_string) from Test_Boundary group by c5_string having sum(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,sum(c5_string) from Test_Boundary_hive group by c5_string having sum(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_062
  test("BVA_SPL_DATA_STRING_062", Include) {
    checkAnswer(s"""select c5_string,sum(c5_string) from Test_Boundary group by c5_string having sum(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,sum(c5_string) from Test_Boundary_hive group by c5_string having sum(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_063
  test("BVA_SPL_DATA_STRING_063", Include) {
    checkAnswer(s"""select c5_string,sum(c5_string) from Test_Boundary group by c5_string having sum(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,sum(c5_string) from Test_Boundary_hive group by c5_string having sum(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_064
  test("BVA_SPL_DATA_STRING_064", Include) {
    checkAnswer(s"""select c5_string,sum(c5_string) from Test_Boundary group by c5_string having sum(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,sum(c5_string) from Test_Boundary_hive group by c5_string having sum(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_069
  test("BVA_SPL_DATA_STRING_069", Include) {
    checkAnswer(s"""select c5_string,avg(c5_string) from Test_Boundary group by c5_string having avg(c5_string) >'d'""",
      s"""select c5_string,avg(c5_string) from Test_Boundary_hive group by c5_string having avg(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_070
  test("BVA_SPL_DATA_STRING_070", Include) {
    checkAnswer(s"""select c5_string,avg(c5_string) from Test_Boundary group by c5_string having avg(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,avg(c5_string) from Test_Boundary_hive group by c5_string having avg(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_071
  test("BVA_SPL_DATA_STRING_071", Include) {
    checkAnswer(s"""select c5_string,avg(c5_string) from Test_Boundary group by c5_string having avg(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,avg(c5_string) from Test_Boundary_hive group by c5_string having avg(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_072
  test("BVA_SPL_DATA_STRING_072", Include) {
    checkAnswer(s"""select c5_string,avg(c5_string) from Test_Boundary group by c5_string having avg(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,avg(c5_string) from Test_Boundary_hive group by c5_string having avg(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_073
  test("BVA_SPL_DATA_STRING_073", Include) {
    checkAnswer(s"""select c5_string,avg(c5_string) from Test_Boundary group by c5_string having avg(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,avg(c5_string) from Test_Boundary_hive group by c5_string having avg(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_077
  test("BVA_SPL_DATA_STRING_077", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) >'d'""",
      s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having max(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_078
  test("BVA_SPL_DATA_STRING_078", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_079
  test("BVA_SPL_DATA_STRING_079", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_080
  test("BVA_SPL_DATA_STRING_080", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_081
  test("BVA_SPL_DATA_STRING_081", Include) {
    sql(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) <'c'  order by c5_string limit 5""").collect
  }


  //BVA_SPL_DATA_STRING_085
  test("BVA_SPL_DATA_STRING_085", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string,c7_datatype_desc""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive where c5_string ='a' or c5_string ='c' and c5_string = 'd' group by c5_string,c7_datatype_desc""")
  }


  //BVA_SPL_DATA_STRING_087
  test("BVA_SPL_DATA_STRING_087", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having count(c5_string) >'d'""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having count(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_088
  test("BVA_SPL_DATA_STRING_088", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_089
  test("BVA_SPL_DATA_STRING_089", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_090
  test("BVA_SPL_DATA_STRING_090", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having count(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_091
  test("BVA_SPL_DATA_STRING_091", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having count(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,count(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having count(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_096
  test("BVA_SPL_DATA_STRING_096", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'d'""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_097
  test("BVA_SPL_DATA_STRING_097", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_098
  test("BVA_SPL_DATA_STRING_098", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_099
  test("BVA_SPL_DATA_STRING_099", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_100
  test("BVA_SPL_DATA_STRING_100", Include) {
    sql(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) <'c'  order by c5_string limit 5""").collect
  }


  //BVA_SPL_DATA_STRING_105
  test("BVA_SPL_DATA_STRING_105", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having sum(c5_string) >'d'""",
      s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having sum(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_106
  test("BVA_SPL_DATA_STRING_106", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_107
  test("BVA_SPL_DATA_STRING_107", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_108
  test("BVA_SPL_DATA_STRING_108", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having sum(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_109
  test("BVA_SPL_DATA_STRING_109", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having sum(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,sum(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having sum(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_114
  test("BVA_SPL_DATA_STRING_114", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having avg(c5_string) >'d'""",
      s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having avg(c5_string) >'d'""")
  }


  //BVA_SPL_DATA_STRING_115
  test("BVA_SPL_DATA_STRING_115", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_116
  test("BVA_SPL_DATA_STRING_116", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_117
  test("BVA_SPL_DATA_STRING_117", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having avg(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_118
  test("BVA_SPL_DATA_STRING_118", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having avg(c5_string) <'c'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,avg(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having avg(c5_string) <'c'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_122
  test("BVA_SPL_DATA_STRING_122", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having max(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_123
  test("BVA_SPL_DATA_STRING_123", Include) {
    sql(s"""select c5_string,c7_datatype_desc,max(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having max(c5_string) <'c'  order by c5_string limit 5""").collect
  }


  //BVA_SPL_DATA_STRING_127
  test("BVA_SPL_DATA_STRING_127", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_128
  test("BVA_SPL_DATA_STRING_128", Include) {
    checkAnswer(s"""select c5_string,min(c5_string) from Test_Boundary group by c5_string having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,min(c5_string) from Test_Boundary_hive group by c5_string having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_129
  test("BVA_SPL_DATA_STRING_129", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_130
  test("BVA_SPL_DATA_STRING_130", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string""")
  }


  //BVA_SPL_DATA_STRING_131
  test("BVA_SPL_DATA_STRING_131", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string asc""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string asc""")
  }


  //BVA_SPL_DATA_STRING_132
  test("BVA_SPL_DATA_STRING_132", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string asc""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string asc""")
  }


  //BVA_SPL_DATA_STRING_133
  test("BVA_SPL_DATA_STRING_133", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string desc""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string desc""")
  }


  //BVA_SPL_DATA_STRING_134
  test("BVA_SPL_DATA_STRING_134", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string desc""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string desc""")
  }


  //BVA_SPL_DATA_STRING_135
  test("BVA_SPL_DATA_STRING_135", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_136
  test("BVA_SPL_DATA_STRING_136", Include) {
    checkAnswer(s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""",
      s"""select c5_string,c7_datatype_desc,min(c5_string) from Test_Boundary_hive group by c5_string,c7_datatype_desc having min(c5_string) >'b'  order by c5_string limit 5""")
  }


  //BVA_SPL_DATA_STRING_146
  test("BVA_SPL_DATA_STRING_146", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string is null""",
      s"""select c5_string from Test_Boundary_hive where c5_string is null""")
  }


  //BVA_SPL_DATA_STRING_147
  test("BVA_SPL_DATA_STRING_147", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string is not null""",
      s"""select c5_string from Test_Boundary_hive where c5_string is not null""")
  }


  //BVA_SPL_DATA_STRING_148
  test("BVA_SPL_DATA_STRING_148", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string not like 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string not like 'b' """)
  }


  //BVA_SPL_DATA_STRING_149
  test("BVA_SPL_DATA_STRING_149", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string like 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string like 'b' """)
  }


  //BVA_SPL_DATA_STRING_150
  test("BVA_SPL_DATA_STRING_150", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string rlike 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string rlike 'b' """)
  }


  //BVA_SPL_DATA_STRING_151
  test("BVA_SPL_DATA_STRING_151", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where c5_string regexp 'b' """,
      s"""select c5_string from Test_Boundary_hive where c5_string regexp 'b' """)
  }


  //BVA_SPL_DATA_STRING_152
  test("BVA_SPL_DATA_STRING_152", Include) {
    checkAnswer(s"""select if(c5_string<'d','t','f') from Test_Boundary""",
      s"""select if(c5_string<'d','t','f') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_153
  test("BVA_SPL_DATA_STRING_153", Include) {
    checkAnswer(s"""select isnull(c5_string) from Test_Boundary""",
      s"""select isnull(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_154
  test("BVA_SPL_DATA_STRING_154", Include) {
    checkAnswer(s"""select isnotnull(c5_string) from Test_Boundary""",
      s"""select isnotnull(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_155
  test("BVA_SPL_DATA_STRING_155", Include) {
    checkAnswer(s"""select nvl(c5_string,10) from Test_Boundary""",
      s"""select nvl(c5_string,10) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_156
  test("BVA_SPL_DATA_STRING_156", Include) {
    checkAnswer(s"""select nvl(c5_string,0) from Test_Boundary""",
      s"""select nvl(c5_string,0) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_157
  test("BVA_SPL_DATA_STRING_157", Include) {
    checkAnswer(s"""select nvl(c5_string,null) from Test_Boundary""",
      s"""select nvl(c5_string,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_158
  test("BVA_SPL_DATA_STRING_158", Include) {
    checkAnswer(s"""select coalesce(c5_string,null,null,null,'d') from Test_Boundary""",
      s"""select coalesce(c5_string,null,null,null,'d') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_159
  test("BVA_SPL_DATA_STRING_159", Include) {
    checkAnswer(s"""select coalesce(c5_string,1,null,null,'d') from Test_Boundary""",
      s"""select coalesce(c5_string,1,null,null,'d') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_160
  test("BVA_SPL_DATA_STRING_160", Include) {
    checkAnswer(s"""select coalesce(c5_string,'a',null,'d') from Test_Boundary""",
      s"""select coalesce(c5_string,'a',null,'d') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_161
  test("BVA_SPL_DATA_STRING_161", Include) {
    checkAnswer(s"""select coalesce(c5_string,'a',0.1,'b','d') from Test_Boundary""",
      s"""select coalesce(c5_string,'a',0.1,'b','d') from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_162
  test("BVA_SPL_DATA_STRING_162", Include) {
    checkAnswer(s"""select coalesce(c5_string,'d',null,null,null) from Test_Boundary""",
      s"""select coalesce(c5_string,'d',null,null,null) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_163
  test("BVA_SPL_DATA_STRING_163", Include) {
    checkAnswer(s"""select case c5_string when 'c' then true else false end from Test_Boundary""",
      s"""select case c5_string when 'c' then true else false end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_164
  test("BVA_SPL_DATA_STRING_164", Include) {
    checkAnswer(s"""select case c5_string when 'c' then true else true end from Test_Boundary""",
      s"""select case c5_string when 'c' then true else true end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_165
  test("BVA_SPL_DATA_STRING_165", Include) {
    checkAnswer(s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_166
  test("BVA_SPL_DATA_STRING_166", Include) {
    checkAnswer(s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_167
  test("BVA_SPL_DATA_STRING_167", Include) {
    checkAnswer(s"""select case when c5_string <'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case when c5_string <'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_168
  test("BVA_SPL_DATA_STRING_168", Include) {
    checkAnswer(s"""select case c5_string when 'c' then true else false end from Test_Boundary""",
      s"""select case c5_string when 'c' then true else false end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_169
  test("BVA_SPL_DATA_STRING_169", Include) {
    checkAnswer(s"""select case c5_string when 'lenovo' then true else false end from Test_Boundary""",
      s"""select case c5_string when 'lenovo' then true else false end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_170
  test("BVA_SPL_DATA_STRING_170", Include) {
    checkAnswer(s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_171
  test("BVA_SPL_DATA_STRING_171", Include) {
    checkAnswer(s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case c5_string when 'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_172
  test("BVA_SPL_DATA_STRING_172", Include) {
    checkAnswer(s"""select case when c5_string <'c' then 'd' else c5_string end from Test_Boundary""",
      s"""select case when c5_string <'c' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_173
  test("BVA_SPL_DATA_STRING_173", Include) {
    checkAnswer(s"""select case when c5_string <'c' then 'd' when c5_string >'a' then 'd' else c5_string end from Test_Boundary""",
      s"""select case when c5_string <'c' then 'd' when c5_string >'a' then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_174
  test("BVA_SPL_DATA_STRING_174", Include) {
    checkAnswer(s"""select case when c5_string <'c' then 'd' when c5_string is null then 'd' else c5_string end from Test_Boundary""",
      s"""select case when c5_string <'c' then 'd' when c5_string is null then 'd' else c5_string end from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_175
  test("BVA_SPL_DATA_STRING_175", Include) {
    checkAnswer(s"""select distinct count(*) from Test_Boundary""",
      s"""select distinct count(*) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_176
  test("BVA_SPL_DATA_STRING_176", Include) {
    checkAnswer(s"""select distinct count(c5_string) from Test_Boundary""",
      s"""select distinct count(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_177
  test("BVA_SPL_DATA_STRING_177", Include) {
    checkAnswer(s"""select max(c5_string) from Test_Boundary""",
      s"""select max(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_178
  test("BVA_SPL_DATA_STRING_178", Include) {
    checkAnswer(s"""select  count(distinct (c5_string)) from Test_Boundary""",
      s"""select  count(distinct (c5_string)) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_179
  test("BVA_SPL_DATA_STRING_179", Include) {
    checkAnswer(s"""select min(c5_string) from Test_Boundary""",
      s"""select min(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_180
  test("BVA_SPL_DATA_STRING_180", Include) {
    checkAnswer(s"""select distinct min(c5_string) from Test_Boundary""",
      s"""select distinct min(c5_string) from Test_Boundary_hive""")
  }


  //BVA_SPL_DATA_STRING_181
  test("BVA_SPL_DATA_STRING_181", Include) {
    checkAnswer(s"""select max(c5_string) from Test_Boundary""",
      s"""select max(c5_string) from Test_Boundary_hive""")
  }


  //boundry_TC_001
  test("boundry_TC_001", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double in (1.7976931348623157E308,2345.0,1234.0)""").collect
  }


  //boundry_TC_003
  test("boundry_TC_003", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double in (0,1234.0)""").collect
  }


  //boundry_TC_004
  test("boundry_TC_004", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double not in (1.7976931348623157E308,2345.0,1234.0)""").collect
  }


  //boundry_TC_005
  test("boundry_TC_005", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double in (1.7976931348623157E308,2345.0,1234.0)""").collect
  }


  //boundry_TC_006
  test("boundry_TC_006", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_007
  test("boundry_TC_007", Include) {
    sql(s"""select c4_double+0.9 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_008
  test("boundry_TC_008", Include) {
    sql(s"""select c4_double+0.9 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_009
  test("boundry_TC_009", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0021
  test("boundry_TC_0021", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double is not null""").collect
  }


  //boundry_TC_0022
  test("boundry_TC_0022", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double =2345.0 and c4_double <> 4567.0""").collect
  }


  //boundry_TC_0023
  test("boundry_TC_0023", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double =2345.0 and c4_double = 4567.0""").collect
  }


  //boundry_TC_0024
  test("boundry_TC_0024", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary where c4_double =2345.0 and c4_double = 4567.0 group by c4_double""").collect
  }


  //boundry_TC_0025
  test("boundry_TC_0025", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double =2345.0 or c4_double <> 4567.0""").collect
  }


  //boundry_TC_0026
  test("boundry_TC_0026", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double =2345.0 or c4_double = 4567.0""").collect
  }


  //boundry_TC_0027
  test("boundry_TC_0027", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary where c4_double =2345.0 or c4_double = 4567.0 group by c4_double""").collect
  }


  //boundry_TC_0030
  test("boundry_TC_0030", Include) {
    sql(s"""select c4_double from (select c4_double from Test_Boundary where c4_double not between 0 and 1.7976931348623157E308) e""").collect
  }


  //boundry_TC_0033
  test("boundry_TC_0033", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double between 0 and 1.7976931348623157E308""").collect
  }


  //boundry_TC_0036
  test("boundry_TC_0036", Include) {
    sql(s"""select c4_double from (select c4_double from Test_Boundary where c4_double between 0 and 1.7976931348623157E308) e""").collect
  }


  //boundry_TC_0037
  test("boundry_TC_0037", Include) {
    sql(s"""select count(*) from Test_Boundary""").collect
  }


  //boundry_TC_0038
  test("boundry_TC_0038", Include) {
    sql(s"""select distinct count(*) from Test_Boundary""").collect
  }


  //boundry_TC_0039
  test("boundry_TC_0039", Include) {
    sql(s"""select distinct count(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0042
  test("boundry_TC_0042", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double not between 0 and 1.7976931348623157E308""").collect
  }


  //boundry_TC_0043
  test("boundry_TC_0043", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0044
  test("boundry_TC_0044", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0045
  test("boundry_TC_0045", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double < 1.7976931348623157E308 and c4_double >3.147483647E9""").collect
  }


  //boundry_TC_0046
  test("boundry_TC_0046", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double < 1.7976931348623157E308 and c4_double >3.147483647E9""").collect
  }


  //boundry_TC_0047
  test("boundry_TC_0047", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double +0.1000= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0053
  test("boundry_TC_0053", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary group by c4_double having max(c4_double) >5000.0""").collect
  }


  //boundry_TC_0054
  test("boundry_TC_0054", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary group by c4_double having max(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0055
  test("boundry_TC_0055", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary group by c4_double having max(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0056
  test("boundry_TC_0056", Include) {
    sql(s"""select c4_double,max(c4_double) from Test_Boundary group by c4_double having max(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0062
  test("boundry_TC_0062", Include) {
    sql(s"""select c4_double,count(c4_double) from Test_Boundary group by c4_double""").collect
  }


  //boundry_TC_0063
  test("boundry_TC_0063", Include) {
    sql(s"""select c4_double,count(c4_double) from Test_Boundary group by c4_double having count(c4_double) >5000.0""").collect
  }


  //boundry_TC_0064
  test("boundry_TC_0064", Include) {
    sql(s"""select c4_double,count(c4_double) from Test_Boundary group by c4_double having count(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0065
  test("boundry_TC_0065", Include) {
    sql(s"""select c4_double,count(c4_double) from Test_Boundary group by c4_double having count(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0066
  test("boundry_TC_0066", Include) {
    sql(s"""select c4_double,count(c4_double) from Test_Boundary group by c4_double having count(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0071
  test("boundry_TC_0071", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double""").collect
  }


  //boundry_TC_0072
  test("boundry_TC_0072", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >5000.0""").collect
  }


  //boundry_TC_0073
  test("boundry_TC_0073", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0074
  test("boundry_TC_0074", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0075
  test("boundry_TC_0075", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0080
  test("boundry_TC_0080", Include) {
    sql(s"""select c4_double,sum(c4_double) from Test_Boundary group by c4_double""").collect
  }


  //boundry_TC_0081
  test("boundry_TC_0081", Include) {
    sql(s"""select c4_double,sum(c4_double) from Test_Boundary group by c4_double having sum(c4_double) >5000.0""").collect
  }


  //boundry_TC_0082
  test("boundry_TC_0082", Include) {
    sql(s"""select c4_double,sum(c4_double) from Test_Boundary group by c4_double having sum(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0083
  test("boundry_TC_0083", Include) {
    sql(s"""select c4_double,sum(c4_double) from Test_Boundary group by c4_double having sum(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0084
  test("boundry_TC_0084", Include) {
    sql(s"""select c4_double,sum(c4_double) from Test_Boundary group by c4_double having sum(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0089
  test("boundry_TC_0089", Include) {
    sql(s"""select c4_double,avg(c4_double) from Test_Boundary group by c4_double""").collect
  }


  //boundry_TC_0090
  test("boundry_TC_0090", Include) {
    sql(s"""select c4_double,avg(c4_double) from Test_Boundary group by c4_double having avg(c4_double) >5000.0""").collect
  }


  //boundry_TC_0091
  test("boundry_TC_0091", Include) {
    sql(s"""select c4_double,avg(c4_double) from Test_Boundary group by c4_double having avg(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0092
  test("boundry_TC_0092", Include) {
    sql(s"""select c4_double,avg(c4_double) from Test_Boundary group by c4_double having avg(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0093
  test("boundry_TC_0093", Include) {
    sql(s"""select c4_double,avg(c4_double) from Test_Boundary group by c4_double having avg(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0098
  test("boundry_TC_0098", Include) {
    sql(s"""select c4_double,c7_datatype_desc,max(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having max(c4_double) >5000.0""").collect
  }


  //boundry_TC_0099
  test("boundry_TC_0099", Include) {
    sql(s"""select c4_double,c7_datatype_desc,max(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having max(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0100
  test("boundry_TC_0100", Include) {
    sql(s"""select c4_double,c7_datatype_desc,max(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having max(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0101
  test("boundry_TC_0101", Include) {
    sql(s"""select c4_double,c7_datatype_desc,max(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having max(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0107
  test("boundry_TC_0107", Include) {
    sql(s"""select c4_double,c7_datatype_desc,count(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc""").collect
  }


  //boundry_TC_0108
  test("boundry_TC_0108", Include) {
    sql(s"""select c4_double,c7_datatype_desc,count(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having count(c4_double) >5000.0""").collect
  }


  //boundry_TC_0109
  test("boundry_TC_0109", Include) {
    sql(s"""select c4_double,c7_datatype_desc,count(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having count(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0110
  test("boundry_TC_0110", Include) {
    sql(s"""select c4_double,c7_datatype_desc,count(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having count(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0111
  test("boundry_TC_0111", Include) {
    sql(s"""select c4_double,c7_datatype_desc,count(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having count(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0116
  test("boundry_TC_0116", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc""").collect
  }


  //boundry_TC_0117
  test("boundry_TC_0117", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >5000.0""").collect
  }


  //boundry_TC_0118
  test("boundry_TC_0118", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0119
  test("boundry_TC_0119", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0120
  test("boundry_TC_0120", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0125
  test("boundry_TC_0125", Include) {
    sql(s"""select c4_double,c7_datatype_desc,sum(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc""").collect
  }


  //boundry_TC_0126
  test("boundry_TC_0126", Include) {
    sql(s"""select c4_double,c7_datatype_desc,sum(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having sum(c4_double) >5000.0""").collect
  }


  //boundry_TC_0127
  test("boundry_TC_0127", Include) {
    sql(s"""select c4_double,c7_datatype_desc,sum(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having sum(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0128
  test("boundry_TC_0128", Include) {
    sql(s"""select c4_double,c7_datatype_desc,sum(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having sum(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0129
  test("boundry_TC_0129", Include) {
    sql(s"""select c4_double,c7_datatype_desc,sum(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having sum(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0134
  test("boundry_TC_0134", Include) {
    sql(s"""select c4_double,c7_datatype_desc,avg(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc""").collect
  }


  //boundry_TC_0135
  test("boundry_TC_0135", Include) {
    sql(s"""select c4_double,c7_datatype_desc,avg(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having avg(c4_double) >5000.0""").collect
  }


  //boundry_TC_0136
  test("boundry_TC_0136", Include) {
    sql(s"""select c4_double,c7_datatype_desc,avg(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having avg(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0137
  test("boundry_TC_0137", Include) {
    sql(s"""select c4_double,c7_datatype_desc,avg(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having avg(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0138
  test("boundry_TC_0138", Include) {
    sql(s"""select c4_double,c7_datatype_desc,avg(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having avg(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0146
  test("boundry_TC_0146", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary sort by c4_double""").collect
  }


  //boundry_TC_0147
  test("boundry_TC_0147", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary sort by c4_double,c7_datatype_desc""").collect
  }


  //boundry_TC_0148
  test("boundry_TC_0148", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0149
  test("boundry_TC_0149", Include) {
    sql(s"""select c4_double,min(c4_double) from Test_Boundary group by c4_double having min(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0150
  test("boundry_TC_0150", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623156E308 order by c4_double""").collect
  }


  //boundry_TC_0151
  test("boundry_TC_0151", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double""").collect
  }


  //boundry_TC_0152
  test("boundry_TC_0152", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623156E308 order by c4_double asc""").collect
  }


  //boundry_TC_0153
  test("boundry_TC_0153", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double asc""").collect
  }


  //boundry_TC_0154
  test("boundry_TC_0154", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623156E308 order by c4_double desc""").collect
  }


  //boundry_TC_0155
  test("boundry_TC_0155", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double desc""").collect
  }


  //boundry_TC_0156
  test("boundry_TC_0156", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623156E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0157
  test("boundry_TC_0157", Include) {
    sql(s"""select c4_double,c7_datatype_desc,min(c4_double) from Test_Boundary group by c4_double,c7_datatype_desc having min(c4_double) >1.7976931348623158E308 order by c4_double limit 5""").collect
  }


  //boundry_TC_0158
  test("boundry_TC_0158", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double=c4_double""").collect
  }


  //boundry_TC_0159
  test("boundry_TC_0159", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double==c4_double""").collect
  }


  //boundry_TC_0160
  test("boundry_TC_0160", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double<=>c4_double""").collect
  }


  //boundry_TC_0161
  test("boundry_TC_0161", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double!=c4_double""").collect
  }


  //boundry_TC_0162
  test("boundry_TC_0162", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double<c4_double""").collect
  }


  //boundry_TC_0163
  test("boundry_TC_0163", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double<c4_double""").collect
  }


  //boundry_TC_0164
  test("boundry_TC_0164", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double<=c4_double""").collect
  }


  //boundry_TC_0165
  test("boundry_TC_0165", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double>c4_double""").collect
  }


  //boundry_TC_0166
  test("boundry_TC_0166", Include) {
    sql(s"""select c4_double,c2_Bigint,c3_Decimal,c4_double,c5_string,c6_Timestamp,c7_Datatype_Desc from Test_Boundary where c4_double>=c4_double""").collect
  }


  //boundry_TC_0169
  test("boundry_TC_0169", Include) {
    sql(s"""select c4_double from Test_Boundary where c4_double not between 1 and 1.7976931348623157E308""").collect
  }


  //boundry_TC_0170
  test("boundry_TC_0170", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double is null""").collect
  }


  //boundry_TC_0171
  test("boundry_TC_0171", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double is not null""").collect
  }


  //boundry_TC_0172
  test("boundry_TC_0172", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double not like 123.0""").collect
  }


  //boundry_TC_0173
  test("boundry_TC_0173", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double like 123.0""").collect
  }


  //boundry_TC_0174
  test("boundry_TC_0174", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double rlike 123.0""").collect
  }


  //boundry_TC_0175
  test("boundry_TC_0175", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double regexp 123.0""").collect
  }


  //boundry_TC_0176
  test("boundry_TC_0176", Include) {
    sql(s"""select c4_double+0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0177
  test("boundry_TC_0177", Include) {
    sql(s"""select c4_double+0.00100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0178
  test("boundry_TC_0178", Include) {
    sql(s"""select c4_double+23 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_0179
  test("boundry_TC_0179", Include) {
    sql(s"""select c4_double+50 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0180
  test("boundry_TC_0180", Include) {
    sql(s"""select c4_double+0.50 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_0181
  test("boundry_TC_0181", Include) {
    sql(s"""select c4_double+75 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0182
  test("boundry_TC_0182", Include) {
    sql(s"""select c4_double-0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0183
  test("boundry_TC_0183", Include) {
    sql(s"""select c4_double-0.00100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0184
  test("boundry_TC_0184", Include) {
    sql(s"""select c4_double-23 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_0185
  test("boundry_TC_0185", Include) {
    sql(s"""select c4_double-50 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0186
  test("boundry_TC_0186", Include) {
    sql(s"""select c4_double-0.50 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_0187
  test("boundry_TC_0187", Include) {
    sql(s"""select c4_double-75 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0188
  test("boundry_TC_0188", Include) {
    sql(s"""select c4_double*0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0189
  test("boundry_TC_0189", Include) {
    sql(s"""select c4_double*0.00100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0190
  test("boundry_TC_0190", Include) {
    sql(s"""select c4_double*23 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_0191
  test("boundry_TC_0191", Include) {
    sql(s"""select c4_double*50 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0192
  test("boundry_TC_0192", Include) {
    sql(s"""select c4_double*0.50 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_0193
  test("boundry_TC_0193", Include) {
    sql(s"""select c4_double*75 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0194
  test("boundry_TC_0194", Include) {
    sql(s"""select c4_double/0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0195
  test("boundry_TC_0195", Include) {
    sql(s"""select c4_double/0.00100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0196
  test("boundry_TC_0196", Include) {
    sql(s"""select c4_double/23 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_0197
  test("boundry_TC_0197", Include) {
    sql(s"""select c4_double/50 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0198
  test("boundry_TC_0198", Include) {
    sql(s"""select c4_double/0.50 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_0199
  test("boundry_TC_0199", Include) {
    sql(s"""select c4_double/75 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0200
  test("boundry_TC_0200", Include) {
    sql(s"""select c4_double%0.100 from Test_Boundary where c4_double <> 1.7976931348623157E308""").collect
  }


  //boundry_TC_0201
  test("boundry_TC_0201", Include) {
    sql(s"""select c4_double%0.00100 from Test_Boundary where c4_double = 1.7976931348623157E308""").collect
  }


  //boundry_TC_0202
  test("boundry_TC_0202", Include) {
    sql(s"""select c4_double%23 from Test_Boundary where c4_double < 1.7976931348623157E308""").collect
  }


  //boundry_TC_0203
  test("boundry_TC_0203", Include) {
    sql(s"""select c4_double%50 from Test_Boundary where c4_double <= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0204
  test("boundry_TC_0204", Include) {
    sql(s"""select c4_double%0.50 from Test_Boundary where c4_double > 1.7976931348623157E308""").collect
  }


  //boundry_TC_0205
  test("boundry_TC_0205", Include) {
    sql(s"""select c4_double%75 from Test_Boundary where c4_double >= 1.7976931348623157E308""").collect
  }


  //boundry_TC_0206
  test("boundry_TC_0206", Include) {
    sql(s"""select round(c4_double,1) from Test_Boundary""").collect
  }


  //boundry_TC_0207
  test("boundry_TC_0207", Include) {
    sql(s"""select round(c4_double,1) from Test_Boundary""").collect
  }


  //boundry_TC_0210
  test("boundry_TC_0210", Include) {
    sql(s"""select floor(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0211
  test("boundry_TC_0211", Include) {
    sql(s"""select ceil(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0212
  test("boundry_TC_0212", Include) {
    sql(s"""select rand(5) from Test_Boundary""").collect
  }


  //boundry_TC_0213
  test("boundry_TC_0213", Include) {
    sql(s"""select exp(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0214
  test("boundry_TC_0214", Include) {
    sql(s"""select ln(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0215
  test("boundry_TC_0215", Include) {
    sql(s"""select log10(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0216
  test("boundry_TC_0216", Include) {
    sql(s"""select log2(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0217
  test("boundry_TC_0217", Include) {
    sql(s"""select log(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0218
  test("boundry_TC_0218", Include) {
    sql(s"""select log(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0219
  test("boundry_TC_0219", Include) {
    sql(s"""select pow(c4_double,c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0220
  test("boundry_TC_0220", Include) {
    sql(s"""select sqrt(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0221
  test("boundry_TC_0221", Include) {
    sql(s"""select pmod(c4_double,1) from Test_Boundary""").collect
  }


  //boundry_TC_0222
  test("boundry_TC_0222", Include) {
    sql(s"""select sin(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0223
  test("boundry_TC_0223", Include) {
    sql(s"""select asin(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0224
  test("boundry_TC_0224", Include) {
    sql(s"""select cos(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0225
  test("boundry_TC_0225", Include) {
    sql(s"""select acos(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0226
  test("boundry_TC_0226", Include) {
    sql(s"""select tan(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0227
  test("boundry_TC_0227", Include) {
    sql(s"""select atan(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0228
  test("boundry_TC_0228", Include) {
    sql(s"""select degrees(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0229
  test("boundry_TC_0229", Include) {
    sql(s"""select radians(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0230
  test("boundry_TC_0230", Include) {
    sql(s"""select positive(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0231
  test("boundry_TC_0231", Include) {
    sql(s"""select negative(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0232
  test("boundry_TC_0232", Include) {
    sql(s"""select sign(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0233
  test("boundry_TC_0233", Include) {
    sql(s"""select exp(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0235
  test("boundry_TC_0235", Include) {
    sql(s"""select factorial(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0236
  test("boundry_TC_0236", Include) {
    sql(s"""select cbrt(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0237
  test("boundry_TC_0237", Include) {
    sql(s"""select shiftleft(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0238
  test("boundry_TC_0238", Include) {
    sql(s"""select shiftleft(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0239
  test("boundry_TC_0239", Include) {
    sql(s"""select shiftright(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0240
  test("boundry_TC_0240", Include) {
    sql(s"""select shiftright(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0241
  test("boundry_TC_0241", Include) {
    sql(s"""select shiftrightunsigned(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0242
  test("boundry_TC_0242", Include) {
    sql(s"""select shiftrightunsigned(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0243
  test("boundry_TC_0243", Include) {
    sql(s"""select greatest(1.0,2.0,3.0,4.0,5.0) from Test_Boundary""").collect
  }


  //boundry_TC_0244
  test("boundry_TC_0244", Include) {
    sql(s"""select least(1.0,2.0,3.0,4.0,5.0) from Test_Boundary""").collect
  }


  //boundry_TC_0245
  test("boundry_TC_0245", Include) {
    sql(s"""select cast(c4_double as decimal) from Test_Boundary""").collect
  }


  //boundry_TC_0246
  test("boundry_TC_0246", Include) {
    sql(s"""select if(c4_double<5000.0,'t','f') from Test_Boundary""").collect
  }


  //boundry_TC_0247
  test("boundry_TC_0247", Include) {
    sql(s"""select isnull(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0248
  test("boundry_TC_0248", Include) {
    sql(s"""select isnotnull(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0249
  test("boundry_TC_0249", Include) {
    sql(s"""select nvl(c4_double,10) from Test_Boundary""").collect
  }


  //boundry_TC_0250
  test("boundry_TC_0250", Include) {
    sql(s"""select nvl(c4_double,0) from Test_Boundary""").collect
  }


  //boundry_TC_0251
  test("boundry_TC_0251", Include) {
    sql(s"""select nvl(c4_double,null) from Test_Boundary""").collect
  }


  //boundry_TC_0252
  test("boundry_TC_0252", Include) {
    sql(s"""select coalesce(c4_double,null,null,null,756) from Test_Boundary""").collect
  }


  //boundry_TC_0253
  test("boundry_TC_0253", Include) {
    sql(s"""select coalesce(c4_double,1,null,null,756) from Test_Boundary""").collect
  }


  //boundry_TC_0254
  test("boundry_TC_0254", Include) {
    sql(s"""select coalesce(c4_double,345,null,756) from Test_Boundary""").collect
  }


  //boundry_TC_0255
  test("boundry_TC_0255", Include) {
    sql(s"""select coalesce(c4_double,345,0.1,456,756) from Test_Boundary""").collect
  }


  //boundry_TC_0256
  test("boundry_TC_0256", Include) {
    sql(s"""select coalesce(c4_double,756,null,null,null) from Test_Boundary""").collect
  }


  //boundry_TC_0257
  test("boundry_TC_0257", Include) {
    sql(s"""select case c4_double when 2345.0 then true else false end from Test_boundary""").collect
  }


  //boundry_TC_0258
  test("boundry_TC_0258", Include) {
    sql(s"""select case c4_double when 2345.0 then true end from Test_boundary""").collect
  }


  //boundry_TC_0259
  test("boundry_TC_0259", Include) {
    sql(s"""select case c4_double when 2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0260
  test("boundry_TC_0260", Include) {
    sql(s"""select case c4_double when 2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0261
  test("boundry_TC_0261", Include) {
    sql(s"""select case when c4_double <2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0262
  test("boundry_TC_0262", Include) {
    sql(s"""select case c4_double when 2345.0 then true else false end from Test_boundary""").collect
  }


  //boundry_TC_0263
  test("boundry_TC_0263", Include) {
    sql(s"""select case c4_double when 2345.0 then true end from Test_boundary""").collect
  }


  //boundry_TC_0264
  test("boundry_TC_0264", Include) {
    sql(s"""select case c4_double when 2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0265
  test("boundry_TC_0265", Include) {
    sql(s"""select case c4_double when 2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0266
  test("boundry_TC_0266", Include) {
    sql(s"""select case when c4_double <2345.0 then 1000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0267
  test("boundry_TC_0267", Include) {
    sql(s"""select case when c4_double <2345.0 then 1000 when c4_double >2535353535.0 then 1000000000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0268
  test("boundry_TC_0268", Include) {
    sql(s"""select case when c4_double <2345.0 then 1000 when c4_double is null then 1000000000 else c4_double end from Test_boundary""").collect
  }


  //boundry_TC_0269
  test("boundry_TC_0269", Include) {
    sql(s"""select distinct count(*) from Test_Boundary""").collect
  }


  //boundry_TC_0270
  test("boundry_TC_0270", Include) {
    sql(s"""select distinct count(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0271
  test("boundry_TC_0271", Include) {
    sql(s"""select max(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0272
  test("boundry_TC_0272", Include) {
    sql(s"""select count(distinct (c4_double)) from Test_Boundary""").collect
  }


  //boundry_TC_0273
  test("boundry_TC_0273", Include) {
    sql(s"""select distinct sum(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0274
  test("boundry_TC_0274", Include) {
    sql(s"""select sum(distinct c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0275
  test("boundry_TC_0275", Include) {
    sql(s"""select distinct avg(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0276
  test("boundry_TC_0276", Include) {
    sql(s"""select avg( c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0277
  test("boundry_TC_0277", Include) {
    sql(s"""select min(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0278
  test("boundry_TC_0278", Include) {
    sql(s"""select distinct min(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0279
  test("boundry_TC_0279", Include) {
    sql(s"""select max(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0280
  test("boundry_TC_0280", Include) {
    sql(s"""select variance(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0281
  test("boundry_TC_0281", Include) {
    sql(s"""select var_samp(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0282
  test("boundry_TC_0282", Include) {
    sql(s"""select stddev_pop(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0283
  test("boundry_TC_0283", Include) {
    sql(s"""select stddev_samp(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0284
  test("boundry_TC_0284", Include) {
    sql(s"""select covar_pop(c4_double,c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0285
  test("boundry_TC_0285", Include) {
    sql(s"""select covar_samp(c4_double,c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0286
  test("boundry_TC_0286", Include) {
    sql(s"""select corr(c4_double,1) from Test_Boundary""").collect
  }


  //boundry_TC_0288
  test("boundry_TC_0288", Include) {
    sql(s"""select histogram_numeric(c4_double,2) from Test_Boundary""").collect
  }


  //boundry_TC_0289
  test("boundry_TC_0289", Include) {
    sql(s"""select collect_set(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0290
  test("boundry_TC_0290", Include) {
    sql(s"""select collect_list(c4_double) from Test_Boundary""").collect
  }


  //boundry_TC_0291
  test("boundry_TC_0291", Include) {
    sql(s"""select cast(c4_double as decimal) from Test_Boundary""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC001
  test("PushUP_FILTER_Test_Boundary_TC001", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC002
  test("PushUP_FILTER_Test_Boundary_TC002", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC003
  test("PushUP_FILTER_Test_Boundary_TC003", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC004
  test("PushUP_FILTER_Test_Boundary_TC004", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC005
  test("PushUP_FILTER_Test_Boundary_TC005", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where round(c1_int,1)=2147483647 or round(c1_int,1)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC006
  test("PushUP_FILTER_Test_Boundary_TC006", Include) {
    checkAnswer(s"""select  c1_int  from Test_Boundary where round(c1_int,1) IS NULL""",
      s"""select  c1_int  from Test_Boundary_hive where round(c1_int,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC007
  test("PushUP_FILTER_Test_Boundary_TC007", Include) {
    checkAnswer(s"""select  c1_int  from Test_Boundary where round(c1_int,1)='NULL'""",
      s"""select  c1_int  from Test_Boundary_hive where round(c1_int,1)='NULL'""")
  }


  //PushUP_FILTER_Test_Boundary_TC008
  test("PushUP_FILTER_Test_Boundary_TC008", Include) {
    checkAnswer(s"""select  c1_int  from Test_Boundary where round(c1_int,1)=NULL""",
      s"""select  c1_int  from Test_Boundary_hive where round(c1_int,1)=NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC009
  test("PushUP_FILTER_Test_Boundary_TC009", Include) {
    checkAnswer(s"""select  c1_int  from Test_Boundary where round(c1_int,1)=2147483647 and round(c1_int,1)=-2147483648""",
      s"""select  c1_int  from Test_Boundary_hive where round(c1_int,1)=2147483647 and round(c1_int,1)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC010
  test("PushUP_FILTER_Test_Boundary_TC010", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint,1)=NULL""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint,1)=NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC011
  test("PushUP_FILTER_Test_Boundary_TC011", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint,1)=9223372036854775807 or round(c2_Bigint,1)=-9223372036854775808""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint,1)=9223372036854775807 or round(c2_Bigint,1)=-9223372036854775808""")
  }


  //PushUP_FILTER_Test_Boundary_TC012
  test("PushUP_FILTER_Test_Boundary_TC012", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint,1)IS NULL""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint,1)IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC013
  test("PushUP_FILTER_Test_Boundary_TC013", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint,1)='NULL'""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint,1)='NULL'""")
  }


  //PushUP_FILTER_Test_Boundary_TC014
  test("PushUP_FILTER_Test_Boundary_TC014", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal,1)=0.9 or round(c3_Decimal,1)=0.0""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal,1)=0.9 or round(c3_Decimal,1)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC015
  test("PushUP_FILTER_Test_Boundary_TC015", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal,1) is NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal,1) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC016
  test("PushUP_FILTER_Test_Boundary_TC016", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal,1) =NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal,1) =NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC017
  test("PushUP_FILTER_Test_Boundary_TC017", Include) {
    sql(s"""select c4_double from Test_Boundary where round(c4_double,1)=0.9 or round(c4_double,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC018
  test("PushUP_FILTER_Test_Boundary_TC018", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal,1) is NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal,1) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC019
  test("PushUP_FILTER_Test_Boundary_TC019", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal,1) =NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal,1) =NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC020
  test("PushUP_FILTER_Test_Boundary_TC020", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where round(c5_string,1)=0.0""",
      s"""select  c5_string  from Test_Boundary_hive where round(c5_string,1)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC021
  test("PushUP_FILTER_Test_Boundary_TC021", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC022
  test("PushUP_FILTER_Test_Boundary_TC022", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC023
  test("PushUP_FILTER_Test_Boundary_TC023", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC024
  test("PushUP_FILTER_Test_Boundary_TC024", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC025
  test("PushUP_FILTER_Test_Boundary_TC025", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC026
  test("PushUP_FILTER_Test_Boundary_TC026", Include) {
    sql(s"""select c2_Bigint from Test_Boundary where bround(c2_Bigint,1)=9223372036854775807 or bround(c2_Bigint,1)=-9223372036854775808""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC027
  test("PushUP_FILTER_Test_Boundary_TC027", Include) {
    sql(s"""select c3_Decimal from Test_Boundary where bround(c3_Decimal)=0 or bround(c3_Decimal)=1""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC028
  test("PushUP_FILTER_Test_Boundary_TC028", Include) {
    sql(s"""select c4_double from Test_Boundary where bround(c4_double,1)=0.9 or round(c4_double,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC029
  test("PushUP_FILTER_Test_Boundary_TC029", Include) {
    sql(s"""select  c5_string  from Test_Boundary where bround(c5_string,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC030
  test("PushUP_FILTER_Test_Boundary_TC030", Include) {
    sql(s"""select c1_int from Test_Boundary where bround(c1_int)=2147483647 or bround(c1_int)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC031
  test("PushUP_FILTER_Test_Boundary_TC031", Include) {
    sql(s"""select c1_int from Test_Boundary where round(c1_int)=2147483647 or bround(c1_int)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC032
  test("PushUP_FILTER_Test_Boundary_TC032", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint)=NULL""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint)=NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC033
  test("PushUP_FILTER_Test_Boundary_TC033", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint)=9223372036854775807 or round(c2_Bigint)=-9223372036854775808""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint)=9223372036854775807 or round(c2_Bigint)=-9223372036854775808""")
  }


  //PushUP_FILTER_Test_Boundary_TC034
  test("PushUP_FILTER_Test_Boundary_TC034", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint)IS NULL""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint)IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC035
  test("PushUP_FILTER_Test_Boundary_TC035", Include) {
    checkAnswer(s"""select  c2_Bigint  from Test_Boundary where round(c2_Bigint)='NULL'""",
      s"""select  c2_Bigint  from Test_Boundary_hive where round(c2_Bigint)='NULL'""")
  }


  //PushUP_FILTER_Test_Boundary_TC036
  test("PushUP_FILTER_Test_Boundary_TC036", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal)=0.9 or round(c3_Decimal)=0.0""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal)=0.9 or round(c3_Decimal)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC037
  test("PushUP_FILTER_Test_Boundary_TC037", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal) is NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC038
  test("PushUP_FILTER_Test_Boundary_TC038", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal) =NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal) =NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC039
  test("PushUP_FILTER_Test_Boundary_TC039", Include) {
    sql(s"""select c4_double from Test_Boundary where round(c4_double)=0.9 or round(c4_double)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC040
  test("PushUP_FILTER_Test_Boundary_TC040", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal) is NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC041
  test("PushUP_FILTER_Test_Boundary_TC041", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where round(c3_Decimal) =NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where round(c3_Decimal) =NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC042
  test("PushUP_FILTER_Test_Boundary_TC042", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where round(c5_string)=0.0""",
      s"""select  c5_string  from Test_Boundary_hive where round(c5_string)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC043
  test("PushUP_FILTER_Test_Boundary_TC043", Include) {
    sql(s"""select c1_int from Test_Boundary where bround(c1_int)=-2147483648 or bround(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC044
  test("PushUP_FILTER_Test_Boundary_TC044", Include) {
    sql(s"""select c2_Bigint from Test_Boundary where bround(c2_Bigint,1)=9223372036854775807 or bround(c2_Bigint,1)=-9223372036854775808""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC045
  test("PushUP_FILTER_Test_Boundary_TC045", Include) {
    sql(s"""select c3_Decimal from Test_Boundary where bround(c3_Decimal)=0 or bround(c3_Decimal)=1""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC046
  test("PushUP_FILTER_Test_Boundary_TC046", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.99,c3_Decimal+1.1,c4_double+1.12,c5_string+1.12 from Test_Boundary where round(c1_int,1)=2147483647 or  round(c2_Bigint,1)=9223372036854775807 or round(c3_Decimal,1)=0.9 or round(c4_double,1)=0.9 or round(c5_string,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC047
  test("PushUP_FILTER_Test_Boundary_TC047", Include) {
    sql(s"""select c1_int-1,c2_Bigint-9.99,c3_Decimal-1.1,c4_double-1.12,c5_string-1.12 from Test_Boundary where bround(c1_int,1)=2147483647 or  bround(c2_Bigint,1)=9223372036854775807 or bround(c3_Decimal,1)=0.9 or bround(c4_double,1)=0.9 or bround(c5_string,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC048
  test("PushUP_FILTER_Test_Boundary_TC048", Include) {
    sql(s"""select c1_int*1,c2_Bigint*9.99,c3_Decimal*1.1,c4_double*1.12,c5_string*1.12 from Test_Boundary where round(c1_int)=2147483647 or  round(c2_Bigint)=9223372036854775807 or round(c3_Decimal)=0.9 or round(c4_double)=0.9 or round(c5_string)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC049
  test("PushUP_FILTER_Test_Boundary_TC049", Include) {
    sql(s"""select c1_int/1,c2_Bigint/9.99,c3_Decimal/1.1,c4_double/1.12,c5_string/1.12 from Test_Boundary where bround(c1_int)=2147483647 or  bround(c2_Bigint)=9223372036854775807 or bround(c3_Decimal)=0.9 or bround(c4_double)=0.9 or bround(c5_string)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC050
  test("PushUP_FILTER_Test_Boundary_TC050", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC051
  test("PushUP_FILTER_Test_Boundary_TC051", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC052
  test("PushUP_FILTER_Test_Boundary_TC052", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC053
  test("PushUP_FILTER_Test_Boundary_TC053", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC054
  test("PushUP_FILTER_Test_Boundary_TC054", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where floor(c1_int)=2.147483647E9 or floor(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC055
  test("PushUP_FILTER_Test_Boundary_TC055", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where floor(c1_int) IS NULL or floor(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where floor(c1_int) IS NULL or floor(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC056
  test("PushUP_FILTER_Test_Boundary_TC056", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where floor(c1_int) IS NULL or floor(c1_int) IS NOT NULL or floor(c1_int)=2.147483647E9""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC057
  test("PushUP_FILTER_Test_Boundary_TC057", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where floor(c2_Bigint)=9.223372036854776E18 or floor(c2_Bigint)=-9.223372036854776E18""",
      s"""select c2_Bigint from Test_Boundary_hive where floor(c2_Bigint)=9.223372036854776E18 or floor(c2_Bigint)=-9.223372036854776E18""")
  }


  //PushUP_FILTER_Test_Boundary_TC058
  test("PushUP_FILTER_Test_Boundary_TC058", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where floor(c2_Bigint) IS NULL or floor(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where floor(c2_Bigint) IS NULL or floor(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC059
  test("PushUP_FILTER_Test_Boundary_TC059", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where floor(c3_Decimal)=0.00 or floor(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where floor(c3_Decimal)=0.00 or floor(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC060
  test("PushUP_FILTER_Test_Boundary_TC060", Include) {
    sql(s"""select c4_double from Test_Boundary where floor(c4_double)= 1.7976931348623157E308 or floor(c4_double)=8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC061
  test("PushUP_FILTER_Test_Boundary_TC061", Include) {
    sql(s"""select c4_double from Test_Boundary where floor(c4_double) IS NULL or floor(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC062
  test("PushUP_FILTER_Test_Boundary_TC062", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where floor(c5_string)<=0.0 or floor(c5_string)>=0.0 """,
      s"""select  c5_string  from Test_Boundary_hive where floor(c5_string)<=0.0 or floor(c5_string)>=0.0 """)
  }


  //PushUP_FILTER_Test_Boundary_TC063
  test("PushUP_FILTER_Test_Boundary_TC063", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where floor(c5_string) IS NULL or floor(c5_string) IS NOT NULL""",
      s"""select  c5_string  from Test_Boundary_hive where floor(c5_string) IS NULL or floor(c5_string) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC064
  test("PushUP_FILTER_Test_Boundary_TC064", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where floor(c2_Bigint)<=9.223372036854776E18 or floor(c2_Bigint)>=-9.223372036854776E18""",
      s"""select c2_Bigint from Test_Boundary_hive where floor(c2_Bigint)<=9.223372036854776E18 or floor(c2_Bigint)>=-9.223372036854776E18""")
  }


  //PushUP_FILTER_Test_Boundary_TC065
  test("PushUP_FILTER_Test_Boundary_TC065", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where floor(c3_Decimal)<=0.0 or floor(c3_Decimal) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where floor(c3_Decimal)<=0.0 or floor(c3_Decimal) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC066
  test("PushUP_FILTER_Test_Boundary_TC066", Include) {
    sql(s"""select c4_double from Test_Boundary where floor(c4_double)<= 1.7976931348623157E308 or floor(c4_double)>=8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC067
  test("PushUP_FILTER_Test_Boundary_TC067", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where floor(c1_int) IS NULL and floor(c1_int) IS NOT NULL and floor(c1_int)=2.147483647E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where floor(c1_int) IS NULL and floor(c1_int) IS NOT NULL and floor(c1_int)=2.147483647E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC068
  test("PushUP_FILTER_Test_Boundary_TC068", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC069
  test("PushUP_FILTER_Test_Boundary_TC069", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC070
  test("PushUP_FILTER_Test_Boundary_TC070", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC071
  test("PushUP_FILTER_Test_Boundary_TC071", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC072
  test("PushUP_FILTER_Test_Boundary_TC072", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC073
  test("PushUP_FILTER_Test_Boundary_TC073", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where ceil(c1_int) IS NULL or ceiling(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where ceil(c1_int) IS NULL or ceiling(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC074
  test("PushUP_FILTER_Test_Boundary_TC074", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""",
      s"""select c1_int from Test_Boundary_hive where ceil(c1_int)=2.147483647E9 or ceiling(c1_int)=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC075
  test("PushUP_FILTER_Test_Boundary_TC075", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where ceil(c1_int) IS NULL or ceiling(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where ceil(c1_int) IS NULL or ceiling(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC076
  test("PushUP_FILTER_Test_Boundary_TC076", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where ceil(c1_int) IS NULL or ceiling(c1_int) IS NOT NULL or ceiling(c1_int)=2.147483647E9""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC077
  test("PushUP_FILTER_Test_Boundary_TC077", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where ceil(c2_Bigint)=9.223372036854776E18 or ceiling(c2_Bigint)=-9.223372036854776E18""",
      s"""select c2_Bigint from Test_Boundary_hive where ceil(c2_Bigint)=9.223372036854776E18 or ceiling(c2_Bigint)=-9.223372036854776E18""")
  }


  //PushUP_FILTER_Test_Boundary_TC078
  test("PushUP_FILTER_Test_Boundary_TC078", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where ceil(c2_Bigint) IS NULL or ceiling(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where ceil(c2_Bigint) IS NULL or ceiling(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC079
  test("PushUP_FILTER_Test_Boundary_TC079", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where ceil(c3_Decimal)=0.0 or ceiling(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where ceil(c3_Decimal)=0.0 or ceiling(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC080
  test("PushUP_FILTER_Test_Boundary_TC080", Include) {
    sql(s"""select c4_double from Test_Boundary where ceil(c4_double)= 1.7976931348623157E308 or ceiling(c4_double)=8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC081
  test("PushUP_FILTER_Test_Boundary_TC081", Include) {
    sql(s"""select c4_double from Test_Boundary where ceil(c4_double) IS NULL or ceiling(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC082
  test("PushUP_FILTER_Test_Boundary_TC082", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where ceil(c5_string)<=0.0 or ceiling(c5_string)>=0.0 """,
      s"""select  c5_string  from Test_Boundary_hive where ceil(c5_string)<=0.0 or ceiling(c5_string)>=0.0 """)
  }


  //PushUP_FILTER_Test_Boundary_TC083
  test("PushUP_FILTER_Test_Boundary_TC083", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where ceil(c5_string) IS NULL or ceiling(c5_string) IS NOT NULL""",
      s"""select  c5_string  from Test_Boundary_hive where ceil(c5_string) IS NULL or ceiling(c5_string) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC084
  test("PushUP_FILTER_Test_Boundary_TC084", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where ceil(c2_Bigint)<=9.223372036854776E18 or ceiling(c2_Bigint)>=-9.223372036854776E18""",
      s"""select c2_Bigint from Test_Boundary_hive where ceil(c2_Bigint)<=9.223372036854776E18 or ceiling(c2_Bigint)>=-9.223372036854776E18""")
  }


  //PushUP_FILTER_Test_Boundary_TC085
  test("PushUP_FILTER_Test_Boundary_TC085", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where ceil(c3_Decimal)<=0.0 or ceiling(c3_Decimal) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where ceil(c3_Decimal)<=0.0 or ceiling(c3_Decimal) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC086
  test("PushUP_FILTER_Test_Boundary_TC086", Include) {
    sql(s"""select c4_double from Test_Boundary where ceil(c4_double)<= 1.7976931348623157E308 or ceiling(c4_double)>=8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC087
  test("PushUP_FILTER_Test_Boundary_TC087", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where ceiling(c1_int) IS NULL and ceil(c1_int) IS NOT NULL and ceiling(c1_int)=2.147483647E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where ceiling(c1_int) IS NULL and ceil(c1_int) IS NOT NULL and ceiling(c1_int)=2.147483647E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC093
  test("PushUP_FILTER_Test_Boundary_TC093", Include) {
    sql(s"""select c1_int from Test_Boundary where rand(c1_int) IS NULL or rand(c1_int) IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC094
  test("PushUP_FILTER_Test_Boundary_TC094", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where rand(c1_int) IS NULL or rand(c1_int) IS NOT NULL or rand(c1_int)=0.45540022789662593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC095
  test("PushUP_FILTER_Test_Boundary_TC095", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where exp(c1_int)=0.0 or exp(c1_int)=1.0""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where exp(c1_int)=0.0 or exp(c1_int)=1.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC096
  test("PushUP_FILTER_Test_Boundary_TC096", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where exp(c1_int)=0.0 or exp(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC097
  test("PushUP_FILTER_Test_Boundary_TC097", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where exp(c1_int)=0.0 or exp(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC098
  test("PushUP_FILTER_Test_Boundary_TC098", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where exp(c1_int)=0.0 or exp(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC099
  test("PushUP_FILTER_Test_Boundary_TC099", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where exp(c1_int)=0.0 or exp(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC100
  test("PushUP_FILTER_Test_Boundary_TC100", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where exp(c1_int) IS NULL or exp(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where exp(c1_int) IS NULL or exp(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC101
  test("PushUP_FILTER_Test_Boundary_TC101", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where exp(c1_int) IS NULL or exp(c1_int) IS NOT NULL or exp(c1_int)=0.45540022789662593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC102
  test("PushUP_FILTER_Test_Boundary_TC102", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where exp(c2_Bigint)=0.0  or exp(c2_Bigint)=1.0""",
      s"""select c2_Bigint from Test_Boundary_hive where exp(c2_Bigint)=0.0  or exp(c2_Bigint)=1.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC103
  test("PushUP_FILTER_Test_Boundary_TC103", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where exp(c2_Bigint) IS NULL or exp(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where exp(c2_Bigint) IS NULL or exp(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC104
  test("PushUP_FILTER_Test_Boundary_TC104", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where exp(c3_Decimal)=0.0 or exp(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where exp(c3_Decimal)=0.0 or exp(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC105
  test("PushUP_FILTER_Test_Boundary_TC105", Include) {
    checkAnswer(s"""select c4_double from Test_Boundary where exp(c4_double)= 0.0 or exp(c4_double)=0.0""",
      s"""select c4_double from Test_Boundary_hive where exp(c4_double)= 0.0 or exp(c4_double)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC106
  test("PushUP_FILTER_Test_Boundary_TC106", Include) {
    sql(s"""select c4_double from Test_Boundary where exp(c4_double) IS NULL or exp(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC107
  test("PushUP_FILTER_Test_Boundary_TC107", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where exp(c5_string)<=0.0 or exp(c5_string)>=0.0 """,
      s"""select  c5_string  from Test_Boundary_hive where exp(c5_string)<=0.0 or exp(c5_string)>=0.0 """)
  }


  //PushUP_FILTER_Test_Boundary_TC108
  test("PushUP_FILTER_Test_Boundary_TC108", Include) {
    checkAnswer(s"""select  c5_string  from Test_Boundary where exp(c5_string) IS NULL or exp(c5_string) IS NOT NULL""",
      s"""select  c5_string  from Test_Boundary_hive where exp(c5_string) IS NULL or exp(c5_string) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC109
  test("PushUP_FILTER_Test_Boundary_TC109", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where exp(c2_Bigint)<=0.0 or exp(c2_Bigint)>=0.0""",
      s"""select c2_Bigint from Test_Boundary_hive where exp(c2_Bigint)<=0.0 or exp(c2_Bigint)>=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC110
  test("PushUP_FILTER_Test_Boundary_TC110", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where exp(c3_Decimal)<=0.0 or exp(c3_Decimal) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where exp(c3_Decimal)<=0.0 or exp(c3_Decimal) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC111
  test("PushUP_FILTER_Test_Boundary_TC111", Include) {
    sql(s"""select c4_double from Test_Boundary where exp(c4_double)<= 0.0 or exp(c4_double)>=8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC112
  test("PushUP_FILTER_Test_Boundary_TC112", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where exp(c1_int) IS NULL and exp(c1_int) IS NOT NULL and exp(c1_int)=2.147483647E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where exp(c1_int) IS NULL and exp(c1_int) IS NOT NULL and exp(c1_int)=2.147483647E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC114
  test("PushUP_FILTER_Test_Boundary_TC114", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""")
  }


  //PushUP_FILTER_Test_Boundary_TC115
  test("PushUP_FILTER_Test_Boundary_TC115", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC116
  test("PushUP_FILTER_Test_Boundary_TC116", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""")
  }


  //PushUP_FILTER_Test_Boundary_TC117
  test("PushUP_FILTER_Test_Boundary_TC117", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC118
  test("PushUP_FILTER_Test_Boundary_TC118", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where log10(c1_int)=9.331929865381182 or log10(c1_int)=3.6596310116070008""")
  }


  //PushUP_FILTER_Test_Boundary_TC119
  test("PushUP_FILTER_Test_Boundary_TC119", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where log10(c1_int) IS NULL or log10(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where log10(c1_int) IS NULL or log10(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC120
  test("PushUP_FILTER_Test_Boundary_TC120", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where log10(c1_int) IS NULL or log10(c1_int) IS NOT NULL or log10(c1_int)=3.6596310116070008""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC121
  test("PushUP_FILTER_Test_Boundary_TC121", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where log10(c2_Bigint)=18.964889726830815  or log10(c2_Bigint)=3.6596310116070008""",
      s"""select c2_Bigint from Test_Boundary_hive where log10(c2_Bigint)=18.964889726830815  or log10(c2_Bigint)=3.6596310116070008""")
  }


  //PushUP_FILTER_Test_Boundary_TC122
  test("PushUP_FILTER_Test_Boundary_TC122", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where log10(c2_Bigint) IS NULL or log10(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where log10(c2_Bigint) IS NULL or log10(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC123
  test("PushUP_FILTER_Test_Boundary_TC123", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where log10(c3_Decimal)=-0.908485022795986 or log10(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where log10(c3_Decimal)=-0.908485022795986 or log10(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC124
  test("PushUP_FILTER_Test_Boundary_TC124", Include) {
    sql(s"""select c4_double from Test_Boundary where log10(c4_double)= 308.25471555991675 or log10(c4_double)=-320.30970367096165""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC125
  test("PushUP_FILTER_Test_Boundary_TC125", Include) {
    sql(s"""select c4_double from Test_Boundary where log10(c4_double) IS NULL or log10(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC126
  test("PushUP_FILTER_Test_Boundary_TC126", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where log10(c5_string)is NULL or log10(c5_string) is NOT NULL""",
      s"""select c5_string from Test_Boundary_hive where log10(c5_string)is NULL or log10(c5_string) is NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC127
  test("PushUP_FILTER_Test_Boundary_TC127", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where log10(c1_int) IS NULL and log10(c1_int) IS NOT NULL and log10(c1_int)<=2.147483647E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where log10(c1_int) IS NULL and log10(c1_int) IS NOT NULL and log10(c1_int)<=2.147483647E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC128
  test("PushUP_FILTER_Test_Boundary_TC128", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where log2(c1_int)=30.999999999328196 or log2(c1_int)=11.19537220740274""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC129
  test("PushUP_FILTER_Test_Boundary_TC129", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where log2(c1_int)=30.999999999328196 or log2(c1_int)=11.19537220740274""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC130
  test("PushUP_FILTER_Test_Boundary_TC130", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where log2(c1_int)=30.999999999328196 or log2(c1_int)=11.19537220740274""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC131
  test("PushUP_FILTER_Test_Boundary_TC131", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where log2(c1_int)=30.999999999328196 or log2(c1_int)=11.19537220740274""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC132
  test("PushUP_FILTER_Test_Boundary_TC132", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where log2(c1_int)=30.999999999328196 or log2(c1_int)=11.19537220740274""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC133
  test("PushUP_FILTER_Test_Boundary_TC133", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where log2(c1_int) IS NULL or log2(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where log2(c1_int) IS NULL or log2(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC134
  test("PushUP_FILTER_Test_Boundary_TC134", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where log2(c1_int) IS NULL or log2(c1_int) IS NOT NULL or log2(c1_int)=10.26912667914942""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC135
  test("PushUP_FILTER_Test_Boundary_TC135", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where log2(c2_Bigint)=43.66827237527655  or log2(c2_Bigint)=7.76004068088038""",
      s"""select c2_Bigint from Test_Boundary_hive where log2(c2_Bigint)=43.66827237527655  or log2(c2_Bigint)=7.76004068088038""")
  }


  //PushUP_FILTER_Test_Boundary_TC136
  test("PushUP_FILTER_Test_Boundary_TC136", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where log2(c2_Bigint) IS NULL or log2(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where log2(c2_Bigint) IS NULL or log2(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC137
  test("PushUP_FILTER_Test_Boundary_TC137", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where log2(c3_Decimal)=-2.091864070698393 or log2(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where log2(c3_Decimal)=-2.091864070698393 or log2(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC138
  test("PushUP_FILTER_Test_Boundary_TC138", Include) {
    sql(s"""select c4_double from Test_Boundary where log2(c4_double)= -1070.6780719051126 or log2(c4_double)=12.07815080773465""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC139
  test("PushUP_FILTER_Test_Boundary_TC139", Include) {
    sql(s"""select c4_double from Test_Boundary where log2(c4_double) IS NULL or log2(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC140
  test("PushUP_FILTER_Test_Boundary_TC140", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where log2(c5_string)is NULL or log2(c5_string) is NOT NULL""",
      s"""select c5_string from Test_Boundary_hive where log2(c5_string)is NULL or log2(c5_string) is NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC141
  test("PushUP_FILTER_Test_Boundary_TC141", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where log2(c1_int) IS NULL and log2(c1_int) IS NOT NULL and log2(c1_int)>=30.999999999328196""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where log2(c1_int) IS NULL and log2(c1_int) IS NOT NULL and log2(c1_int)>=30.999999999328196""")
  }


  //PushUP_FILTER_Test_Boundary_TC142
  test("PushUP_FILTER_Test_Boundary_TC142", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where log(c1_int,1)=0.0 or log(c1_int,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC143
  test("PushUP_FILTER_Test_Boundary_TC143", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where log(c1_int,1)=0.0 or log(c1_int,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC144
  test("PushUP_FILTER_Test_Boundary_TC144", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where log(c1_int,1)=0.0 or log(c1_int,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC145
  test("PushUP_FILTER_Test_Boundary_TC145", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where log(c1_int,1)=0.0 or log(c1_int,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC146
  test("PushUP_FILTER_Test_Boundary_TC146", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where log(c1_int,1)=0.0 or log(c1_int,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC147
  test("PushUP_FILTER_Test_Boundary_TC147", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where log(c1_int,1) IS NULL or log(c1_int,1) IS NOT NULL or log(c1_int,1)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC148
  test("PushUP_FILTER_Test_Boundary_TC148", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where log(c2_Bigint,1)=0.0  or log(c2_Bigint,1) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where log(c2_Bigint,1)=0.0  or log(c2_Bigint,1) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC149
  test("PushUP_FILTER_Test_Boundary_TC149", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where log(c3_Decimal,1)=-0.0 or log(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where log(c3_Decimal,1)=-0.0 or log(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC150
  test("PushUP_FILTER_Test_Boundary_TC150", Include) {
    sql(s"""select c4_double from Test_Boundary where log(c4_double,1)= -0.0 or log(c4_double,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC151
  test("PushUP_FILTER_Test_Boundary_TC151", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where log(c5_string,1)is NULL or log(c5_string,1) is NOT NULL""",
      s"""select c5_string from Test_Boundary_hive where log(c5_string,1)is NULL or log(c5_string,1) is NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC152
  test("PushUP_FILTER_Test_Boundary_TC152", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where log(c1_int,1) IS NULL and log(c1_int,1) >=0.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where log(c1_int,1) IS NULL and log(c1_int,1) >=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC153
  test("PushUP_FILTER_Test_Boundary_TC153", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC154
  test("PushUP_FILTER_Test_Boundary_TC154", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC155
  test("PushUP_FILTER_Test_Boundary_TC155", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC156
  test("PushUP_FILTER_Test_Boundary_TC156", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC157
  test("PushUP_FILTER_Test_Boundary_TC157", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where pow(c1_int,1)=2.147483647E9 or pow(c1_int,1)=-2.147483645E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC158
  test("PushUP_FILTER_Test_Boundary_TC158", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where pow(c1_int,1) IS NULL or pow(c1_int,1) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where pow(c1_int,1) IS NULL or pow(c1_int,1) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC159
  test("PushUP_FILTER_Test_Boundary_TC159", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where pow(c1_int,1) IS NULL or pow(c1_int,1) IS NOT NULL or pow(c1_int,1)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC160
  test("PushUP_FILTER_Test_Boundary_TC160", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where pow(c2_Bigint,1)=9.223372036854776E18  or pow(c2_Bigint,1)=-9.223372036854776E18""",
      s"""select c2_Bigint from Test_Boundary_hive where pow(c2_Bigint,1)=9.223372036854776E18  or pow(c2_Bigint,1)=-9.223372036854776E18""")
  }


  //PushUP_FILTER_Test_Boundary_TC161
  test("PushUP_FILTER_Test_Boundary_TC161", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where pow(c2_Bigint,1) IS NULL or pow(c2_Bigint,1) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where pow(c2_Bigint,1) IS NULL or pow(c2_Bigint,1) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC162
  test("PushUP_FILTER_Test_Boundary_TC162", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where pow(c3_Decimal,1)=0.12345678900987654 or pow(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where pow(c3_Decimal,1)=0.12345678900987654 or pow(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC164
  test("PushUP_FILTER_Test_Boundary_TC164", Include) {
    sql(s"""select c4_double from Test_Boundary where pow(c4_double,1) IS NULL or pow(c4_double,1)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC165
  test("PushUP_FILTER_Test_Boundary_TC165", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where pow(c5_string,1)=0.0 or pow(c5_string,1) is NULL""",
      s"""select c5_string from Test_Boundary_hive where pow(c5_string,1)=0.0 or pow(c5_string,1) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC166
  test("PushUP_FILTER_Test_Boundary_TC166", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where pow(c1_int,1) IS NULL and pow(c1_int,1) <=2.147483647E9  and pow(c1_int,1)>=-2.147483648E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where pow(c1_int,1) IS NULL and pow(c1_int,1) <=2.147483647E9  and pow(c1_int,1)>=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC167
  test("PushUP_FILTER_Test_Boundary_TC167", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC168
  test("PushUP_FILTER_Test_Boundary_TC168", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""")
  }


  //PushUP_FILTER_Test_Boundary_TC169
  test("PushUP_FILTER_Test_Boundary_TC169", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""")
  }


  //PushUP_FILTER_Test_Boundary_TC170
  test("PushUP_FILTER_Test_Boundary_TC170", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC171
  test("PushUP_FILTER_Test_Boundary_TC171", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where sqrt(c1_int)=46340.950001051984 or sqrt(c1_int)='NaN'""")
  }


  //PushUP_FILTER_Test_Boundary_TC172
  test("PushUP_FILTER_Test_Boundary_TC172", Include) {
    checkAnswer(s"""select c1_int from Test_Boundary where sqrt(c1_int) IS NULL or sqrt(c1_int) IS NOT NULL""",
      s"""select c1_int from Test_Boundary_hive where sqrt(c1_int) IS NULL or sqrt(c1_int) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC173
  test("PushUP_FILTER_Test_Boundary_TC173", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where sqrt(c1_int) IS NULL or sqrt(c1_int) IS NOT NULL or sqrt(c1_int)=46340.950001051984""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC174
  test("PushUP_FILTER_Test_Boundary_TC174", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where sqrt(c2_Bigint)=3.03700049997605E9  or sqrt(c2_Bigint)=0.0""",
      s"""select c2_Bigint from Test_Boundary_hive where sqrt(c2_Bigint)=3.03700049997605E9  or sqrt(c2_Bigint)=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC175
  test("PushUP_FILTER_Test_Boundary_TC175", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where sqrt(c2_Bigint) IS NULL or sqrt(c2_Bigint) IS NOT NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where sqrt(c2_Bigint) IS NULL or sqrt(c2_Bigint) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC176
  test("PushUP_FILTER_Test_Boundary_TC176", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where sqrt(c3_Decimal)=0.3513641828785008 or sqrt(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where sqrt(c3_Decimal)=0.3513641828785008 or sqrt(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC177
  test("PushUP_FILTER_Test_Boundary_TC177", Include) {
    sql(s"""select c4_double from Test_Boundary where sqrt(c4_double)= 1.3407807929942596E154 or sqrt(c4_double)=93.62157870918435""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC178
  test("PushUP_FILTER_Test_Boundary_TC178", Include) {
    sql(s"""select c4_double from Test_Boundary where sqrt(c4_double) IS NULL or sqrt(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC179
  test("PushUP_FILTER_Test_Boundary_TC179", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where sqrt(c5_string)=0.0 or sqrt(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where sqrt(c5_string)=0.0 or sqrt(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC180
  test("PushUP_FILTER_Test_Boundary_TC180", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where sqrt(c1_int) IS NULL and sqrt(c1_int) <=46340.950001051984  and sqrt(c1_int)>=-2.147483648E9""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where sqrt(c1_int) IS NULL and sqrt(c1_int) <=46340.950001051984  and sqrt(c1_int)>=-2.147483648E9""")
  }


  //PushUP_FILTER_Test_Boundary_TC181
  test("PushUP_FILTER_Test_Boundary_TC181", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where pmod(c1_int,1)=0 or pmod(c1_int,1)IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC182
  test("PushUP_FILTER_Test_Boundary_TC182", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where pmod(c1_int,1)=0 or pmod(c1_int,1)IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC183
  test("PushUP_FILTER_Test_Boundary_TC183", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where pmod(c1_int,1)=0 or pmod(c1_int,1)IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC184
  test("PushUP_FILTER_Test_Boundary_TC184", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where pmod(c1_int,1)=0 or pmod(c1_int,1)IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC185
  test("PushUP_FILTER_Test_Boundary_TC185", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where pmod(c1_int,1)=0 or pmod(c1_int,1)IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC186
  test("PushUP_FILTER_Test_Boundary_TC186", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where pmod(c1_int,1) IS NULL or pmod(c1_int,1) IS NOT NULL or pmod(c1_int,1)=0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC187
  test("PushUP_FILTER_Test_Boundary_TC187", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where pmod(c2_Bigint,1)=0  or pmod(c2_Bigint,1) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where pmod(c2_Bigint,1)=0  or pmod(c2_Bigint,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC188
  test("PushUP_FILTER_Test_Boundary_TC188", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where pmod(c3_Decimal,1) IS NULL or pmod(c3_Decimal,1) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where pmod(c3_Decimal,1) IS NULL or pmod(c3_Decimal,1) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC190
  test("PushUP_FILTER_Test_Boundary_TC190", Include) {
    sql(s"""select c4_double from Test_Boundary where pmod(c4_double,1) IS NULL or pmod(c4_double,1)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC191
  test("PushUP_FILTER_Test_Boundary_TC191", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where pmod(c5_string,1)=0.0 or pmod(c5_string,1) is NULL""",
      s"""select c5_string from Test_Boundary_hive where pmod(c5_string,1)=0.0 or pmod(c5_string,1) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC192
  test("PushUP_FILTER_Test_Boundary_TC192", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where pmod(c1_int,1) IS NULL and pmod(c1_int,1) <=0.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where pmod(c1_int,1) IS NULL and pmod(c1_int,1) <=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC193
  test("PushUP_FILTER_Test_Boundary_TC193", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""")
  }


  //PushUP_FILTER_Test_Boundary_TC194
  test("PushUP_FILTER_Test_Boundary_TC194", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""")
  }


  //PushUP_FILTER_Test_Boundary_TC195
  test("PushUP_FILTER_Test_Boundary_TC195", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC196
  test("PushUP_FILTER_Test_Boundary_TC196", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC197
  test("PushUP_FILTER_Test_Boundary_TC197", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where sin(c1_int)=0.18796200317975467 or sin(c1_int)=-0.18796200317975467""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC198
  test("PushUP_FILTER_Test_Boundary_TC198", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where sin(c1_int) IS NULL or sin(c1_int) IS NOT NULL or sin(c1_int)=0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC199
  test("PushUP_FILTER_Test_Boundary_TC199", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where sin(c2_Bigint)=0  or sin(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where sin(c2_Bigint)=0  or sin(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC200
  test("PushUP_FILTER_Test_Boundary_TC200", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where sin(c3_Decimal) IS NULL or sin(c3_Decimal) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where sin(c3_Decimal) IS NULL or sin(c3_Decimal) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC202
  test("PushUP_FILTER_Test_Boundary_TC202", Include) {
    sql(s"""select c4_double from Test_Boundary where sin(c4_double) IS NULL or sin(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC203
  test("PushUP_FILTER_Test_Boundary_TC203", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where sin(c5_string)=0.0 or sin(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where sin(c5_string)=0.0 or sin(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC204
  test("PushUP_FILTER_Test_Boundary_TC204", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where sin(c1_int) IS NULL and sin(c1_int) <=0.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where sin(c1_int) IS NULL and sin(c1_int) <=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC205
  test("PushUP_FILTER_Test_Boundary_TC205", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where asin(c1_int)=0.0 or asin(c1_int) IS NULL""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where asin(c1_int)=0.0 or asin(c1_int) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC206
  test("PushUP_FILTER_Test_Boundary_TC206", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where asin(c1_int)=0.0 or asin(c1_int) IS NULL""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where asin(c1_int)=0.0 or asin(c1_int) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC207
  test("PushUP_FILTER_Test_Boundary_TC207", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where asin(c1_int)=0.0 or asin(c1_int) IS NULL""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where asin(c1_int)=0.0 or asin(c1_int) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC208
  test("PushUP_FILTER_Test_Boundary_TC208", Include) {
    checkAnswer(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where asin(c1_int)=0.0 or asin(c1_int) IS NULL""",
      s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary_hive where asin(c1_int)=0.0 or asin(c1_int) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC209
  test("PushUP_FILTER_Test_Boundary_TC209", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where asin(c1_int)=0.0 or asin(c1_int) IS NULL""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where asin(c1_int)=0.0 or asin(c1_int) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC210
  test("PushUP_FILTER_Test_Boundary_TC210", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where asin(c1_int) IS NULL or asin(c1_int) IS NOT NULL or asin(c1_int)=0.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC211
  test("PushUP_FILTER_Test_Boundary_TC211", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where asin(c2_Bigint)=0  or asin(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where asin(c2_Bigint)=0  or asin(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC212
  test("PushUP_FILTER_Test_Boundary_TC212", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where asin(c3_Decimal) IS NULL or asin(c3_Decimal) IS NOT NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where asin(c3_Decimal) IS NULL or asin(c3_Decimal) IS NOT NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC214
  test("PushUP_FILTER_Test_Boundary_TC214", Include) {
    sql(s"""select c4_double from Test_Boundary where asin(c4_double) IS NULL or asin(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC215
  test("PushUP_FILTER_Test_Boundary_TC215", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where asin(c5_string)=0.0 or asin(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where asin(c5_string)=0.0 or asin(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC216
  test("PushUP_FILTER_Test_Boundary_TC216", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where asin(c1_int) IS NULL and asin(c1_int) <=0.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where asin(c1_int) IS NULL and asin(c1_int) <=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC217
  test("PushUP_FILTER_Test_Boundary_TC217", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where cos(c1_int)=-0.982176300549272 or cos(c1_int) = 0.23781619457280337""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC218
  test("PushUP_FILTER_Test_Boundary_TC218", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where cos(c1_int)=-0.982176300549272 or cos(c1_int) = 0.23781619457280337""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC219
  test("PushUP_FILTER_Test_Boundary_TC219", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where cos(c1_int)=-0.982176300549272 or cos(c1_int) = 0.23781619457280337""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC220
  test("PushUP_FILTER_Test_Boundary_TC220", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where cos(c1_int)=-0.982176300549272 or cos(c1_int) = 0.23781619457280337""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC221
  test("PushUP_FILTER_Test_Boundary_TC221", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where cos(c1_int)=-0.982176300549272 or cos(c1_int) = 0.23781619457280337""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC222
  test("PushUP_FILTER_Test_Boundary_TC222", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where cos(c1_int) IS NULL or cos(c1_int) IS NOT NULL or cos(c1_int)=-0.982176300549272""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC223
  test("PushUP_FILTER_Test_Boundary_TC223", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where cos(c2_Bigint)=0.011800076512800236  or cos(c2_Bigint) =-0.7985506235875843""",
      s"""select c2_Bigint from Test_Boundary_hive where cos(c2_Bigint)=0.011800076512800236  or cos(c2_Bigint) =-0.7985506235875843""")
  }


  //PushUP_FILTER_Test_Boundary_TC224
  test("PushUP_FILTER_Test_Boundary_TC224", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where cos(c3_Decimal) = 0.9923888851124961 or cos(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where cos(c3_Decimal) = 0.9923888851124961 or cos(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC225
  test("PushUP_FILTER_Test_Boundary_TC225", Include) {
    sql(s"""select c4_double from Test_Boundary where cos(c4_double)= -0.9999876894265599 or cos(c4_double)=0.3915244017195126""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC226
  test("PushUP_FILTER_Test_Boundary_TC226", Include) {
    sql(s"""select c4_double from Test_Boundary where cos(c4_double) IS NULL or cos(c4_double)IS NOT NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC227
  test("PushUP_FILTER_Test_Boundary_TC227", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where cos(c5_string)=1.0 or cos(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where cos(c5_string)=1.0 or cos(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC228
  test("PushUP_FILTER_Test_Boundary_TC228", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where cos(c1_int) IS NULL and cos(c1_int) >=0.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where cos(c1_int) IS NULL and cos(c1_int) >=0.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC229
  test("PushUP_FILTER_Test_Boundary_TC229", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where acos(c1_int)=1.5707963267948966 or acos(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC230
  test("PushUP_FILTER_Test_Boundary_TC230", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where acos(c1_int)=1.5707963267948966 or acos(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC231
  test("PushUP_FILTER_Test_Boundary_TC231", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where acos(c1_int)=1.5707963267948966 or acos(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC232
  test("PushUP_FILTER_Test_Boundary_TC232", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where acos(c1_int)=1.5707963267948966 or acos(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC233
  test("PushUP_FILTER_Test_Boundary_TC233", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where acos(c1_int)=1.5707963267948966 or acos(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC234
  test("PushUP_FILTER_Test_Boundary_TC234", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where acos(c1_int) IS NULL or acos(c1_int) IS NOT NULL or acos(c1_int)=1.5707963267948966""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC235
  test("PushUP_FILTER_Test_Boundary_TC235", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where acos(c2_Bigint)=1.5707963267948966  or acos(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where acos(c2_Bigint)=1.5707963267948966  or acos(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC236
  test("PushUP_FILTER_Test_Boundary_TC236", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where acos(c3_Decimal) = 1.447023754358227 or acos(c3_Decimal) =0.45102681179626236""",
      s"""select c3_Decimal from Test_Boundary_hive where acos(c3_Decimal) = 1.447023754358227 or acos(c3_Decimal) =0.45102681179626236""")
  }


  //PushUP_FILTER_Test_Boundary_TC237
  test("PushUP_FILTER_Test_Boundary_TC237", Include) {
    sql(s"""select c4_double from Test_Boundary where acos(c4_double)= 1.5707963267948966 or acos(c4_double) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC238
  test("PushUP_FILTER_Test_Boundary_TC238", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where acos(c5_string)=1.5707963267948966 or acos(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where acos(c5_string)=1.5707963267948966 or acos(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC239
  test("PushUP_FILTER_Test_Boundary_TC239", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where acos(c1_int) IS NULL and acos(c1_int) >0.5707963267948966""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where acos(c1_int) IS NULL and acos(c1_int) >0.5707963267948966""")
  }


  //PushUP_FILTER_Test_Boundary_TC240
  test("PushUP_FILTER_Test_Boundary_TC240", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where tan(c1_int)=-0.19137297761576905 or tan(c1_int)=4.084289455298593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC241
  test("PushUP_FILTER_Test_Boundary_TC241", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where tan(c1_int)=-0.19137297761576905 or tan(c1_int)=4.084289455298593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC242
  test("PushUP_FILTER_Test_Boundary_TC242", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where tan(c1_int)=-0.19137297761576905 or tan(c1_int)=4.084289455298593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC243
  test("PushUP_FILTER_Test_Boundary_TC243", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where tan(c1_int)=-0.19137297761576905 or tan(c1_int)=4.084289455298593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC244
  test("PushUP_FILTER_Test_Boundary_TC244", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where tan(c1_int)=-0.19137297761576905 or tan(c1_int)=4.084289455298593""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC245
  test("PushUP_FILTER_Test_Boundary_TC245", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where tan(c1_int) IS NULL or tan(c1_int) IS NOT NULL or tan(c1_int)=1.5707963267948966""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC246
  test("PushUP_FILTER_Test_Boundary_TC246", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where tan(c2_Bigint)=-84.73931296875567  or tan(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where tan(c2_Bigint)=-84.73931296875567  or tan(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC247
  test("PushUP_FILTER_Test_Boundary_TC247", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where tan(c3_Decimal) = 1.0000000033333334E-4 or tan(c3_Decimal) =0.05405254929434814""",
      s"""select c3_Decimal from Test_Boundary_hive where tan(c3_Decimal) = 1.0000000033333334E-4 or tan(c3_Decimal) =0.05405254929434814""")
  }


  //PushUP_FILTER_Test_Boundary_TC248
  test("PushUP_FILTER_Test_Boundary_TC248", Include) {
    sql(s"""select c4_double from Test_Boundary where tan(c4_double)= -0.004962015874444895 or tan(c4_double) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC249
  test("PushUP_FILTER_Test_Boundary_TC249", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where tan(c5_string)=0.0 or tan(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where tan(c5_string)=0.0 or tan(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC250
  test("PushUP_FILTER_Test_Boundary_TC250", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where tan(c1_int) IS NULL and tan(c1_int) >4.084289455298593""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where tan(c1_int) IS NULL and tan(c1_int) >4.084289455298593""")
  }


  //PushUP_FILTER_Test_Boundary_TC251
  test("PushUP_FILTER_Test_Boundary_TC251", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where atan(c1_int)=1.5707963263292353 or atan(c1_int)=-1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC252
  test("PushUP_FILTER_Test_Boundary_TC252", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where atan(c1_int)=1.5707963263292353 or atan(c1_int)=-1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC253
  test("PushUP_FILTER_Test_Boundary_TC253", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where atan(c1_int)=1.5707963263292353 or atan(c1_int)=-1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC254
  test("PushUP_FILTER_Test_Boundary_TC254", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where atan(c1_int)=1.5707963263292353 or atan(c1_int)=-1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC255
  test("PushUP_FILTER_Test_Boundary_TC255", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where atan(c1_int)=1.5707963263292353 or atan(c1_int)=-1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC256
  test("PushUP_FILTER_Test_Boundary_TC256", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where atan(c1_int) IS NULL or atan(c1_int) IS NOT NULL or atan(c1_int)=1.5707963263292353""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC257
  test("PushUP_FILTER_Test_Boundary_TC257", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where atan(c2_Bigint)=1.5707963267948966  or atan(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where atan(c2_Bigint)=1.5707963267948966  or atan(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC258
  test("PushUP_FILTER_Test_Boundary_TC258", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where atan(c3_Decimal) = 0.12283523778103266 or atan(c3_Decimal) =9.999999966666667E-5""",
      s"""select c3_Decimal from Test_Boundary_hive where atan(c3_Decimal) = 0.12283523778103266 or atan(c3_Decimal) =9.999999966666667E-5""")
  }


  //PushUP_FILTER_Test_Boundary_TC260
  test("PushUP_FILTER_Test_Boundary_TC260", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where atan(c5_string)=0.0 or atan(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where atan(c5_string)=0.0 or atan(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC261
  test("PushUP_FILTER_Test_Boundary_TC261", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where atan(c1_int) IS NULL and atan(c1_int) >0.5707963263292353""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where atan(c1_int) IS NULL and atan(c1_int) >0.5707963263292353""")
  }


  //PushUP_FILTER_Test_Boundary_TC262
  test("PushUP_FILTER_Test_Boundary_TC262", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where radians(c1_int)=-3.748066023797906E7 or radians(c1_int)=3.748066025543235E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC263
  test("PushUP_FILTER_Test_Boundary_TC263", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where radians(c1_int)=-3.748066023797906E7 or radians(c1_int)=3.748066025543235E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC264
  test("PushUP_FILTER_Test_Boundary_TC264", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where radians(c1_int)=-3.748066023797906E7 or radians(c1_int)=3.748066025543235E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC265
  test("PushUP_FILTER_Test_Boundary_TC265", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where radians(c1_int)=-3.748066023797906E7 or radians(c1_int)=3.748066025543235E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC266
  test("PushUP_FILTER_Test_Boundary_TC266", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where radians(c1_int)=-3.748066023797906E7 or radians(c1_int)=3.748066025543235E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC267
  test("PushUP_FILTER_Test_Boundary_TC267", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where radians(c1_int) IS NULL or radians(c1_int) IS NOT NULL or radians(c1_int)=-3.748066023797906E7""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC268
  test("PushUP_FILTER_Test_Boundary_TC268", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where radians(c2_Bigint)=-1.60978210179491616E17  or radians(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where radians(c2_Bigint)=-1.60978210179491616E17  or radians(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC269
  test("PushUP_FILTER_Test_Boundary_TC269", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where radians(c3_Decimal) = 0.013962634015954637 or radians(c3_Decimal) =9.424777960769378E-4""",
      s"""select c3_Decimal from Test_Boundary_hive where radians(c3_Decimal) = 0.013962634015954637 or radians(c3_Decimal) =9.424777960769378E-4""")
  }


  //PushUP_FILTER_Test_Boundary_TC271
  test("PushUP_FILTER_Test_Boundary_TC271", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where radians(c5_string)=0.0 or radians(c5_string) is NULL""",
      s"""select c5_string from Test_Boundary_hive where radians(c5_string)=0.0 or radians(c5_string) is NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC272
  test("PushUP_FILTER_Test_Boundary_TC272", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where radians(c1_int) IS NULL and radians(c1_int) >2.748066025543235E7""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where radians(c1_int) IS NULL and radians(c1_int) >2.748066025543235E7""")
  }


  //PushUP_FILTER_Test_Boundary_TC273
  test("PushUP_FILTER_Test_Boundary_TC273", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where positive(c1_int)=-2147483648 or positive(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC274
  test("PushUP_FILTER_Test_Boundary_TC274", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where positive(c1_int)=-2147483648 or positive(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC275
  test("PushUP_FILTER_Test_Boundary_TC275", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where positive(c1_int)=-2147483648 or positive(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC276
  test("PushUP_FILTER_Test_Boundary_TC276", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where positive(c1_int)=-2147483648 or positive(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC277
  test("PushUP_FILTER_Test_Boundary_TC277", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where positive(c1_int)=-2147483648 or positive(c1_int)=2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC278
  test("PushUP_FILTER_Test_Boundary_TC278", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where positive(c1_int) IS NULL or positive(c1_int) IS NOT NULL or positive(c1_int)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC279
  test("PushUP_FILTER_Test_Boundary_TC279", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where positive(c2_Bigint)=-9223372036854775808  or positive(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where positive(c2_Bigint)=-9223372036854775808  or positive(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC280
  test("PushUP_FILTER_Test_Boundary_TC280", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where positive(c3_Decimal) = 0.12345678900987654321123456789009876544 or positive(c3_Decimal) =0E-38""",
      s"""select c3_Decimal from Test_Boundary_hive where positive(c3_Decimal) = 0.12345678900987654321123456789009876544 or positive(c3_Decimal) =0E-38""")
  }


  //PushUP_FILTER_Test_Boundary_TC282
  test("PushUP_FILTER_Test_Boundary_TC282", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where positive(c1_int) IS NULL and positive(c1_int) >-2147483648""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where positive(c1_int) IS NULL and positive(c1_int) >-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC283
  test("PushUP_FILTER_Test_Boundary_TC283", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where negative(c1_int)=-2147483647 or negative(c1_int)=2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC284
  test("PushUP_FILTER_Test_Boundary_TC284", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where negative(c1_int)=-2147483647 or negative(c1_int)=2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC285
  test("PushUP_FILTER_Test_Boundary_TC285", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where negative(c1_int)=-2147483647 or negative(c1_int)=2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC286
  test("PushUP_FILTER_Test_Boundary_TC286", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where negative(c1_int)=-2147483647 or negative(c1_int)=2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC287
  test("PushUP_FILTER_Test_Boundary_TC287", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where negative(c1_int)=-2147483647 or negative(c1_int)=2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC288
  test("PushUP_FILTER_Test_Boundary_TC288", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where negative(c1_int) IS NULL or negative(c1_int) IS NOT NULL or negative(c1_int)=-2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC289
  test("PushUP_FILTER_Test_Boundary_TC289", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where negative(c2_Bigint)=-9223372036854775807  or negative(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where negative(c2_Bigint)=-9223372036854775807  or negative(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC290
  test("PushUP_FILTER_Test_Boundary_TC290", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where negative(c3_Decimal) = 0E-38 or negative(c3_Decimal) =-0.12345678900987654321123456789009876544""",
      s"""select c3_Decimal from Test_Boundary_hive where negative(c3_Decimal) = 0E-38 or negative(c3_Decimal) =-0.12345678900987654321123456789009876544""")
  }


  //PushUP_FILTER_Test_Boundary_TC291
  test("PushUP_FILTER_Test_Boundary_TC291", Include) {
    sql(s"""select c4_double from Test_Boundary where negative(c4_double)=  -1.7976931348623157E308  or negative(c4_double) IS NULL or negative(c4_double) =-8765.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC292
  test("PushUP_FILTER_Test_Boundary_TC292", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where negative(c1_int) IS NULL and negative(c1_int) >-2147483648""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where negative(c1_int) IS NULL and negative(c1_int) >-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC293
  test("PushUP_FILTER_Test_Boundary_TC293", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where sign(c1_int)=-1.0 or sign(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC294
  test("PushUP_FILTER_Test_Boundary_TC294", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where sign(c1_int)=-1.0 or sign(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC295
  test("PushUP_FILTER_Test_Boundary_TC295", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where sign(c1_int)=-1.0 or sign(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC296
  test("PushUP_FILTER_Test_Boundary_TC296", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where sign(c1_int)=-1.0 or sign(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC297
  test("PushUP_FILTER_Test_Boundary_TC297", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where sign(c1_int)=-1.0 or sign(c1_int)=1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC298
  test("PushUP_FILTER_Test_Boundary_TC298", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where sign(c1_int) IS NULL or sign(c1_int) IS NOT NULL or sign(c1_int)=-1.0""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC299
  test("PushUP_FILTER_Test_Boundary_TC299", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where sign(c2_Bigint)=-1.0  or sign(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where sign(c2_Bigint)=-1.0  or sign(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC300
  test("PushUP_FILTER_Test_Boundary_TC300", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where sign(c3_Decimal) = 0.0 or sign(c3_Decimal) =0.1""",
      s"""select c3_Decimal from Test_Boundary_hive where sign(c3_Decimal) = 0.0 or sign(c3_Decimal) =0.1""")
  }


  //PushUP_FILTER_Test_Boundary_TC301
  test("PushUP_FILTER_Test_Boundary_TC301", Include) {
    sql(s"""select c4_double from Test_Boundary where sign(c4_double)=  1.0  or sign(c4_double) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC302
  test("PushUP_FILTER_Test_Boundary_TC302", Include) {
    checkAnswer(s"""select c5_string from Test_Boundary where sign(c5_string)=  0.0  or sign(c5_string) IS NULL""",
      s"""select c5_string from Test_Boundary_hive where sign(c5_string)=  0.0  or sign(c5_string) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC303
  test("PushUP_FILTER_Test_Boundary_TC303", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where sign(c1_int) IS NULL and sign(c1_int) >1.0""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where sign(c1_int) IS NULL and sign(c1_int) >1.0""")
  }


  //PushUP_FILTER_Test_Boundary_TC304
  test("PushUP_FILTER_Test_Boundary_TC304", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where factorial(c1_int)=1 or factorial(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC305
  test("PushUP_FILTER_Test_Boundary_TC305", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where factorial(c1_int)=1 or factorial(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC306
  test("PushUP_FILTER_Test_Boundary_TC306", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where factorial(c1_int)=1 or factorial(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC307
  test("PushUP_FILTER_Test_Boundary_TC307", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where factorial(c1_int)=1 or factorial(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC308
  test("PushUP_FILTER_Test_Boundary_TC308", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where factorial(c1_int)=1 or factorial(c1_int) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC309
  test("PushUP_FILTER_Test_Boundary_TC309", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where factorial(c1_int) IS NULL or factorial(c1_int) IS NOT NULL or factorial(c1_int)=1""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC310
  test("PushUP_FILTER_Test_Boundary_TC310", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where factorial(c2_Bigint)=6  or factorial(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where factorial(c2_Bigint)=6  or factorial(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC311
  test("PushUP_FILTER_Test_Boundary_TC311", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where factorial(c3_Decimal) = 1 or factorial(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where factorial(c3_Decimal) = 1 or factorial(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC312
  test("PushUP_FILTER_Test_Boundary_TC312", Include) {
    sql(s"""select c4_double from Test_Boundary where factorial(c4_double)=  1  or factorial(c4_double) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC313
  test("PushUP_FILTER_Test_Boundary_TC313", Include) {
    sql(s"""select c4_double from Test_Boundary where factorial(c5_string)=  0.0  or factorial(c5_string) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC314
  test("PushUP_FILTER_Test_Boundary_TC314", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where factorial(c1_int) IS NULL and factorial(c1_int) >1""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where factorial(c1_int) IS NULL and factorial(c1_int) >1""")
  }


  //PushUP_FILTER_Test_Boundary_TC315
  test("PushUP_FILTER_Test_Boundary_TC315", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC316
  test("PushUP_FILTER_Test_Boundary_TC316", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC317
  test("PushUP_FILTER_Test_Boundary_TC317", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC318
  test("PushUP_FILTER_Test_Boundary_TC318", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC319
  test("PushUP_FILTER_Test_Boundary_TC319", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC320
  test("PushUP_FILTER_Test_Boundary_TC320", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where cbrt(c1_int)=-1290.1591550923501 or cbrt(c1_int) =1290.159154892091""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC321
  test("PushUP_FILTER_Test_Boundary_TC321", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where cbrt(c2_Bigint)=-2097152.0  or cbrt(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where cbrt(c2_Bigint)=-2097152.0  or cbrt(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC322
  test("PushUP_FILTER_Test_Boundary_TC322", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where cbrt(c3_Decimal) =0.9283177667225558 or cbrt(c3_Decimal) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where cbrt(c3_Decimal) =0.9283177667225558 or cbrt(c3_Decimal) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC324
  test("PushUP_FILTER_Test_Boundary_TC324", Include) {
    sql(s"""select c4_double from Test_Boundary where cbrt(c5_string)=  0.0  or cbrt(c5_string) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC325
  test("PushUP_FILTER_Test_Boundary_TC325", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where cbrt(c1_int) IS NULL and cbrt(c1_int) >-1290.1591550923501""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where cbrt(c1_int) IS NULL and cbrt(c1_int) >-1290.1591550923501""")
  }


  //PushUP_FILTER_Test_Boundary_TC326
  test("PushUP_FILTER_Test_Boundary_TC326", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where shiftleft(c1_int,1)= -4294967296 or shiftleft(c1_int,1)=4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC327
  test("PushUP_FILTER_Test_Boundary_TC327", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where shiftleft(c1_int,1)= -4294967296 or shiftleft(c1_int,1)=4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC328
  test("PushUP_FILTER_Test_Boundary_TC328", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where shiftleft(c1_int,1)= -4294967296 or shiftleft(c1_int,1)=4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC329
  test("PushUP_FILTER_Test_Boundary_TC329", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where shiftleft(c1_int,1)= -4294967296 or shiftleft(c1_int,1)=4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC330
  test("PushUP_FILTER_Test_Boundary_TC330", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where shiftleft(c1_int,1)= -4294967296 or shiftleft(c1_int,1)=4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC331
  test("PushUP_FILTER_Test_Boundary_TC331", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where shiftleft(c1_int,1)=-4294967296 or shiftleft(c1_int,1) =4294967294""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC332
  test("PushUP_FILTER_Test_Boundary_TC332", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where shiftleft(c2_Bigint,1)=-2  or shiftleft(c2_Bigint,1) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where shiftleft(c2_Bigint,1)=-2  or shiftleft(c2_Bigint,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC333
  test("PushUP_FILTER_Test_Boundary_TC333", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where shiftleft(c3_Decimal,1) =0 or shiftleft(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where shiftleft(c3_Decimal,1) =0 or shiftleft(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC334
  test("PushUP_FILTER_Test_Boundary_TC334", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftleft(c4_double,1)=  -2   or shiftleft(c4_double,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC335
  test("PushUP_FILTER_Test_Boundary_TC335", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftleft(c5_string,1)= 0  or shiftleft(c5_string,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC336
  test("PushUP_FILTER_Test_Boundary_TC336", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where shiftleft(c1_int,1) IS NULL and shiftleft(c1_int,1) >-4294967296""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where shiftleft(c1_int,1) IS NULL and shiftleft(c1_int,1) >-4294967296""")
  }


  //PushUP_FILTER_Test_Boundary_TC337
  test("PushUP_FILTER_Test_Boundary_TC337", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where shiftright(c1_int,1)= -1073741824 or shiftright(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC338
  test("PushUP_FILTER_Test_Boundary_TC338", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where shiftright(c1_int,1)= -1073741824 or shiftright(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC339
  test("PushUP_FILTER_Test_Boundary_TC339", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where shiftright(c1_int,1)= -1073741824 or shiftright(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC340
  test("PushUP_FILTER_Test_Boundary_TC340", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where shiftright(c1_int,1)= -1073741824 or shiftright(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC341
  test("PushUP_FILTER_Test_Boundary_TC341", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where shiftright(c1_int,1)= -1073741824 or shiftright(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC342
  test("PushUP_FILTER_Test_Boundary_TC342", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where shiftright(c1_int,1)=-1073741824 or shiftright(c1_int,1) =1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC343
  test("PushUP_FILTER_Test_Boundary_TC343", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where shiftright(c2_Bigint,1)=-4611686018427387904  or shiftright(c2_Bigint,1) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where shiftright(c2_Bigint,1)=-4611686018427387904  or shiftright(c2_Bigint,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC344
  test("PushUP_FILTER_Test_Boundary_TC344", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where shiftright(c3_Decimal,1) =0 or shiftright(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where shiftright(c3_Decimal,1) =0 or shiftright(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC345
  test("PushUP_FILTER_Test_Boundary_TC345", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftright(c4_double,1)= 1073741823  or shiftright(c4_double,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC346
  test("PushUP_FILTER_Test_Boundary_TC346", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftright(c5_string,1)= 0  or shiftright(c5_string,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC347
  test("PushUP_FILTER_Test_Boundary_TC347", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where shiftright(c1_int,1) IS NULL and shiftright(c1_int,1) >-1073741824""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where shiftright(c1_int,1) IS NULL and shiftright(c1_int,1) >-1073741824""")
  }


  //PushUP_FILTER_Test_Boundary_TC348
  test("PushUP_FILTER_Test_Boundary_TC348", Include) {
    sql(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC349
  test("PushUP_FILTER_Test_Boundary_TC349", Include) {
    sql(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC350
  test("PushUP_FILTER_Test_Boundary_TC350", Include) {
    sql(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC351
  test("PushUP_FILTER_Test_Boundary_TC351", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC352
  test("PushUP_FILTER_Test_Boundary_TC352", Include) {
    sql(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC353
  test("PushUP_FILTER_Test_Boundary_TC353", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where shiftrightunsigned(c1_int,1)= 9223372035781033984 or shiftrightunsigned(c1_int,1)=1073741823""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC354
  test("PushUP_FILTER_Test_Boundary_TC354", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where shiftrightunsigned(c2_Bigint,1)=4611686018427387903  or shiftrightunsigned(c2_Bigint,1) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where shiftrightunsigned(c2_Bigint,1)=4611686018427387903  or shiftrightunsigned(c2_Bigint,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC355
  test("PushUP_FILTER_Test_Boundary_TC355", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where shiftrightunsigned(c3_Decimal,1) =0 or shiftrightunsigned(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where shiftrightunsigned(c3_Decimal,1) =0 or shiftrightunsigned(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC356
  test("PushUP_FILTER_Test_Boundary_TC356", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftrightunsigned(c4_double,1)= 1073741823  or shiftrightunsigned(c4_double,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC357
  test("PushUP_FILTER_Test_Boundary_TC357", Include) {
    sql(s"""select c4_double from Test_Boundary where shiftrightunsigned(c5_string,1)= 0  or shiftrightunsigned(c5_string,1) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC358
  test("PushUP_FILTER_Test_Boundary_TC358", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where shiftrightunsigned(c1_int,1) IS NULL and shiftrightunsigned(c1_int,1) >9223372035781033983""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where shiftrightunsigned(c1_int,1) IS NULL and shiftrightunsigned(c1_int,1) >9223372035781033983""")
  }


  //PushUP_FILTER_Test_Boundary_TC359
  test("PushUP_FILTER_Test_Boundary_TC359", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC360
  test("PushUP_FILTER_Test_Boundary_TC360", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC361
  test("PushUP_FILTER_Test_Boundary_TC361", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC362
  test("PushUP_FILTER_Test_Boundary_TC362", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC363
  test("PushUP_FILTER_Test_Boundary_TC363", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""")
  }


  //PushUP_FILTER_Test_Boundary_TC364
  test("PushUP_FILTER_Test_Boundary_TC364", Include) {
    sql(s"""select c1_int+1,c1_int+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where greatest(c1_int,c1_int)= 9223372036854775807 or greatest(c1_int,c1_int)=-2147483648""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC365
  test("PushUP_FILTER_Test_Boundary_TC365", Include) {
    checkAnswer(s"""select c1_int-1,c1_int*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where greatest(c1_int,c1_int) IS NULL and greatest(c1_int,c1_int)>9223372036854775806""",
      s"""select c1_int-1,c1_int*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where greatest(c1_int,c1_int) IS NULL and greatest(c1_int,c1_int)>9223372036854775806""")
  }


  //PushUP_FILTER_Test_Boundary_TC366
  test("PushUP_FILTER_Test_Boundary_TC366", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""")
  }


  //PushUP_FILTER_Test_Boundary_TC367
  test("PushUP_FILTER_Test_Boundary_TC367", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""")
  }


  //PushUP_FILTER_Test_Boundary_TC368
  test("PushUP_FILTER_Test_Boundary_TC368", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""")
  }


  //PushUP_FILTER_Test_Boundary_TC369
  test("PushUP_FILTER_Test_Boundary_TC369", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC370
  test("PushUP_FILTER_Test_Boundary_TC370", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""")
  }


  //PushUP_FILTER_Test_Boundary_TC371
  test("PushUP_FILTER_Test_Boundary_TC371", Include) {
    sql(s"""select c1_int+1,c1_int+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where least(c1_int,c1_int)= -9223372036854775808 or least(c1_int,c1_int)=-2147483647""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC372
  test("PushUP_FILTER_Test_Boundary_TC372", Include) {
    checkAnswer(s"""select c1_int-1,c1_int*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where least(c1_int,c1_int) IS NULL and least(c1_int,c1_int) >-9223372036854775808""",
      s"""select c1_int-1,c1_int*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where least(c1_int,c1_int) IS NULL and least(c1_int,c1_int) >-9223372036854775808""")
  }


  //PushUP_FILTER_Test_Boundary_TC373
  test("PushUP_FILTER_Test_Boundary_TC373", Include) {
    checkAnswer(s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""",
      s"""select min(c1_int),max(c1_int),sum(c1_int),avg(c1_int) , count(c1_int), variance(c1_int) from Test_Boundary_hive where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""")
  }


  //PushUP_FILTER_Test_Boundary_TC374
  test("PushUP_FILTER_Test_Boundary_TC374", Include) {
    checkAnswer(s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""",
      s"""select min(c2_Bigint),max(c2_Bigint),sum(c2_Bigint),avg(c2_Bigint) , count(c2_Bigint), variance(c2_Bigint) from Test_Boundary_hive where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""")
  }


  //PushUP_FILTER_Test_Boundary_TC375
  test("PushUP_FILTER_Test_Boundary_TC375", Include) {
    checkAnswer(s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""",
      s"""select min(c3_Decimal),max(c3_Decimal),sum(c3_Decimal),avg(c3_Decimal) , count(c3_Decimal), variance(c3_Decimal) from Test_Boundary_hive where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""")
  }


  //PushUP_FILTER_Test_Boundary_TC376
  test("PushUP_FILTER_Test_Boundary_TC376", Include) {
    sql(s"""select min(c4_double),max(c4_double),sum(c4_double),avg(c4_double) , count(c4_double), variance(c4_double) from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC377
  test("PushUP_FILTER_Test_Boundary_TC377", Include) {
    checkAnswer(s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""",
      s"""select min(c5_string),max(c5_string),sum(c5_string),avg(c5_string) , count(c5_string), variance(c5_string) from Test_Boundary_hive where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""")
  }


  //PushUP_FILTER_Test_Boundary_TC378
  test("PushUP_FILTER_Test_Boundary_TC378", Include) {
    sql(s"""select c1_int+1,c2_Bigint+9.999999,c3_Decimal+1212.121,c4_double+131231.12,c5_string+0.9999 from Test_Boundary where from_unixtime(c1_int)='1970-01-01 05:30:00' or from_unixtime(c1_int)='2038-01-19 08:44:07'""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC379
  test("PushUP_FILTER_Test_Boundary_TC379", Include) {
    checkAnswer(s"""select c2_Bigint from Test_Boundary where from_unixtime(c2_Bigint)='1970-01-01 05:30:00'  or from_unixtime(c2_Bigint) IS NULL""",
      s"""select c2_Bigint from Test_Boundary_hive where from_unixtime(c2_Bigint)='1970-01-01 05:30:00'  or from_unixtime(c2_Bigint) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC380
  test("PushUP_FILTER_Test_Boundary_TC380", Include) {
    checkAnswer(s"""select c3_Decimal from Test_Boundary where from_unixtime(c3_Decimal) ='1970-01-01 05:30:00' or from_unixtime(c3_Decimal,1) IS NULL""",
      s"""select c3_Decimal from Test_Boundary_hive where from_unixtime(c3_Decimal) ='1970-01-01 05:30:00' or from_unixtime(c3_Decimal,1) IS NULL""")
  }


  //PushUP_FILTER_Test_Boundary_TC381
  test("PushUP_FILTER_Test_Boundary_TC381", Include) {
    sql(s"""select c4_double from Test_Boundary where from_unixtime(c4_double)= '1970-01-01 06:46:07'  or from_unixtime(c4_double) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC382
  test("PushUP_FILTER_Test_Boundary_TC382", Include) {
    sql(s"""select c4_double from Test_Boundary where from_unixtime(c5_string)= '1970-01-01 05:30:00'  or from_unixtime(c5_string) IS NULL""").collect
  }


  //PushUP_FILTER_Test_Boundary_TC383
  test("PushUP_FILTER_Test_Boundary_TC383", Include) {
    checkAnswer(s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary where from_unixtime(c1_int) IS NULL and from_unixtime(c1_int) >'1970-01-01 05:30:00'""",
      s"""select c1_int-1,c2_Bigint*9.999999,c3_Decimal/1212.121,c4_double+131231.12,c5_string-0.9999 from Test_Boundary_hive where from_unixtime(c1_int) IS NULL and from_unixtime(c1_int) >'1970-01-01 05:30:00'""")
  }
       
override def afterAll {
sql("drop table if exists test_boundary1")
sql("drop table if exists test_boundary1_hive")
sql("drop table if exists Test_Boundary1")
sql("drop table if exists Test_Boundary1_hive")
sql("drop table if exists test_boundary")
sql("drop table if exists test_boundary_hive")
sql("drop table if exists Test_Boundary")
sql("drop table if exists Test_Boundary_hive")
}
}
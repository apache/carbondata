
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
 * Test Class for sequential to verify all scenerios
 */

class SEQUENTIALTestCase extends QueryTest with BeforeAndAfterAll {
         

//Sequential_QueryType3_TC001_Drop
test("Sequential_QueryType3_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential3""").collect

  sql(s"""drop table if exists  sequential3_hive""").collect

}
       

//Sequential_QueryType13_TC001_Drop
test("Sequential_QueryType13_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential13""").collect

  sql(s"""drop table if exists  sequential13_hive""").collect

}
       

//Sequential_QueryType16_TC003_Drop
test("Sequential_QueryType16_TC003_Drop", Include) {
  sql(s"""drop table if exists  sequential16""").collect

  sql(s"""drop table if exists  sequential16_hive""").collect

}
       

//Sequential_QueryType17_TC001_Drop
test("Sequential_QueryType17_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential17""").collect

  sql(s"""drop table if exists  sequential17_hive""").collect

}
       

//Sequential_QueryType29_TC001_Drop
test("Sequential_QueryType29_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential29""").collect

  sql(s"""drop table if exists  sequential29_hive""").collect

}
       

//Sequential_QueryType40_TC001_Drop
test("Sequential_QueryType40_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential40""").collect

  sql(s"""drop table if exists  sequential40_hive""").collect

}
       

//Sequential_QueryType41_TC001_Drop
test("Sequential_QueryType41_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential41""").collect

  sql(s"""drop table if exists  sequential41_hive""").collect

}
       

//Sequential_QueryType42_TC001_Drop
test("Sequential_QueryType42_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential42""").collect

  sql(s"""drop table if exists  sequential42_hive""").collect

}
       

//Sequential_QueryType43_TC001_Drop
test("Sequential_QueryType43_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential43""").collect

  sql(s"""drop table if exists  sequential43_hive""").collect

}
       

//Sequential_QueryType44_TC001_Drop
test("Sequential_QueryType44_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential44""").collect

  sql(s"""drop table if exists  sequential44_hive""").collect

}
       

//Sequential_QueryType45_TC001_Drop
test("Sequential_QueryType45_TC001_Drop", Include) {
  sql(s"""drop table if exists  sequential45""").collect

  sql(s"""drop table if exists  sequential45_hive""").collect

}
       

//Sequential_QueryType1_TC001
test("Sequential_QueryType1_TC001", Include) {
  sql(s"""create table sequential1 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei","columnproperties.gamePointId.shared_column"="shared.gamePointId",'DICTIONARY_INCLUDE'='gamePointId')""").collect

  sql(s"""create table sequential1_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType1_TC002
test("Sequential_QueryType1_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential1 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential1_hive """).collect

}
       

//Sequential_QueryType1_TC003
test("Sequential_QueryType1_TC003", Include) {
  sql(s"""select count(*) from sequential1""").collect
}
       

//Sequential_QueryType1_TC004
test("Sequential_QueryType1_TC004", Include) {
  sql(s"""drop table if exists  sequential1""").collect

  sql(s"""drop table if exists  sequential1_hive""").collect

}
       

//drop_sequential
test("drop_sequential", Include) {
  sql(s"""drop table if exists  sequential2""").collect

  sql(s"""drop table if exists  sequential2_hive""").collect

}
       

//Sequential_QueryType2_TC001
test("Sequential_QueryType2_TC001", Include) {
  sql(s"""create table sequential2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei","columnproperties.gamePointId.shared_column"="shared.gamePointId",'DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect

  sql(s"""create table sequential2_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType2_TC002
test("Sequential_QueryType2_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential2 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential2_hive """).collect

}
       

//Sequential_QueryType2_TC003
test("Sequential_QueryType2_TC003", Include) {
  sql(s"""drop table if exists  sequential2""").collect

  sql(s"""drop table if exists  sequential2_hive""").collect

}
       

//Sequential_QueryType2_TC004
test("Sequential_QueryType2_TC004", Include) {
  sql(s"""create table sequential2 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei")""").collect

  sql(s"""create table sequential2_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType3_TC001
test("Sequential_QueryType3_TC001", Include) {
  sql(s"""create table sequential3 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei",'DICTIONARY_EXCLUDE'='AMSize')""").collect

  sql(s"""create table sequential3_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType3_TC002
test("Sequential_QueryType3_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential3 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential3_hive """).collect

}
       

//Sequential_QueryType3_TC003
test("Sequential_QueryType3_TC003", Include) {
  sql(s"""drop table if exists  sequential3""").collect

  sql(s"""drop table if exists  sequential3_hive""").collect

}
       

//Sequential_QueryType3_TC004
test("Sequential_QueryType3_TC004", Include) {
  sql(s"""create table sequential3 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei",'DICTIONARY_EXCLUDE'='AMSize,channelsId')""").collect

  sql(s"""create table sequential3_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType3_TC005
test("Sequential_QueryType3_TC005", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential3 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential3_hive """).collect

}
       

//Sequential_QueryType3_TC006
test("Sequential_QueryType3_TC006", Include) {
  sql(s"""select * from sequential3""").collect
}
       

//Sequential_QueryType4_TC001_drop
test("Sequential_QueryType4_TC001_drop", Include) {
  sql(s"""Drop table if exists sequential4""").collect

  sql(s"""Drop table if exists sequential4_hive""").collect

}
       

//Sequential_QueryType4_TC001
test("Sequential_QueryType4_TC001", Include) {
  sql(s"""create table sequential4 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='AMSize,channelsId,Activecity,imei,ActiveCountry','DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential4_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType4_TC002
test("Sequential_QueryType4_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential4 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential4_hive """).collect

}
       

//Sequential_QueryType5_TC001_drop
test("Sequential_QueryType5_TC001_drop", Include) {
  sql(s"""drop table if exists sequential5""").collect

  sql(s"""drop table if exists sequential5_hive""").collect

}
       

//Sequential_QueryType5_TC001
test("Sequential_QueryType5_TC001", Include) {
  sql(s"""create table sequential5 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='AMSize,channelsId,Activecity,imei,ActiveCountry','DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential5_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType5_TC002
test("Sequential_QueryType5_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential5 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential5_hive """).collect

}
       

//Sequential_QueryType8_TC001
test("Sequential_QueryType8_TC001", Include) {
  sql(s"""CREATE TABLE sequential8 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.age.shared_column"="shared.age",'DICTIONARY_Include'='age') """).collect

  sql(s"""CREATE TABLE sequential8_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType8_TC002
test("Sequential_QueryType8_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential8 options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='') """).collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential8_hive """).collect

}
       

//Sequential_QueryType8_TC005
test("Sequential_QueryType8_TC005", Include) {
  sql(s""" drop table if exists  sequential8""").collect

  sql(s""" drop table if exists  sequential8_hive""").collect

}
       

//Sequential_QueryType9_TC001_drop
test("Sequential_QueryType9_TC001_drop", Include) {
  sql(s""" drop table if exists  sequential9""").collect

  sql(s""" drop table if exists  sequential9_hive""").collect

}
       

//Sequential_QueryType9_TC001
test("Sequential_QueryType9_TC001", Include) {
  sql(s"""CREATE TABLE sequential9 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_Include'='num1')""").collect

  sql(s"""CREATE TABLE sequential9_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType9_TC002
test("Sequential_QueryType9_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential9 options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential9_hive """).collect

}
       

//Sequential_QueryType9_TC003
test("Sequential_QueryType9_TC003", Include) {
  sql(s"""select * from sequential9""").collect
}
       

//Sequential_QueryType10_TC001_drop
test("Sequential_QueryType10_TC001_drop", Include) {
  sql(s""" drop table if exists  sequential10""").collect

  sql(s""" drop table if exists  sequential10_hive""").collect

}
       

//Sequential_QueryType10_TC001
test("Sequential_QueryType10_TC001", Include) {
  sql(s"""CREATE TABLE sequential10 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.game.shared_column"="shared.game",'DICTIONARY_Include'='game')""").collect

  sql(s"""CREATE TABLE sequential10_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType10_TC002
test("Sequential_QueryType10_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential10 options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential10_hive """).collect

}
       

//Sequential_QueryType10_TC003
test("Sequential_QueryType10_TC003", Include) {
  sql(s"""select * from sequential10""").collect
}
       

//Sequential_QueryType11_TC001_drop
test("Sequential_QueryType11_TC001_drop", Include) {
  sql(s""" drop table if exists  sequential11""").collect

  sql(s""" drop table if exists  sequential11_hive""").collect

}
       

//Sequential_QueryType11_TC001
test("Sequential_QueryType11_TC001", Include) {
  sql(s"""CREATE TABLE sequential11 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.num.shared_column"="shared.num",'DICTIONARY_Include'='num')   """).collect

  sql(s"""CREATE TABLE sequential11_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType11_TC002
test("Sequential_QueryType11_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential11 options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')   """).collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential11_hive """).collect

}
       

//Sequential_QueryType11_TC003
test("Sequential_QueryType11_TC003", Include) {
  sql(s"""select * from sequential11""").collect
}
       

//Sequential_QueryType12_TC001_drop
test("Sequential_QueryType12_TC001_drop", Include) {
  sql(s""" drop table if exists  sequential12""").collect

  sql(s""" drop table if exists  sequential12_hive""").collect

}
       

//Sequential_QueryType12_TC001
test("Sequential_QueryType12_TC001", Include) {
  sql(s"""CREATE TABLE sequential12 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei") """).collect

  sql(s"""CREATE TABLE sequential12_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType12_TC002
test("Sequential_QueryType12_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential12 options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')  """).collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential12_hive """).collect

}
       

//Sequential_QueryType12_TC003
test("Sequential_QueryType12_TC003", Include) {
  sql(s"""select * from sequential12""").collect
}
       

//Sequential_QueryType13_TC001
test("Sequential_QueryType13_TC001", Include) {
  sql(s"""CREATE TABLE sequential13 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.age.shared_column"="shared.age",'DICTIONARY_Include'='age')""").collect

  sql(s"""CREATE TABLE sequential13_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType13_TC002
test("Sequential_QueryType13_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13 options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13_hive """).collect

}
       

//Sequential_QueryType13_TC003
test("Sequential_QueryType13_TC003", Include) {
  sql(s"""Select * from sequential13 """).collect
}
       

//Sequential_QueryType13_TC004
test("Sequential_QueryType13_TC004", Include) {
  sql(s"""drop table if exists  sequential13""").collect

  sql(s"""drop table if exists  sequential13_hive""").collect

}
       

//Sequential_QueryType13_TC005
test("Sequential_QueryType13_TC005", Include) {
  sql(s"""CREATE TABLE sequential13 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.game.shared_column"="shared.game",'DICTIONARY_Include'='game')""").collect

  sql(s"""CREATE TABLE sequential13_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType13_TC006
test("Sequential_QueryType13_TC006", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13 options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13_hive """).collect

}
       

//Sequential_QueryType13_TC007
test("Sequential_QueryType13_TC007", Include) {
  sql(s"""select * from sequential13""").collect
}
       

//Sequential_QueryType13_TC008
test("Sequential_QueryType13_TC008", Include) {
  sql(s"""Select game from sequential13 """).collect
}
       

//Sequential_QueryType13_TC009
test("Sequential_QueryType13_TC009", Include) {
  sql(s"""drop table if exists  sequential13""").collect

  sql(s"""drop table if exists  sequential13_hive""").collect

}
       

//Sequential_QueryType13_TC010
test("Sequential_QueryType13_TC010", Include) {
  sql(s"""CREATE TABLE sequential13 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.num1.shared_column"="shared.num1",'DICTIONARY_Include'='num1')""").collect

  sql(s"""CREATE TABLE sequential13_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType13_TC011
test("Sequential_QueryType13_TC011", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13 options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential13_hive """).collect

}
       

//Sequential_QueryType13_TC012
test("Sequential_QueryType13_TC012", Include) {
  sql(s"""select * from sequential13""").collect
}
       

//Sequential_QueryType15_TC001_drop
test("Sequential_QueryType15_TC001_drop", Include) {
  sql(s""" drop table if exists  sequential15""").collect

  sql(s""" drop table if exists  sequential15_hive""").collect

}
       

//Sequential_QueryType15_TC001
test("Sequential_QueryType15_TC001", Include) {
  sql(s"""CREATE TABLE sequential15  (id int,desc string,num double,desc1 string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.id.shared_column"="shared.id","columnproperties.desc.shared_column"="shared.desc",'DICTIONARY_Include'='id')""").collect

  sql(s"""CREATE TABLE sequential15_hive  (id int,desc string,num double,desc1 string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType15_TC002
test("Sequential_QueryType15_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper1.csv' INTO table sequential15  options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='id,desc,num,desc1')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper1.csv' INTO table sequential15_hive  """).collect

}
       

//Sequential_QueryType15_TC003
test("Sequential_QueryType15_TC003", Include) {
  sql(s"""select * from sequential15""").collect
}
       

//Sequential_QueryType16_TC003
test("Sequential_QueryType16_TC003", Include) {
  sql(s"""CREATE TABLE sequential16  (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.age.shared_column"="shared.age",'DICTIONARY_Include'='age')""").collect

  sql(s"""CREATE TABLE sequential16_hive  (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType16_TC004
test("Sequential_QueryType16_TC004", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential16  options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential16_hive  """).collect

}
       

//Sequential_QueryType16_TC005
test("Sequential_QueryType16_TC005", Include) {
  sql(s"""select * from sequential16""").collect
}
       

//Sequential_QueryType16_TC006
test("Sequential_QueryType16_TC006", Include) {
  sql(s"""drop table if exists  sequential16""").collect

  sql(s"""drop table if exists  sequential16_hive""").collect

}
       

//Sequential_QueryType16_TC007
test("Sequential_QueryType16_TC007", Include) {
  sql(s"""CREATE TABLE sequential16 (imei string,age int,num bigint,game double,num1 decimal,date timestamp,num2 string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.num2.shared_column"="shared.num2")""").collect

  sql(s"""CREATE TABLE sequential16_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp,num2 string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType16_TC008
test("Sequential_QueryType16_TC008", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential16  options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table sequential16_hive  """).collect

}
       

//Sequential_QueryType16_TC009
test("Sequential_QueryType16_TC009", Include) {
  sql(s"""Select * from sequential16 """).collect
}
       

//Sequential_QueryType17_TC001
test("Sequential_QueryType17_TC001", Include) {
  sql(s"""CREATE TABLE sequential17  (id int,desc string,num double,desc1 string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.id.shared_column"="shared.id","columnproperties.desc.shared_column"="shared.desc",'DICTIONARY_Include'='id')""").collect

  sql(s"""CREATE TABLE sequential17_hive  (id int,desc string,num double,desc1 string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType17_TC002
test("Sequential_QueryType17_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper1.csv' INTO table sequential17  options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='id,desc,num,desc1')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper1.csv' INTO table sequential17_hive  """).collect

}
       

//Sequential_QueryType17_TC003
test("Sequential_QueryType17_TC003", Include) {
  sql(s"""select * from sequential17""").collect
}
       

//Sequential_QueryType17_TC004
test("Sequential_QueryType17_TC004", Include) {
  sql(s"""drop table if exists  sequential17""").collect

  sql(s"""drop table if exists  sequential17_hive""").collect

}
       

//Sequential_QueryType17_TC005
test("Sequential_QueryType17_TC005", Include) {
  sql(s"""CREATE TABLE sequential17  (id int,desc string,num double,desc1 string,num2 bigint) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.id.shared_column"="shared.id","columnproperties.desc.shared_column"="shared.desc",'DICTIONARY_Include'='id')""").collect

  sql(s"""CREATE TABLE sequential17_hive  (id int,desc string,num double,desc1 string,num2 bigint)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType17_TC006
test("Sequential_QueryType17_TC006", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper.csv' INTO table sequential17  options ('DELIMITER'=',', 'QUOTECHAR'='"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='id,desc,num,desc1,num2')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/improper.csv' INTO table sequential17_hive  """).collect

}
       

//Sequential_QueryType18_TC001
test("Sequential_QueryType18_TC001", Include) {
  sql(s"""create table sequential18 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential18_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType18_TC002
test("Sequential_QueryType18_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential18 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential18_hive """).collect

}
       

//Sequential_QueryType18_TC003
test("Sequential_QueryType18_TC003", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential18""").collect
}
       

//Sequential_QueryType18_TC004
test("Sequential_QueryType18_TC004", Include) {
  sql(s"""select productionDate from sequential18 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType18_TC005
test("Sequential_QueryType18_TC005", Include) {
  sql(s"""drop table if exists  sequential18""").collect

  sql(s"""drop table if exists  sequential18_hive""").collect

}
       

//Sequential_QueryType18_TC006
test("Sequential_QueryType18_TC006", Include) {
  sql(s"""create table sequential18 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential18_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType18_TC007
test("Sequential_QueryType18_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential18 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential18_hive """).collect

}
       

//Sequential_QueryType18_TC008
test("Sequential_QueryType18_TC008", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential18""").collect
}
       

//Sequential_QueryType18_TC009
test("Sequential_QueryType18_TC009", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential18 limit 20""").collect
}
       

//Sequential_QueryType18_TC010
test("Sequential_QueryType18_TC010", Include) {
  sql(s"""drop table if exists  sequential18""").collect

  sql(s"""drop table if exists  sequential18_hive""").collect

}
       

//Sequential_QueryType19_TC001
test("Sequential_QueryType19_TC001", Include) {
  sql(s"""create table sequential19 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential19_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType19_TC002
test("Sequential_QueryType19_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential19 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential19_hive """).collect

}
       

//Sequential_QueryType19_TC003
test("Sequential_QueryType19_TC003", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential19""").collect
}
       

//Sequential_QueryType19_TC004
test("Sequential_QueryType19_TC004", Include) {
  sql(s"""select productionDate from sequential19 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType19_TC005
test("Sequential_QueryType19_TC005", Include) {
  sql(s"""drop table if exists  sequential19""").collect

  sql(s"""drop table if exists  sequential19_hive""").collect

}
       

//Sequential_QueryType19_TC006
test("Sequential_QueryType19_TC006", Include) {
  sql(s"""create table sequential19 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,AMSize,channelsId,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential19_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType19_TC007
test("Sequential_QueryType19_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential19 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential19_hive """).collect

}
       

//Sequential_QueryType19_TC008
test("Sequential_QueryType19_TC008", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential19""").collect
}
       

//Sequential_QueryType19_TC009
test("Sequential_QueryType19_TC009", Include) {
  sql(s"""select productionDate from sequential19 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType19_TC010
test("Sequential_QueryType19_TC010", Include) {
  sql(s"""drop table if exists  sequential19""").collect

  sql(s"""drop table if exists  sequential19_hive""").collect

}
       

//Sequential_QueryType20_TC001
test("Sequential_QueryType20_TC001", Include) {
  sql(s"""create table sequential20 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential20_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType20_TC002
test("Sequential_QueryType20_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC003
test("Sequential_QueryType20_TC003", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential20""").collect
}
       

//Sequential_QueryType20_TC004
test("Sequential_QueryType20_TC004", Include) {
  sql(s"""select productionDate from sequential20 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType20_TC005
test("Sequential_QueryType20_TC005", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC006
test("Sequential_QueryType20_TC006", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC007
test("Sequential_QueryType20_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC008
test("Sequential_QueryType20_TC008", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC009
test("Sequential_QueryType20_TC009", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC012
test("Sequential_QueryType20_TC012", Include) {
  sql(s"""drop table if exists  sequential20""").collect

  sql(s"""drop table if exists  sequential20_hive""").collect

}
       

//Sequential_QueryType20_TC013
test("Sequential_QueryType20_TC013", Include) {
  sql(s"""create table sequential20 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,AMSize,channelsId,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential20_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType20_TC014
test("Sequential_QueryType20_TC014", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential20_hive """).collect

}
       

//Sequential_QueryType20_TC015
test("Sequential_QueryType20_TC015", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential20""").collect
}
       

//Sequential_QueryType20_TC016
test("Sequential_QueryType20_TC016", Include) {
  sql(s"""select productionDate from sequential20 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType20_TC017
test("Sequential_QueryType20_TC017", Include) {
  sql(s"""drop table if exists  sequential20""").collect

  sql(s"""drop table if exists  sequential20_hive""").collect

}
       

//Sequential_QueryType21_TC001
test("Sequential_QueryType21_TC001", Include) {
  sql(s"""create table sequential21 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='imei,AMSize,channelsId,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential21_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType21_TC002
test("Sequential_QueryType21_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC003
test("Sequential_QueryType21_TC003", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential21""").collect
}
       

//Sequential_QueryType21_TC004
test("Sequential_QueryType21_TC004", Include) {
  sql(s"""select productionDate from sequential21 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType21_TC005
test("Sequential_QueryType21_TC005", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC006
test("Sequential_QueryType21_TC006", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC007
test("Sequential_QueryType21_TC007", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential21""").collect
}
       

//Sequential_QueryType21_TC009
test("Sequential_QueryType21_TC009", Include) {
  sql(s"""select productionDate from sequential21 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType21_TC010
test("Sequential_QueryType21_TC010", Include) {
  sql(s"""drop table if exists  sequential21""").collect

  sql(s"""drop table if exists  sequential21_hive""").collect

}
       

//Sequential_QueryType21_TC011
test("Sequential_QueryType21_TC011", Include) {
  sql(s"""create table sequential21 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,AMSize,channelsId,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential21_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType21_TC012
test("Sequential_QueryType21_TC012", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC013
test("Sequential_QueryType21_TC013", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential21""").collect
}
       

//Sequential_QueryType21_TC014
test("Sequential_QueryType21_TC014", Include) {
  sql(s"""select productionDate from sequential21 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType21_TC015
test("Sequential_QueryType21_TC015", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC016
test("Sequential_QueryType21_TC016", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential21_hive """).collect

}
       

//Sequential_QueryType21_TC017
test("Sequential_QueryType21_TC017", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential21""").collect
}
       

//Sequential_QueryType21_TC019
test("Sequential_QueryType21_TC019", Include) {
  sql(s"""select productionDate from sequential21 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType21_TC020
test("Sequential_QueryType21_TC020", Include) {
  sql(s"""drop table if exists  sequential21""").collect

  sql(s"""drop table if exists  sequential21_hive""").collect

}
       

//Sequential_QueryType22_TC001
test("Sequential_QueryType22_TC001", Include) {
  sql(s"""create table sequential22 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential22_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType22_TC002
test("Sequential_QueryType22_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC003
test("Sequential_QueryType22_TC003", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential22""").collect
}
       

//Sequential_QueryType22_TC004
test("Sequential_QueryType22_TC004", Include) {
  sql(s"""select productionDate from sequential22 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType22_TC005
test("Sequential_QueryType22_TC005", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC006
test("Sequential_QueryType22_TC006", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC007
test("Sequential_QueryType22_TC007", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential22""").collect
}
       

//Sequential_QueryType22_TC009
test("Sequential_QueryType22_TC009", Include) {
  sql(s"""select productionDate from sequential22 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType22_TC010
test("Sequential_QueryType22_TC010", Include) {
  sql(s"""drop table if exists  sequential22""").collect

  sql(s"""drop table if exists  sequential22_hive""").collect

}
       

//Sequential_QueryType22_TC011
test("Sequential_QueryType22_TC011", Include) {
  sql(s"""create table sequential22 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential22_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType22_TC012
test("Sequential_QueryType22_TC012", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC013
test("Sequential_QueryType22_TC013", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential22""").collect
}
       

//Sequential_QueryType22_TC014
test("Sequential_QueryType22_TC014", Include) {
  sql(s"""select productionDate from sequential22 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType22_TC015
test("Sequential_QueryType22_TC015", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC016
test("Sequential_QueryType22_TC016", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential22_hive """).collect

}
       

//Sequential_QueryType22_TC017
test("Sequential_QueryType22_TC017", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential22""").collect
}
       

//Sequential_QueryType22_TC019
test("Sequential_QueryType22_TC019", Include) {
  sql(s"""select productionDate from sequential22 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType22_TC020
test("Sequential_QueryType22_TC020", Include) {
  sql(s"""drop table if exists  sequential22""").collect

  sql(s"""drop table if exists  sequential22_hive""").collect

}
       

//Sequential_QueryType23_TC007
test("Sequential_QueryType23_TC007", Include) {
  sql(s"""drop table if exists  sequential23""").collect

  sql(s"""drop table if exists  sequential23_hive""").collect

}
       

//Sequential_QueryType23_TC008
test("Sequential_QueryType23_TC008", Include) {
  sql(s"""create table sequential23 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential23_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType23_TC009
test("Sequential_QueryType23_TC009", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23_hive """).collect

}
       

//Sequential_QueryType23_TC010
test("Sequential_QueryType23_TC010", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential23""").collect
}
       

//Sequential_QueryType23_TC011
test("Sequential_QueryType23_TC011", Include) {
  sql(s"""select productionDate from sequential23 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType23_TC012
test("Sequential_QueryType23_TC012", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23_hive """).collect

}
       

//Sequential_QueryType23_TC013
test("Sequential_QueryType23_TC013", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential23_hive """).collect

}
       

//Sequential_QueryType23_TC014
test("Sequential_QueryType23_TC014", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential23""").collect
}
       

//Sequential_QueryType23_TC016
test("Sequential_QueryType23_TC016", Include) {
  sql(s"""select productionDate from sequential23 where productionDate=productionDate""").collect
}
       

//Sequential_QueryType23_TC017
test("Sequential_QueryType23_TC017", Include) {
  sql(s"""drop table if exists  sequential23""").collect

  sql(s"""drop table if exists  sequential23_hive""").collect

}
       

//Sequential_QueryType26_TC001
test("Sequential_QueryType26_TC001", Include) {
  sql(s"""create table sequential26 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.deliverycharge.shared_column"="shared.deliverycharge",'DICTIONARY_INCLUDE'='deliverycharge')""").collect

  sql(s"""create table sequential26_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType26_TC002
test("Sequential_QueryType26_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC003
test("Sequential_QueryType26_TC003", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC006
test("Sequential_QueryType26_TC006", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC007
test("Sequential_QueryType26_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC008
test("Sequential_QueryType26_TC008", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC011
test("Sequential_QueryType26_TC011", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential26""").collect
}
       

//Sequential_QueryType26_TC012
test("Sequential_QueryType26_TC012", Include) {
  sql(s"""select deliverycharge from sequential26""").collect
}
       

//Sequential_QueryType26_TC013
test("Sequential_QueryType26_TC013", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential26_hive """).collect

}
       

//Sequential_QueryType26_TC016
test("Sequential_QueryType26_TC016", Include) {
  sql(s"""drop table if exists  sequential26""").collect

  sql(s"""drop table if exists  sequential26_hive""").collect

}
       

//Sequential_QueryType27_TC001
test("Sequential_QueryType27_TC001", Include) {
  sql(s"""create table sequential27 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.deliverycharge.shared_column"="shared.deliverycharge",'DICTIONARY_INCLUDE'='deliverycharge')""").collect

  sql(s"""create table sequential27_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge decimal)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType27_TC002
test("Sequential_QueryType27_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC003
test("Sequential_QueryType27_TC003", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC006
test("Sequential_QueryType27_TC006", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC007
test("Sequential_QueryType27_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC008
test("Sequential_QueryType27_TC008", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC011
test("Sequential_QueryType27_TC011", Include) {
  sql(s"""select imei,gamePointId,productionDate from sequential27""").collect
}
       

//Sequential_QueryType27_TC012
test("Sequential_QueryType27_TC012", Include) {
  sql(s"""select deliverycharge from sequential27""").collect
}
       

//Sequential_QueryType27_TC013
test("Sequential_QueryType27_TC013", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge') """).collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential27_hive """).collect

}
       

//Sequential_QueryType27_TC016
test("Sequential_QueryType27_TC016", Include) {
  sql(s"""drop table if exists  sequential27""").collect

  sql(s"""drop table if exists  sequential27_hive""").collect

}
       

//Sequential_QueryType31_TC001
test("Sequential_QueryType31_TC001", Include) {
  sql(s"""create table sequential31 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect

  sql(s"""create table sequential31_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType31_TC002
test("Sequential_QueryType31_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential31 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential31_hive """).collect

}
       

//Sequential_QueryType31_TC003
test("Sequential_QueryType31_TC003", Include) {
  sql(s"""Select * from sequential31""").collect
}
       

//Sequential_QueryType31_TC004
test("Sequential_QueryType31_TC004", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential31 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType31_TC005
test("Sequential_QueryType31_TC005", Include) {
  sql(s"""select gamePointId from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC006
test("Sequential_QueryType31_TC006", Include) {
  sql(s"""select productionDate,imei from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC008
test("Sequential_QueryType31_TC008", Include) {
  sql(s"""Select * from sequential31""").collect
}
       

//Sequential_QueryType31_TC009
test("Sequential_QueryType31_TC009", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential31 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType31_TC010
test("Sequential_QueryType31_TC010", Include) {
  sql(s"""select gamePointId from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC011
test("Sequential_QueryType31_TC011", Include) {
  sql(s"""select productionDate,imei from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC013
test("Sequential_QueryType31_TC013", Include) {
  sql(s"""Select * from sequential31""").collect
}
       

//Sequential_QueryType31_TC014
test("Sequential_QueryType31_TC014", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential31 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType31_TC015
test("Sequential_QueryType31_TC015", Include) {
  sql(s"""select gamePointId from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC016
test("Sequential_QueryType31_TC016", Include) {
  sql(s"""select productionDate,imei from sequential31 limit 10""").collect
}
       

//Sequential_QueryType31_TC017
test("Sequential_QueryType31_TC017", Include) {
  sql(s"""drop table if exists  sequential31""").collect

  sql(s"""drop table if exists  sequential31_hive""").collect

}
       

//Sequential_QueryType31_TC018
test("Sequential_QueryType31_TC018", Include) {
  sql(s"""create table sequential31 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect

  sql(s"""create table sequential31_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType31_TC019
test("Sequential_QueryType31_TC019", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential31 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential31_hive """).collect

}
       

//Sequential_QueryType31_TC020
test("Sequential_QueryType31_TC020", Include) {
  sql(s"""Select * from sequential31""").collect
}
       

//Sequential_QueryType31_TC021
test("Sequential_QueryType31_TC021", Include) {
  sql(s"""drop table if exists  sequential31""").collect

  sql(s"""drop table if exists  sequential31_hive""").collect

}
       

//Sequential_QueryType32_TC001
test("Sequential_QueryType32_TC001", Include) {
  sql(s"""create table sequential32 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential32_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType32_TC002
test("Sequential_QueryType32_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential32 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential32_hive """).collect

}
       

//Sequential_QueryType32_TC003
test("Sequential_QueryType32_TC003", Include) {
  sql(s"""Select * from sequential32""").collect
}
       

//Sequential_QueryType32_TC004
test("Sequential_QueryType32_TC004", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential32 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType32_TC005
test("Sequential_QueryType32_TC005", Include) {
  sql(s"""select gamePointId from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC006
test("Sequential_QueryType32_TC006", Include) {
  sql(s"""select productionDate,imei from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC008
test("Sequential_QueryType32_TC008", Include) {
  sql(s"""Select * from sequential32""").collect
}
       

//Sequential_QueryType32_TC009
test("Sequential_QueryType32_TC009", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential32 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType32_TC010
test("Sequential_QueryType32_TC010", Include) {
  sql(s"""select gamePointId from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC011
test("Sequential_QueryType32_TC011", Include) {
  sql(s"""select productionDate,imei from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC013
test("Sequential_QueryType32_TC013", Include) {
  sql(s"""Select * from sequential32""").collect
}
       

//Sequential_QueryType32_TC014
test("Sequential_QueryType32_TC014", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential32 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType32_TC015
test("Sequential_QueryType32_TC015", Include) {
  sql(s"""select gamePointId from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC016
test("Sequential_QueryType32_TC016", Include) {
  sql(s"""select productionDate,imei from sequential32 limit 10""").collect
}
       

//Sequential_QueryType32_TC017
test("Sequential_QueryType32_TC017", Include) {
  sql(s"""drop table if exists  sequential32""").collect

  sql(s"""drop table if exists  sequential32_hive""").collect

}
       

//Sequential_QueryType32_TC018
test("Sequential_QueryType32_TC018", Include) {
  sql(s"""create table sequential32 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='channelsId')""").collect

  sql(s"""create table sequential32_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType32_TC019
test("Sequential_QueryType32_TC019", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential32 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential32_hive """).collect

}
       

//Sequential_QueryType32_TC020
test("Sequential_QueryType32_TC020", Include) {
  sql(s"""Select * from sequential32""").collect
}
       

//Sequential_QueryType32_TC021
test("Sequential_QueryType32_TC021", Include) {
  sql(s"""drop table if exists  sequential32""").collect

  sql(s"""drop table if exists  sequential32_hive""").collect

}
       

//Sequential_QueryType33_TC001
test("Sequential_QueryType33_TC001", Include) {
  sql(s"""create table sequential33 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential33_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType33_TC002
test("Sequential_QueryType33_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential33 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential33_hive """).collect

}
       

//Sequential_QueryType33_TC003
test("Sequential_QueryType33_TC003", Include) {
  sql(s"""Select * from sequential33""").collect
}
       

//Sequential_QueryType33_TC004
test("Sequential_QueryType33_TC004", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential33 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType33_TC005
test("Sequential_QueryType33_TC005", Include) {
  sql(s"""select gamePointId from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC006
test("Sequential_QueryType33_TC006", Include) {
  sql(s"""select productionDate,imei from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC008
test("Sequential_QueryType33_TC008", Include) {
  sql(s"""Select * from sequential33""").collect
}
       

//Sequential_QueryType33_TC009
test("Sequential_QueryType33_TC009", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential33 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType33_TC010
test("Sequential_QueryType33_TC010", Include) {
  sql(s"""select gamePointId from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC011
test("Sequential_QueryType33_TC011", Include) {
  sql(s"""select productionDate,imei from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC013
test("Sequential_QueryType33_TC013", Include) {
  sql(s"""Select * from sequential33""").collect
}
       

//Sequential_QueryType33_TC014
test("Sequential_QueryType33_TC014", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential33 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType33_TC015
test("Sequential_QueryType33_TC015", Include) {
  sql(s"""select gamePointId from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC016
test("Sequential_QueryType33_TC016", Include) {
  sql(s"""select productionDate,imei from sequential33 limit 10""").collect
}
       

//Sequential_QueryType33_TC017
test("Sequential_QueryType33_TC017", Include) {
  sql(s"""drop table if exists  sequential33""").collect

  sql(s"""drop table if exists  sequential33_hive""").collect

}
       

//Sequential_QueryType33_TC018
test("Sequential_QueryType33_TC018", Include) {
  sql(s"""create table sequential33 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential33_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType33_TC019
test("Sequential_QueryType33_TC019", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential33 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential33_hive """).collect

}
       

//Sequential_QueryType33_TC020
test("Sequential_QueryType33_TC020", Include) {
  sql(s"""Select * from sequential33""").collect
}
       

//Sequential_QueryType33_TC021
test("Sequential_QueryType33_TC021", Include) {
  sql(s"""drop table if exists  sequential33""").collect

  sql(s"""drop table if exists  sequential33_hive""").collect

}
       

//Sequential_QueryType34_TC001
test("Sequential_QueryType34_TC001", Include) {
  sql(s"""create table sequential34 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential34_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType34_TC002
test("Sequential_QueryType34_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential34 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential34_hive """).collect

}
       

//Sequential_QueryType34_TC003
test("Sequential_QueryType34_TC003", Include) {
  sql(s"""Select * from sequential34""").collect
}
       

//Sequential_QueryType34_TC004
test("Sequential_QueryType34_TC004", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential34 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType34_TC005
test("Sequential_QueryType34_TC005", Include) {
  sql(s"""select gamePointId from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC006
test("Sequential_QueryType34_TC006", Include) {
  sql(s"""select productionDate,imei from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC008
test("Sequential_QueryType34_TC008", Include) {
  sql(s"""Select * from sequential34""").collect
}
       

//Sequential_QueryType34_TC009
test("Sequential_QueryType34_TC009", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential34 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType34_TC010
test("Sequential_QueryType34_TC010", Include) {
  sql(s"""select gamePointId from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC011
test("Sequential_QueryType34_TC011", Include) {
  sql(s"""select productionDate,imei from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC013
test("Sequential_QueryType34_TC013", Include) {
  sql(s"""Select * from sequential34""").collect
}
       

//Sequential_QueryType34_TC014
test("Sequential_QueryType34_TC014", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential34 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType34_TC015
test("Sequential_QueryType34_TC015", Include) {
  sql(s"""select gamePointId from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC016
test("Sequential_QueryType34_TC016", Include) {
  sql(s"""select productionDate,imei from sequential34 limit 10""").collect
}
       

//Sequential_QueryType34_TC017
test("Sequential_QueryType34_TC017", Include) {
  sql(s"""drop table if exists  sequential34""").collect

  sql(s"""drop table if exists  sequential34_hive""").collect

}
       

//Sequential_QueryType34_TC018
test("Sequential_QueryType34_TC018", Include) {
  sql(s"""create table sequential34 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential34_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType34_TC019
test("Sequential_QueryType34_TC019", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential34 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential34_hive """).collect

}
       

//Sequential_QueryType34_TC020
test("Sequential_QueryType34_TC020", Include) {
  sql(s"""Select * from sequential34""").collect
}
       

//Sequential_QueryType34_TC021
test("Sequential_QueryType34_TC021", Include) {
  sql(s"""drop table if exists  sequential34""").collect

  sql(s"""drop table if exists  sequential34_hive""").collect

}
       

//Sequential_QueryType35_TC001
test("Sequential_QueryType35_TC001", Include) {
  sql(s"""create table sequential35 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity','DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential35_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType35_TC002
test("Sequential_QueryType35_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential35 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential35_hive """).collect

}
       

//Sequential_QueryType35_TC003
test("Sequential_QueryType35_TC003", Include) {
  sql(s"""Select * from sequential35""").collect
}
       

//Sequential_QueryType35_TC004
test("Sequential_QueryType35_TC004", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential35 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType35_TC005
test("Sequential_QueryType35_TC005", Include) {
  sql(s"""select gamePointId from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC006
test("Sequential_QueryType35_TC006", Include) {
  sql(s"""select productionDate,imei from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC008
test("Sequential_QueryType35_TC008", Include) {
  sql(s"""Select * from sequential35""").collect
}
       

//Sequential_QueryType35_TC009
test("Sequential_QueryType35_TC009", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential35 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType35_TC010
test("Sequential_QueryType35_TC010", Include) {
  sql(s"""select gamePointId from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC011
test("Sequential_QueryType35_TC011", Include) {
  sql(s"""select productionDate,imei from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC013
test("Sequential_QueryType35_TC013", Include) {
  sql(s"""Select * from sequential35""").collect
}
       

//Sequential_QueryType35_TC014
test("Sequential_QueryType35_TC014", Include) {
  sql(s"""select imei,deviceInformationId,AMSize,channelsId from sequential35 group by imei,deviceInformationId,channelsId,AMSize order by imei,deviceInformationId,channelsId,AMSize limit 10""").collect
}
       

//Sequential_QueryType35_TC015
test("Sequential_QueryType35_TC015", Include) {
  sql(s"""select gamePointId from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC016
test("Sequential_QueryType35_TC016", Include) {
  sql(s"""select productionDate,imei from sequential35 limit 10""").collect
}
       

//Sequential_QueryType35_TC017
test("Sequential_QueryType35_TC017", Include) {
  sql(s"""drop table if exists  sequential35""").collect

  sql(s"""drop table if exists  sequential35_hive""").collect

}
       

//Sequential_QueryType35_TC018
test("Sequential_QueryType35_TC018", Include) {
  sql(s"""create table sequential35 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential35_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType35_TC019
test("Sequential_QueryType35_TC019", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential35 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/vardhandaterestruct.csv' INTO TABLE sequential35_hive """).collect

}
       

//Sequential_QueryType35_TC020
test("Sequential_QueryType35_TC020", Include) {
  sql(s"""Select * from sequential35""").collect
}
       

//Sequential_QueryType35_TC021
test("Sequential_QueryType35_TC021", Include) {
  sql(s"""drop table if exists  sequential35""").collect

  sql(s"""drop table if exists  sequential35_hive""").collect

}
       

//Sequential_QueryType38_TC001
test("Sequential_QueryType38_TC001", Include) {
  sql(s"""CREATE table sequential38 (column1 STRING, column2 STRING,column3 INT, column4 INT,column5 INT, column6 INT) stored by 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.column1.shared_column"="shared.column1","columnproperties.column2.shared_column"="shared.column2")""").collect

  sql(s"""CREATE table sequential38_hive (column1 STRING, column2 STRING,column3 INT, column4 INT,column5 INT, column6 INT)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType38_TC002
test("Sequential_QueryType38_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38_hive """).collect

}
       

//Sequential_QueryType38_TC003
test("Sequential_QueryType38_TC003", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38_hive """).collect

}
       

//Sequential_QueryType38_TC004
test("Sequential_QueryType38_TC004", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38_hive """).collect

}
       

//Sequential_QueryType38_TC005
test("Sequential_QueryType38_TC005", Include) {
  sql(s"""SELECT * FROM sequential38""").collect
}
       

//Sequential_QueryType38_TC006
test("Sequential_QueryType38_TC006", Include) {
  sql(s"""select * from sequential38 where column1!='aa'""").collect
}
       

//Sequential_QueryType38_TC007
test("Sequential_QueryType38_TC007", Include) {
  sql(s"""select * from sequential38 where column3=-2147483648""").collect
}
       

//Sequential_QueryType38_TC009
test("Sequential_QueryType38_TC009", Include) {
  sql(s"""select * from sequential38 where column4=-9223372036854775808""").collect
}
       

//Sequential_QueryType38_TC010
test("Sequential_QueryType38_TC010", Include) {
  sql(s"""select count(column3),sum(column4) from sequential38""").collect
}
       

//Sequential_QueryType38_TC011
test("Sequential_QueryType38_TC011", Include) {
  sql(s"""select upper(column3) from sequential38""").collect
}
       

//Sequential_QueryType38_TC012
test("Sequential_QueryType38_TC012", Include) {
  sql(s"""select count(distinct column4) from sequential38""").collect
}
       

//Sequential_QueryType38_TC013
test("Sequential_QueryType38_TC013", Include) {
  sql(s"""select avg(column4),avg(column3) from sequential38""").collect
}
       

//Sequential_QueryType38_TC015
test("Sequential_QueryType38_TC015", Include) {
  sql(s"""Select  column1,column2 from sequential38 group by  column1,column2 order by column1,column2""").collect
}
       

//Sequential_QueryType38_TC016
test("Sequential_QueryType38_TC016", Include) {
  sql(s"""Select  column1,column2 from sequential38 where column1  LIKE 'aa'""").collect
}
       

//Sequential_QueryType38_TC017
test("Sequential_QueryType38_TC017", Include) {
  sql(s"""Select  column1,column2 from sequential38 where sequential38.column2 in  ('bb','aa')""").collect
}
       

//Sequential_QueryType38_TC018
test("Sequential_QueryType38_TC018", Include) {
  sql(s"""drop table if exists  sequential38""").collect

  sql(s"""drop table if exists  sequential38_hive""").collect

}
       

//Sequential_QueryType38_TC019
test("Sequential_QueryType38_TC019", Include) {
  sql(s"""CREATE table sequential38 (column1 STRING, column2 STRING,column3 INT, column4 INT,column5 INT, column6 INT) stored by 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.column1.shared_column"="shared.column1","columnproperties.column2.shared_column"="shared.column2")""").collect

  sql(s"""CREATE table sequential38_hive (column1 STRING, column2 STRING,column3 INT, column4 INT,column5 INT, column6 INT)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType38_TC020
test("Sequential_QueryType38_TC020", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38 OPTIONS('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/file.csv' INTO TABLE sequential38_hive """).collect

}
       

//Sequential_QueryType38_TC021
test("Sequential_QueryType38_TC021", Include) {
  sql(s"""select * from sequential38""").collect
}
       

//Sequential_QueryType38_TC022
test("Sequential_QueryType38_TC022", Include) {
  sql(s"""drop table if exists  sequential38""").collect

  sql(s"""drop table if exists  sequential38_hive""").collect

}
       

//Sequential_QueryType39_TC001
test("Sequential_QueryType39_TC001", Include) {
  sql(s"""CREATE table sequential39 (imei String, productdate String, updatetime String, gamepointid Int, contractnumber Int,deviceinformationid Int) stored by 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.imei.shared_column"="shared.imei","columnproperties.productdate.shared_column"="shared.productdate")""").collect

  sql(s"""CREATE table sequential39_hive (imei String, productdate String, updatetime String, gamepointid Int, contractnumber Int,deviceinformationid Int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType39_TC002
test("Sequential_QueryType39_TC002", Include) {
  sql(s"""select * from sequential39""").collect
}
       

//Sequential_QueryType39_TC004
test("Sequential_QueryType39_TC004", Include) {
  sql(s"""select * from sequential39""").collect
}
       

//Sequential_QueryType39_TC005
test("Sequential_QueryType39_TC005", Include) {
  sql(s"""select imei,contractNumber from sequential39""").collect
}
       

//Sequential_QueryType39_TC006
test("Sequential_QueryType39_TC006", Include) {
  sql(s"""select count(*) from sequential39""").collect
}
       

//Sequential_QueryType39_TC007
test("Sequential_QueryType39_TC007", Include) {
  sql(s"""drop table if exists  sequential39""").collect

  sql(s"""drop table if exists  sequential39_hive""").collect

}
       

//Sequential_QueryType40_TC001
test("Sequential_QueryType40_TC001", Include) {
  sql(s"""create table sequential40 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential40_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType40_TC002
test("Sequential_QueryType40_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential40 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential40_hive """).collect

}
       

//Sequential_QueryType40_TC003
test("Sequential_QueryType40_TC003", Include) {
  sql(s"""Select * from sequential40""").collect
}
       

//Sequential_QueryType40_TC004
test("Sequential_QueryType40_TC004", Include) {
  sql(s"""select imei, channelsId+ 10 as a  from sequential40""").collect
}
       

//Sequential_QueryType40_TC006
test("Sequential_QueryType40_TC006", Include) {
  sql(s"""drop table if exists  sequential40""").collect

  sql(s"""drop table if exists  sequential40_hive""").collect

}
       

//Sequential_QueryType40_TC007
test("Sequential_QueryType40_TC007", Include) {
  sql(s"""create table sequential40 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""create table sequential40_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType40_TC008
test("Sequential_QueryType40_TC008", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential40 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential40_hive """).collect

}
       

//Sequential_QueryType40_TC009
test("Sequential_QueryType40_TC009", Include) {
  sql(s"""Select * from sequential40""").collect
}
       

//Sequential_QueryType41_TC001
test("Sequential_QueryType41_TC001", Include) {
  sql(s"""create table sequential41 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect

  sql(s"""create table sequential41_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType41_TC002
test("Sequential_QueryType41_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential41 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential41_hive """).collect

}
       

//Sequential_QueryType41_TC003
test("Sequential_QueryType41_TC003", Include) {
  sql(s"""Select * from sequential41""").collect
}
       

//Sequential_QueryType41_TC004
test("Sequential_QueryType41_TC004", Include) {
  sql(s"""select imei, channelsId+imei as a from sequential41""").collect
}
       

//Sequential_QueryType41_TC006
test("Sequential_QueryType41_TC006", Include) {
  sql(s"""drop table if exists  sequential41""").collect

  sql(s"""drop table if exists  sequential41_hive""").collect

}
       

//Sequential_QueryType41_TC007
test("Sequential_QueryType41_TC007", Include) {
  sql(s"""create table sequential41 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId')""").collect

  sql(s"""create table sequential41_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType41_TC008
test("Sequential_QueryType41_TC008", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential41 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential41_hive """).collect

}
       

//Sequential_QueryType41_TC009
test("Sequential_QueryType41_TC009", Include) {
  sql(s"""Select * from sequential41""").collect
}
       

//Sequential_QueryType42_TC001
test("Sequential_QueryType42_TC001", Include) {
  sql(s"""create table sequential42(imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential42_hive(imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType42_TC002
test("Sequential_QueryType42_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/100.csv' into table sequential42 options('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/100.csv' into table sequential42_hive """).collect

}
       

//Sequential_QueryType42_TC003
test("Sequential_QueryType42_TC003", Include) {
  sql(s"""Select * from sequential42""").collect
}
       

//Sequential_QueryType42_TC004
test("Sequential_QueryType42_TC004", Include) {
  sql(s"""Select * from sequential42 limit 4""").collect
}
       

//Sequential_QueryType42_TC006
test("Sequential_QueryType42_TC006", Include) {
  sql(s"""drop table if exists  sequential42""").collect

  sql(s"""drop table if exists  sequential42_hive""").collect

}
       

//Sequential_QueryType42_TC007
test("Sequential_QueryType42_TC007", Include) {
  sql(s"""create table sequential42(imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId')""").collect

  sql(s"""create table sequential42_hive(imei string,deviceInformationId int,MAC string,deviceColor string,device_backColor string,modelId string,marketName string,AMSize string,ROMSize string,CUPAudit string,CPIClocked string,series string,productionDate timestamp,bomCode string,internalModels string, deliveryTime string, channelsId string, channelsName string , deliveryAreaId string, deliveryCountry string, deliveryProvince string, deliveryCity string,deliveryDistrict string, deliveryStreet string, oxSingleNumber string, ActiveCheckTime string, ActiveAreaId string, ActiveCountry string, ActiveProvince string, Activecity string, ActiveDistrict string, ActiveStreet string, ActiveOperatorId string, Active_releaseId string, Active_EMUIVersion string, Active_operaSysVersion string, Active_BacVerNumber string, Active_BacFlashVer string, Active_webUIVersion string, Active_webUITypeCarrVer string,Active_webTypeDataVerNumber string, Active_operatorsVersion string, Active_phonePADPartitionedVersions string, Latest_YEAR int, Latest_MONTH int, Latest_DAY int, Latest_HOUR string, Latest_areaId string, Latest_country string, Latest_province string, Latest_city string, Latest_district string, Latest_street string, Latest_releaseId string, Latest_EMUIVersion string, Latest_operaSysVersion string, Latest_BacVerNumber string, Latest_BacFlashVer string, Latest_webUIVersion string, Latest_webUITypeCarrVer string, Latest_webTypeDataVerNumber string, Latest_operatorsVersion string, Latest_phonePADPartitionedVersions string, Latest_operatorId string, gamePointDescription string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType42_TC008
test("Sequential_QueryType42_TC008", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/100.csv' into table sequential42 options('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='imei,deviceInformationId,MAC,deviceColor,device_backColor,modelId,marketName,AMSize,ROMSize,CUPAudit,CPIClocked,series,productionDate,bomCode,internalModels,deliveryTime,channelsId,channelsName,deliveryAreaId,deliveryCountry,deliveryProvince,deliveryCity,deliveryDistrict,deliveryStreet,oxSingleNumber,contractNumber,ActiveCheckTime,ActiveAreaId,ActiveCountry,ActiveProvince,Activecity,ActiveDistrict,ActiveStreet,ActiveOperatorId,Active_releaseId,Active_EMUIVersion,Active_operaSysVersion,Active_BacVerNumber,Active_BacFlashVer,Active_webUIVersion,Active_webUITypeCarrVer,Active_webTypeDataVerNumber,Active_operatorsVersion,Active_phonePADPartitionedVersions,Latest_YEAR,Latest_MONTH,Latest_DAY,Latest_HOUR,Latest_areaId,Latest_country,Latest_province,Latest_city,Latest_district,Latest_street,Latest_releaseId,Latest_EMUIVersion,Latest_operaSysVersion,Latest_BacVerNumber,Latest_BacFlashVer,Latest_webUIVersion,Latest_webUITypeCarrVer,Latest_webTypeDataVerNumber,Latest_operatorsVersion,Latest_phonePADPartitionedVersions,Latest_operatorId,gamePointId,gamePointDescription')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/100.csv' into table sequential42_hive """).collect

}
       

//Sequential_QueryType42_TC009
test("Sequential_QueryType42_TC009", Include) {
  sql(s"""Select * from sequential42""").collect
}
       

//Sequential_QueryType43_TC001
test("Sequential_QueryType43_TC001", Include) {
  sql(s"""create table sequential43 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential43_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType43_TC002
test("Sequential_QueryType43_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential43 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential43_hive """).collect

}
       

//Sequential_QueryType43_TC003
test("Sequential_QueryType43_TC003", Include) {
  sql(s"""Select * from sequential43""").collect
}
       

//Sequential_QueryType43_TC005
test("Sequential_QueryType43_TC005", Include) {
  sql(s"""drop table if exists  sequential43""").collect

  sql(s"""drop table if exists  sequential43_hive""").collect

}
       

//Sequential_QueryType43_TC006
test("Sequential_QueryType43_TC006", Include) {
  sql(s"""create table sequential43 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format'TBLPROPERTIES('DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential43_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType43_TC007
test("Sequential_QueryType43_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential43 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential43_hive """).collect

}
       

//Sequential_QueryType43_TC008
test("Sequential_QueryType43_TC008", Include) {
  sql(s"""Select * from sequential43""").collect
}
       

//Sequential_QueryType44_TC001
test("Sequential_QueryType44_TC001", Include) {
  sql(s"""create table sequential44 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential44_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType44_TC002
test("Sequential_QueryType44_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential44 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential44_hive """).collect

}
       

//Sequential_QueryType44_TC003
test("Sequential_QueryType44_TC003", Include) {
  sql(s"""Select * from sequential44""").collect
}
       

//Sequential_QueryType44_TC005
test("Sequential_QueryType44_TC005", Include) {
  sql(s"""drop table if exists  sequential44""").collect

  sql(s"""drop table if exists  sequential44_hive""").collect

}
       

//Sequential_QueryType44_TC006
test("Sequential_QueryType44_TC006", Include) {
  sql(s"""create table sequential44 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity')""").collect

  sql(s"""create table sequential44_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType44_TC007
test("Sequential_QueryType44_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential44 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential44_hive """).collect

}
       

//Sequential_QueryType44_TC008
test("Sequential_QueryType44_TC008", Include) {
  sql(s"""Select * from sequential44""").collect
}
       

//Sequential_QueryType45_TC001
test("Sequential_QueryType45_TC001", Include) {
  sql(s"""create table sequential45 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity','DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential45_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType45_TC002
test("Sequential_QueryType45_TC002", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential45 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"', 'BAD_RECORDS_ACTION'='FORCE','FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential45_hive """).collect

}
       

//Sequential_QueryType45_TC003
test("Sequential_QueryType45_TC003", Include) {
  sql(s"""Select * from sequential45""").collect
}
       

//Sequential_QueryType45_TC005
test("Sequential_QueryType45_TC005", Include) {
  sql(s"""drop table if exists  sequential45""").collect

  sql(s"""drop table if exists  sequential45_hive""").collect

}
       

//Sequential_QueryType45_TC006
test("Sequential_QueryType45_TC006", Include) {
  sql(s"""create table sequential45 (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='imei,channelsId,AMSize,ActiveCountry,Activecity','DICTIONARY_INCLUDE'='gamePointId,deviceInformationId,deliverycharge')""").collect

  sql(s"""create table sequential45_hive (imei string,AMSize string,channelsId string,ActiveCountry string, Activecity string,gamePointId double,deviceInformationId double,productionDate Timestamp,deliveryDate timestamp,deliverycharge double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType45_TC007
test("Sequential_QueryType45_TC007", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential45 OPTIONS('DELIMITER'=',', 'QUOTECHAR'= '"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'= 'imei,deviceInformationId,AMSize,channelsId,ActiveCountry,Activecity,gamePointId,productionDate,deliveryDate,deliverycharge')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/sequencedata/records.csv' INTO TABLE sequential45_hive """).collect

}
       

//Sequential_QueryType45_TC008
test("Sequential_QueryType45_TC008", Include) {
  sql(s"""Select * from sequential45""").collect
}
       
override def afterAll {
sql("drop table if exists sequential32")
sql("drop table if exists sequential32_hive")
sql("drop table if exists sequential2")
sql("drop table if exists sequential2_hive")
sql("drop table if exists sequential38")
sql("drop table if exists sequential38_hive")
sql("drop table if exists sequential43")
sql("drop table if exists sequential43_hive")
sql("drop table if exists sequential8")
sql("drop table if exists sequential8_hive")
sql("drop table if exists sequential21")
sql("drop table if exists sequential21_hive")
sql("drop table if exists sequential40")
sql("drop table if exists sequential40_hive")
sql("drop table if exists sequential10")
sql("drop table if exists sequential10_hive")
sql("drop table if exists sequential17")
sql("drop table if exists sequential17_hive")
sql("drop table if exists sequential13")
sql("drop table if exists sequential13_hive")
sql("drop table if exists sequential3")
sql("drop table if exists sequential3_hive")
sql("drop table if exists sequential16")
sql("drop table if exists sequential16_hive")
sql("drop table if exists sequential22")
sql("drop table if exists sequential22_hive")
sql("drop table if exists sequential11")
sql("drop table if exists sequential11_hive")
sql("drop table if exists sequential42")
sql("drop table if exists sequential42_hive")
sql("drop table if exists sequential31")
sql("drop table if exists sequential31_hive")
sql("drop table if exists sequential27")
sql("drop table if exists sequential27_hive")
sql("drop table if exists sequential39")
sql("drop table if exists sequential39_hive")
sql("drop table if exists sequential23")
sql("drop table if exists sequential23_hive")
sql("drop table if exists sequential45")
sql("drop table if exists sequential45_hive")
sql("drop table if exists sequential34")
sql("drop table if exists sequential34_hive")
sql("drop table if exists sequential15")
sql("drop table if exists sequential15_hive")
sql("drop table if exists sequential41")
sql("drop table if exists sequential41_hive")
sql("drop table if exists sequential35")
sql("drop table if exists sequential35_hive")
sql("drop table if exists sequential19")
sql("drop table if exists sequential19_hive")
sql("drop table if exists sequential26")
sql("drop table if exists sequential26_hive")
sql("drop table if exists sequential4")
sql("drop table if exists sequential4_hive")
sql("drop table if exists sequential44")
sql("drop table if exists sequential44_hive")
sql("drop table if exists sequential20")
sql("drop table if exists sequential20_hive")
sql("drop table if exists sequential1")
sql("drop table if exists sequential1_hive")
sql("drop table if exists sequential29")
sql("drop table if exists sequential29_hive")
sql("drop table if exists sequential33")
sql("drop table if exists sequential33_hive")
sql("drop table if exists sequential5")
sql("drop table if exists sequential5_hive")
sql("drop table if exists sequential12")
sql("drop table if exists sequential12_hive")
sql("drop table if exists sequential18")
sql("drop table if exists sequential18_hive")
sql("drop table if exists sequential9")
sql("drop table if exists sequential9_hive")
}
}
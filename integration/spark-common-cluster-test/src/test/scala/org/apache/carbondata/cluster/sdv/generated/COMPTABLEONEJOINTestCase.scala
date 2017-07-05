
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
 * Test Class for comptableonejoin to verify all scenerios
 */

class COMPTABLEONEJOINTestCase extends QueryTest with BeforeAndAfterAll {
         

//comp_TC_Drop1
test("comp_TC_Drop1", Include) {
  sql(s"""drop table if exists  comp_table_one_join""").collect

  sql(s"""drop table if exists  comp_table_one_join_hive""").collect

}
       

//Add joins queries for two or more tables after major compaction
test("Add joins queries for two or more tables after major compaction", Include) {
  sql(s"""create table Comp_TABLE_ONE_JOIN (customer_uid String,customer_id int, gender String, first_name String, middle_name String, last_name String,customer_address String, country String) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='customer_uid')""").collect

  sql(s"""create table Comp_TABLE_ONE_JOIN_hive (customer_uid String,customer_id int, gender String, first_name String, middle_name String, last_name String,customer_address String, country String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//comp_TC1
test("comp_TC1", Include) {
  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH  '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC3
test("comp_TC3", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC4
test("comp_TC4", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC5
test("comp_TC5", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC6
test("comp_TC6", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC7
test("comp_TC7", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC8
test("comp_TC8", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC9
test("comp_TC9", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC10
test("comp_TC10", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC24
test("comp_TC24", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN order by customer_uid limit 10""").collect
}
       

//comp_TC_Drop2
test("comp_TC_Drop2", Include) {
  sql(s"""drop table if exists  comp_table_two_join""").collect

  sql(s"""drop table if exists  comp_table_two_join_hive""").collect

}
       

//comp_TC12
test("comp_TC12", Include) {
  sql(s"""create table Comp_TABLE_TWO_JOIN (customer_payment_id String,customer_id int,payment_amount Decimal(15,5), payment_mode String,payment_details String) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='customer_payment_id')""").collect

  sql(s"""create table Comp_TABLE_TWO_JOIN_hive (customer_payment_id String,customer_id int,payment_amount Decimal(15,5), payment_mode String,payment_details String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//comp_TC13
test("comp_TC13", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC14
test("comp_TC14", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC15
test("comp_TC15", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC16
test("comp_TC16", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC17
test("comp_TC17", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC18
test("comp_TC18", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC19
test("comp_TC19", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC20
test("comp_TC20", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC21
test("comp_TC21", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC22
test("comp_TC22", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC25
test("comp_TC25", Include) {
  sql(s"""select * from Comp_TABLE_TWO_JOIN order by customer_payment_id limit 10""").collect
}
       

//comp_TC26
test("comp_TC26", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC27
test("comp_TC27", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC28
test("comp_TC28", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN right join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC29
test("comp_TC29", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN full join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC30
test("comp_TC30", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC31
test("comp_TC31", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN right outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC32
test("comp_TC32", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN full outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC33
test("comp_TC33", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left semi join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC34
test("comp_TC34", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN cross join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//Add joins queries for two or more tables after minor compaction_35
test("Add joins queries for two or more tables after minor compaction_35", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC37
test("comp_TC37", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC38
test("comp_TC38", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC39
test("comp_TC39", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC40
test("comp_TC40", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC41
test("comp_TC41", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC42
test("comp_TC42", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC43
test("comp_TC43", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC44
test("comp_TC44", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC45
test("comp_TC45", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_uid,customer_id,gender,first_name,middle_name,last_name,customer_address,country')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/customer_C1.csv' INTO table Comp_TABLE_ONE_JOIN_hive """).collect

}
       

//comp_TC59
test("comp_TC59", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN""").collect
}
       

//comp_TC47
test("comp_TC47", Include) {
  sql(s"""create table if not exists Comp_TABLE_TWO_JOIN (customer_payment_id String,customer_id int,payment_amount Decimal(15,5), payment_mode String,payment_details String) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='customer_payment_id')""").collect

  sql(s"""create table if not exists Comp_TABLE_TWO_JOIN_hive (customer_payment_id String,customer_id int,payment_amount Decimal(15,5), payment_mode String,payment_details String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//comp_TC48
test("comp_TC48", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC49
test("comp_TC49", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC50
test("comp_TC50", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC51
test("comp_TC51", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC52
test("comp_TC52", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC53
test("comp_TC53", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC54
test("comp_TC54", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC55
test("comp_TC55", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC56
test("comp_TC56", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC57
test("comp_TC57", Include) {
  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN options ('DELIMITER'=',','QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE','FILEHEADER'='customer_payment_id ,customer_id,payment_amount ,payment_mode, payment_details')""").collect

  sql(s"""LOAD DATA INPATH '$resourcesPath/Data/payment_C1.csv' INTO table Comp_TABLE_TWO_JOIN_hive """).collect

}
       

//comp_TC60
test("comp_TC60", Include) {
  sql(s"""select * from Comp_TABLE_TWO_JOIN""").collect
}
       

//comp_TC61
test("comp_TC61", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC62
test("comp_TC62", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC63
test("comp_TC63", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN right join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC64
test("comp_TC64", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN full join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC65
test("comp_TC65", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC66
test("comp_TC66", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN right outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC67
test("comp_TC67", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN full outer join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC68
test("comp_TC68", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN left semi join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       

//comp_TC69
test("comp_TC69", Include) {
  sql(s"""select * from Comp_TABLE_ONE_JOIN cross join Comp_TABLE_TWO_JOIN on Comp_TABLE_ONE_JOIN.customer_id=Comp_TABLE_TWO_JOIN.customer_id""").collect
}
       
override def afterAll {
sql("drop table if exists comp_table_one_join")
sql("drop table if exists comp_table_one_join_hive")
sql("drop table if exists Comp_TABLE_ONE_JOIN")
sql("drop table if exists Comp_TABLE_ONE_JOIN_hive")
sql("drop table if exists comp_table_two_join")
sql("drop table if exists comp_table_two_join_hive")
sql("drop table if exists Comp_TABLE_TWO_JOIN")
sql("drop table if exists Comp_TABLE_TWO_JOIN_hive")
}
}

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
 * Test Class for alldatatypes1 to verify all scenerios
 */

class ALLDATATYPES1TestCase extends QueryTest with BeforeAndAfterAll {
         

//Sequential_QueryType7_TC001
test("Sequential_QueryType7_TC001", Include) {
  sql(s"""CREATE TABLE all_datatypes1 (imei string,age int,num bigint,game double,num1 decimal,date timestamp) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.age.shared_column"="shared.age","columnproperties.num.shared_column"="shared.num","columnproperties.imei.shared_column"="shared.imei","columnproperties.game.shared_column"="shared.game","columnproperties.num1.shared_column"="shared.num1",'DICTIONARY_Include'='age,num,game,num1')""").collect

  sql(s"""CREATE TABLE all_datatypes1_hive (imei string,age int,num bigint,game double,num1 decimal,date timestamp)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType7_TC002
test("Sequential_QueryType7_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table all_datatypes1 options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='imei,age,num,game,num1,date')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/dict.csv' INTO table all_datatypes1_hive """).collect

}
       

//Sequential_QueryType7_TC003
test("Sequential_QueryType7_TC003", Include) {
  sql(s"""select * from all_datatypes1""").collect
}
       

//Sequential_QueryType7_TC004
test("Sequential_QueryType7_TC004", Include) {
  sql(s"""drop table if exists  all_datatypes1""").collect

  sql(s"""drop table if exists  all_datatypes1_hive""").collect

}
       
override def afterAll {
sql("drop table if exists all_datatypes1")
sql("drop table if exists all_datatypes1_hive")
}
}

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
 * Test Class for nulltable to verify all scenerios
 */

class NULLTABLETestCase extends QueryTest with BeforeAndAfterAll {
         

//Sequential_QueryType14_TC001_drop
test("Sequential_QueryType14_TC001_drop", Include) {
  sql(s""" drop table if exists  nulltable""").collect

  sql(s""" drop table if exists  nulltable_hive""").collect

}
       

//Sequential_QueryType14_TC001
test("Sequential_QueryType14_TC001", Include) {
  sql(s"""CREATE TABLE nulltable  (id int,desc string,num double) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES("columnproperties.id.shared_column"="shared.id","columnproperties.desc.shared_column"="shared.desc",'DICTIONARY_Include'='id')""").collect

  sql(s"""CREATE TABLE nulltable_hive  (id int,desc string,num double)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Sequential_QueryType14_TC002
test("Sequential_QueryType14_TC002", Include) {
  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/nulltable.csv' INTO table nulltable  options ('DELIMITER'=',', 'QUOTECHAR'='"','BAD_RECORDS_ACTION'='FORCE', 'FILEHEADER'='id,desc,num')""").collect

  sql(s"""LOAD DATA inpath '$resourcesPath/Data/sequencedata/nulltable_hive.csv' INTO table nulltable_hive  """).collect

}
       

//Sequential_QueryType14_TC003
test("Sequential_QueryType14_TC003", Include) {
  sql(s"""select * from nulltable""").collect
}
       
override def afterAll {
sql("drop table if exists nulltable")
sql("drop table if exists nulltable_hive")
}
}
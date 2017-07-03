
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
 * Test Class for student1in to verify all scenerios
 */

class STUDENT1INTestCase extends QueryTest with BeforeAndAfterAll {
         

//drop_student_in_02
test("drop_student_in_02", Include) {
  sql(s"""drop table if exists student1_in""").collect

  sql(s"""drop table if exists student1_in_hive""").collect

}
       

//Insert_62
test("Insert_62", Include) {
  sql(s"""Create TABLE student1_in (CUST_ID int,CUST_NAME1 String) STORED BY 'org.apache.carbondata.format'""").collect

  sql(s"""Create TABLE student1_in_hive (CUST_ID int,CUST_NAME1 String)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','""").collect

}
       

//Insert_63a
test("Insert_63a", Include) {
  sql(s"""Select * from  student1_in""").collect
}
       
override def afterAll {
sql("drop table if exists student1_in")
sql("drop table if exists student1_in_hive")
}
}
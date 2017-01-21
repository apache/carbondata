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
package org.apache.carbondata.spark.testsuite.aggquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test cases for aggregate query
 */
class AggregateQueryTestCase extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("create table normal (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format'")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/10dim_4msr.csv' INTO table normal options('FILEHEADER'='column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
  }

  test("group by with having") {
    checkAnswer(
      sql("select column1,count(*) from normal group by column1 having count(*)>5"),
      Seq(Row("column1119", 6)))
  }

  override def afterAll {
    sql("drop table normal")
  }

}
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

package org.carbondata.integration.spark.testsuite.detailquery

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll


/**
 * Test Class for aggregate query on multiple datatypes
 * @author N00902756
 *
 */
class HighCardinalityDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

 /* override def beforeAll {

    sql("create cube hybridstore dimensions(column1 string in row,column2 string in row,column3 string,column4 string in row,column5 string, column6 string,column7 string,column8 string,column9 string,column10 string) measures(measure1 numeric,measure2 numeric,measure3 numeric,measure4 numeric) OPTIONS (HIGH_CARDINALITY_DIMS(column10),PARTITIONER [CLASS = 'org.carbondata.integration.spark.partition.api.impl.SampleDataPartitionerImpl' columns= (column1) PARTITION_COUNT=1])")
    sql("LOAD DATA FACT FROM './src/test/resources/10dim_4msr.csv' INTO Cube hybridstore partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
    sql("create table hybridstore_hive(column1 string,column2 string,column3 string,column4 string,column5 string, column6 string,column7 string,column8 string,column9 string,column10 string,measure1 double,measure2 double,measure3 double,measure4 double) row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
    sql("load data local inpath './src/test/resources/10dim_4msr.csv' into table hybridstore_hive");

  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(

      sql("select * from hybridstore"),
      sql("select * from hybridstore_hive")
    )
  }

  override def afterAll {
    sql("drop cube hybridstore")
    sql("drop cube hybridstore_hive")
  }*/
}
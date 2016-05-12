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
 *
 */
class ColumnGroupDataTypesTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("create table colgrp (column1 string,column2 string,column3 string,column4 string,column5 string,column6 string,column7 string,column8 string,column9 string,column10 string,measure1 int,measure2 int,measure3 int,measure4 int) STORED BY 'org.apache.carbondata.format' TBLPROPERTIES (\"COLUMN_GROUPS\"=\"(column2,column3,column4),(column7,column8,column9)\")")
  }

  test("column group data loading") {
    sql("LOAD DATA FACT FROM './src/test/resources/10dim_4msr.csv' INTO Cube colgrp partitionData(DELIMITER ',' ,QUOTECHAR '\"', FILEHEADER 'column1,column2,column3,column4,column5,column6,column7,column8,column9,column10,measure1,measure2,measure3,measure4')");
  }

  override def afterAll {
    sql("drop cube colgrp")
  }
}
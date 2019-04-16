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

package org.apache.carbondata.spark.testsuite.alterTable

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestAlterTableSortColumnsProperty extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {

  }

  test("test NO_SORT table without SORT_COLUMNS") {
    val tableName = "alter_sc_1"
    sql(s"drop table if exists $tableName")
    sql(
      s"""create table $tableName(
         | smallIntField smallInt,
         | intField int,
         | bigIntField bigint,
         | floatField float,
         | doubleField double,
         | decimalField decimal(25, 4),
         | timestampField timestamp,
         | dateField date,
         | stringField string,
         | varcharField varchar(10),
         | charField char(10),
         | arrayField array<string>,
         | structField struct<col1:string, col2:string, col3:string>
         | )
         | stored as carbondata
      """.stripMargin)
     sql(s"describe formatted $tableName").show(100, false)

    // segment 0
    sql(
      s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
         | into table $tableName
         | options ('COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    // alter table to local_sort with new SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='timestampField, intField, stringField, smallIntField')").show(100, false)
    sql(s"describe formatted $tableName").show(100, false)
    // segment 1
    sql(
      s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
         | into table $tableName
         | options ('COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    // alter table to revert SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_columns'='smallIntField, stringField, intField, timestampField')").show(100, false)
    sql(s"describe formatted $tableName").show(100, false)
    // segment 2
    sql(
      s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
         | into table $tableName
         | options ('COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    // alter table to change SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_columns'='stringField, intField')").show(100, false)
    sql(s"describe formatted $tableName").show(100, false)
    // segment 3
    sql(
      s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
         | into table $tableName
         | options ('COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    // alter table to change SORT_SCOPE and SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='charField, bigIntField, smallIntField')").show(100, false)
    sql(s"describe formatted $tableName").show(100, false)
    // segment 4
    sql(
      s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
         | into table $tableName
         | options ('COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    // alter table to change SORT_SCOPE
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='charField, bigIntField, smallIntField')").show(100, false)
    sql(s"describe formatted $tableName").show(100, false)
    sql(s"select * from $tableName").show(100, false)
  }

  override def afterAll(): Unit = {

  }

}

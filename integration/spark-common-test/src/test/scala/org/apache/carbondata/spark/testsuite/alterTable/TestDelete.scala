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
    dropTable()
    prepareTable()
  }

  override def afterAll(): Unit = {
    dropTable()
  }

  private def prepareTable(): Unit = {
    createTable(
      "alter_sc_base",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )

  }

  private def dropTable(): Unit = {
    sql(s"drop table if exists alter_sc_base")
  }

  private def createTable(
      tableName: String,
      tblProperties: Map[String, String] = Map.empty,
      withComplex: Boolean = false
  ): Unit = {
    val complexSql =
      if (withComplex) {
        ", arrayField array<string>, structField struct<col1:string, col2:string, col3:string>"
      } else {
        ""
      }
    val tblPropertiesSql =
      if (tblProperties.isEmpty) {
        ""
      } else {
        val propertiesString =
          tblProperties
            .map { entry =>
              s"'${ entry._1 }'='${ entry._2 }'"
            }
            .mkString(",")
        s"tblproperties($propertiesString)"
      }

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
         | charField char(10)
         | $complexSql
         | )
         | stored as carbondata
         | $tblPropertiesSql
      """.stripMargin)
  }

  private def loadData(tableNames: String*): Unit = {
    tableNames.foreach { tableName =>
      sql(
        s"""load data local inpath '$resourcesPath/alldatatypeforpartition.csv'
           | into table $tableName
           | options ('global_sort_partitions'='2', 'COMPLEX_DELIMITER_LEVEL_1'='$$', 'COMPLEX_DELIMITER_LEVEL_2'=':')
      """.stripMargin)
    }
  }

  private def insertData(insertTable: String, tableNames: String*): Unit = {
    tableNames.foreach { tableName =>
      sql(
        s"""insert into table $tableName select * from $insertTable
      """.stripMargin)
    }
  }



  test("IUD and Query") {
    testIUDAndQuery("alter_sc_base")
  }

  private def testIUDAndQuery(tableName: String): Unit = {
    loadData(tableName)
    loadData(tableName)
    loadData(tableName)
    loadData(tableName)
    loadData(tableName)
    loadData(tableName)

    // delete
    sql(s"select * from $tableName order by smallIntField").show(100, false)
    sql(s"delete from $tableName where smallIntField = 2")
    sql(s"select * from $tableName order by smallIntField").show(100, false)


  }

}

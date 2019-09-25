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

import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class TestAlterTableSortColumnsProperty extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      "yyyy-MM-dd")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      "yyyy-MM-dd HH:mm:ss")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    dropTable()
    prepareTable()
  }

  override def afterAll(): Unit = {
    dropTable()
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
        CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT)
  }

  private def prepareTable(): Unit = {
    createTable(
      "alter_sc_base",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )
    createTable(
      "alter_sc_base_complex",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField"),
      true
    )
    createTable(
      "alter_sc_validate",
      Map("dictionary_include"->"charField"),
      true
    )
    createTable(
      "alter_sc_iud",
      Map("dictionary_include"->"charField")
    )
    createTable(
      "alter_sc_iud_complex",
      Map("dictionary_include"->"charField"),
      true
    )
    createTable(
      "alter_sc_long_string",
      Map("LONG_STRING_COLUMNS"->"stringField"),
      true
    )
    createTable(
      "alter_sc_insert",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )
    loadData("alter_sc_insert")
    createTable(
      "alter_sc_insert_complex",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField"),
      true
    )
    loadData("alter_sc_insert_complex")
    createTable(
      "alter_sc_range_column",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField", "range_column"->"smallIntField")
    )
    createTable(
      "alter_sc_range_column_base",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )

    Array("alter_sc_add_column", "alter_sc_add_column_base").foreach { tableName =>
      sql(
        s"""create table $tableName(
           | smallIntField smallInt,
           | intField int,
           | bigIntField bigint,
           | floatField float,
           | doubleField double,
           | timestampField timestamp,
           | dateField date,
           | stringField string
           | )
           | stored as carbondata
      """.stripMargin)
    }
    // decimalField decimal(25, 4),

    createTable(
      "alter_sc_bloom",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )
    createBloomDataMap("alter_sc_bloom", "alter_sc_bloom_dm1")
    createTable(
      "alter_sc_bloom_base",
      Map("sort_scope"->"local_sort", "sort_columns"->"stringField")
    )
    createBloomDataMap("alter_sc_bloom_base", "alter_sc_bloom_base_dm1")
    createTable(
      "alter_sc_agg",
      Map("sort_scope"->"local_sort", "sort_columns"->"intField")
    )
    createAggDataMap("alter_sc_agg", "alter_sc_agg_dm1")
    createTable(
      "alter_sc_agg_base",
      Map("sort_scope"->"local_sort", "sort_columns"->"intField")
    )
    createAggDataMap("alter_sc_agg_base", "alter_sc_agg_base_dm1")

    createTable(
      "alter_sc_cli",
      Map("dictionary_include"->"charField")
    )
  }

  private def dropTable(): Unit = {
    sql(s"drop table if exists alter_sc_base")
    sql(s"drop table if exists alter_sc_base_complex")
    sql(s"drop table if exists alter_sc_validate")
    sql(s"drop table if exists alter_sc_iud")
    sql(s"drop table if exists alter_sc_iud_complex")
    sql(s"drop table if exists alter_sc_long_string")
    sql(s"drop table if exists alter_sc_insert")
    sql(s"drop table if exists alter_sc_insert_complex")
    sql(s"drop table if exists alter_sc_range_column")
    sql(s"drop table if exists alter_sc_range_column_base")
    sql(s"drop table if exists alter_sc_add_column")
    sql(s"drop table if exists alter_sc_add_column_base")
    sql(s"drop table if exists alter_sc_bloom")
    sql(s"drop table if exists alter_sc_bloom_base")
    sql(s"drop table if exists alter_sc_agg")
    sql(s"drop table if exists alter_sc_agg_base")
    sql(s"drop table if exists alter_sc_cli")
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
    // decimalField decimal(25, 4),
  }

  private def createBloomDataMap(tableName: String, dataMapName: String): Unit = {
    sql(
      s"""
         | CREATE DATAMAP $dataMapName ON TABLE $tableName
         | USING 'bloomfilter'
         | DMPROPERTIES(
         | 'INDEX_COLUMNS'='smallIntField,floatField,timestampField,dateField,stringField',
         | 'BLOOM_SIZE'='6400',
         | 'BLOOM_FPP'='0.001',
         | 'BLOOM_COMPRESS'='TRUE')
       """.stripMargin)
  }

  private def createAggDataMap(tableName: String, dataMapName: String): Unit = {
    sql(s"create datamap PreAggSum$dataMapName on table $tableName using 'preaggregate' as " +
        s"select stringField,sum(intField) as sum from $tableName group by stringField")
    sql(s"create datamap PreAggAvg$dataMapName on table $tableName using 'preaggregate' as " +
        s"select stringField,avg(intField) as avg from $tableName group by stringField")
    sql(s"create datamap PreAggCount$dataMapName on table $tableName using 'preaggregate' as " +
        s"select stringField,count(intField) as count from $tableName group by stringField")
    sql(s"create datamap PreAggMin$dataMapName on table $tableName using 'preaggregate' as " +
        s"select stringField,min(intField) as min from $tableName group by stringField")
    sql(s"create datamap PreAggMax$dataMapName on table $tableName using 'preaggregate' as " +
        s"select stringField,max(intField) as max from $tableName group by stringField")
  }

  private def loadData(tableNames: String*): Unit = {
    tableNames.foreach { tableName =>
      sql(
        s"""load data local inpath '$resourcesPath/sort_columns'
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

  test("pre-aggregate") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    val tableName = "alter_sc_agg"
    val dataMapName = "alter_sc_agg_dm1"
    val baseTableName = "alter_sc_agg_base"
    loadData(tableName, baseTableName)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $tableName"), true, "preaggregate", dataMapName)
    checkExistence(sql(s"EXPLAIN select stringField,sum(intField) as sum from $tableName where stringField = 'abc2' group by stringField"), true, "preaggregate", dataMapName)
    checkAnswer(sql(s"select stringField,sum(intField) as sum from $tableName where stringField = 'abc2' group by stringField"), sql(s"select stringField,sum(intField) as sum from $baseTableName where stringField = 'abc2' group by stringField"))

    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='smallIntField, charField')")
    loadData(tableName, baseTableName)
    checkExistence(sql(s"EXPLAIN select stringField,max(intField) as sum from $tableName where stringField = 'abc2' group by stringField"), true, "preaggregate", dataMapName)
    checkAnswer(sql(s"select stringField,max(intField) as sum from $tableName where stringField = 'abc2' group by stringField"), sql(s"select stringField,max(intField) as sum from $baseTableName where stringField = 'abc2' group by stringField"))
  }
}

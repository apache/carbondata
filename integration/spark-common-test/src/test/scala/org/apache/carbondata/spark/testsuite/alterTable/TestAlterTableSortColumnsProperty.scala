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

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.tool.CarbonCli

class TestAlterTableSortColumnsProperty extends QueryTest with BeforeAndAfterAll {

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

  test("validate sort_scope and sort_columns") {
    // invalid combination
    var ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_scope'='local_sort')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_SCOPE as local_sort when table has no SORT_COLUMNS"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_scope'='global_sort')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_SCOPE as global_sort when table has no SORT_COLUMNS"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_scope'='local_sort', 'sort_columns'='')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_COLUMNS as empty when setting SORT_SCOPE as local_sort"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_scope'='global_sort', 'sort_columns'=' ')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_COLUMNS as empty when setting SORT_SCOPE as global_sort"))

    sql("alter table alter_sc_validate set tblproperties('sort_columns'='stringField', 'sort_scope'='local_sort')")
    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'=' ')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_COLUMNS as empty when SORT_SCOPE is LOCAL_SORT"))

    sql("alter table alter_sc_validate set tblproperties('sort_scope'='global_sort')")
    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'='')")
    }
    assert(ex.getMessage.contains("Cannot set SORT_COLUMNS as empty when SORT_SCOPE is GLOBAL_SORT"))

    // wrong/duplicate sort_columns
    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'=' stringField1 , intField')")
    }
    assert(ex.getMessage.contains("stringField1 does not exist in table"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'=' stringField1 , intField, stringField1')")
    }
    assert(ex.getMessage.contains("SORT_COLUMNS Either having duplicate columns : stringField1 or it contains illegal argumnet"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'=' stringField , intField, stringField')")
    }
    assert(ex.getMessage.contains("SORT_COLUMNS Either having duplicate columns : stringField or it contains illegal argumnet"))

    // not supported data type
//    ex = intercept[RuntimeException] {
//      sql("alter table alter_sc_validate set tblproperties('sort_columns'='decimalField')")
//    }
//    assert(ex.getMessage.contains("sort_columns is unsupported for DECIMAL data type column: decimalField"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'='doubleField')")
    }
    assert(ex.getMessage.contains("sort_columns is unsupported for DOUBLE datatype column: doubleField"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'='arrayField')")
    }
    assert(ex.getMessage.contains("sort_columns is unsupported for ARRAY datatype column: arrayField"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'='structField')")
    }
    assert(ex.getMessage.contains("sort_columns is unsupported for STRUCT datatype column: structField"))

    ex = intercept[RuntimeException] {
      sql("alter table alter_sc_validate set tblproperties('sort_columns'='structField.col1')")
    }
    assert(ex.getMessage.contains("sort_columns: structField.col1 does not exist in table"))
  }

  test("long string column") {
    val ex = intercept[RuntimeException] {
      sql("alter table alter_sc_long_string set tblproperties('sort_columns'='intField, stringField')")
    }
    assert(ex.getMessage.contains("sort_columns is unsupported for long string datatype column: stringField"))
  }

  test("describe formatted") {
    // valid combination
    sql("alter table alter_sc_validate set tblproperties('sort_scope'='no_sort', 'sort_columns'='')")
    checkExistence(sql("describe formatted alter_sc_validate"), true, "NO_SORT")

    sql("alter table alter_sc_validate set tblproperties('sort_scope'='no_sort', 'sort_columns'='bigIntField,stringField')")
    checkExistence(sql("describe formatted alter_sc_validate"), true, "no_sort", "bigIntField, stringField".toLowerCase())

    sql("alter table alter_sc_validate set tblproperties('sort_scope'='local_sort', 'sort_columns'='stringField,bigIntField')")
    checkExistence(sql("describe formatted alter_sc_validate"), true, "local_sort", "stringField, bigIntField".toLowerCase())

    // global dictionary or direct dictionary
    sql("alter table alter_sc_validate set tblproperties('sort_scope'='global_sort', 'sort_columns'=' charField , bigIntField , timestampField ')")
    checkExistence(sql("describe formatted alter_sc_validate"), true, "global_sort", "charField, bigIntField, timestampField".toLowerCase())

    // supported data type
    sql("alter table alter_sc_validate set tblproperties('sort_scope'='local_sort', 'sort_columns'='smallIntField, intField, bigIntField, timestampField, dateField, stringField, varcharField, charField')")
    checkExistence(sql("describe formatted alter_sc_validate"), true, "local_sort", "smallIntField, intField, bigIntField, timestampField, dateField, stringField, varcharField, charField".toLowerCase())
  }

  test("IUD and Query") {
    testIUDAndQuery("alter_sc_iud", "alter_sc_base", "alter_sc_insert")
  }

  test("IUD and Query with complex data type") {
    testIUDAndQuery("alter_sc_iud_complex", "alter_sc_base_complex", "alter_sc_insert_complex")
  }

  private def testIUDAndQuery(tableName: String, baseTableName: String, insertTableName: String): Unit = {
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // alter table to change SORT_SCOPE and SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='charField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // alter table to local_sort with new SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='timestampField, intField, stringField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // alter table to revert SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_columns'='stringField, intField, timestampField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // alter table to change SORT_COLUMNS
    sql(s"alter table $tableName set tblproperties('sort_columns'='intField, stringField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // alter table to change SORT_SCOPE
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='charField, smallIntField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // set input segments
    (0 to 5).foreach { segment =>
      sql(s"set carbon.input.segments.default.$tableName=$segment").collect()
      sql(s"set carbon.input.segments.default.$baseTableName=$segment").collect()
      checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
      checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))
      checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
      checkAnswer(sql(s"select * from $tableName where intField >= 2 order by floatField"), sql(s"select * from $baseTableName where intField >= 2 order by floatField"))
      checkAnswer(sql(s"select * from $tableName where smallIntField = 2 or intField >= 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 or intField >= 2 order by floatField"))
    }
    sql(s"set carbon.input.segments.default.$tableName=*").collect()
    sql(s"set carbon.input.segments.default.$baseTableName=*").collect()

    // query
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where intField >= 2 order by floatField"), sql(s"select * from $baseTableName where intField >= 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 or intField >= 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 or intField >= 2 order by floatField"))

    // delete
    sql(s"delete from $tableName where smallIntField = 2")
    sql(s"delete from $baseTableName where smallIntField = 2")
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // compaction for column drift
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='charField, intField')")
    // [Segment info]:
    //   | sorted | dimension order(sort_columns is in [])                                                     | measure order
    // -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    // 0 | false  | timestampField, dateField, stringField, varcharField, charField                            | smallIntField, intField, bigIntField, floatField, doubleField
    // 1 | true   | [charField], timestampField, dateField, stringField, varcharField                          | smallIntField, intField, bigIntField, floatField, doubleField
    // 2 | false  | [timestampField, intField, stringField], charField, dateField, varcharField                | smallIntField, bigIntField, floatField, doubleField
    // 3 | false  | [stringField, intField, timestampField], charField, dateField, varcharField                | smallIntField, bigIntField, floatField, doubleField
    // 4 | false  | [intField, stringField], timestampField, charField, dateField, varcharField                | smallIntField, bigIntField, floatField, doubleField
    // 5 | true   | [charField, smallIntField], intField, stringField, timestampField, dateField, varcharField | bigIntField, floatField, doubleField
    // Column drift happened, intField and smallIntField became dimension.
    // The order of columns also changed.
    //
    // [Table info]:
    //            | dimension order(sort_columns is in [])                                                     | measure order
    // --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    // table      | [charField], smallIntField, intField, stringField, timestampField, dateField, varcharField | bigIntField, floatField, doubleField
    sql(s"alter table $tableName compact 'minor'")
    sql(s"alter table $baseTableName compact 'minor'")
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where intField >= 2 order by floatField"), sql(s"select * from $baseTableName where intField >= 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 or intField >= 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 or intField >= 2 order by floatField"))

    sql(s"delete from $tableName")
    checkAnswer(sql(s"select count(*) from $tableName"), Seq(Row(0)))
    sql(s"delete from $baseTableName")
    checkAnswer(sql(s"select count(*) from $baseTableName"), Seq(Row(0)))

    // insert & load data
    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='timestampField')")
    insertData(insertTableName, tableName, baseTableName)
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    sql(s"alter table $tableName set tblproperties('sort_scope'='no_sort', 'sort_columns'='')")
    insertData(insertTableName, tableName, baseTableName)
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='charField, bigIntField, smallIntField')")
    insertData(insertTableName, tableName, baseTableName)
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // update
    sql(s"update $tableName set (smallIntField, intField, bigIntField, floatField, doubleField) = (smallIntField + 3, intField + 3, bigIntField + 3, floatField + 3, doubleField + 3) where smallIntField = 2").collect()
    sql(s"update $baseTableName set (smallIntField, intField, bigIntField, floatField, doubleField) = (smallIntField + 3, intField + 3, bigIntField + 3, floatField + 3, doubleField + 3) where smallIntField = 2").collect()
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))

    // query
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where intField >= 2 order by floatField"), sql(s"select * from $baseTableName where intField >= 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 or intField >= 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 or intField >= 2 order by floatField"))

    // set input segments
    (6 to 11).foreach { segment =>
      sql(s"set carbon.input.segments.default.$tableName=$segment").collect()
      sql(s"set carbon.input.segments.default.$baseTableName=$segment").collect()
      checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
    }
    sql(s"set carbon.input.segments.default.$tableName=*").collect()
    sql(s"set carbon.input.segments.default.$baseTableName=*").collect()

    // no_sort compaction flow for column drift
    sql(s"alter table $tableName set tblproperties('sort_scope'='no_sort', 'sort_columns'='charField, intField')")
    // sort_scope become no_sort
    sql(s"alter table $tableName compact 'minor'")
    sql(s"alter table $baseTableName compact 'minor'")
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where intField >= 2 order by floatField"), sql(s"select * from $baseTableName where intField >= 2 order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 or intField >= 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 or intField >= 2 order by floatField"))
  }

  test("range column") {
    val tableName = "alter_sc_range_column"
    val baseTableName = "alter_sc_range_column_base"
    loadData(tableName, baseTableName)
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='smallIntField, charField')")
    loadData(tableName, baseTableName)

    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField"), sql(s"select * from $baseTableName order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField"))
  }

  test("add/drop column for sort_columns") {
    val tableName = "alter_sc_add_column"
    val baseTableName = "alter_sc_add_column_base"
    loadData(tableName, baseTableName)
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='smallIntField, stringField')")
    loadData(tableName, baseTableName)
    // add column
    sql(s"alter table $tableName add columns( varcharField varchar(10), charField char(10))")
    sql(s"alter table $baseTableName add columns( varcharField varchar(10), charField char(10))")
    loadData(tableName, baseTableName)

    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField, charField"), sql(s"select * from $baseTableName order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField, charField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is null order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is not null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is not null order by floatField"))

    // add new column to sort_columns
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='smallIntField, charField')")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField, charField"), sql(s"select * from $baseTableName order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField, charField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is null order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is not null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is not null order by floatField"))

    // drop column of old sort_columns
    sql(s"alter table $tableName drop columns(stringField)")
    sql(s"alter table $baseTableName drop columns(stringField)")
    loadData(tableName, baseTableName)
    checkAnswer(sql(s"select count(*) from $tableName"), sql(s"select count(*) from $baseTableName"))
    checkAnswer(sql(s"select * from $tableName order by floatField, charField"), sql(s"select * from $baseTableName order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 order by floatField, charField"), sql(s"select * from $baseTableName where smallIntField = 2 order by floatField, charField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is null order by floatField"))
    checkAnswer(sql(s"select * from $tableName where smallIntField = 2 and charField is not null order by floatField"), sql(s"select * from $baseTableName where smallIntField = 2 and charField is not null order by floatField"))
  }

  test("bloom filter") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    val tableName = "alter_sc_bloom"
    val dataMapName = "alter_sc_bloom_dm1"
    val baseTableName = "alter_sc_bloom_base"
    loadData(tableName, baseTableName)
    checkExistence(sql(s"SHOW DATAMAP ON TABLE $tableName"), true, "bloomfilter", dataMapName)
    checkExistence(sql(s"EXPLAIN SELECT * FROM $tableName WHERE smallIntField = 3"), true, "bloomfilter", dataMapName)
    checkAnswer(sql(s"select * from $tableName where smallIntField = 3 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 3 order by floatField"))

    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='smallIntField, charField')")
    loadData(tableName, baseTableName)
    checkExistence(sql(s"EXPLAIN SELECT * FROM $tableName WHERE smallIntField = 3"), true, "bloomfilter", dataMapName)
    checkAnswer(sql(s"select * from $tableName where smallIntField = 3 order by floatField"), sql(s"select * from $baseTableName where smallIntField = 3 order by floatField"))
  }

  test("carboncli -cmd sort_columns -p <segment folder>") {
    val tableName = "alter_sc_cli"
    // no_sort
    loadData(tableName)
    sql(s"alter table $tableName set tblproperties('sort_scope'='global_sort', 'sort_columns'='charField, timestampField')")
    // global_sort
    loadData(tableName)
    sql(s"alter table $tableName set tblproperties('sort_scope'='local_sort', 'sort_columns'='intField, stringField')")
    // local_sort
    loadData(tableName)
    // update table to generate one more index in each segment
    sql(s"update $tableName set (smallIntField, intField, bigIntField, floatField, doubleField) = (smallIntField + 3, intField + 3, bigIntField + 3, floatField + 3, doubleField + 3) where smallIntField = 2").collect()

    val table = CarbonEnv.getCarbonTable(Option("default"), tableName)(sqlContext.sparkSession)
    val tablePath = table.getTablePath
    (0 to 2).foreach { segmentId =>
      val segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId.toString)
      val args: Array[String] = Array("-cmd", "sort_columns", "-p", segmentPath)
      val out: ByteArrayOutputStream = new ByteArrayOutputStream
      val stream: PrintStream = new PrintStream(out)
      CarbonCli.run(args, stream)
      val output: String = new String(out.toByteArray)
      if (segmentId == 2) {
        assertResult(s"Input Folder: $segmentPath\nsorted by intfield,stringfield\n")(output)
      } else {
        assertResult(s"Input Folder: $segmentPath\nunsorted\n")(output)
      }
    }
  }
}

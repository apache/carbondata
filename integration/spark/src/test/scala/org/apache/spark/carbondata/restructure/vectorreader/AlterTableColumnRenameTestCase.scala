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

package org.apache.spark.carbondata.restructure.vectorreader

import scala.collection.JavaConverters
import scala.collection.mutable.WrappedArray.make

import org.apache.spark.sql.{AnalysisException, CarbonEnv, DataFrame, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class AlterTableColumnRenameTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable()
    createNonPartitionTableAndLoad()
  }

  test("test only column rename operation") {
    sql("alter table rename change empname empAddress string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("empAddress"))
    assert(null == carbonTable.getColumnByName("empname"))
  }

  test("CARBONDATA-4053 test rename column, column name in table properties changed correctly") {
    sql("create table simple_table(a string, aa1 string) stored as carbondata" +
        " tblproperties(\"sort_columns\"=\"a,aa1\")")
    sql("alter table simple_table change a a1 string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "simple_table")
    val sort_columns = carbonTable.getTableInfo.getFactTable.getTableProperties.get("sort_columns")
    assert(sort_columns.equals("a1,aa1"))
    sql("drop table simple_table")
  }

  test("Rename more than one column at a time in one operation") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (str struct<a:struct<b:int, d:int>, c:int>) STORED AS carbondata")
    sql("insert into test_rename values(named_struct('a',named_struct('b',12,'d',12), 'c', 12))")
    sql("alter table test_rename change str str22 struct<a11:struct<b2:int, d:int>, c:int>")
    sql("insert into test_rename values(named_struct('a11',named_struct('b2',24,'d',24), 'c', 24))")

    val rows = sql("select str22.a11.b2 from test_rename").collect()
    checkAnswer(sql("select str22.a11.b2 from test_rename"), Seq(Row(12), Row(24)))
    // check if old column names are still present
    val ex1 = intercept[AnalysisException] {
      sql("select str from test_rename").show(false)
    }
    assert(ex1.getMessage.contains("cannot resolve '`str`'"))

    val ex2 = intercept[AnalysisException] {
      sql("select str.a from test_rename").show(false)
    }
    assert(ex2.getMessage.contains("cannot resolve '`str.a`'"))

    // check un-altered columns
    checkAnswer(sql("select str22.c from test_rename"), Seq(Row(12), Row(24)))
    checkAnswer(sql("select str22.a11.b2 from test_rename"), Seq(Row(12), Row(24)))
  }

  test("rename complex columns with invalid structure/duplicate-names/Map-type") {
    sql("drop table if exists test_rename")
    sql(
      "CREATE TABLE test_rename (str struct<a:int,b:long>, str2 struct<a:int,b:long>, map1 " +
      "map<string, string>, str3 struct<a:int, b:map<string, string>>) STORED AS carbondata")

    val ex2 = intercept[ProcessMetaDataException] {
      sql("alter table test_rename change str str struct<a:int,b:long,c:int>")
    }
    assert(ex2.getMessage
      .contains(
        "column rename operation failed: Number of children of old and new complex columns are " +
        "not the same"))

    val ex3 = intercept[ProcessMetaDataException] {
      sql("alter table test_rename change str str int")
    }
    assert(ex3.getMessage
      .contains(
        "column rename operation failed: Old and new complex columns are not compatible " +
        "in structure"))

    val ex4 = intercept[ProcessMetaDataException] {
      sql("alter table test_rename change str str struct<a:int,a:long>")
    }
    assert(ex4.getMessage
      .contains(
        "Column Rename Operation failed. New column name str.a already exists in table " +
        "test_rename"))

    val ex5 = intercept[ProcessMetaDataException] {
      sql("alter table test_rename change str str2 struct<a:int,b:long>")
    }
    assert(ex5.getMessage
      .contains(
        "Column Rename Operation failed. New column name str2 already exists in table test_rename"))

    val ex6 = intercept[ProcessMetaDataException] {
      sql("alter table test_rename change map1 map2 map<string, struct<a:int>>")
    }
    assert(ex6.getMessage
      .contains(
        "operation failed for default.test_rename: Alter table data type change or column rename " +
        "operation failed: Given column map1.val.value with data type STRING cannot be modified. " +
        "Only Int and Decimal data types are allowed for modification"))

    // ensure all failed rename operations have been reverted to original state
    val describe = sql("desc table test_rename")
    assert(describe.collect().size == 4)
    assertResult(1)(describe.filter(
      "col_name='str' and data_type = 'struct<a:int,b:bigint>'").count())
    assertResult(1)(describe.filter(
      "col_name='str2' and data_type = 'struct<a:int,b:bigint>'").count())
    assertResult(1)(describe.filter(
      "col_name='map1' and data_type = 'map<string,string>'").count())
    assertResult(1)(describe.filter(
      "col_name='str3' and data_type = 'struct<a:int,b:map<string,string>>'").count())
  }

  def checkAnswerUtil1(df1: DataFrame, df2: DataFrame, df3: DataFrame) {
    checkAnswer(df1, Seq(Row(Row(Row(2)))))
    checkAnswer(df2, Seq(Row(Row(2))))
    checkAnswer(df3, Seq(Row(2)))
  }

  def checkAnswerUtil2(df1: DataFrame, df2: DataFrame, df3: DataFrame) {
    checkAnswer(df1, Seq(Row(Row(Row(2))), Row(Row(Row(3)))))
    checkAnswer(df2, Seq(Row(Row(2)), Row(Row(3))))
    checkAnswer(df3, Seq(Row(2), Row(3)))
  }

  test("test alter rename struct of (primitive/struct/array)") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (str1 struct<a:int>, str2 struct<a:struct<b:int>>, str3 " +
        "struct<a:struct<b:struct<c:int>>>, intfield int) STORED AS carbondata")
    sql("insert into test_rename values(named_struct('a', 2), " +
        "named_struct('a', named_struct('b', 2)), named_struct('a', named_struct('b', " +
        "named_struct('c', 2))), 1)")

    // Operation 1: rename parent column from str2 to str22 and read old rows
    sql("alter table test_rename change str2 str22 struct<a:struct<b:int>>")
    var df1 = sql("select str22 from test_rename")
    var df2 = sql("select str22.a from test_rename")
    var df3 = sql("select str22.a.b from test_rename")
    assert(df1.collect().size == 1 && df2.collect().size == 1 && df3.collect().size == 1)
    checkAnswerUtil1(df1, df2, df3)

    // Operation 2: rename child column from a to a11
    sql("alter table test_rename change str22 str22 struct<a11:struct<b:int>>")
    df1 = sql("select str22 from test_rename")
    df2 = sql("select str22.a11 from test_rename")
    df3 = sql("select str22.a11.b from test_rename")
    assert(df1.collect().size == 1 && df2.collect().size == 1 && df3.collect().size == 1)
    checkAnswerUtil1(df1, df2, df3)

    // Operation 3: rename parent column from str22 to str33
    sql("alter table test_rename change str22 str33 struct<a11:struct<b:int>>")
    df1 = sql("select str33 from test_rename")
    df2 = sql("select str33.a11 from test_rename")
    df3 = sql("select str33.a11.b from test_rename")
    assert(df1.collect().size == 1 && df2.collect().size == 1 && df3.collect().size == 1)
    checkAnswerUtil1(df1, df2, df3)

    // insert new rows
    sql("insert into test_rename values(named_struct('a', 3), " +
        "named_struct('a', named_struct('b', 3)), named_struct('a', named_struct('b', " +
        "named_struct('c', 3))), 2)")
    df1 = sql("select str33 from test_rename")
    df2 = sql("select str33.a11 from test_rename")
    df3 = sql("select str33.a11.b from test_rename")
    assert(df1.collect().size == 2 && df2.collect().size == 2 && df3.collect().size == 2)
    checkAnswerUtil2(df1, df2, df3)

    // Operation 4: rename child column from a11 to a22 & b to b11
    sql("alter table test_rename change str33 str33 struct<a22:struct<b11:int>>")
    df1 = sql("select str33 from test_rename")
    df2 = sql("select str33.a22 from test_rename")
    df3 = sql("select str33.a22.b11 from test_rename")
    assert(df1.collect().size == 2 && df2.collect().size == 2 && df3.collect().size == 2)
    checkAnswerUtil2(df1, df2, df3)

    // Operation 5: rename primitive column from intField to intField2
    sql("alter table test_rename change intField intField2 int")

    val describe = sql("desc table test_rename")
    assert(describe.collect().size == 4)
    assertResult(1)(describe.filter(
      "col_name='str1' and data_type = 'struct<a:int>'").count())
    assertResult(1)(describe.filter(
      "col_name='str33' and data_type = 'struct<a22:struct<b11:int>>'").count())
    assertResult(1)(describe.filter(
      "col_name='str3' and data_type = 'struct<a:struct<b:struct<c:int>>>'").count())

    // validate schema evolution entries for 4 above alter operations
    val (addedColumns, removedColumns, noOfEvolutions) = returnValuesAfterSchemaEvolution(
      "test_rename")
    validateSchemaEvolution(addedColumns, removedColumns, noOfEvolutions)
  }

  def returnValuesAfterSchemaEvolution(tableName: String): (Seq[ColumnSchema], Seq[ColumnSchema],
    Int) = {
    val carbonTable = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    val schemaEvolutionList = carbonTable.getTableInfo
      .getFactTable
      .getSchemaEvolution()
      .getSchemaEvolutionEntryList()
    var addedColumns = Seq[ColumnSchema]()
    var removedColumns = Seq[ColumnSchema]()
    for (i <- 0 until schemaEvolutionList.size()) {
      addedColumns ++=
      JavaConverters
        .asScalaIteratorConverter(schemaEvolutionList.get(i).getAdded.iterator())
        .asScala
        .toSeq

      removedColumns ++=
      JavaConverters
        .asScalaIteratorConverter(schemaEvolutionList.get(i).getRemoved.iterator())
        .asScala
        .toSeq
    }
    (addedColumns, removedColumns, schemaEvolutionList.size() - 1)
  }

  def validateSchemaEvolution(added: Seq[ColumnSchema], removed: Seq[ColumnSchema],
      noOfEvolutions: Int): Unit = {
    assert(noOfEvolutions == 5 && added.size == 11 && removed.size == 11)
    // asserting only first 6 entries of added and removed columns
    assert(
      added(0).getColumnName.equals("str22") && removed(0).getColumnName.equals("str2") &&
      added(1).getColumnName.equals("str22.a") && removed(1).getColumnName.equals("str2.a") &&
      added(2).getColumnName.equals("str22.a.b") && removed(2).getColumnName.equals("str2.a.b") &&
      added(3).getColumnName.equals("str22.a11") && removed(3).getColumnName.equals("str22.a") &&
      added(4).getColumnName.equals("str22.a11.b") && removed(4).getColumnName.equals("str22.a.b")&&
      added(5).getColumnName.equals("str33") && removed(5).getColumnName.equals("str22"))
  }

  test("test alter rename array of (primitive/array/struct)") {
    sql("drop table if exists test_rename")
    sql(
      "CREATE TABLE test_rename (arr1 array<int>, arr2 array<array<int>>, arr3 array<string>, " +
      "arr4 array<struct<a:int>>) STORED AS carbondata")
    sql(
      "insert into test_rename values (array(1,2,3), array(array(1,2),array(3,4)), array('hello'," +
      "'world'), array(named_struct('a',45)))")

    sql("alter table test_rename change arr1 arr11 array<int>")
    val df1 = sql("select arr11 from test_rename")
    assert(df1.collect.size == 1)
    checkAnswer(df1, Seq(Row(make(Array(1, 2, 3)))))

    sql("alter table test_rename change arr2 arr22 array<array<int>>")
    val df2 = sql("select arr22 from test_rename")
    assert(df2.collect.size == 1)
    checkAnswer(df2, Seq(Row(make(Array(make(Array(1, 2)), make(Array(3, 4)))))))

    sql("alter table test_rename change arr3 arr33 array<string>")
    val df3 = sql("select arr33 from test_rename")
    assert(df3.collect.size == 1)
    checkAnswer(sql("select arr33 from test_rename"), Seq(Row(make(Array("hello", "world")))))

    sql("alter table test_rename change arr4 arr44 array<struct<a:int>>")
    sql("alter table test_rename change arr44 arr44 array<struct<a11:int>>")

    val df4 = sql("select arr44.a11 from test_rename")
    assert(df4.collect.size == 1)
    checkAnswer(df4, Seq(Row(make(Array(45)))))

    // test for new inserted row
    sql(
      "insert into test_rename values (array(11,22,33), array(array(11,22),array(33,44)), array" +
      "('hello11', 'world11'), array(named_struct('a',4555)))")
    checkAnswer(sql("select arr11, arr22, arr33, arr44.a11 from test_rename"),
      Seq(Row(make(Array(1, 2, 3)), make(Array(make(Array(1, 2)), make(Array(3, 4)))),
        make(Array("hello", "world")), make(Array(45))),
        Row(make(Array(11, 22, 33)), make(Array(make(Array(11, 22)), make(Array(33, 44)))),
          make(Array("hello11", "world11")), make(Array(4555)))))
  }

  test("test alter rename and change datatype for map of (primitive/array/struct)") {
    sql("drop table if exists test_rename")
    sql(
      "CREATE TABLE test_rename (map1 map<int,int>, map2 map<string,array<int>>, " +
      "map3 map<int, map<string,int>>, map4 map<string,struct<b:int>>) STORED AS carbondata")
    sql("insert into test_rename values (map(1,2), map('a',array(1,2)), " +
      "map(2,map('hello',1)), map('hi',named_struct('b',3)))")
    // rename parent column from map1 to map11 and read old rows
    sql("alter table test_rename change map1 map11 map<int,int>")
    sql("insert into test_rename values (map(1,2), map('a',array(1,2)), " +
      "map(2,map('hello',1)), map('hi',named_struct('b',3)))")
    checkAnswer(sql("select map11 from test_rename"), Seq(Row(Map(1 -> 2)),
      Row(Map(1 -> 2))))
    // rename parent column from map2 to map22 and read old rows
    sql("alter table test_rename change map2 map22 map<string,array<int>>")
    sql("insert into test_rename values (map(1,2), map('a',array(1,2)), " +
      "map(2,map('hello',1)), map('hi',named_struct('b',3)))")
    checkAnswer(sql("select map22 from test_rename"), Seq(Row(Map("a" -> make(Array(1, 2)))),
      Row(Map("a" -> make(Array(1, 2)))), Row(Map("a" -> make(Array(1, 2))))))
    // rename child column and change datatype
    sql("alter table test_rename change map4 map4 map<string,struct<b2:long>>")
    sql("insert into test_rename values (map(1,2), map('a',array(1,2)), " +
        "map(2,map('hello',1)), map('hi',named_struct('b',26557544541)))")
    checkAnswer(sql("describe test_rename"),
      Seq(Row("map11", "map<int,int>", null),
        Row("map22", "map<string,array<int>>", null),
        Row("map3", "map<int,map<string,int>>", null),
        Row("map4", "map<string,struct<b2:bigint>>", null)))
    checkAnswer(sql("select map4['hi']['b2'] from test_rename"),
      Seq(Row(3), Row(3), Row(3), Row(26557544541L)))
  }

  test("test alter rename and change datatype for struct integer") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (str struct<a:int>) STORED AS carbondata")
    sql("insert into test_rename values(named_struct('a', 1234))")
    sql("insert into test_rename values(named_struct('a', 3456))")
    // only rename operation
    sql("alter table test_rename change str str1 struct<a1:int>")
    // both rename and change datatype operation
    sql("alter table test_rename change str1 str1 struct<a2:long>")
    sql("insert into test_rename values(named_struct('a2', 26557544541))")
    // rename child column
    sql("alter table test_rename change str1 str2 struct<a3:long>")
    sql("insert into test_rename values(named_struct('a3', 26557544541))")
    checkAnswer(sql("describe test_rename"), Seq(Row("str2", "struct<a3:bigint>", null)))
    checkAnswer(sql("select str2 from test_rename"),
      Seq(Row(Row(1234L)), Row(Row(3456L)), Row(Row(26557544541L)), Row(Row(26557544541L))))
  }

  test("test alter rename and change datatype for map integer") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (name string,mapField1 MAP<int, int>) STORED AS carbondata")
    sql("insert into test_rename values('a',map(1,2))")
    sql("insert into test_rename values('v',map(3,4))")
    sql(s"create index si_1 on test_rename(name) as 'carbondata'")
    // only rename operation
    sql("alter table test_rename change mapField1 mapField2 MAP<int, int>")
    sql("insert into test_rename values('df',map(5, 6))")
    // both rename and change datatype operation
    sql("alter table test_rename change mapField2 mapField3 MAP<int, long>")
    sql("insert into test_rename values('sdf',map(7, 26557544541))")
    sql("describe test_rename").show(false)
    checkAnswer(sql("describe test_rename"),
      Seq(Row("name", "string", null), Row("mapfield3", "map<int,bigint>", null)))
    checkAnswer(sql("select mapField3 from test_rename"),
      Seq(Row(Map(1 -> 2L)), Row(Map(3 -> 4L)), Row(Map(5 -> 6L)), Row(Map(7 -> 26557544541L))))
  }

  test("test alter rename and change datatype for array integer") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (arr array<int>) STORED AS carbondata")
    sql("insert into test_rename values(array(1,2,3))")
    sql("insert into test_rename values(array(4,5,6))")
    // only rename operation
    sql("alter table test_rename change arr arr1 array<int>")
    sql("insert into test_rename values(array(7,8,9))")
    // both rename and change datatype operation
    sql("alter table test_rename change arr1 arr2 array<long>")
    sql("insert into test_rename values(array(26557544541,3,46557544541))")
    checkAnswer(sql("describe test_rename"), Seq(Row("arr2", "array<bigint>", null)))
    checkAnswer(sql("select arr2 from test_rename"),
      Seq(Row(make(Array(1, 2, 3))), Row(make(Array(4, 5, 6))), Row(make(Array(7, 8, 9))),
        Row(make(Array(26557544541L, 3, 46557544541L)))))
  }

  test("test alter rename and change datatype for complex decimal types") {
    sql("drop table if exists test_rename")
    sql("CREATE TABLE test_rename (strField struct<a:decimal(5,2)>," +
        "mapField1 map<int,decimal(5,2)>, mapField2 map<int,struct<a:decimal(5,2)>>, " +
        "arrField array<decimal(5,2)>) STORED AS carbondata")
    sql("insert into test_rename values(named_struct('a', 123.45),map(1, 123.45)," +
        "map(1, named_struct('a', 123.45)),array(123.45))")
    sql("insert into test_rename values(named_struct('a', 123.45),map(2, 123.45)," +
        "map(2, named_struct('a', 123.45)),array(123.45))")
    // rename and change datatype
    sql("alter table test_rename change strField strField1 struct<a1:decimal(6,2)>")
    sql("alter table test_rename change mapField1 mapField11 map<int,decimal(6,2)>")
    // rename and change nested decimal datatype
    sql("alter table test_rename change mapField2 mapField22 map<int,struct<a2:decimal(6,2)>>")
    sql("alter table test_rename change arrField arrField1 array<decimal(6,2)>")
    sql("insert into test_rename values(named_struct('a', 1234.45),map(1, 1234.45)," +
        "map(1, named_struct('a2', 1234.45)),array(1234.45))")
    sql("insert into test_rename values(named_struct('a', 1234.45),map(2, 1234.45)," +
        "map(2, named_struct('a2', 1234.45)),array(1234.45))")
    sql("alter table test_rename compact 'minor'")
    checkAnswer(sql("describe test_rename"),
      Seq(Row("strfield1", "struct<a1:decimal(6,2)>", null),
        Row("mapfield11", "map<int,decimal(6,2)>", null),
        Row("mapfield22", "map<int,struct<a2:decimal(6,2)>>", null),
        Row("arrfield1", "array<decimal(6,2)>", null)))
    val result1 = java.math.BigDecimal.valueOf(123.45).setScale(2)
    val result2 = java.math.BigDecimal.valueOf(1234.45).setScale(2)
    checkAnswer(sql("select strField1,mapField11,mapField22,arrField1 from test_rename"),
      Seq(Row(Row(result1), Map(1 -> result1), Map(1 -> Row(result1)),
        make(Array(result1))),
        Row(Row(result1), Map(2 -> result1), Map(2 -> Row(result1)),
          make(Array(result1))),
        Row(Row(result2), Map(1 -> result2), Map(1 -> Row(result2)),
          make(Array(result2))),
        Row(Row(result2), Map(2 -> result2), Map(2 -> Row(result2)),
          make(Array(result2)))))
  }

  test("test change comment in case of complex types") {
    sql("drop table if exists test_rename")
    sql(
      "CREATE TABLE test_rename (str struct<a:int> comment 'comment') STORED AS carbondata")
    sql("alter table test_rename change str str struct<a:int> comment 'new comment'")
    var describe = sql("desc table test_rename")
    var count = describe.filter("col_name='str' and comment = 'new comment'").count()
    assertResult(1)(count)

    sql("alter table test_rename change str str struct<a1:int> comment 'new comment 2'")
    describe = sql("desc table test_rename")
    count = describe.filter("col_name='str' and comment = 'new comment 2'").count()
    assertResult(1)(count)
  }

  test("test only column rename operation with datatype change also") {
    dropTable()
    createTable()
    intercept[ProcessMetaDataException] {
      sql("alter table rename change empname empAddress Bigint")
    }
    sql("alter table rename change deptno classNo Bigint")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("classNo"))
    assert(null == carbonTable.getColumnByName("deptno"))
  }

  test("test trying to rename column which does not exists") {
    dropTable()
    createTable()
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename change carbon empAddress Bigint")
    }
    assert(ex.getMessage.contains("Column does not exist: carbon"))
  }

  test("test rename when new column name already in schema") {
    dropTable()
    createTable()
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename change empname workgroupcategoryname string")
    }
    assert(ex.getMessage.contains(
      "New column name workgroupcategoryname already exists in table rename"))
  }

  test("test change column command with comment") {
    dropTable()
    createNonPartitionTableAndLoad()
    createPartitionTableAndLoad()

    // Non-Partition Column with Non-Complex Datatype
    testChangeColumnWithComment("rename")
    testChangeColumnWithComment("rename_partition")

    // Non-Partition Column with Complex Datatype
    sql("DROP TABLE IF EXISTS rename_complextype")
    sql(s"create table rename_complextype(mapcol map<string,string>," +
      s" arraycol array<string>) stored as carbondata")
    testChangeColumnWithComment("rename_complextype", "mapcol",
      "mapcol", "map<string,string>", "map<string,string>", "map comment", false)
    testChangeColumnWithComment("rename_complextype", "arraycol",
      "arraycol", "array<string>", "array<string>", "array comment", false)

    // Partition Column
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table rename_partition " +
          "change projectcode projectcode int comment 'partitoncolumn comment'")
    }
    ex.getMessage.contains(s"Alter on partition column projectcode is not supported")
    checkExistence(sql(s"describe formatted rename_partition"), false, "partitoncolumn comment")

    // Bucket Column
    sql("DROP TABLE IF EXISTS rename_bucket")
    sql("CREATE TABLE rename_bucket (ID Int, date Timestamp, country String, name String)" +
      " STORED AS carbondata TBLPROPERTIES ('BUCKET_NUMBER'='4', 'BUCKET_COLUMNS'='name')")
    testChangeColumnWithComment("rename_bucket", "name",
      "name", "string", "string", "bucket comment", false)
  }

  test("column rename for different datatype") {
    dropTable()
    createTable()
    sql("alter table rename change projectenddate newDate Timestamp")
    sql("alter table rename change workgroupcategory newCategory int")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("newDate"))
    assert(null == carbonTable.getColumnByName("projectenddate"))
    assert(null != carbonTable.getColumnByName("newCategory"))
    assert(null == carbonTable.getColumnByName("workgroupcategory"))
  }

  test("query count after column rename and filter results") {
    dropTable()
    createNonPartitionTableAndLoad()
    val df1 = sql("select empname from rename").collect()
    val df3 =
      sql("select workgroupcategory from rename where empname = 'bill' or empname = 'sibi'")
        .collect()
    sql("alter table rename change empname empAddress string")
    val df2 = sql("select empAddress from rename").collect()
    val df4 =
      sql("select workgroupcategory from rename where empAddress = 'bill' or empAddress = 'sibi'")
        .collect()
    intercept[Exception] {
      sql("select empname from rename")
    }
    assert(df1.length == df2.length)
    assert(df3.length == df4.length)
  }

  test("compaction after column rename and count") {
    dropTable()
    createNonPartitionTableAndLoad()
    for (i <- 0 to 2) {
      loadToTable()
    }
    val df1 = sql("select empname,deptno from rename")
    sql("alter table rename change empname empAddress string")
    sql("alter table rename change deptno classNo Bigint")
    sql("alter table rename compact 'minor'")
    val df2 = sql("select empAddress,classNo from rename")
    assert(df1.count() == df2.count())
  }

  test("test rename after adding column and drop column") {
    dropTable()
    createNonPartitionTableAndLoad()
    sql("alter table rename add columns(newAdded string)")
    var carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("newAdded"))
    sql("alter table rename change newAdded addedRename string")
    carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null != carbonTable.getColumnByName("addedRename"))
    assert(null == carbonTable.getColumnByName("newAdded"))
    sql("alter table rename drop columns(addedRename)")
    carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "rename")
    assert(null == carbonTable.getColumnByName("addedRename"))
    intercept[ProcessMetaDataException] {
      sql("alter table rename change addedRename test string")
    }
  }

  test("test column rename and update and insert and delete") {
    dropTable()
    createNonPartitionTableAndLoad()
    sql("alter table rename change empname name string")
    sql("update rename set (name) = ('joey') where workgroupcategory = 'developer'").collect()
    sql("insert into rename select 20,'bill','PM','01-12-2015',3,'manager',14,'Learning'," +
        "928479,'01-01-2016','30-11-2016',75,94,13547")
    val df1Count = sql("select * from rename where name = 'joey'").count
    sql("alter table rename change name empname string")
    val df2 = sql("select * from rename where empname = 'joey'")
    assert(df1Count == df2.count())
    sql("delete from rename where empname = 'joey'")
    val df3 = sql("select empname from rename")
    sql("alter table rename change empname newname string")
    intercept[Exception] {
      sql("delete from rename where empname = 'joey'")
    }
    val df4 = sql("select newname from rename")
    assert(df3.count() == df4.count())
  }

  test("test sort columns, local dictionary and other column properties in DESC formatted, " +
       "check case sensitive also") {
    dropTable()
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata tblproperties(" +
      "'local_dictionary_include'='workgroupcategoryname','local_dictionary_exclude'='deptname'," +
      "'COLUMN_META_CACHE'='projectcode,attendance'," +
      "'SORT_COLUMNS'='workgroupcategory,utilization,salary')")
    sql("alter table rename change eMPName name string")
    sql("alter table rename change workgroupcategoryname workgroup string")
    sql("alter table rename change DEPtNaMe depTADDress string")
    sql("alter table rename change attEnDance bUNk int")
    sql("alter table rename change uTiLIZation utILIty int")

    val descLoc = sql("describe formatted rename").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("workgroup"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Exclude")) match {
      case Some(row) => assert(row.get(1).toString.contains("name,designation,deptaddress"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Sort Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("workgroupcategory, utility, salary"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Cached Min/Max Index Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("projectcode, bunk"))
      case None => assert(false)
    }
  }

  test("test rename on partition column") {
    sql("drop table if exists partitiontwo")
    sql(
      """
        | CREATE TABLE partitiontwo (empno int, designation String,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,
        |  utilization int,salary int)
        | PARTITIONED BY (doj Timestamp, empname String)
        | STORED AS carbondata
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table partitiontwo change empname name string")
    }
    ex.getMessage.contains("Renaming the partition column name is not allowed")
  }

  test("test rename column with lucene") {
    sql("DROP TABLE IF EXISTS index_test")
    sql(
      """
        | CREATE TABLE index_test(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='LOCAL_SORT')
      """.stripMargin)
    sql(
      s"""
         | CREATE INDEX dm
         | ON TABLE index_test (name, city)
         | AS 'lucene'
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table index_test change Name myName string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index indexSchema")
    sql("DROP TABLE IF EXISTS index_test")
  }

  test("test rename column with bloom indexSchema") {
    sql("DROP TABLE IF EXISTS bloomtable")
    sql(
      s"""
         | CREATE TABLE bloomtable(id INT, name STRING, city STRING, age INT,
         | s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING, s6 STRING, s7 STRING, s8 STRING)
         | STORED AS carbondata TBLPROPERTIES('table_blocksize'='128', 'sort_columns'='id')
         |  """.stripMargin)
    sql(
      s"""
         | CREATE INDEX dm3
         | ON TABLE bloomtable (city, id)
         | AS 'bloomfilter'
         | Properties('BLOOM_SIZE'='640000')
      """.stripMargin)
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table bloomtable change city nation string")
    }
    ex.getMessage.contains("alter table column rename is not supported for index")
    sql("drop table if exists bloomtable")
  }

  test("test SET command with column rename") {
    dropTable()
    createTable()
    sql("alter table rename change workgroupcategoryname testset string")
    val ex = intercept[Exception] {
      sql("alter table rename set tblproperties('column_meta_cache'='workgroupcategoryname')")
    }
    assert(ex.getMessage.contains(
      "Column workgroupcategoryname does not exists in the table rename"))
    sql("alter table rename set tblproperties('column_meta_cache'='testset')")
    val descLoc = sql("describe formatted rename").collect
    descLoc.find(_.get(0).toString.contains("Cached Min/Max Index Columns")) match {
      case Some(row) => assert(row.get(1).toString.contains("testset"))
      case None => assert(false)
    }
  }

  test("test column rename with change datatype for decimal datatype") {
    sql("drop table if exists deciTable")
    sql("create table decitable(name string, age int, avg decimal(30,10)) STORED AS carbondata")
    sql("alter table decitable change avg newAvg decimal(32,11)")
    val descLoc = sql("describe formatted decitable").collect
    descLoc.find(_.get(0).toString.contains("newavg")) match {
      case Some(row) => assert(row.get(1).toString.contains("decimal(32,11)"))
      case None => assert(false)
    }
    sql("drop table if exists decitable")
  }

  test("test column rename of bigint column") {
    sql("drop table if exists biginttable")
    sql("create table biginttable(name string, age int, bigintfield bigint) STORED AS carbondata")
    sql("alter table biginttable change bigintfield testfield bigint")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "biginttable")
    assert(null != carbonTable.getColumnByName("testfield"))
    assert(null == carbonTable.getColumnByName("bigintfield"))
    sql("drop table if exists biginttable")
  }

  test("test column comment after column rename") {
    dropTable()
    createTable()
    checkExistence(sql("describe formatted rename"), true, "This column has comment ")
    sql("alter table rename change deptno classno bigint")
    checkExistence(sql("describe formatted rename"), true, "This column has comment ")
  }

  test("test compaction after table rename and alter set tblproerties") {
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
    sql(
      "CREATE TABLE test_rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_rename rename to test_rename_compact")
    sql("alter table test_rename_compact set tblproperties(" +
        "'sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_rename_compact OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    val res1 = sql("select * from test_rename_compact")
    sql("alter table test_rename_compact compact 'major'")
    val res2 = sql("select * from test_rename_compact")
    assert(res1.collectAsList().containsAll(res2.collectAsList()))
    checkExistence(sql("show segments for table test_rename_compact"), true, "Compacted")
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
  }

  test("test compaction after alter set tblproerties- add and drop") {
    sql("DROP TABLE IF EXISTS test_alter")
    sql(
      "CREATE TABLE test_alter (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_alter set tblproperties(" +
        "'sort_columns'='deptno,projectcode', 'sort_scope'='local_sort')")
    sql("alter table test_alter drop columns(deptno)")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    sql("alter table test_alter add columns(deptno int)")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE test_alter OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
    val res1 = sql("select * from test_alter")
    sql("alter table test_alter compact 'major'")
    val res2 = sql("select * from test_alter")
    assert(res1.collectAsList().containsAll(res2.collectAsList()))
    sql("DROP TABLE IF EXISTS test_alter")
  }

  override def afterAll(): Unit = {
    dropTable()
  }

  def dropTable(): Unit = {
    sql("DROP TABLE IF EXISTS rename")
    sql("DROP TABLE IF EXISTS rename_partition")
    sql("DROP TABLE IF EXISTS test_rename")
    sql("DROP TABLE IF EXISTS test_rename_compact")
    sql("DROP TABLE IF EXISTS test_alter")
    sql("DROP TABLE IF EXISTS simple_table")
  }

  def testChangeColumnWithComment(tableName: String): Unit = {
    // testcase1: columnrename: no; datatypechange: no;
    testChangeColumnWithComment(tableName, "testcase1_1_col",
      "testcase1_1_col", "string", "string", "testcase1_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase1_2_col",
      "testcase1_2_col", "int", "int", "testcase1_2 comment", true)
    testChangeColumnWithComment(tableName, "testcase1_3_col",
      "testcase1_3_col", "decimal(30,10)", "decimal(30,10)", "testcase1_3 comment", true)

    // testcase2: columnrename: yes; datatypechange: no;
    testChangeColumnWithComment(tableName, "testcase2_col",
      "testcase2_col_renamed", "string", "string", "testcase2 comment", true)

    // testcase3: columnrename: no; datatypechange: yes
    testChangeColumnWithComment(tableName, "testcase3_1_col",
      "testcase3_1_col", "int", "bigint", "testcase3_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase3_2_col",
      "testcase3_2_col", "decimal(30,10)", "decimal(32,11)", "testcase3_2 comment", true)

    // testcase4: columnrename: yes; datatypechange: yes,
    testChangeColumnWithComment(tableName, "testcase4_1_col",
      "testcase4_1_col_renamed", "int", "bigint", "testcase4_1 comment", true)
    testChangeColumnWithComment(tableName, "testcase4_2_col",
      "testcase4_2_col_renmaed", "decimal(30,10)", "decimal(32,11)", "testcase4_2 comment", true)

    // testcase5: special characters in comments
    // scalastyle:off
    testChangeColumnWithComment(tableName, "testcase5_1_col",
      "testcase5_1_col_renamed", "string", "string", "测试comment", true)
    // scalastyle:on
    testChangeColumnWithComment(tableName, "testcase5_2_col",
      "testcase5_2_col_renmaed", "decimal(30,10)", "decimal(32,11)", "\001\002comment", true)
  }

  def testChangeColumnWithComment(tableName: String, oldColumnName: String,
      newColumnName: String, oldDataType: String, newDataType: String, comment: String,
      needCreateOldColumn: Boolean): Unit = {
    checkExistence(sql(s"describe formatted $tableName"), false, comment)
    if (needCreateOldColumn) {
      sql(s"alter table $tableName add columns ($oldColumnName $oldDataType)")
    }
    sql(s"alter table $tableName " +
        s"change $oldColumnName $newColumnName $newDataType comment '$comment'")
    checkExistence(sql(s"describe formatted $tableName"), true, comment)
    if (!newDataType.equalsIgnoreCase(oldDataType)) {
      sql(s"describe formatted $tableName")
        .collect.find(_.get(0).toString.contains(newColumnName)) match {
        case Some(row) => assert(row.get(1).toString.contains(newDataType))
        case None => assert(false)
      }
    }
  }

  def createNonPartitionTableAndLoad(): Unit = {
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def createPartitionTableAndLoad(): Unit = {
    sql(
      "CREATE TABLE rename_partition (empno int, empname String, designation String," +
        " doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int," +
        " deptname String," +
        " projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
        " utilization int,salary int) PARTITIONED BY (projectcode int) STORED AS carbondata")
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def loadToTable(): Unit = {
    sql(
      s"""LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO TABLE rename OPTIONS
         |('DELIMITER'= ',', 'QUOTECHAR'= '\"')""".stripMargin)
  }

  def createTable(): Unit = {
    sql(
      "CREATE TABLE rename (empno int, empname String, designation String, doj Timestamp, " +
      "workgroupcategory int, workgroupcategoryname String, deptno int comment \"This column " +
      "has comment\", deptname String, " +
      "projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int," +
      "utilization int,salary int) STORED AS carbondata")
  }
}

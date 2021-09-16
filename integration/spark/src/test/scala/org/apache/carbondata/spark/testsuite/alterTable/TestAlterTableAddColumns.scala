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

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters
import scala.collection.mutable
import scala.collection.mutable.WrappedArray.make

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonProperties

class TestAlterTableAddColumns extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable()
  }

  override def afterAll(): Unit = {
    dropTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
  }

  private def dropTable(): Unit = {
    sql("drop table if exists test_add_column_with_comment")
    sql("drop table if exists test_add_column_for_complex_table")
  }

  private def testAddColumnForComplexTable(): Unit = {
    val tableName = "test_add_column_for_complex_table"
    sql(s"""DROP TABLE IF EXISTS ${ tableName }""")
    sql(
      s"""
        | CREATE TABLE IF NOT EXISTS ${ tableName }(id INT, name STRING, file array<array<float>>,
        | city STRING, salary FLOAT, ls STRING, map_column map<short,int>,
        |  struct_column struct<s:short>)
        | STORED AS carbondata
        | TBLPROPERTIES('sort_columns'='name', 'SORT_SCOPE'='LOCAL_SORT',
        |  'LONG_STRING_COLUMNS'='ls',
        | 'LOCAL_DICTIONARY_ENABLE'='true', 'LOCAL_DICTIONARY_INCLUDE'='city')
      """.stripMargin)
    sql(
      s"""
        | insert into table ${tableName} values
        | (1, 'name1', array(array(1.1, 2.1), array(1.1, 2.1)), 'city1', 40000.0,
        |  '${ ("123" * 12000) }', map(1,1), named_struct('s',1)),
        | (2, 'name2', array(array(1.2, 2.2), array(1.2, 2.2)), 'city2', 50000.0,
        |  '${ ("456" * 12000) }', map(2,2), named_struct('s',2)),
        | (3, 'name3', array(array(1.3, 2.3), array(1.3, 2.3)), 'city3', 60000.0,
        |  '${ ("789" * 12000) }', map(3,3), named_struct('s',3))
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(3)))
    checkAnswer(sql(s"select name, city from ${ tableName } where id = 3"),
      Seq(Row("name3", "city3")))

    sql(s"""desc formatted ${tableName}""").collect()
    sql(
      s"""alter table ${tableName} add columns (add_column string)
         | TBLPROPERTIES('LOCAL_DICTIONARY_INCLUDE'='add_column')""".stripMargin)
    sql(s"""ALTER TABLE ${tableName} SET TBLPROPERTIES('SORT_COLUMNS'='id, add_column, city')""")
    sql(s"""desc formatted ${tableName}""").collect()

    sql(
      s"""
        | insert into table ${tableName} values
        | (4, 'name4', array(array(1.4, 2.4), array(1.4, 2.4)), 'city4', 70000.0,
        | '${ ("123" * 12000) }', map(4,4), named_struct('s',4), 'add4'),
        | (5, 'name5', array(array(1.5, 2.5), array(1.5, 2.5)), 'city5', 80000.0,
        | '${ ("456" * 12000) }', map(5,5), named_struct('s',5), 'add5'),
        | (6, 'name6', array(array(1.6, 2.6), array(1.6, 2.6)), 'city6', 90000.0,
        | '${ ("789" * 12000) }', map(6,6), named_struct('s',6), 'add6')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(6)))
    checkAnswer(sql(s"""select add_column, id, city, name from ${ tableName } where id = 6"""),
        Seq(Row("add6", 6, "city6", "name6")))

    sql(s"""desc formatted ${tableName}""").collect()
    sql(s"""ALTER TABLE ${ tableName } DROP COLUMNS (city)""")
    sql(s"""desc formatted ${tableName}""").collect()

    sql(
      s"""
        | insert into table ${tableName} values
        | (7, 'name7', array(array(1.4, 2.4), array(1.4, 2.4)), 70000.0,
        |  '${ ("123" * 12000) }', map(7,7), named_struct('s',7), 'add7'),
        | (8, 'name8', array(array(1.5, 2.5), array(1.5, 2.5)), 80000.0,
        |  '${ ("456" * 12000) }', map(8,8), named_struct('s',8), 'add8'),
        | (9, 'name9', array(array(1.6, 2.6), array(1.6, 2.6)), 90000.0,
        |  '${ ("789" * 12000) }', map(9,9), named_struct('s',9), 'add9')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(9)))
    checkAnswer(sql(s"""select id, add_column, name from ${ tableName } where id = 9"""),
      Seq(Row(9, "add9", "name9")))

    sql(s"""desc formatted ${tableName}""").collect()
    sql(
      s"""alter table ${tableName} add columns (add_column_ls string)
         | TBLPROPERTIES('LONG_STRING_COLUMNS'='add_column_ls')""".stripMargin)
    sql(s"""desc formatted ${tableName}""").collect()

    sql(
      s"""
        | insert into table ${tableName} values
        | (10, 'name10', array(array(1.4, 2.4), array(1.4, 2.4)), 100000.0,
        |  '${ ("123" * 12000) }', map(4,4), named_struct('s',4), 'add4', '${ ("999" * 12000) }'),
        | (11, 'name11', array(array(1.5, 2.5), array(1.5, 2.5)), 110000.0,
        |  '${ ("456" * 12000) }', map(5,5), named_struct('s',5), 'add5', '${ ("888" * 12000) }'),
        | (12, 'name12', array(array(1.6, 2.6), array(1.6, 2.6)), 120000.0,
        |  '${ ("789" * 12000) }', map(6,6), named_struct('s',6), 'add6', '${ ("777" * 12000) }')
      """.stripMargin)
    checkAnswer(sql(s"select count(1) from ${ tableName }"), Seq(Row(12)))
    checkAnswer(sql(
      s"""select id, name, file, add_column_ls, map_column, struct_column
         | from ${ tableName } where id = 10""".stripMargin),
      Seq(Row(10, "name10", mutable.WrappedArray.make(Array(mutable.WrappedArray.make(
        Array(1.4, 2.4)), mutable.WrappedArray.make(Array(1.4, 2.4)))),
        ("999" * 12000), Map(4 -> 4), Row(4))))

    sql(s"""DROP TABLE IF EXISTS ${ tableName }""")
  }

  def addedColumnsInSchemaEvolutionEntry(tableName: String): Seq[ColumnSchema] = {
    val carbonTable = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    val schemaEvolutionList = carbonTable.getTableInfo
      .getFactTable
      .getSchemaEvolution()
      .getSchemaEvolutionEntryList()
    var addedColumns = Seq[ColumnSchema]()
    for (i <- 0 until schemaEvolutionList.size()) {
      addedColumns ++=
      JavaConverters
        .asScalaIteratorConverter(schemaEvolutionList.get(i).getAdded.iterator())
        .asScala
        .toSeq
    }
    addedColumns
  }

  test("Test adding of array of all primitive datatypes") {
    import scala.collection.mutable.WrappedArray.make
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intfield int) STORED AS carbondata")
    sql(
      "ALTER TABLE alter_com ADD COLUMNS(arr1 array<short>, arr2 array<int>, arr3 " +
      "array<long>, arr4 array<double>, arr5 array<decimal(8,2)>, arr6 array<string>, arr7 " +
      "array<char(5)>, arr8 array<varchar(50)>, arr9 array<boolean>, arr10 array<date>, arr11 " +
      "array<timestamp> )")
    val columns = sql("desc table alter_com").collect()
    assert(columns.size.equals(12))
    sql(
      "insert into alter_com values(1,array(1,5),array(1,2),array(1,2,3),array(1.2d,2.3d),array" +
      "(4.5,6.7),array('hello','world'),array('a','bcd'),array('abcd','efg'),array(true,false)," +
      "array('2017-02-01','2018-09-11'),array('2017-02-01 00:01:00','2018-02-01 02:21:00') )")
    checkAnswer(sql(
      "select * from alter_com"),
      Seq(Row(1,
        make(Array(1, 5)),
        make(Array(1, 2)),
        make(Array(1, 2, 3)),
        make(Array(1.2, 2.3)),
        make(Array(java.math.BigDecimal.valueOf(4.5).setScale(2),
          java.math.BigDecimal.valueOf(6.7).setScale(2))),
        make(Array("hello", "world")),
        make(Array("a", "bcd")),
        make(Array("abcd", "efg")),
        make(Array(true, false)),
        make(Array(Date.valueOf("2017-02-01"),
          Date.valueOf("2018-09-11"))),
        make(Array(Timestamp.valueOf("2017-02-01 00:01:00"),
          Timestamp.valueOf("2018-02-01 02:21:00")))
      )))

    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 11)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("Test adding of struct of all primitive datatypes") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intField INT) STORED AS " +
      "carbondata")
    sql("ALTER TABLE alter_com ADD COLUMNS(structField struct<a:short,b:int,c:long,d:double, " +
      "e:decimal(8,2),f:string,g:char(5),h:varchar(50),i:boolean,j:date,k:timestamp>)")
    sql("insert into alter_com values(1, named_struct('a',1,'b',2,'c',3,'d',1.23,'e',2.34,'f'," +
      "'hello','g','abc','h','def','i',true,'j','2017-02-01','k','2018-02-01 02:00:00.0') ) ")
    checkAnswer(sql("select structField from alter_com"),
      Seq(Row(Row(1, 2, 3, 1.23, java.math.BigDecimal.valueOf(2.34).setScale(2), "hello", "abc",
        "def", true, Date.valueOf("2017-02-01"), Timestamp.valueOf("2018-02-01 02:00:00.0")))))

    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 1)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("Test adding of map of all primitive datatypes") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intfield int) STORED AS carbondata")
    sql("ALTER TABLE alter_com ADD COLUMNS(map1 Map<short,int>, map2 Map<long,double>, " +
        "map3 Map<decimal(3,2),string>, map4 Map<char(5),varchar(50)>, map5 Map<boolean,date>, " +
        "map6 Map<string,timestamp>)")
    sql("insert into alter_com values(1, map(1,2),map(3,2.34), map(1.23,'hello')," +
        "map('abc','def'), map(true,'2017-02-01')," + "map('time','2018-02-01 02:00:00.0')) ")
    sql("select * from alter_com").show(false)
    checkAnswer(sql("select * from alter_com"),
      Seq(Row(1,
        Map(1 -> 2),
        Map(3 -> 2.34),
        Map(java.math.BigDecimal.valueOf(1.23).setScale(2) -> "hello"),
        Map("abc" -> "def"),
        Map(true -> Date.valueOf("2017-02-01")),
        Map("time" -> Timestamp.valueOf("2018-02-01 02:00:00.0")))))
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 6)
  }


  test("Test alter add complex type with long string column and compaction") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("create table alter_com (a int, b string, arr1 array<string>) stored as carbondata" +
        " tblproperties('long_string_columns'='b')")
    sql("insert into alter_com select 1,'a',array('hi')")
    sql("insert into alter_com select 2,'b',array('hello','world')")
    sql("ALTER TABLE alter_com ADD COLUMNS(struct1 STRUCT<a:int, b:string>)")
    sql("insert into alter_com select 3,'c',array('hi'),null")
    sql("insert into alter_com select 4,'d',array('hi'),named_struct('s1',4,'s2','d')")
    sql("alter table alter_com compact 'minor'")
    checkAnswer(sql("""Select count(*) from alter_com"""), Seq(Row(4)))
    checkAnswer(sql("Select * from alter_com"),
      Seq(Row(1, "a", mutable.WrappedArray.make(Array("hi")), null),
        Row(2, "b", mutable.WrappedArray.make(Array("hello", "world")), null),
        Row(3, "c", mutable.WrappedArray.make(Array("hi")), null),
        Row(4, "d", mutable.WrappedArray.make(Array("hi")), Row(4, "d"))))
    sql("DROP TABLE IF EXISTS alter_com")
  }

  def insertIntoTableForArrayType(): Unit = {
    sql("insert into alter_com values(4,array(2),array(1,2,3,4),array('abc','def'))")
    sql("insert into alter_com values(5,array(1,2),array(1), array('Hulk','Thor'))")
    sql(
      "insert into alter_com values(6,array(1,2,3),array(1234,8,33333),array('Iron','Man'," +
      "'Jarvis'))")
  }

  def checkRestulForArrayType(): Unit = {
    val totalRows = sql("select * from alter_com").collect()
    val a = sql("select * from alter_com where array_contains(arr2,2)").collect
    val b = sql("select * from alter_com where array_contains(arr3,'Thor')").collect
    val c = sql("select * from alter_com where intField = 1").collect
    assert(totalRows.size == 6)
    assert(a.size == 1)
    assert(b.size == 1)
    // check default value for newly added array column that is index - 3 and 4
    assert(c(0)(2) == null && c(0)(3) == null)
  }

  def createTableForComplexTypes(dictionary: String, complexType: String): Unit = {
    if (complexType.equals("ARRAY")) {
      sql("DROP TABLE IF EXISTS alter_com")
      sql("CREATE TABLE alter_com(intField INT, arr1 array<int>) STORED AS carbondata")
      sql("insert into alter_com values(1,array(1) )")
      sql("insert into alter_com values(2,array(9,0) )")
      sql("insert into alter_com values(3,array(11,12,13) )")
      sql(s"ALTER TABLE alter_com ADD COLUMNS(arr2 array<int>, arr3 array<string>) TBLPROPERTIES" +
          s"('$dictionary'='arr3')")
      val schema = sql("describe alter_com").collect()
      assert(schema.size == 4)
    } else if (complexType.equals("STRUCT")) {
      sql("DROP TABLE IF EXISTS alter_struct")
      sql(
        "create table alter_struct(roll int, department struct<id1:string,name1:string>) STORED " +
        "AS carbondata")
      sql("insert into alter_struct values(1, named_struct('id1', 'id1','name1','name1'))")
      sql("ALTER TABLE alter_struct ADD COLUMNS(struct1 struct<a:string,b:string>, temp string," +
          " intField int, struct2 struct<c:string,d:string,e:int>, arr array<int>) TBLPROPERTIES " +
          s"('$dictionary'='struct1, struct2')")
      val schema = sql("describe alter_struct").collect()
      assert(schema.size == 7)
    } else if (complexType.equals("MAP")) {
      sql("DROP TABLE IF EXISTS alter_com")
      sql(
        "create table alter_com(roll int, department map<string,string>) STORED " +
        "AS carbondata")
      sql("insert into alter_com values(1, map('id1','name1'))")
      sql("ALTER TABLE alter_com ADD COLUMNS(map1 map<string,string>, " +
          "map2 map<string,string>)")
      sql(s"alter table alter_com set tblproperties('$dictionary'='map1, map2')")
      val schema = sql("describe alter_com").collect()
      assert(schema.size == 4)
    }
  }

  test("Test alter add for arrays enabling local dictionary") {
    import scala.collection.mutable.WrappedArray.make
    createTableForComplexTypes("LOCAL_DICTIONARY_INCLUDE", "ARRAY")
    // For the previous segments the default value for newly added array column is null
    insertIntoTableForArrayType
    checkRestulForArrayType
    sql(s"ALTER TABLE alter_com ADD COLUMNS(arr4 array<int>) ")
    sql(s"ALTER TABLE alter_com ADD COLUMNS(arr5 array<int>, str struct<a:int,b:string>) ")
    sql(
      "insert into alter_com values(2,array(9,0),array(1,2,3),array('hello','world'),array(6,7)," +
      "array(8,9), named_struct('a',1,'b','abcde') )")
    checkAnswer(sql("select * from alter_com where array_contains(arr4,6)"),
      Seq(Row(2,
        make(Array(9, 0)),
        make(Array(1, 2, 3)),
        make(Array("hello", "world")),
        make(Array(6, 7)),
        make(Array(8, 9)),
        Row(1, "abcde"))))

    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 5)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("Test alter add for arrays disabling local dictionary") {
    createTableForComplexTypes("LOCAL_DICTIONARY_EXCLUDE", "ARRAY")
    // For the previous segments the default value for newly added array column is null
    insertIntoTableForArrayType
    checkRestulForArrayType
    sql("DROP TABLE IF EXISTS alter_com")
  }

  def insertIntoTableForMapType(): Unit = {
    sql("insert into alter_com values(2,map('id2','name2'),map('key1','val1'),map('key2','val2'))")
    sql("insert into alter_com values(3,map('id3','name3'),map('key3','val3'), map('key4','val4'))")
    sql("insert into alter_com values(4,map('id4','name4'),map('key5','val5'),map('key6','val6'))")
  }

  def checkRestulForMapType(): Unit = {
    val totalRows = sql("select * from alter_com").collect()
    val a = sql("select * from alter_com where map1['key1']='val1'").collect
    val b = sql("select * from alter_com where map2['key4']='val4'").collect
    val c = sql("select * from alter_com where roll = 1").collect
    assert(totalRows.size == 4)
    assert(a.size == 1)
    assert(b.size == 1)
    // check default value for newly added map columns that is index - 3 and 4
    assert(c(0)(2) == null && c(0)(3) == null)
  }

  test("Test alter add for map enabling local dictionary") {
    createTableForComplexTypes("LOCAL_DICTIONARY_INCLUDE", "MAP")
    insertIntoTableForMapType()
    checkRestulForMapType()
    sql(s"ALTER TABLE alter_com ADD COLUMNS(map3 map<int,int>) ")
    sql(s"ALTER TABLE alter_com ADD COLUMNS(map4 map<int,int>, str struct<a:int,b:string>) ")
    sql(
      "insert into alter_com values(5,map('df','dfg'),map('df','dfg'), map('df','dfg'),map(6,7)," +
      "map(5,9),named_struct('a',1,'b','abcde'))")
    sql("alter table alter_com compact 'minor'")
    assert(sql("select * from alter_com where map3[6]=7").collect().size == 1)
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 5)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("Test alter add for map disabling local dictionary") {
    createTableForComplexTypes("LOCAL_DICTIONARY_EXCLUDE", "MAP")
    insertIntoTableForMapType()
    checkRestulForMapType()
    sql(s"ALTER TABLE alter_com ADD COLUMNS(map3 map<int,int>) ")
    sql(s"ALTER TABLE alter_com ADD COLUMNS(map4 map<int,int>, str struct<a:int,b:string>) ")
    sql(
      "insert into alter_com values(5,map('df','dfg'),map('df','dfg'), map('df','dfg'),map(6,7)," +
      "map(5,9),named_struct('a',1,'b','abcde'))")
    sql("alter table alter_com compact 'minor'")
    assert(sql("select * from alter_com where map3[6]=7").collect().size == 1)
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 5)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  def insertIntoTableForStructType(): Unit = {
    sql("insert into alter_struct values(2, named_struct('id1', 'id2','name1','name2'), " +
      "named_struct('a','id2','b', 'abc2'), 'hello world', 5, named_struct('c','id3'," +
      "'d', 'abc3','e', 22), array(1,2,3)  )")
    sql("insert into alter_struct values(3, named_struct('id1', 'id3','name1','name3'), " +
      "named_struct('a','id2.1','b', 'abc2.1'), 'India', 5, named_struct('c','id3.1'," +
      "'d', 'abc3.1','e', 25), array(4,5)  )")
  }

  def checkResultForStructType(): Unit = {
    val totalRows = sql("select * from alter_struct").collect()
    val a = sql("select * from alter_struct where struct1.a = 'id2' ").collect()
    val b = sql(
      "select * from alter_struct where struct1.a = 'id2' or struct2.c = 'id3.1' or " +
      "array_contains(arr,5) ")
      .collect()
    val c = sql("select * from alter_struct where roll = 1").collect()

    assert(totalRows.size == 3)
    assert(a.size == 1)
    assert(b.size == 2)
    // check default value for newly added struct column
    assert(c(0)(2) == null)
  }

  ignore("Test alter add for structs enabling local dictionary") {
    createTableForComplexTypes("LOCAL_DICTIONARY_INCLUDE", "STRUCT")
    // For the previous segments the default value for newly added struct column is null
    insertIntoTableForStructType
    checkResultForStructType
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_struct")
    assert(addedColumns.size == 5)
    sql("DROP TABLE IF EXISTS alter_struct")
  }

  ignore("Test alter add for structs, disabling local dictionary") {
    createTableForComplexTypes("LOCAL_DICTIONARY_EXCLUDE", "STRUCT")
    // For the previous segments the default value for newly added struct column is null
    insertIntoTableForStructType
    checkResultForStructType
    sql("DROP TABLE IF EXISTS alter_struct")
  }

  test("test alter add multi-level complex columns") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intField INT) STORED AS carbondata ")
    sql("insert into alter_com values(1)")
    // multi-level nested array
    sql(
      "ALTER TABLE alter_com ADD COLUMNS(arr1 array<array<int>>, arr2 array<struct<a1:string, " +
      "map1:Map<string, string>>>) ")
    sql(
      "insert into alter_com values(1, array(array(1,2)), array(named_struct('a1','st','map1', " +
      "map('a','b'))))")
    // multi-level nested struct
    sql("ALTER TABLE alter_com ADD COLUMNS(struct1 struct<s1:string, arr: array<int>>," +
        " struct2 struct<num:double,contact:map<string,array<int>>>) ")
    sql("insert into alter_com values(1, " +
        "array(array(1,2)), array(named_struct('a1','st','map1', map('a','b'))), " +
        "named_struct('s1','hi','arr',array(1,2)), named_struct('num',2.3,'contact',map('ph'," +
        "array(1,2))))")
    // multi-level nested map
    sql(
      "ALTER TABLE alter_com ADD COLUMNS(map1 map<string,array<string>>, map2 map<string," +
      "struct<d:int, s:struct<im:string>>>)")
    sql("insert into alter_com values(1,  " +
        "array(array(1,2)), array(named_struct('a1','st','map1', map('a','b'))), " +
        "named_struct('s1','hi','arr',array(1,2)), named_struct('num',2.3,'contact',map('ph'," +
        "array(1,2))),map('a',array('hi')), map('a',named_struct('d',23,'s',named_struct('im'," +
        "'sh'))))")
    sql("alter table alter_com compact 'minor'")
    checkAnswer(sql("select * from alter_com"),
      Seq(Row(1, null, null, null, null, null, null),
        Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
          null, null, null, null),
        Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
          Row("hi", make(Array(1, 2))), Row(2.3, Map("ph" -> make(Array(1, 2)))), null, null),
        Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
          Row("hi", make(Array(1, 2))), Row(2.3, Map("ph" -> make(Array(1, 2)))),
          Map("a" -> make(Array("hi"))), Map("a" -> Row(23, Row("sh"))))
      ))
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 6)
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("test add column to partition table with complex column") {
    sql("drop table if exists alter_com")
    sql("create table alter_com(id int, map1 map<int,int>) " +
        "partitioned by(name string) stored as carbondata")
    sql("insert into alter_com values( 1,map(1,2),'sh')")
    sql("ALTER TABLE alter_com ADD COLUMNS(intF int)")
    sql("insert into alter_com values(1,map(1,2),1,'df')")
    checkAnswer(sql("select * from alter_com"),
      Seq(Row(1, Map(1 -> 2), null, "sh"), Row(1, Map(1 -> 2), 1, "df")))
  }

  test("Validate default values of complex columns added by alter command") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(doubleField double, arr1 array<long> ) STORED AS carbondata")
    sql("insert into alter_com values(1.1,array(77))")
    sql("ALTER TABLE alter_com ADD COLUMNS(arr2 array<int>)")
    val exception = intercept[Exception] {
      sql(
        "ALTER TABLE alter_com ADD COLUMNS(arr3 array<int>) TBLPROPERTIES('DEFAULT.VALUE.arr3'= " +
        "'array(77)')")
    }
    val exceptionMessage =
      "operation failed for default.alter_com: Alter table add operation failed: Cannot add a " +
      "default value in case of complex columns."
    assert(exception.getMessage.contains(exceptionMessage))
  }

  test("alter table add complex columns with comment") {
    sql("""create table test_add_column_with_comment(
          | col1 string comment 'col1 comment',
          | col2 string)
          | stored as carbondata""".stripMargin)
    sql("""alter table test_add_column_with_comment add columns(
          | col3 array<int> comment "col3 comment",
          | col4 struct<a:int,b:string> comment "col4 comment",
          | col5 array<string> comment "",
          | col6 array<string> )""".stripMargin)
    val describe = sql("describe test_add_column_with_comment")
    var count = describe.filter("col_name='col3' and comment = 'col3 comment'").count()
    assertResult(1)(count)
    count = describe.filter("col_name='col4' and comment = 'col4 comment'").count()
    assertResult(1)(count)
    count = describe.filter("col_name='col5' and comment = ''").count()
    assertResult(1)(count)
    count = describe.filter("col_name='col6' and comment is null").count()
    assertResult(1)(count)
    sql("DROP TABLE IF EXISTS test_add_column_with_comment")
  }

  test("alter table add columns with comment") {
    sql("""create table test_add_column_with_comment(
        | col1 string comment 'col1 comment',
        | col2 int,
        | col3 string)
        | stored as carbondata""".stripMargin)
    sql("""alter table test_add_column_with_comment add columns(
        | col4 string comment "col4 comment",
        | col5 int,
        | col6 string comment "")""".stripMargin)
    val describe = sql("describe test_add_column_with_comment")
    var count = describe.filter("col_name='col5' and comment is null").count()
    assertResult(1)(count)
    count = describe.filter("col_name='col4' and comment = 'col4 comment'").count()
    assertResult(1)(count)
    count = describe.filter("col_name='col6' and comment = ''").count()
    assertResult(1)(count)
  }

  test("Validate datatypes and column names of added complex columns") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql(
      """create table alter_com(
        | col1 array<string>)
        | stored as carbondata""".stripMargin)
    sql(
      """alter table alter_com add columns(
        | col2 array<string>,
        | col3 struct<a:int,b:long>,
        | col4 string,
        | col5 array<long>,
        | col6 int,
        | col7 struct<a:string> )""".stripMargin)
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_com")
    assert(addedColumns.size == 6)
    for (i <- 0 until addedColumns.size) {
      val column = addedColumns(i)
      assert((column.getColumnName.equals("col2") && DataTypes.isArrayType(column.getDataType) &&
              column.getNumberOfChild.equals(1)) ||
             (column.getColumnName.equals("col3") && DataTypes.isStructType(column.getDataType) &&
              column.getNumberOfChild.equals(2)) ||
             (column.getColumnName.equals("col5") && DataTypes.isArrayType(column.getDataType) &&
              column.getNumberOfChild.equals(1)) ||
             (column.getColumnName.equals("col7") && DataTypes.isStructType(column.getDataType) &&
              column.getNumberOfChild.equals(1)) ||
             column.getColumnName.equals("col4") || column.getColumnName.equals("col6")
      )
    }
    sql("DROP TABLE IF EXISTS alter_com")
  }

  test("[CARBONDATA-3596] Fix exception when execute load data command or " +
       "select sql on a table which includes complex columns after execute 'add column' command") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
    // test for not vector reader
    testAddColumnForComplexTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "false")
    // test for vector reader
    testAddColumnForComplexTable()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_VECTOR_READER, "true")
  }

  test("test the complex columns with global sort compaction") {
    sql("DROP TABLE IF EXISTS alter_global1")
    sql("CREATE TABLE alter_global1(intField INT) STORED AS carbondata " +
        "TBLPROPERTIES('sort_columns'='intField','sort_scope'='global_sort')")
    sql("insert into alter_global1 values(1)")
    sql("insert into alter_global1 values(2)")
    sql("insert into alter_global1 values(3)")
    sql( "ALTER TABLE alter_global1 ADD COLUMNS(str1 array<int>)")
    sql("insert into alter_global1 values(4, array(1))")
    sql("insert into alter_global1 values(5, null)")
    sql( "ALTER TABLE alter_global1 ADD COLUMNS(str2 array<string>)")
    sql("insert into alter_global1 values(6, array(1), array('', 'hi'))")
    sql("insert into alter_global1 values(7, array(1), array('bye', 'hi'))")
    sql("ALTER TABLE alter_global1 ADD COLUMNS(str3 array<date>, str4 struct<s1:timestamp>)")
    sql(
      "insert into alter_global1 values(8, array(1), array('bye', 'hi'), array('2017-02-01'," +
      "'2018-09-11'),named_struct('s1', '2017-02-01 00:01:00'))")
    val expected = Seq(Row(1, null, null, null, null),
      Row(2, null, null, null, null),
      Row(3, null, null, null, null),
      Row(4, make(Array(1)), null, null, null),
      Row(5, null, null, null, null),
      Row(6, make(Array(1)), make(Array("", "hi")), null, null),
      Row(7, make(Array(1)), make(Array("bye", "hi")), null, null),
      Row(8, make(Array(1)), make(Array("bye", "hi")),
        make(Array(Date.valueOf("2017-02-01"), Date.valueOf("2018-09-11"))),
        Row(Timestamp.valueOf("2017-02-01 00:01:00"))))
    checkAnswer(sql("select * from alter_global1"), expected)
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_global1")
    assert(addedColumns.size == 4)
    sql("alter table alter_global1 compact 'minor'")
    checkAnswer(sql("select * from alter_global1"), expected)
    sql("DROP TABLE IF EXISTS alter_global1")
  }

  test("test the multi-level complex columns with global sort compaction") {
    sql("DROP TABLE IF EXISTS alter_global2")
    sql("CREATE TABLE alter_global2(intField INT) STORED AS carbondata " +
        "TBLPROPERTIES('sort_columns'='intField','sort_scope'='global_sort')")
    sql("insert into alter_global2 values(1)")
    // multi-level nested array
    sql(
      "ALTER TABLE alter_global2 ADD COLUMNS(arr1 array<array<int>>, arr2 array<struct<a1:string," +
      "map1:Map<string, string>>>) ")
    sql(
      "insert into alter_global2 values(1, array(array(1,2)), array(named_struct('a1','st'," +
      "'map1', map('a','b'))))")
    // multi-level nested struct
    sql("ALTER TABLE alter_global2 ADD COLUMNS(struct1 struct<s1:string, arr: array<int>>," +
        " struct2 struct<num:double,contact:map<string,array<int>>>) ")
    sql("insert into alter_global2 values(1, " +
        "array(array(1,2)), array(named_struct('a1','st','map1', map('a','b'))), " +
        "named_struct('s1','hi','arr',array(1,2)), named_struct('num',2.3,'contact',map('ph'," +
        "array(1,2))))")
    // multi-level nested map
    sql(
      "ALTER TABLE alter_global2 ADD COLUMNS(map1 map<string,array<string>>, map2 map<string," +
      "struct<d:int, s:struct<im:string>>>)")
    sql("insert into alter_global2 values(1,  " +
    "array(array(1,2)), array(named_struct('a1','st','map1', map('a','b'))), " +
    "named_struct('s1','hi','arr',array(1,2)), named_struct('num',2.3,'contact',map('ph'," +
    "array(1,2))),map('a',array('hi')), map('a',named_struct('d',23,'s',named_struct('im'," +
    "'sh'))))")
    val expected = Seq(Row(1, null, null, null, null, null, null),
      Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
        null, null, null, null),
      Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
        Row("hi", make(Array(1, 2))), Row(2.3, Map("ph" -> make(Array(1, 2)))), null, null),
      Row(1, make(Array(make(Array(1, 2)))), make(Array(Row("st", Map("a" -> "b")))),
        Row("hi", make(Array(1, 2))), Row(2.3, Map("ph" -> make(Array(1, 2)))),
        Map("a" -> make(Array("hi"))), Map("a" -> Row(23, Row("sh"))))
    )
    checkAnswer(sql("select * from alter_global2"), expected)
    val addedColumns = addedColumnsInSchemaEvolutionEntry("alter_global2")
    assert(addedColumns.size == 6)
    sql("alter table alter_global2 compact 'minor'")
    checkAnswer(sql("select * from alter_global2"), expected)
    sql("DROP TABLE IF EXISTS alter_global2")
  }
}

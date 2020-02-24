/*

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements. See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License. You may obtain a copy of the License at
    *
    http://www.apache.org/licenses/LICENSE-2.0
    *
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
    */
package org.apache.carbondata.spark.testsuite.createTable.TestCreateDDLForComplexMapType

import java.io.{BufferedWriter, File, FileWriter}
import java.util

import au.com.bytecode.opencsv.CSVWriter
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk
import scala.collection.JavaConversions._

class TestCreateDDLForComplexMapType extends QueryTest with BeforeAndAfterAll {
  private val conf: Configuration = new Configuration(false)

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath

  val path = s"$rootPath/integration/spark/src/test/resources/maptest2.csv"

  private def checkForLocalDictionary(dimensionRawColumnChunks: util
  .List[DimensionRawColumnChunk]): Boolean = {
    var isLocalDictionaryGenerated = false
    import scala.collection.JavaConversions._
    isLocalDictionaryGenerated = dimensionRawColumnChunks
      .filter(dimensionRawColumnChunk => dimensionRawColumnChunk.getDataChunkV3
        .isSetLocal_dictionary).size > 0
    isLocalDictionaryGenerated
  }

  def createCSVFile(): Unit = {
    val out = new BufferedWriter(new FileWriter(path));
    val writer = new CSVWriter(out);

    val employee1 = Array("1\u0002Nalla\u00012\u0002Singh\u00011\u0002Gupta\u00014\u0002Kumar")

    val employee2 = Array("10\u0002Nallaa\u000120\u0002Sissngh\u0001100\u0002Gusspta\u000140" +
                          "\u0002Kumar")

    var listOfRecords = List(employee1, employee2)

    writer.writeAll(listOfRecords)
    out.close()
  }

  override def beforeAll(): Unit = {
    createCSVFile()
    sql("DROP TABLE IF EXISTS carbon")
  }

  override def afterAll(): Unit = {
    new File(path).delete()
  }

  test("Single Map One Level") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,string>"))
  }

  test("Single Map with Two Nested Level") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED AS carbondata
         |"""
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,map<int,string>>"))
  }

  test("Map Type with array type as value") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,array<INT>>
         | )
         | STORED AS carbondata
         |
         """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("map<string,array<int>>"))
  }

  test("Map Type with struct type as value") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,struct<key:INT,val:INT>>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim
      .equals("map<string,struct<key:int,val:int>>"))
  }

  test("Map Type as child to struct type") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField struct<key:INT,val:map<INT,INT>>
         | )
         | STORED AS carbondata """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim
      .equals("struct<key:int,val:map<int,int>>"))
  }

  test("Map Type as child to array type") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField array<map<INT,INT>>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon """.stripMargin).collect()
    assert(desc(0).get(1).asInstanceOf[String].trim.equals("array<map<int,int>>"))
    sql("insert into carbon values(array(map(1,2,2,3), map(100,200,200,300)))")
    sql("select * from carbon").show(false)
  }

  test("Test Load data in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    sql("insert into carbon values(map(1,'Nalla',2,'Singh',3,'Gupta',4,'Kumar'))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar"))))
  }

  test("Test Load data in map with empty value") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    sql("insert into carbon values(map(1,'Nalla',2,'',3,'Gupta',4,'Kumar'))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "", 3 -> "Gupta", 4 -> "Kumar"))))
  }

  // Global Dictionary for Map type
  test("Test Load data in map with dictionary include") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("insert into carbon values(map('vi','Nalla','sh','Singh','al','Gupta'))")
    sql("select * from carbon").show(false)
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map("vi" -> "Nalla", "sh" -> "Singh", "al" -> "Gupta"))))
  }

  test("Test Load data in map with partition columns") {
    sql("DROP TABLE IF EXISTS carbon")
    val exception = intercept[AnalysisException](
      sql(
        s"""
           | CREATE TABLE carbon(
           | a INT,
           | mapField array<STRING>,
           | b STRING
           | )
           | PARTITIONED BY (mp map<int,string>)
           | STORED AS carbondata
           | """
          .stripMargin)
    )
    assertResult("Cannot use map<int,string> for partition column;")(exception.getMessage())
  }

  test("Test IUD in map columns") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | a INT,
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("insert into carbon values(1, map(1,'Nalla',2,'Singh',3,'Gupta',4,'Kumar'))")
    sql("insert into carbon values(2, map(1,'abc',2,'xyz',3,'hello',4,'mno'))")
    val exception = intercept[UnsupportedOperationException](
      sql("update carbon set(mapField)=('1,haha') where a=1").show(false))
    assertResult("Unsupported operation on Complex data type")(exception.getMessage())
    sql("delete from carbon where mapField[1]='abc'")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(1, Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar"))))

  }

  test("Test Load duplicate keys data in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    val desc = sql(
      s"""
         | Describe Formatted
         | carbon
         | """.stripMargin).collect()
    sql("insert into carbon values(map(1,'Nalla',2,'Singh',1,'Gupta',4,'Kumar'))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar"))))
  }

  test("Test Load data in map of map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED AS carbondata """
        .stripMargin)
    sql("insert into carbon values(map('manish', map(1,'nalla',2,'gupta'), 'kunal', map(1, 'kapoor', 2, 'sharma')))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map("manish" -> Map(1 -> "nalla", 2 -> "gupta"),
        "kunal" -> Map(1 -> "kapoor", 2 -> "sharma")))))
  }

  test("Test Load duplicate keys data in map of map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<STRING,map<INT,STRING>>
         | )
         | STORED AS carbondata
         |"""
        .stripMargin)
    sql("insert into carbon values(map('manish', map(1,'nalla',1,'gupta'), 'kunal', map(1, 'kapoor', 2, 'sharma')))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map("manish" -> Map(1 -> "gupta"),
        "kunal" -> Map(1 -> "kapoor", 2 -> "sharma")))))
  }

  test("Test Create table as select with map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql("DROP TABLE IF EXISTS carbon1")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("insert into carbon values(map(1,'Nalla',2,'Singh',3,'Gupta',4,'Kumar'))")
    sql(
      s"""
         | CREATE TABLE carbon1
         | AS
         | Select *
         | From carbon
         | """
        .stripMargin)
    checkAnswer(sql("select * from carbon1"), Seq(
      Row(Map(1 -> "Nalla", 2 -> "Singh", 3 -> "Gupta", 4 -> "Kumar"))))
  }

  test("Test Create table with double datatype in map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<DOUBLE,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("insert into carbon values(map(1.23,'Nalla',2.34,'Singh',3.67676,'Gupta',3.67676,'Kumar'))")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1.23 -> "Nalla", 2.34 -> "Singh", 3.67676 -> "Kumar"))))
  }

  test("Load Map data from CSV File") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar"))
    ))
  }

  test("test compaction with map data type") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    sql("alter table carbon compact 'minor'")
    sql("show segments for table carbon").show(false)
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar")),
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar")),
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar")),
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar"))
    ))
    sql("DROP TABLE IF EXISTS carbon")
  }

  test("Sort Column table property blocking for Map type") {
    sql("DROP TABLE IF EXISTS carbon")
    val exception1 = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE carbon(
           | mapField map<STRING,STRING>
           | )
           | STORED AS carbondata
           | TBLPROPERTIES('SORT_COLUMNS'='mapField')
           | """
          .stripMargin)
    }
    assert(exception1.getMessage
      .contains(
        "sort_columns is unsupported for map datatype column: mapfield"))
  }

  test("Data Load Fail Issue") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,STRING>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon OPTIONS(
         | 'header' = 'false')
       """.stripMargin)
    sql("INSERT INTO carbon SELECT * FROM carbon")
    checkAnswer(sql("select * from carbon"), Seq(
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(1 -> "Gupta", 2 -> "Singh", 4 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar")),
      Row(Map(10 -> "Nallaa", 20 -> "Sissngh", 100 -> "Gusspta", 40 -> "Kumar"))
      ))
  }

  test("Struct inside map") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,struct<kk:STRING,mm:STRING>>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("INSERT INTO carbon values(map(1, named_struct('kk', 'man', 'mm', 'nan'), 2, named_struct('kk', 'kands', 'mm', 'dsnknd')))")
    sql("INSERT INTO carbon SELECT * FROM carbon")
    checkAnswer(sql("SELECT * FROM carbon limit 1"),
      Seq(Row(Map(1 -> Row("man", "nan"), (2 -> Row("kands", "dsnknd"))))))
  }

  test("Struct inside map pushdown") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | mapField map<INT,struct<kk:STRING,mm:STRING>>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("INSERT INTO carbon values(map(1, named_struct('kk', 'man', 'mm', 'nan'), 2, named_struct('kk', 'kands', 'mm', 'dsnknd')))")
    checkAnswer(sql("SELECT mapField[1].kk FROM carbon"), Row("man"))
  }

  test("Map inside struct") {
    sql("DROP TABLE IF EXISTS carbon")
    sql(
      s"""
         | CREATE TABLE carbon(
         | structField struct<intVal:INT,map1:MAP<STRING,STRING>>
         | )
         | STORED AS carbondata
         | """
        .stripMargin)
    sql("INSERT INTO carbon values(named_struct('intVal', 1, 'map1', map('man','nan','kands','dsnknd')))")
    val res = sql("SELECT structField.intVal FROM carbon").show(false)
    checkAnswer(sql("SELECT structField.intVal FROM carbon"), Seq(Row(1)))
  }

}
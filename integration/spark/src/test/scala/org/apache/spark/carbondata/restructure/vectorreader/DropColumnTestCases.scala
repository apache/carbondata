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

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters

import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class DropColumnTestCases extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
  }

  test("test drop column and insert into hive table") {
    def test_drop_and_insert(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(
        "CREATE TABLE hivetable(intField INT,stringField STRING,timestampField TIMESTAMP," +
        "decimalField DECIMAL(6,2)) STORED AS PARQUET")
      sql("INSERT INTO TABLE hivetable SELECT * FROM dropcolumntest")
      checkAnswer(sql("SELECT * FROM hivetable"), sql("SELECT * FROM dropcolumntest"))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_insert()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_insert()
  }

  test("test drop column and load data") {
    def test_drop_and_load(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
      checkAnswer(sql("SELECT count(*) FROM dropcolumntest"), Row(2))
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_load
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_load

  }

  test("test drop column and compaction") {
    def test_drop_and_compaction(): Unit = {
      beforeAll
      sql(
        "CREATE TABLE dropcolumntest(intField INT,stringField STRING,charField STRING," +
        "timestampField TIMESTAMP,decimalField DECIMAL(6,2)) STORED AS carbondata")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data1.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,charField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest DROP COLUMNS(charField)")
      sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/restructure/data4.csv' INTO TABLE dropcolumntest"
          + s" OPTIONS('FILEHEADER'='intField,stringField,timestampField,decimalField')")
      sql("ALTER TABLE dropcolumntest COMPACT 'major'")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "0 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "1 Compacted")
      checkExistence(sql("SHOW SEGMENTS FOR TABLE dropcolumntest"), true, "0.1 Success")
      afterAll
    }
    sqlContext.setConf("carbon.enable.vector.reader", "true")
    test_drop_and_compaction()
    sqlContext.setConf("carbon.enable.vector.reader", "false")
    test_drop_and_compaction()
  }

  def checkSchemaSize(value: Integer): Unit = {
    val schema = sql("describe alter_com").collect()
    assert(schema.size.equals(value))
  }

  def checkDroppedColumnsInSchemaEvolutionEntry(tableName: String, value: Integer): Unit = {
    val carbonTable = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    val schemaEvolutionList = carbonTable.getTableInfo
      .getFactTable
      .getSchemaEvolution()
      .getSchemaEvolutionEntryList()
    var droppedColumns = Seq[ColumnSchema]()
    for (i <- 0 until schemaEvolutionList.size()) {
      droppedColumns ++=
      JavaConverters
        .asScalaIteratorConverter(schemaEvolutionList.get(i).getRemoved.iterator())
        .asScala
        .toSeq
    }
    assert(droppedColumns.size.equals(value))
  }

  test("test dropping of array of all primitive types") {
    import scala.collection.mutable.WrappedArray.make
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intfield int, arr array<int>, arr1 array<short>, " +
        "arr2 array<int>, arr3 array<long>, arr4 array<double>, arr5 array<decimal(8,2)>, " +
        "arr6 array<string>, arr7 array<char(5)>, arr8 array<varchar(50)>, arr9 array<boolean>, " +
        "arr10 array<date>, arr11 array<timestamp>) STORED AS carbondata")
    sql("insert into alter_com values(1,array(1,5),array(1,5),array(1,2),array(1,2,3)," +
        "array(1.2d,2.3d),array(4.5,6.7),array('hello','world'),array('a','bcd')," +
        "array('abcd','efg'),array(true,false),array('2017-02-01','2018-09-11')," +
        "array('2017-02-01 00:01:00','2018-02-01 02:21:00') )")
    sql("ALTER TABLE alter_com DROP COLUMNS(arr1,arr2,arr3,arr4,arr5,arr6) ")
    sql("ALTER TABLE alter_com DROP COLUMNS(arr7,arr8,arr9) ")
    sql("ALTER TABLE alter_com DROP COLUMNS(arr10,arr11) ")
    val exception = intercept[Exception] {
      sql("ALTER TABLE alter_com DROP COLUMNS(arr10,arr10) ")
    }
    val exceptionMessage =
      "arr10 is duplicate. Duplicate columns not allowed"
    assert(exception.getMessage.contains(exceptionMessage))

    checkSchemaSize(2)
    checkAnswer(sql("select * from alter_com"), Seq(Row(1, make(Array(1, 5)))))
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 11)
    // check adding columns with same names again
    sql(
      "ALTER TABLE alter_com ADD COLUMNS(arr1 array<short>, arr2 array<int>, arr3 " +
      "array<long>, arr4 array<double>, arr5 array<decimal(8,2)>, arr6 array<string>, arr7 " +
      "array<char(5)>, arr8 array<varchar(50)>, arr9 array<boolean>, arr10 array<date>, arr11 " +
      "array<timestamp> )")
    val columns = sql("desc table alter_com").collect()
    assert(columns.size == 13)
    sql(
      "insert into alter_com values(2,array(2,5),array(2,5),array(2,2),array(2,2,3),array(2.2d," +
      "2.3d),array(2.5,6.7),array('hello2','world'),array('a2','bcd'),array('abcd2','efg'),array" +
      "(true,false), array('2017-02-01','2018-09-11'),array('2017-02-01 00:01:00','2018-02-01 " +
      "02:21:00') )")
    checkAnswer(sql(
      "select * from alter_com"),
      Seq(Row(1, make(Array(1, 5)), null, null, null, null, null, null, null, null, null, null,
        null), Row(2,
        make(Array(2, 5)),
        make(Array(2, 5)),
        make(Array(2, 2)),
        make(Array(2, 2, 3)),
        make(Array(2.2, 2.3)),
        make(Array(java.math.BigDecimal.valueOf(2.5).setScale(2),
          java.math.BigDecimal.valueOf(6.7).setScale(2))),
        make(Array("hello2", "world")),
        make(Array("a2", "bcd")),
        make(Array("abcd2", "efg")),
        make(Array(true, false)),
        make(Array(Date.valueOf("2017-02-01"),
          Date.valueOf("2018-09-11"))),
        make(Array(Timestamp.valueOf("2017-02-01 00:01:00"),
          Timestamp.valueOf("2018-02-01 02:21:00")))
      )))
  }

  test("test dropping of struct of all primitive types") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql("CREATE TABLE alter_com(intField INT,struct1 struct<a:short,b:int,c:long,d:double," +
        "e:decimal(8,2),f:string,g:char(5),h:varchar(50),i:boolean,j:date,k:timestamp>) " +
        "STORED AS carbondata")
    sql("insert into alter_com values(1, named_struct('a',1,'b',2,'c',3,'d',1.23,'e',2.34,'f'," +
        "'hello','g','abc','h','def','i',true,'j','2017-02-01','k','2018-02-01 02:00:00.0') ) ")
    sql("ALTER TABLE alter_com DROP COLUMNS(struct1) ")
    checkSchemaSize(1)
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 1)
    // check adding column with same name again
    sql("ALTER TABLE alter_com ADD COLUMNS(struct1 struct<a:short,b:int,c:long,d:double, " +
        "e:decimal(8,2),f:string,g:char(5),h:varchar(50),i:boolean,j:date,k:timestamp>)")
    checkSchemaSize(2)
    sql("insert into alter_com values(2, named_struct('a',1,'b',2,'c',3,'d',1.23,'e',2.34,'f'," +
        "'hello','g','abc','h','def','i',true,'j','2017-02-01','k','2018-02-01 02:00:00.0') ) ")
    checkAnswer(sql("select struct1 from alter_com"),
      Seq(Row(Row(1, 2, 3, 1.23, java.math.BigDecimal.valueOf(2.34).setScale(2), "hello", "abc",
        "def", true, Date.valueOf("2017-02-01"), Timestamp.valueOf("2018-02-01 02:00:00.0"))),
        Row(null)))
  }

  test("test dropping of map of all primitive types") {
    import scala.collection.mutable.WrappedArray.make
    sql("DROP TABLE IF EXISTS alter_com")
    sql(
      "CREATE TABLE alter_com(intField INT,arr array<int>, map1 map<int,long>, map2 map<short," +
      "double>, map3 map<decimal(8,2),string>, map4 map<char(5),varchar(50)>,map5 map<boolean," +
      "date>, map6 map<int, timestamp>) STORED AS carbondata")
    sql("insert into alter_com values(1,array(1,2),map(2,3),map(3,4.5d),map((cast(\"1.2\" as " +
        "decimal(8,2))),'hello'),map('abc','hello world'),map(true,'2017-02-01'),map(1," +
        "'2017-02-01 00:01:00') ) ")
    sql("ALTER TABLE alter_com DROP COLUMNS(map1,map2,map3,map4,map5,map6) ")
    checkSchemaSize(2)
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 6)
    checkAnswer(sql("select * from alter_com"), Seq(Row(1, make(Array(1, 2)))))
  }

  test("Test alter drop for multi-level array") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql(
      "CREATE TABLE alter_com(intField INT, arr1 array<struct<a:int, b:string>> comment 'arr1 " +
      "comment', arr2 array<struct<a:int, b:array<long>>>, arr3 array<array<string>> comment " +
      "'arr3 comment', arr4 array<array<array<string>>> ) STORED AS carbondata ")
    sql("ALTER TABLE alter_com DROP COLUMNS(arr1, arr2, arr3, arr4) ")
    checkSchemaSize(1)
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 4)
  }

  test("Test alter drop for multi-level STRUCT") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql(
      "CREATE TABLE alter_com(struc struct<a:string,b:int>, struct1 struct<a:int," +
      "b:array<struct<id:int, name:string>>>, struct2 struct<a:struct<b:array<int>,c:int>," +
      "d:string> ) STORED AS carbondata ")
    sql("ALTER TABLE alter_com DROP COLUMNS(struct1,struct2) ")
    checkSchemaSize(1)
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 2)
  }

  test("Test alter drop for multi-level MAP") {
    sql("DROP TABLE IF EXISTS alter_com")
    sql(
      "CREATE TABLE alter_com(arr array<int>, map1 map<int,array<int>>, map2 map<string," +
      "array<map<int,int>>>, map3 map<int, struct<a:map<int,int>,b:string>>, map4 map<int," +
      "map<int,map<int,int>>> ) STORED AS carbondata ")
    sql("ALTER TABLE alter_com DROP COLUMNS(map1,map2,map3,map4) ")
    checkSchemaSize(1)
    checkDroppedColumnsInSchemaEvolutionEntry("alter_com", 4)
  }

  override def afterAll {
    sqlContext.setConf(
      "carbon.enable.vector.reader", CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
    sql("DROP TABLE IF EXISTS dropcolumntest")
    sql("DROP TABLE IF EXISTS hivetable")
  }
}

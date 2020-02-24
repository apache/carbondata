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
package org.apache.spark.sql.carbondata.datasource

import java.io.File

import org.apache.commons.codec.binary.{Base64, Hex}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.util.BinaryUtil

class SparkCarbonDataSourceBinaryTest extends QueryTest with BeforeAndAfterAll {

  var writerPath = s"$target/SparkCarbonFileFormat/WriterOutput/"
  var outputPath = writerPath + 2
  //getCanonicalPath gives path with \, but the code expects /.
  writerPath = writerPath.replace("\\", "/")

  var sdkPath = new File(this.getClass.getResource("/").getPath + "../../../../sdk/sdk/")
    .getCanonicalPath

  def buildTestBinaryData(): Any = {
    FileUtils.deleteDirectory(new File(writerPath))
    FileUtils.deleteDirectory(new File(outputPath))

    val sourceImageFolder = sdkPath + "/src/test/resources/image/flowers"
    val sufAnnotation = ".txt"
    BinaryUtil.binaryToCarbon(sourceImageFolder, writerPath, sufAnnotation, ".jpg")
  }

  def cleanTestData() = {
    FileUtils.deleteDirectory(new File(writerPath))
    FileUtils.deleteDirectory(new File(outputPath))
  }

  override def beforeAll(): Unit = {
    defaultConfig()
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    buildTestBinaryData()

    FileUtils.deleteDirectory(new File(outputPath))
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  override def afterAll(): Unit = {
    cleanTestData()
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  test("Test direct sql read carbon") {
    assert(new File(writerPath).exists())
    checkAnswer(
      sql(s"SELECT COUNT(*) FROM carbon.`$writerPath`"),
      Seq(Row(3)))
  }

  test("Test read image carbon with spark carbon file format, generate by sdk, CTAS") {
    sql("DROP TABLE IF EXISTS binaryCarbon")
    sql("DROP TABLE IF EXISTS binaryCarbon3")
    FileUtils.deleteDirectory(new File(outputPath))
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      sql(s"CREATE TABLE binaryCarbon USING CARBON OPTIONS(PATH '$writerPath')")
      sql(s"CREATE TABLE binaryCarbon3 USING CARBON OPTIONS(PATH '$outputPath')" +
          " AS SELECT * FROM binaryCarbon")
    } else {
      sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
      sql(s"CREATE TABLE binaryCarbon3 USING CARBON LOCATION '$outputPath'" +
          " AS SELECT * FROM binaryCarbon")
    }
    checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
      Seq(Row(3)))
    checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
      Seq(Row(3)))
    sql("DROP TABLE IF EXISTS binaryCarbon")
    sql("DROP TABLE IF EXISTS binaryCarbon3")
    FileUtils.deleteDirectory(new File(outputPath))
  }

  test("Don't support sort_columns") {
    sql("DROP TABLE IF EXISTS binaryTable")
    var exception = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE binaryTable (
           |    id DOUBLE,
           |    label BOOLEAN,
           |    name STRING,
           |    image BINARY,
           |    autoLabel BOOLEAN)
           | using carbon
           | options('SORT_COLUMNS'='image')
            """.stripMargin)
      // TODO: it should throw exception when create table
      sql("SELECT COUNT(*) FROM binaryTable").show()
    }
    assert(exception.getCause.getMessage
      .contains(
        "sort columns not supported for array, struct, map, double, float, decimal, varchar, " +
        "binary"))

    sql("DROP TABLE IF EXISTS binaryTable")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      exception = intercept[Exception] {
        sql(
          s"""
             | CREATE TABLE binaryTable
             | using carbon
             | options(PATH '$writerPath',
             |  'SORT_COLUMNS'='image')
                """.stripMargin)
        sql("SELECT COUNT(*) FROM binaryTable").show()
      }
    } else {
      exception = intercept[Exception] {
        sql(
          s"""
             | CREATE TABLE binaryTable
             | using carbon
             | options('SORT_COLUMNS'='image')
             | LOCATION '$writerPath'
                """.stripMargin)
        sql("SELECT COUNT(*) FROM binaryTable").show()
      }
    }
    assert(exception.getMessage.contains("Cannot use sort columns during infer schema"))


    sql("DROP TABLE IF EXISTS binaryTable")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      exception = intercept[Exception] {
        sql(
          s"""
             | CREATE TABLE binaryTable (
             |    id DOUBLE,
             |    label BOOLEAN,
             |    name STRING,
             |    image BINARY,
             |    autoLabel BOOLEAN)
             | using carbon
             | options(PATH '$writerPath',
             | 'SORT_COLUMNS'='image')
                 """.stripMargin)
        sql("SELECT COUNT(*) FROM binaryTable").show()
      }
    } else {
      exception = intercept[Exception] {
        sql(
          s"""
             | CREATE TABLE binaryTable (
             |    id DOUBLE,
             |    label BOOLEAN,
             |    name STRING,
             |    image BINARY,
             |    autoLabel BOOLEAN)
             | using carbon
             | options('SORT_COLUMNS'='image')
             | LOCATION '$writerPath'
                 """.stripMargin)
        sql("SELECT COUNT(*) FROM binaryTable").show()
      }
    }
    assert(exception.getCause.getMessage
      .contains(
        "sort columns not supported for array, struct, map, double, float, decimal, varchar, " +
        "binary"))
  }

  test("Don't support long_string_columns for binary") {
    sql("DROP TABLE IF EXISTS binaryTable")
    val exception = intercept[Exception] {
      sql(
        s"""
           | CREATE TABLE binaryTable (
           |    id DOUBLE,
           |    label BOOLEAN,
           |    name STRING,
           |    image BINARY,
           |    autoLabel BOOLEAN)
           | using carbon
           | options('long_string_columns'='image')
       """.stripMargin)
      sql("SELECT COUNT(*) FROM binaryTable").show()
    }
    assert(exception.getCause.getMessage
      .contains("long string column : image is not supported for data type: BINARY"))
  }

  test("Don't support insert into partition table") {
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql("DROP TABLE IF EXISTS binaryCarbon")
      sql("DROP TABLE IF EXISTS binaryCarbon2")
      sql("DROP TABLE IF EXISTS binaryCarbon3")
      sql("DROP TABLE IF EXISTS binaryCarbon4")
      sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
      sql(
        s"""
           | CREATE TABLE binaryCarbon2(
           |    binaryId INT,
           |    binaryName STRING,
           |    binary BINARY,
           |    labelName STRING,
           |    labelContent STRING
           |) USING CARBON""".stripMargin)
      sql(
        s"""
           | CREATE TABLE binaryCarbon3(
           |    binaryId INT,
           |    binaryName STRING,
           |    binary BINARY,
           |    labelName STRING,
           |    labelContent STRING
           |) USING CARBON partitioned by (binary) """.stripMargin)
      sql(
        "select binaryId,binaryName,binary,labelName,labelContent from binaryCarbon where " +
        "binaryId=0")
        .show()

      sql(
        "insert into binaryCarbon2 select binaryId,binaryName,binary,labelName,labelContent from " +
        "binaryCarbon where binaryId=0 ")
      val carbonResult2 = sql("SELECT * FROM binaryCarbon2")

      sql(
        "create table binaryCarbon4 using carbon select binaryId,binaryName,binary,labelName," +
        "labelContent from binaryCarbon where binaryId=0 ")
      val carbonResult4 = sql("SELECT * FROM binaryCarbon4")
      val carbonResult = sql("SELECT * FROM binaryCarbon")

      assert(3 == carbonResult.collect().length)
      assert(1 == carbonResult4.collect().length)
      assert(1 == carbonResult2.collect().length)
      checkAnswer(carbonResult4, carbonResult2)

      try {
        sql(
          "insert into binaryCarbon3 select binaryId,binaryName,binary,labelName,labelContent " +
          "from binaryCarbon where binaryId=0 ")
        assert(false)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          assert(true)
      }
      sql("DROP TABLE IF EXISTS binaryCarbon")
      sql("DROP TABLE IF EXISTS binaryCarbon2")
      sql("DROP TABLE IF EXISTS binaryCarbon3")
      sql("DROP TABLE IF EXISTS binaryCarbon4")
    }
  }

  test("Test unsafe as false") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "false")
    FileUtils.deleteDirectory(new File(outputPath))
    sql("DROP TABLE IF EXISTS binaryCarbon")
    sql("DROP TABLE IF EXISTS binaryCarbon3")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      sql(s"CREATE TABLE binaryCarbon USING CARBON OPTIONS(PATH '$writerPath')")
      sql(s"CREATE TABLE binaryCarbon3 USING CARBON OPTIONS(PATH '$outputPath')" +
          " AS SELECT * FROM binaryCarbon")
    } else {
      sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
      sql(s"CREATE TABLE binaryCarbon3 USING CARBON LOCATION '$outputPath'" +
          " AS SELECT * FROM binaryCarbon")
    }
    checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
      Seq(Row(3)))
    checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
      Seq(Row(3)))
    sql("DROP TABLE IF EXISTS binaryCarbon")
    sql("DROP TABLE IF EXISTS binaryCarbon3")

    FileUtils.deleteDirectory(new File(outputPath))
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT)
  }

  test("insert into for hive and carbon, CTAS") {
    sql("DROP TABLE IF EXISTS hiveTable")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS hiveTable2")
    sql("DROP TABLE IF EXISTS carbon_table2")
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS hivetable (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | row format delimited fields terminated by ','
             """.stripMargin)
    sql("insert into hivetable values(1,true,'Bob','binary',false)")
    sql("insert into hivetable values(2,false,'Xu','test',true)")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | using carbon
             """.stripMargin)
    sql("insert into carbon_table values(1,true,'Bob','binary',false)")
    sql("insert into carbon_table values(2,false,'Xu','test',true)")

    val hexHiveResult = sql("SELECT hex(image) FROM hivetable")
    val hexCarbonResult = sql("SELECT hex(image) FROM carbon_table")
    checkAnswer(hexHiveResult, hexCarbonResult)
    hexCarbonResult.collect().foreach { each =>
      val result = new String(Hex.decodeHex((each.getAs[Array[Char]](0)).toString.toCharArray))
      assert("binary".equals(result)
             || "test".equals(result))
    }

    val base64HiveResult = sql("SELECT base64(image) FROM hivetable")
    val base64CarbonResult = sql("SELECT base64(image) FROM carbon_table")
    checkAnswer(base64HiveResult, base64CarbonResult)
    base64CarbonResult.collect().foreach { each =>
      val result = new String(Base64.decodeBase64((each.getAs[Array[Char]](0)).toString))
      assert("binary".equals(result)
             || "test".equals(result))
    }

    val carbonResult = sql("SELECT * FROM carbon_table")
    val hiveResult = sql("SELECT * FROM hivetable")

    assert(2 == carbonResult.collect().length)
    assert(2 == hiveResult.collect().length)
    checkAnswer(hiveResult, carbonResult)
    carbonResult.collect().foreach { each =>
      if (1 == each.get(0)) {
        assert("binary".equals(new String(each.getAs[Array[Byte]](3))))
      } else if (2 == each.get(0)) {
        assert("test".equals(new String(each.getAs[Array[Byte]](3))))
      } else {
        assert(false)
      }
    }

    sql("CREATE TABLE hivetable2 AS SELECT * FROM carbon_table")
    sql("CREATE TABLE carbon_table2  USING CARBON AS SELECT * FROM hivetable")
    val carbonResult2 = sql("SELECT * FROM carbon_table2")
    val hiveResult2 = sql("SELECT * FROM hivetable2")
    checkAnswer(hiveResult2, carbonResult2)
    checkAnswer(carbonResult, carbonResult2)
    checkAnswer(hiveResult, hiveResult2)
    assert(2 == carbonResult2.collect().length)
    assert(2 == hiveResult2.collect().length)

    sql("INSERT INTO hivetable2 SELECT * FROM carbon_table")
    sql("INSERT INTO carbon_table2 SELECT * FROM hivetable")
    val carbonResult3 = sql("SELECT * FROM carbon_table2")
    val hiveResult3 = sql("SELECT * FROM hivetable2")
    checkAnswer(carbonResult3, hiveResult3)
    assert(4 == carbonResult3.collect().length)
    assert(4 == hiveResult3.collect().length)
  }

  test("insert into for parquet and carbon, CTAS") {
    sql("DROP TABLE IF EXISTS parquetTable")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS parquetTable2")
    sql("DROP TABLE IF EXISTS carbon_table2")
    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS parquettable (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | using parquet
             """.stripMargin)
    sql("insert into parquettable values(1,true,'Bob','binary',false)")
    sql("insert into parquettable values(2,false,'Xu','test',true)")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | using carbon
             """.stripMargin)
    sql("insert into carbon_table values(1,true,'Bob','binary',false)")
    sql("insert into carbon_table values(2,false,'Xu','test',true)")
    val carbonResult = sql("SELECT * FROM carbon_table")
    val parquetResult = sql("SELECT * FROM parquettable")

    assert(2 == carbonResult.collect().length)
    assert(2 == parquetResult.collect().length)
    checkAnswer(parquetResult, carbonResult)
    carbonResult.collect().foreach { each =>
      if (1 == each.get(0)) {
        assert("binary".equals(new String(each.getAs[Array[Byte]](3))))
      } else if (2 == each.get(0)) {
        assert("test".equals(new String(each.getAs[Array[Byte]](3))))
      } else {
        assert(false)
      }
    }

    sql("CREATE TABLE parquettable2 AS SELECT * FROM carbon_table")
    sql("CREATE TABLE carbon_table2  USING CARBON AS SELECT * FROM parquettable")
    val carbonResult2 = sql("SELECT * FROM carbon_table2")
    val parquetResult2 = sql("SELECT * FROM parquettable2")
    checkAnswer(parquetResult2, carbonResult2)
    checkAnswer(carbonResult, carbonResult2)
    checkAnswer(parquetResult, parquetResult2)
    assert(2 == carbonResult2.collect().length)
    assert(2 == parquetResult2.collect().length)

    sql("INSERT INTO parquettable2 SELECT * FROM carbon_table")
    sql("INSERT INTO carbon_table2 SELECT * FROM parquettable")
    val carbonResult3 = sql("SELECT * FROM carbon_table2")
    val parquetResult3 = sql("SELECT * FROM parquettable2")
    checkAnswer(carbonResult3, parquetResult3)
    assert(4 == carbonResult3.collect().length)
    assert(4 == parquetResult3.collect().length)
  }

  test("insert into carbon as select from hive after hive load data") {
    sql("DROP TABLE IF EXISTS hiveTable")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS hiveTable2")
    sql("DROP TABLE IF EXISTS carbon_table2")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS hivetable (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | row format delimited fields terminated by '|'
             """.stripMargin)
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$resourcesPath/binarystringdata.csv'
         | INTO TABLE hivetable
             """.stripMargin)

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | using carbon
             """.stripMargin)
    sql("insert into carbon_table select * from hivetable")

    sqlContext.udf.register("decodeHex", (str: String) =>
      Hex.decodeHex(str.toList.map(_.toInt.toBinaryString).mkString.toCharArray))
    sqlContext.udf.register("unHexValue", (str: String) =>
      org.apache.spark.sql.catalyst.expressions.Hex
        .unhex(str.toList.map(_.toInt.toBinaryString).mkString.getBytes))
    sqlContext.udf.register("decodeBase64", (str: String) => Base64.decodeBase64(str.getBytes()))

    val udfHexResult = sql("SELECT decodeHex(image) FROM carbon_table")
    val unHexResult = sql("SELECT unHexValue(image) FROM carbon_table")
    checkAnswer(udfHexResult, unHexResult)

    val udfBase64Result = sql("SELECT decodeBase64(image) FROM carbon_table")
    val unbase64Result = sql("SELECT unbase64(image) FROM carbon_table")
    checkAnswer(udfBase64Result, unbase64Result)

    val carbonResult = sql("SELECT * FROM carbon_table")
    val hiveResult = sql("SELECT * FROM hivetable")

    assert(3 == carbonResult.collect().length)
    assert(3 == hiveResult.collect().length)
    checkAnswer(hiveResult, carbonResult)
    carbonResult.collect().foreach { each =>
      if (2 == each.get(0)) {
        assert("\u0001history\u0002".equals(new String(each.getAs[Array[Byte]](3))))
      } else if (1 == each.get(0)) {
        assert("\u0001education\u0002".equals(new String(each.getAs[Array[Byte]](3))))
      } else if (3 == each.get(0)) {
        assert("".equals(new String(each.getAs[Array[Byte]](3)))
               || "\u0001biology\u0002".equals(new String(each.getAs[Array[Byte]](3))))
      } else {
        assert(false)
      }
    }

    sql("CREATE TABLE hivetable2 AS SELECT * FROM carbon_table")
    sql("CREATE TABLE carbon_table2  USING CARBON AS SELECT * FROM hivetable")
    val carbonResult2 = sql("SELECT * FROM carbon_table2")
    val hiveResult2 = sql("SELECT * FROM hivetable2")
    checkAnswer(hiveResult2, carbonResult2)
    checkAnswer(carbonResult, carbonResult2)
    checkAnswer(hiveResult, hiveResult2)
    assert(3 == carbonResult2.collect().length)
    assert(3 == hiveResult2.collect().length)

    sql("INSERT INTO hivetable2 SELECT * FROM carbon_table")
    sql("INSERT INTO carbon_table2 SELECT * FROM hivetable")
    val carbonResult3 = sql("SELECT * FROM carbon_table2")
    val hiveResult3 = sql("SELECT * FROM hivetable2")
    checkAnswer(carbonResult3, hiveResult3)
    assert(6 == carbonResult3.collect().length)
    assert(6 == hiveResult3.collect().length)

  }

  test("filter for hive and carbon") {
    sql("DROP TABLE IF EXISTS hiveTable")
    sql("DROP TABLE IF EXISTS carbon_table")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS hivetable (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | row format delimited fields terminated by ','
             """.stripMargin)
    sql("insert into hivetable values(1,true,'Bob','binary',false)")
    sql("insert into hivetable values(2,false,'Xu','test',true)")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table (
         |    id int,
         |    label boolean,
         |    name string,
         |    image binary,
         |    autoLabel boolean)
         | using carbon
             """.stripMargin)
    sql("insert into carbon_table values(1,true,'Bob','binary',false)")
    sql("insert into carbon_table values(2,false,'Xu','test',true)")

    // filter with equal
    val hiveResult = sql("SELECT * FROM hivetable where image=cast('binary' as binary)")
    val carbonResult = sql("SELECT * FROM carbon_table where image=cast('binary' as binary)")

    checkAnswer(hiveResult, carbonResult)
    assert(1 == carbonResult.collect().length)
    carbonResult.collect().foreach { each =>
      assert(1 == each.get(0))
      assert("binary".equals(new String(each.getAs[Array[Byte]](3))))
    }

    // filter with non string
    val exception = intercept[Exception] {
      sql("SELECT * FROM carbon_table where image=binary").collect()
    }
    assert(exception.getMessage.contains("cannot resolve '`binary`' given input columns"))

    // filter with not equal
    val hiveResult3 = sql("SELECT * FROM hivetable where image!=cast('binary' as binary)")
    val carbonResult3 = sql("SELECT * FROM carbon_table where image!=cast('binary' as binary)")
    checkAnswer(hiveResult3, carbonResult3)
    assert(1 == carbonResult3.collect().length)
    carbonResult3.collect().foreach { each =>
      assert(2 == each.get(0))
      assert("test".equals(new String(each.getAs[Array[Byte]](3))))
    }

    // filter with in
    val hiveResult4 = sql("SELECT * FROM hivetable where image in (cast('binary' as binary))")
    val carbonResult4 = sql("SELECT * FROM carbon_table where image in (cast('binary' as binary))")
    checkAnswer(hiveResult4, carbonResult4)
    assert(1 == carbonResult4.collect().length)
    carbonResult4.collect().foreach { each =>
      assert(1 == each.get(0))
      assert("binary".equals(new String(each.getAs[Array[Byte]](3))))
    }

    // filter with not in
    val hiveResult5 = sql("SELECT * FROM hivetable where image not in (cast('binary' as binary))")
    val carbonResult5 = sql(
      "SELECT * FROM carbon_table where image not in (cast('binary' as binary))")
    checkAnswer(hiveResult5, carbonResult5)
    assert(1 == carbonResult5.collect().length)
    carbonResult5.collect().foreach { each =>
      assert(2 == each.get(0))
      assert("test".equals(new String(each.getAs[Array[Byte]](3))))
    }
  }

  test("Spark DataSource don't support update, delete") {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table2")

    sql(
      s"""
         | CREATE TABLE IF NOT EXISTS carbon_table (
         |    id int,
         |    label boolean,
         |    name string,
         |    binaryField binary,
         |    autoLabel boolean)
         | using carbon
             """.stripMargin)
    sql("insert into carbon_table values(1,true,'Bob','binary',false)")
    sql("insert into carbon_table values(2,false,'Xu','test',true)")

    val carbonResult = sql("SELECT * FROM carbon_table")

    carbonResult.collect().foreach { each =>
      if (1 == each.get(0)) {
        assert("binary".equals(new String(each.getAs[Array[Byte]](3))))
      } else if (2 == each.get(0)) {
        assert("test".equals(new String(each.getAs[Array[Byte]](3))))
      } else {
        assert(false)
      }
    }

    var exception = intercept[Exception] {
      sql("UPDATE carbon_table SET binaryField = 'binary2' WHERE id = 1").show()
    }
    assert(exception.getMessage.contains("mismatched input 'UPDATE' expecting"))

    exception = intercept[Exception] {
      sql("DELETE FROM carbon_table WHERE id = 1").show()
    }
    assert(exception.getMessage.contains("only CarbonData table support delete operation"))
  }

  test("test array of binary data type with sparkfileformat ") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField array<binary>, autoLabel boolean) using carbon")
    sql("insert into carbon_table values(1,true,'abc',array('binary'),false)")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField array<binary>, autoLabel boolean) using parquet")
    sql("insert into parquet_table values(1,true,'abc',array('binary'),false)")
    checkAnswer(sql("SELECT binaryField[0] FROM carbon_table"),
      sql("SELECT binaryField[0] FROM parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test struct of binary data type with sparkfileformat ") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField struct<b:binary>, autoLabel boolean) using carbon")
    sql("insert into carbon_table values(1,true,'abc',named_struct('b','binary'),false)")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField struct<b:binary>, autoLabel boolean) using parquet")
    sql("insert into parquet_table values(1,true,'abc',named_struct('b','binary'),false)")
    checkAnswer(sql("SELECT binaryField.b FROM carbon_table"),
      sql("SELECT binaryField.b FROM parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test map of binary data type with sparkfileformat") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField map<int, binary>, autoLabel boolean) using parquet")
    sql("insert into parquet_table values(1,true,'abc',map(1,'binary'),false)")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField map<int, binary>, autoLabel boolean) using carbon")
    sql("insert into carbon_table values(1,true,'abc',map(1,'binary'),false)")
    checkAnswer(sql("SELECT binaryField[1] FROM carbon_table"),
      sql("SELECT binaryField[1] FROM parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test map of array and struct binary data type with sparkfileformat") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField1 map<int, array<binary>>, binaryField2 map<int, struct<b:binary>> ) " +
        "using parquet")
    sql("insert into parquet_table values(1,true,'abc',map(1,array('binary')),map(1," +
        "named_struct('b','binary')))")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField1 map<int, array<binary>>, binaryField2 map<int, struct<b:binary>> ) " +
        "using carbon")
    sql("insert into carbon_table values(1,true,'abc',map(1,array('binary')),map(1," +
        "named_struct('b','binary')))")
    checkAnswer(sql("SELECT binaryField1[1][1] FROM carbon_table"),
      sql("SELECT binaryField1[1][1] FROM parquet_table"))
    checkAnswer(sql("SELECT binaryField2[1].b FROM carbon_table"),
      sql("SELECT binaryField2[1].b FROM parquet_table"))
    sql("drop table if exists carbon_table")
  }

  test("test of array of struct and struct of array of binary data type with sparkfileformat") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    sql("create table if not exists parquet_table(id int, label boolean, name string," +
        "binaryField1 array<struct<b1:binary>>, binaryField2 struct<b2:array<binary>> ) " +
        "using parquet")
    sql("insert into parquet_table values(1,true,'abc',array(named_struct('b1','binary'))," +
        "named_struct('b2',array('binary')))")
    sql("create table if not exists carbon_table(id int, label boolean, name string," +
        "binaryField1 array<struct<b1:binary>>, binaryField2 struct<b2:array<binary>> ) " +
        "using carbon")
    sql("insert into carbon_table values(1,true,'abc',array(named_struct('b1','binary'))," +
        "named_struct('b2',array('binary')))")
    checkAnswer(sql("SELECT binaryField1[1].b1 FROM carbon_table"),
      sql("SELECT  binaryField1[1].b1 FROM parquet_table"))
    checkAnswer(sql("SELECT binaryField2.b2[0] FROM carbon_table"),
      sql("SELECT binaryField2.b2[0] FROM parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

}

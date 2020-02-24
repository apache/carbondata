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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, File, InputStream}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructType, StructField => SparkStructField}
import org.apache.spark.util.SparkUtil
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.testutil.StoreCreator
import org.apache.carbondata.sdk.file.{CarbonWriter, Field, Schema}

class SparkCarbonDataSourceTest extends QueryTest with BeforeAndAfterAll {

  var writerOutputPath = s"$target/SparkCarbonFileFormat/SDKWriterOutput/"
  //getCanonicalPath gives path with \, but the code expects /.
  writerOutputPath = writerOutputPath.replace("\\", "/")

  def buildTestData(rows: Int,
      sortColumns: List[String]): Any = {
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"stringField\":\"string\"},\n")
      .append("   {\"byteField\":\"byte\"},\n")
      .append("   {\"shortField\":\"short\"},\n")
      .append("   {\"intField\":\"int\"},\n")
      .append("   {\"longField\":\"long\"},\n")
      .append("   {\"doubleField\":\"double\"},\n")
      .append("   {\"floatField\":\"float\"},\n")
      .append("   {\"decimalField\":\"decimal(17,2)\"},\n")
      .append("   {\"boolField\":\"boolean\"},\n")
      .append("   {\"dateField\":\"DATE\"},\n")
      .append("   {\"timeField\":\"TIMESTAMP\"},\n")
      .append("   {\"varcharField\":\"varchar\"},\n")
      .append("   {\"varcharField2\":\"varchar\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerOutputPath)
          .sortBy(sortColumns.toArray)
          .uniqueIdentifier(System.currentTimeMillis)
          .withBlockSize(2)
          .withCsvInput(Schema.parseJson(schema))
          .writtenBy("TestNonTransactionalCarbonTable")
          .build()
      var i = 0
      while (i < rows) {
        writer.write(Array[String]("robot" + i,
          String.valueOf(i / 100),
          String.valueOf(i / 100),
          String.valueOf(i),
          String.valueOf(i),
          String.valueOf(i),
          String.valueOf(i),
          String.valueOf(i),
          "true",
          "2019-03-02",
          "2019-02-12 03:03:34",
          "var1",
          "var2"))
        i += 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
  }

  test("Carbon DataSource read SDK data with varchar") {
    FileUtils.deleteDirectory(new File(writerOutputPath))
    val num = 10000
    buildTestData(num, List("stringField", "intField"))
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql("DROP TABLE IF EXISTS carbontable_varchar")
      sql("DROP TABLE IF EXISTS carbontable_varchar2")
      sql(s"CREATE TABLE carbontable_varchar USING CARBON LOCATION '$writerOutputPath'")
      val e = intercept[Exception] {
        sql("SELECT COUNT(*) FROM carbontable_varchar").show()
      }
      assert(e.getMessage
        .contains(
          "Datatype of the Column VARCHAR present in index file, is varchar and not same as " +
          "datatype of the column with same name present in table, because carbon convert varchar" +
          " of carbon to string of spark, please set long_string_columns for varchar column"))

      sql(s"CREATE TABLE carbontable_varchar2 USING CARBON OPTIONS" +
          s"('long_String_columns'='varcharField,varcharField2') LOCATION '$writerOutputPath'")
      checkAnswer(sql("SELECT COUNT(*) FROM carbontable_varchar2"), Seq(Row(num)))
    }
  }

  test("test write using dataframe") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("carbon").saveAsTable("testformat")
    assert(sql("select * from testformat").count() == 10)
    assert(sql("select * from testformat where c1='a0'").count() == 1)
    assert(sql("select * from testformat").count() == 10)
    sql("drop table if exists testformat")
  }

  test("test write using ddl") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    sql("drop table if exists testparquet")
    sql("drop table if exists testformat")
    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
    sql("insert into carbon_table select * from testparquet")
    checkAnswer(sql("select * from carbon_table where c1='a1'"),
      sql("select * from testparquet where c1='a1'"))
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      LOGGER.error("!!!" + warehouse + "/carbon_table")
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
    }
    sql("drop table if exists testparquet")
    sql("drop table if exists testformat")
  }

  test("test add columns for table of using carbon with sql") {
    // TODO: should support add columns for carbon dataSource table
    // Limit from spark
    try {
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
      // Saves dataFrame to carbon file
      df.write
        .format("parquet").saveAsTable("test_parquet")
      sql("CREATE TABLE carbon_table(c1 STRING, c2 STRING, number INT) USING carbon")
      sql("INSERT INTO carbon_table SELECT * FROM test_parquet")
      checkAnswer(sql("SELECT * FROM carbon_table WHERE c1='a1'"),
        sql("SELECT * FROM test_parquet WHERE c1='a1'"))
      if (!SparkUtil.isSparkVersionEqualTo("2.1")) {
        val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
        DataMapStoreManager.getInstance()
          .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
        assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      }
      assert(df.schema.map(_.name) === Seq("c1", "c2", "number"))
      sql("ALTER TABLE carbon_table ADD COLUMNS (a1 INT, b1 STRING) ")
      assert(false)
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("Operation not allowed: ALTER TABLE ADD COLUMNS"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          assert(e.getMessage
            .contains("ALTER ADD COLUMNS does not support datasource table with type carbon."))
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test add columns for table of using carbon with DF") {
    try {
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("DROP TABLE IF EXISTS carbon_table")
      // Saves dataFrame to carbon file
      df.write
        .format("carbon").saveAsTable("carbon_table")
      val customSchema = StructType(Array(
        SparkStructField("c1", StringType),
        SparkStructField("c2", StringType),
        SparkStructField("number", IntegerType)))

      val carbonDF = sqlContext.sparkSession.read
        .format("carbon")
        .option("tableName", "carbon_table")
        .schema(customSchema)
        .load()

      assert(carbonDF.schema.map(_.name) === Seq("c1", "c2", "number"))
      val carbonDF2 = carbonDF.drop("c1")
      assert(carbonDF2.schema.map(_.name) === Seq("c2", "number"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false)
    } finally {
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test drop columns for table of using carbon") {
    // TODO: should support drop columns for carbon dataSource table
    // Limit from spark
    try {
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
      // Saves dataFrame to carbon file
      df.write
        .format("parquet").saveAsTable("test_parquet")
      sql("CREATE TABLE carbon_table(c1 STRING, c2 STRING, number INT) USING carbon")
      sql("INSERT INTO carbon_table SELECT * FROM test_parquet")
      checkAnswer(sql("SELECT * FROM carbon_table WHERE c1='a1'"),
        sql("SELECT * FROM test_parquet WHERE c1='a1'"))
      if (!sqlContext.sparkContext.version.startsWith("2.1")) {
        val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
        DataMapStoreManager.getInstance()
          .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
        assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      }
      assert(df.schema.map(_.name) === Seq("c1", "c2", "number"))
      sql("ALTER TABLE carbon_table drop COLUMNS (a1 INT, b1 STRING) ")
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("mismatched input 'COLUMNS' expecting"))
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test rename table name for table of using carbon") {
    try {
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
      sql("DROP TABLE IF EXISTS carbon_table2")
      // Saves dataFrame to carbon file
      df.write
        .format("parquet").saveAsTable("test_parquet")
      sql("CREATE TABLE carbon_table(c1 STRING, c2 STRING, number INT) USING carbon")
      sql("INSERT INTO carbon_table SELECT * FROM test_parquet")
      checkAnswer(sql("SELECT * FROM carbon_table WHERE c1='a1'"),
        sql("SELECT * FROM test_parquet WHERE c1='a1'"))
      if (!sqlContext.sparkContext.version.startsWith("2.1")) {
        val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
        DataMapStoreManager.getInstance()
          .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
        assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      }
      assert(df.schema.map(_.name) === Seq("c1", "c2", "number"))
      sql("ALTER TABLE carbon_table RENAME TO carbon_table2 ")
      checkAnswer(sql("SELECT COUNT(*) FROM carbon_table2"), Seq(Row(10)));
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false)
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test change data type for table of using carbon") {
    //TODO: Limit from spark
    try {
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
      sql("DROP TABLE IF EXISTS carbon_table2")
      // Saves dataFrame to carbon file
      df.write
        .format("parquet").saveAsTable("test_parquet")
      sql("CREATE TABLE carbon_table(c1 STRING, c2 STRING, number decimal(8,2)) USING carbon")
      sql("INSERT INTO carbon_table SELECT * FROM test_parquet")
      checkAnswer(sql("SELECT * FROM carbon_table WHERE c1='a1'"),
        sql("SELECT * FROM test_parquet WHERE c1='a1'"))
      if (!SparkUtil.isSparkVersionEqualTo("2.1")) {
        val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
        DataMapStoreManager.getInstance()
          .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
        assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      }
      assert(df.schema.map(_.name) === Seq("c1", "c2", "number"))
      sql("ALTER TABLE carbon_table change number number decimal(9,4)")
      assert(false)
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("Operation not allowed: ALTER TABLE change"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          assert(e.getMessage
            .contains("ALTER TABLE CHANGE COLUMN is not supported for changing column"))
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test add columns for table of using parquet") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")
    try {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS test_parquet2")
      df.write
        .format("parquet").saveAsTable("test_parquet")
      sql("ALTER TABLE test_parquet ADD COLUMNS(a1 INT, b1 STRING) ")
      sql("INSERT INTO test_parquet VALUES('Bob','xu',12,1,'parquet')")
      checkAnswer(sql("SELECT COUNT(*) FROM test_parquet"), Seq(Row(11)))

      sql("DROP TABLE IF EXISTS test_parquet2")
      sql("CREATE TABLE test_parquet2(c1 STRING, c2 STRING, number INT) USING parquet")
      sql("INSERT INTO test_parquet2 VALUES('Bob','xu',12)")
      sql("ALTER TABLE test_parquet2 ADD COLUMNS (a1 INT, b1 STRING) ")
      sql("INSERT INTO test_parquet2 VALUES('Bob','xu',12,1,'parquet')")
      checkAnswer(sql("SELECT COUNT(*) FROM test_parquet2"), Seq(Row(2)))
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("ALTER TABLE test_parquet ADD COLUMNS"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          e.printStackTrace()
          assert(false)
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS test_parquet2")
    }
  }

  test("test drop columns for table of using parquet") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    sql("DROP TABLE IF EXISTS test_parquet")
    sql("DROP TABLE IF EXISTS test_parquet2")
    df.write
      .format("parquet").saveAsTable("test_parquet")

    val df2 = df.drop("c1")

    assert(df.schema.map(_.name) === Seq("c1", "c2", "number"))
    assert(df2.schema.map(_.name) === Seq("c2", "number"))

    try {
      sql("ALTER TABLE test_parquet DROP COLUMNS(c1)")
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Only carbondata table support drop column"))
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
    }

    sql("DROP TABLE IF EXISTS test_parquet2")
    sql("CREATE TABLE test_parquet2(c1 STRING, c2 STRING, number INT) USING parquet")
    sql("INSERT INTO test_parquet2 VALUES('Bob','xu',12)")
    try {
      sql("ALTER TABLE test_parquet2 DROP COLUMNS (a1 INT, b1 STRING) ")
      assert(false)
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("mismatched input 'COLUMNS' expecting"))
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet2")
    }
  }

  test("test rename table name for table of using parquet") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    sql("DROP TABLE IF EXISTS test_parquet")
    sql("DROP TABLE IF EXISTS test_parquet2")
    sql("DROP TABLE IF EXISTS test_parquet3")
    sql("DROP TABLE IF EXISTS test_parquet22")
    df.write
      .format("parquet").saveAsTable("test_parquet")

    try {
      sql("ALTER TABLE test_parquet rename to test_parquet3")
      checkAnswer(sql("SELECT COUNT(*) FROM test_parquet3"), Seq(Row(10)));
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false)
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
      sql("DROP TABLE IF EXISTS test_parquet3")
    }

    sql("DROP TABLE IF EXISTS test_parquet2")
    sql("CREATE TABLE test_parquet2(c1 STRING, c2 STRING, number INT) USING parquet")
    sql("INSERT INTO test_parquet2 VALUES('Bob','xu',12)")
    try {
      sql("ALTER TABLE test_parquet2 rename to test_parquet22")
      checkAnswer(sql("SELECT COUNT(*) FROM test_parquet22"), Seq(Row(1)));
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("Operation not allowed: ALTER TABLE CHANGE"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          e.printStackTrace()
          assert(false)
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet2")
      sql("DROP TABLE IF EXISTS test_parquet22")
    }
  }

  test("test change data type for table of using parquet") {
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    sql("DROP TABLE IF EXISTS test_parquet")
    sql("DROP TABLE IF EXISTS test_parquet2")
    df.write
      .format("parquet").saveAsTable("test_parquet")
    try {
      sql("ALTER TABLE test_parquet CHANGE number number long")
      assert(false)
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("Operation not allowed: ALTER TABLE CHANGE"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          assert(e.getMessage
            .contains("ALTER TABLE CHANGE COLUMN is not supported for changing column"))
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet")
    }
    sql("DROP TABLE IF EXISTS test_parquet2")
    sql("CREATE TABLE test_parquet2(c1 STRING, c2 STRING, number INT) USING parquet")
    sql("INSERT INTO test_parquet2 VALUES('Bob','xu',12)")
    try {
      sql("ALTER TABLE test_parquet2 CHANGE number number long")
      assert(false)
    } catch {
      case e: Exception =>
        if (SparkUtil.isSparkVersionEqualTo("2.1")) {
          assert(e.getMessage.contains("Operation not allowed: ALTER TABLE CHANGE"))
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          assert(e.getMessage
            .contains("ALTER TABLE CHANGE COLUMN is not supported for changing column"))
        }
    } finally {
      sql("DROP TABLE IF EXISTS test_parquet2")
    }
  }

  test("test read with df write") {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse + "/test_folder/")

    val frame = sqlContext.sparkSession.read.format("carbon").load(warehouse + "/test_folder")
    frame.show()
    assert(frame.count() == 10)
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
  }

  test("test write using subfolder") {
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")

      // Saves dataframe to carbon file
      df.write.format("carbon").save(warehouse + "/test_folder/" + System.nanoTime())
      df.write.format("carbon").save(warehouse + "/test_folder/" + System.nanoTime())
      df.write.format("carbon").save(warehouse + "/test_folder/" + System.nanoTime())

      val frame = sqlContext.sparkSession.read.format("carbon").load(warehouse + "/test_folder")
      assert(frame.where("c1='a1'").count() == 3)

      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/test_folder"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    }
  }

  test("test write using partition ddl") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").partitionBy("c2").saveAsTable("testparquet")
    sql(
      "create table carbon_table(c1 string, c2 string, number int) using carbon  PARTITIONED by" +
      " (c2)")
    sql("insert into carbon_table select * from testparquet")
    // TODO fix in 2.1
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      assert(sql("select * from carbon_table").count() == 10)
      checkAnswer(sql("select * from carbon_table"),
          sql("select * from testparquet"))
    }
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
  }

  test("test write with struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:string>, number int) using " +
      "carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"), sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with nested array and struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("describe parquet_table").show(false)
    sql(
      "create table carbon_table(c1 string, c2 array<struct<a1:string, a2:string>>, number int)" +
      " using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
      sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with nested struct and array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, " +
      "a2:string>>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 array<map<string,string>>, number int) using " +
      "carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with array type with value as nested array<array<map>> type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array(Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 array<array<map<string,string>>>, number int) " +
      "using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:map<string,string>>, number" +
      " int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with value as nested struct<array<map>> type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, ("a", Array(Map("b" -> "c"))), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:string, a2:array<map<string,string>>>," +
      " number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("b" -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 map<string, string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with Int data type as key") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map(99 -> "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 map<int, string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested map type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Map("b" -> "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, map<string, string>>, number int) " +
      "using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested struct type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> ("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, struct<a1:string, a2:string>>, " +
      "number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with map type with value as nested array type") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Map("a" -> Array("b", "c")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 map<string, array<string>>, number int) using " +
      "carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write using ddl and options") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x))
      .toDF("c1", "c2", "number")

    // Saves dataframe to carbon file
    df.write
      .format("parquet").saveAsTable("testparquet")
    sql(
      "create table carbon_table(c1 string, c2 string, number int) using carbon options" +
      "('table_blocksize'='256','inverted_index'='c1')")
    sql("describe formatted carbon_table").show()
    checkExistence(sql("describe formatted carbon_table"), true, "table_blocksize")
    checkExistence(sql("describe formatted carbon_table"), true, "inverted_index")
    sql("insert into carbon_table select * from testparquet")
    sql("select * from carbon_table").show()
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
  }

  test("test read with nested struct and array type without creating table") {
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_carbon_folder"))
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    val frame = sql("select * from parquet_table")
    frame.write.format("carbon").save(warehouse + "/test_carbon_folder")
    val dfread = sqlContext.read.format("carbon").load(warehouse + "/test_carbon_folder")
    dfread.show(false)
    FileFactory
      .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_carbon_folder"))
    sql("drop table if exists parquet_table")
  }

  test("test read and write with date datatype") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    sql("insert into  date_table select 11, 'ravi', '2017-11-11'")
    sql("create table date_parquet_table(empno int, empname string, projdate Date) using " +
        "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11'")
    checkAnswer(sql("select * from date_table"),
      sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test date filter datatype") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    sql("insert into  date_table select 11, 'ravi', '2017-11-11'")
    sql("select * from date_table where projdate=cast('2017-11-11' as date)").show()
    sql("create table date_parquet_table(empno int, empname string, projdate Date) using " +
        "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11'")
    checkAnswer(sql("select * from date_table where projdate=cast('2017-11-11' as date)"),
      sql("select * from date_parquet_table where projdate=cast('2017-11-11' as date)"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with date datatype with wrong format") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate Date) using carbon")
    sql("insert into  date_table select 11, 'ravi', '11-11-2017'")
    sql("create table date_parquet_table(empno int, empname string, projdate Date) using " +
        "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017'")
    checkAnswer(sql("select * from date_table"),
      sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    sql("insert into  date_table select 11, 'ravi', '2017-11-11 00:00:01'")
    sql(
      "create table date_parquet_table(empno int, empname string, projdate timestamp) using " +
      "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '2017-11-11 00:00:01'")
    checkAnswer(sql("select * from date_table"),
      sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test read and write with timestamp datatype with wrong format") {
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
    sql("create table date_table(empno int, empname string, projdate timestamp) using carbon")
    sql("insert into  date_table select 11, 'ravi', '11-11-2017 00:00:01'")
    sql(
      "create table date_parquet_table(empno int, empname string, projdate timestamp) using " +
      "parquet")
    sql("insert into  date_parquet_table select 11, 'ravi', '11-11-2017 00:00:01'")
    checkAnswer(sql("select * from date_table"),
      sql("select * from date_parquet_table"))
    sql("drop table if exists date_table")
    sql("drop table if exists date_parquet_table")
  }

  test("test write with array type with filter") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, Array("b", "c"), x))
      .toDF("c1", "c2", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql("create table carbon_table(c1 string, c2 array<string>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table where c1='a1' and c2[0]='b'"),
        sql("select * from parquet_table where c1='a1' and c2[0]='b'"))
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }

  test("test write with struct type with filter") {
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, (Array("1", "2"), ("3", "4")), Array(("1", 1), ("2", 2)), x))
      .toDF("c1", "c2", "c3", "number")

    df.write
      .format("parquet").saveAsTable("parquet_table")
    sql(
      "create table carbon_table(c1 string, c2 struct<a1:array<string>, a2:struct<a1:string, " +
      "a2:string>>, c3 array<struct<a1:string, a2:int>>, number int) using carbon")
    sql("insert into carbon_table select * from parquet_table")
    assert(sql("select * from carbon_table").count() == 10)
    checkAnswer(sql("select * from carbon_table"),
        sql("select * from parquet_table"))
    checkAnswer(sql("select * from carbon_table where c2.a1[0]='1' and c1='a1'"),
        sql("select * from parquet_table where c2._1[0]='1' and c1='a1'"))
    checkAnswer(sql("select * from carbon_table where c2.a1[0]='1' and c3[0].a2=1"),
        sql("select * from parquet_table where c2._1[0]='1' and c3[0]._2=1"))
    sql("drop table if exists carbon_table")
    sql("drop table if exists parquet_table")
  }


  test("test read with df write string issue") {
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort, x, x.toLong, x.toDouble, BigDecimal.apply(x),
        Array(x + 1,
          x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("carbon").save(warehouse + "/test_folder/")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc " +
          s"bigint,  doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>) using carbon location " +
          s"'$warehouse/test_folder/'")

      checkAnswer(sql("select * from test123"),
        sqlContext.sparkSession.read.format("carbon").load(warehouse + "/test_folder/"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    sql("drop table if exists test123")
  }

  test("test read with df write with empty data") {
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    // Saves dataframe to carbon file
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18)) using carbon location " +
          s"'$warehouse/test_folder/'")

      sql(s"create table test123_par (c1 string, c2 string, arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>, shortc smallint,intc int, longc bigint,  " +
          s"doublec double, bigdecimalc decimal(38,18)) using carbon location " +
          s"'$warehouse/test_folder/'")
      checkAnswer(sql("select count(*) from test123"),
          sql("select count(*) from test123_par"))
    }
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
  }

  test("test write with nosort columns") {
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", x.toShort, x, x.toLong, x.toDouble, BigDecimal.apply(x),
        Array(x + 1,
          x), ("b", BigDecimal.apply(x))))
      .toDF("c1", "c2", "shortc", "intc", "longc", "doublec", "bigdecimalc", "arrayc", "structc")


    // Saves dataframe to carbon file
    df.write.format("parquet").saveAsTable("test123_par")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql(s"create table test123 (c1 string, c2 string, shortc smallint,intc int, longc " +
          s"bigint,  doublec double, bigdecimalc decimal(38,18), arrayc array<int>, structc " +
          s"struct<_1:string, _2:decimal(38,18)>) using carbon options('sort_columns'='') " +
          s"location '$warehouse/test_folder/'")

      sql(s"insert into test123 select * from test123_par")
      checkAnswer(sql("select * from test123"), sql(s"select * from test123_par"))
    }
    sql("drop table if exists test123")
    sql("drop table if exists test123_par")
  }

  test("test complex columns mismatch") {
    sql("drop table if exists array_com_hive")
    sql(s"drop table if exists array_com")
    sql(
      "create table array_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER " +
      "string, EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING " +
      "array<string>,ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT " +
      "int, DEPOSIT double, HQ_DEPOSIT double) row format delimited fields terminated by ',' " +
      "collection items terminated by '$'")
    val sourceFile = FileFactory.getPath(s"$resourcesPath/Array.csv").toString
    sql(s"load data local inpath '$sourceFile' into table array_com_hive")
    sql(
      "create table Array_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER string, " +
      "EDUCATED string, IS_MARRIED string, ARRAY_INT array<int>,ARRAY_STRING array<string>," +
      "ARRAY_DATE array<timestamp>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT " +
      "double, HQ_DEPOSIT double) using carbon")
    sql("insert into Array_com select * from array_com_hive")
    checkAnswer(sql("select * from Array_com order by CUST_ID ASC limit 3"),
        sql("select * from array_com_hive order by CUST_ID ASC limit 3"))
    sql("drop table if exists array_com_hive")
    sql(s"drop table if exists array_com")
  }

  test("test complex columns fail while insert ") {
    sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    sql(s"drop table if exists STRUCT_OF_ARRAY_com")
    sql(
      " create table STRUCT_OF_ARRAY_com_hive (CUST_ID string, YEAR int, MONTH int, AGE int, " +
      "GENDER string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int," +
      "CHECK_DATE: timestamp ,SNo: array<int>,sal1: array<double>,state: array<string>," +
      "date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT " +
      "float, HQ_DEPOSIT double) row format delimited fields terminated by ',' collection items" +
      " terminated by '$' map keys terminated by '&'")
    val sourceFile = FileFactory.getPath(s"$resourcesPath/structofarray.csv").toString
    sql(s"load data local inpath '$sourceFile' into table STRUCT_OF_ARRAY_com_hive")
    sql(
      "create table STRUCT_OF_ARRAY_com (CUST_ID string, YEAR int, MONTH int, AGE int, GENDER " +
      "string, EDUCATED string, IS_MARRIED string, STRUCT_OF_ARRAY struct<ID: int," +
      "CHECK_DATE: timestamp,SNo: array<int>,sal1: array<double>,state: array<string>," +
      "date1: array<timestamp>>,CARD_COUNT int,DEBIT_COUNT int, CREDIT_COUNT int, DEPOSIT " +
      "double, HQ_DEPOSIT double) using carbon")
    sql(" insert into STRUCT_OF_ARRAY_com select * from STRUCT_OF_ARRAY_com_hive")
    checkAnswer(sql("select * from STRUCT_OF_ARRAY_com  order by CUST_ID ASC"),
        sql("select * from STRUCT_OF_ARRAY_com_hive  order by CUST_ID ASC"))
    sql("drop table if exists STRUCT_OF_ARRAY_com_hive")
    sql(s"drop table if exists STRUCT_OF_ARRAY_com")
  }

  test("test partition error in carbon") {
    sql("drop table if exists carbon_par")
    sql("drop table if exists parquet_par")
    sql(
      "create table carbon_par (name string, age int, country string) using carbon partitioned " +
      "by (country)")
    sql("insert into carbon_par select 'b', '12', 'aa'")
    sql(
      "create table parquet_par (name string, age int, country string) using carbon partitioned" +
      " by (country)")
    sql("insert into parquet_par select 'b', '12', 'aa'")
    checkAnswer(sql("select * from carbon_par"), sql("select * from parquet_par"))
    sql("drop table if exists carbon_par")
    sql("drop table if exists parquet_par")
  }

  test("test more cols error in carbon") {
    sql("drop table if exists h_jin")
    sql("drop table if exists c_jin")
    sql(
      s"""create table h_jin(RECORD_ID string,
      CDR_ID string,LOCATION_CODE int,SYSTEM_ID string,
      CLUE_ID string,HIT_ELEMENT string,CARRIER_CODE string,CAP_TIME date,
      DEVICE_ID string,DATA_CHARACTER string,
      NETCELL_ID string,NETCELL_TYPE int,EQU_CODE string,CLIENT_MAC string,
      SERVER_MAC string,TUNNEL_TYPE string,TUNNEL_IP_CLIENT string,TUNNEL_IP_SERVER string,
      TUNNEL_ID_CLIENT string,TUNNEL_ID_SERVER string,SIDE_ONE_TUNNEL_ID string,
      SIDE_TWO_TUNNEL_ID string,
      CLIENT_IP string,SERVER_IP string,TRANS_PROTOCOL string,CLIENT_PORT int,SERVER_PORT int,
      APP_PROTOCOL string,
      CLIENT_AREA bigint,SERVER_AREA bigint,LANGUAGE string,STYPE string,SUMMARY string,FILE_TYPE
       string,FILENAME string,
      FILESIZE string,BILL_TYPE string,ORIG_USER_NUM string,USER_NUM string,USER_IMSI string,
      USER_IMEI string,USER_BELONG_AREA_CODE string,USER_BELONG_COUNTRY_CODE string,
      USER_LONGITUDE double,USER_LATITUDE double,USER_MSC string,USER_BASE_STATION string,
      USER_CURR_AREA_CODE string,USER_CURR_COUNTRY_CODE string,USER_SIGNAL_POINT string,USER_IP
      string,
      ORIG_OPPO_NUM string,OPPO_NUM string,OPPO_IMSI string,OPPO_IMEI string,
      OPPO_BELONG_AREA_CODE string,
      OPPO_BELONG_COUNTRY_CODE string,OPPO_LONGITUDE double,OPPO_LATITUDE double,OPPO_MSC string,
      OPPO_BASE_STATION string,
      OPPO_CURR_AREA_CODE string,OPPO_CURR_COUNTRY_CODE string,OPPO_SIGNAL_POINT string,OPPO_IP
      string,RING_TIME timestamp,
      CALL_ESTAB_TIME timestamp,END_TIME timestamp,CALL_DURATION bigint,CALL_STATUS_CODE int,DTMF
       string,ORIG_OTHER_NUM string,
      OTHER_NUM string,ROAM_NUM string,SEND_TIME timestamp,ORIG_SMS_CONTENT string,ORIG_SMS_CODE
      int,SMS_CONTENT string,SMS_NUM int,
      SMS_COUNT int,REMARK string,CONTENT_STATUS int,VOC_LENGTH bigint,FAX_PAGE_COUNT int,
      COM_OVER_CAUSE int,ROAM_TYPE int,SGSN_ADDR string,GGSN_ADDR string,
      PDP_ADDR string,APN_NI string,APN_OI string,CARD_ID string,TIME_OUT int,LOGIN_TIME
      timestamp,USER_IMPU string,OPPO_IMPU string,USER_LAST_IMPI string,
      USER_CURR_IMPI string,SUPSERVICE_TYPE bigint,SUPSERVICE_TYPE_SUBCODE bigint,SMS_CENTERNUM
      string,USER_LAST_LONGITUDE double,USER_LAST_LATITUDE double,
      USER_LAST_MSC string,USER_LAST_BASE_STATION string,LOAD_ID bigint,P_CAP_TIME string)  ROW
      format delimited FIELDS terminated by '|'"""
        .stripMargin)
    val sourceFile = FileFactory.getPath(s"$resourcesPath/j2.csv").toString
    sql(s"load data local inpath '$sourceFile' into table h_jin")
    sql(
      s"""create table c_jin(RECORD_ID string,
      CDR_ID string,LOCATION_CODE int,SYSTEM_ID string,
      CLUE_ID string,HIT_ELEMENT string,CARRIER_CODE string,CAP_TIME date,
      DEVICE_ID string,DATA_CHARACTER string,
      NETCELL_ID string,NETCELL_TYPE int,EQU_CODE string,CLIENT_MAC string,
      SERVER_MAC string,TUNNEL_TYPE string,TUNNEL_IP_CLIENT string,TUNNEL_IP_SERVER string,
      TUNNEL_ID_CLIENT string,TUNNEL_ID_SERVER string,SIDE_ONE_TUNNEL_ID string,
      SIDE_TWO_TUNNEL_ID string,
      CLIENT_IP string,SERVER_IP string,TRANS_PROTOCOL string,CLIENT_PORT int,SERVER_PORT int,
      APP_PROTOCOL string,
      CLIENT_AREA string,SERVER_AREA string,LANGUAGE string,STYPE string,SUMMARY string,FILE_TYPE
       string,FILENAME string,
      FILESIZE string,BILL_TYPE string,ORIG_USER_NUM string,USER_NUM string,USER_IMSI string,
      USER_IMEI string,USER_BELONG_AREA_CODE string,USER_BELONG_COUNTRY_CODE string,
      USER_LONGITUDE double,USER_LATITUDE double,USER_MSC string,USER_BASE_STATION string,
      USER_CURR_AREA_CODE string,USER_CURR_COUNTRY_CODE string,USER_SIGNAL_POINT string,USER_IP
      string,
      ORIG_OPPO_NUM string,OPPO_NUM string,OPPO_IMSI string,OPPO_IMEI string,
      OPPO_BELONG_AREA_CODE string,
      OPPO_BELONG_COUNTRY_CODE string,OPPO_LONGITUDE double,OPPO_LATITUDE double,OPPO_MSC string,
      OPPO_BASE_STATION string,
      OPPO_CURR_AREA_CODE string,OPPO_CURR_COUNTRY_CODE string,OPPO_SIGNAL_POINT string,OPPO_IP
      string,RING_TIME timestamp,
      CALL_ESTAB_TIME timestamp,END_TIME timestamp,CALL_DURATION string,CALL_STATUS_CODE int,DTMF
       string,ORIG_OTHER_NUM string,
      OTHER_NUM string,ROAM_NUM string,SEND_TIME timestamp,ORIG_SMS_CONTENT string,ORIG_SMS_CODE
      int,SMS_CONTENT string,SMS_NUM int,
      SMS_COUNT int,REMARK string,CONTENT_STATUS int,VOC_LENGTH string,FAX_PAGE_COUNT int,
      COM_OVER_CAUSE int,ROAM_TYPE int,SGSN_ADDR string,GGSN_ADDR string,
      PDP_ADDR string,APN_NI string,APN_OI string,CARD_ID string,TIME_OUT int,LOGIN_TIME
      timestamp,USER_IMPU string,OPPO_IMPU string,USER_LAST_IMPI string,
      USER_CURR_IMPI string,SUPSERVICE_TYPE string,SUPSERVICE_TYPE_SUBCODE string,SMS_CENTERNUM
      string,USER_LAST_LONGITUDE double,USER_LAST_LATITUDE double,
      USER_LAST_MSC string,USER_LAST_BASE_STATION string,LOAD_ID string,P_CAP_TIME string) using
      carbon"""
        .stripMargin)
    sql(
      s"""insert into c_jin
      select
      RECORD_ID,CDR_ID,LOCATION_CODE,SYSTEM_ID,
      CLUE_ID,HIT_ELEMENT,CARRIER_CODE,CAP_TIME,
      DEVICE_ID,DATA_CHARACTER,NETCELL_ID,NETCELL_TYPE,EQU_CODE,CLIENT_MAC,
      SERVER_MAC,TUNNEL_TYPE,TUNNEL_IP_CLIENT,TUNNEL_IP_SERVER,
      TUNNEL_ID_CLIENT,TUNNEL_ID_SERVER,SIDE_ONE_TUNNEL_ID,SIDE_TWO_TUNNEL_ID,
      CLIENT_IP,SERVER_IP,TRANS_PROTOCOL,CLIENT_PORT,SERVER_PORT,APP_PROTOCOL,
      CLIENT_AREA,SERVER_AREA,LANGUAGE,STYPE,SUMMARY,FILE_TYPE,FILENAME,
      FILESIZE,BILL_TYPE,ORIG_USER_NUM,USER_NUM,USER_IMSI,
      USER_IMEI,USER_BELONG_AREA_CODE,USER_BELONG_COUNTRY_CODE,
      USER_LONGITUDE,USER_LATITUDE,USER_MSC,USER_BASE_STATION,
      USER_CURR_AREA_CODE,USER_CURR_COUNTRY_CODE,USER_SIGNAL_POINT,USER_IP,
      ORIG_OPPO_NUM,OPPO_NUM,OPPO_IMSI,OPPO_IMEI,OPPO_BELONG_AREA_CODE,
      OPPO_BELONG_COUNTRY_CODE,OPPO_LONGITUDE,OPPO_LATITUDE,OPPO_MSC,OPPO_BASE_STATION,
      OPPO_CURR_AREA_CODE,OPPO_CURR_COUNTRY_CODE,OPPO_SIGNAL_POINT,OPPO_IP,RING_TIME,
      CALL_ESTAB_TIME,END_TIME,CALL_DURATION,CALL_STATUS_CODE,DTMF,ORIG_OTHER_NUM,
      OTHER_NUM,ROAM_NUM,SEND_TIME,ORIG_SMS_CONTENT,ORIG_SMS_CODE,SMS_CONTENT,SMS_NUM,
      SMS_COUNT,REMARK,CONTENT_STATUS,VOC_LENGTH,FAX_PAGE_COUNT,COM_OVER_CAUSE,ROAM_TYPE,
      SGSN_ADDR,GGSN_ADDR,
      PDP_ADDR,APN_NI,APN_OI,CARD_ID,TIME_OUT,LOGIN_TIME,USER_IMPU,OPPO_IMPU,USER_LAST_IMPI,
      USER_CURR_IMPI,SUPSERVICE_TYPE,SUPSERVICE_TYPE_SUBCODE,SMS_CENTERNUM,USER_LAST_LONGITUDE,
      USER_LAST_LATITUDE,
      USER_LAST_MSC,USER_LAST_BASE_STATION,LOAD_ID,P_CAP_TIME
      from h_jin""".stripMargin)
    assert(sql("select * from c_jin").collect().length == 1)
    sql("drop table if exists h_jin")
    sql("drop table if exists c_jin")
  }

  test("test write and create table with sort columns not allow") {
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    import sqlContext.sparkSession.implicits._
    val df = sqlContext.sparkContext.parallelize(1 to 10)
      .map(x => ("a" + x % 10, "b", "c" + x, "d" + x, x.toShort, x, x.toLong, x.toDouble, BigDecimal
        .apply(x)))
      .toDF("c1", "c2", "c3", "c4", "shortc", "intc", "longc", "doublec", "bigdecimalc")

    // Saves dataframe to carbon file
    df.write.format("carbon").save(s"$warehouse/test_folder/")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      intercept[UnsupportedOperationException] {
        sql(s"create table test123 using carbon options('sort_columns'='shortc,c2') location " +
            s"'$warehouse/test_folder/'")
      }
    }
    sql("drop table if exists test123")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
  }

  test("valdate if path not specified during table creation") {
    sql("drop table if exists test123")
    val ex = intercept[AnalysisException] {
      sql(s"create table test123 using carbon options('sort_columns'='shortc,c2')")
    }
    assert(ex.getMessage().contains("Unable to infer schema for carbon"))
  }

  test("test double boundary") {
    sql("drop table if exists par")
    sql("drop table if exists car")

    sql("create table par (c1 string, c2 double, n int) using parquet")
    sql("create table car (c1 string, c2 double, n int) using carbon")
    sql("insert into par select 'a', 1.7986931348623157E308, 215565665556")
    sql("insert into car select 'a', 1.7986931348623157E308, 215565665556")

    checkAnswer(sql("select * from car"), sql("select * from par"))
    sql("drop table if exists par")
    sql("drop table if exists car")
  }

  test("test clearing datamaps") {
    import sqlContext.sparkSession.implicits._
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")
      sql("drop table if exists testparquet")
      sql("drop table if exists carbon_table")
      sql("drop table if exists carbon_table1")
      // Saves dataframe to carbon file
      df.write
        .format("parquet").saveAsTable("testparquet")
      sql("create table carbon_table(c1 string, c2 string, number int) using carbon")
      sql("create table carbon_table1(c1 string, c2 string, number int) using carbon")
      sql("insert into carbon_table select * from testparquet")
      sql("insert into carbon_table1 select * from testparquet")
      DataMapStoreManager.getInstance().getAllDataMaps.clear()
      sql("select * from carbon_table where c1='a1'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      sql("select * from carbon_table where c1='a2'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      sql("select * from carbon_table1 where c1='a1'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 2)
      sql("select * from carbon_table1 where c1='a2'").collect()
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 2)
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table"))
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 1)
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/carbon_table1"))
      assert(DataMapStoreManager.getInstance().getAllDataMaps.size() == 0)
      sql("drop table if exists testparquet")
      sql("drop table if exists carbon_table")
      sql("drop table if exists carbon_table1")
    }
  }

  test("test write using multi subfolder") {
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
      import sqlContext.sparkSession.implicits._
      val df = sqlContext.sparkContext.parallelize(1 to 10)
        .map(x => ("a" + x % 10, "b", x))
        .toDF("c1", "c2", "number")

      // Saves dataframe to carbon file
      df.write.format("carbon").save(warehouse + "/test_folder/1/" + System.nanoTime())
      df.write.format("carbon").save(warehouse + "/test_folder/2/" + System.nanoTime())
      df.write.format("carbon").save(warehouse + "/test_folder/3/" + System.nanoTime())

      val frame = sqlContext.sparkSession.read.format("carbon").load(warehouse + "/test_folder")
      assert(frame.count() == 30)
      assert(frame.where("c1='a1'").count() == 3)
      val mapSize = DataMapStoreManager.getInstance().getAllDataMaps.size()
      DataMapStoreManager.getInstance()
        .clearDataMaps(AbsoluteTableIdentifier.from(warehouse + "/test_folder"))
      assert(mapSize > DataMapStoreManager.getInstance().getAllDataMaps.size())
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(warehouse + "/test_folder"))
    }
  }

  test("test read using old data") {
    val store = new StoreCreator(warehouse, s"$projectPath/hadoop/src/test/resources/data.csv")
    store.createCarbonStore()
    FileFactory
      .deleteAllFilesOfDir(new File(warehouse + "/testdb/testtable/Fact/Part0/Segment_0/0"))
    val dfread = sqlContext.sparkSession.read.format("carbon")
      .load(warehouse + "/testdb/testtable/Fact/Part0/Segment_0")
    dfread.show(false)
    sql("drop table if exists parquet_table")
  }

  test("test read using different sort order data") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "test")
    if (!sqlContext.sparkContext.version.startsWith("2.1")) {
      sql("drop table if exists old_comp")
      FileFactory.deleteAllFilesOfDir(new File(warehouse + "/testdb"))
      val store = new StoreCreator(warehouse, s"$projectPath/hadoop/src/test/resources/data.csv")
      store.setSortColumns(new util.ArrayList[String](Seq("name").asJava))
      var model = store.createTableAndLoadModel(false)
      model.setSegmentId("0")
      store.createCarbonStore(model)
      FileFactory
        .deleteAllFilesOfDir(new File(warehouse + "/testdb/testtable/Fact/Part0/Segment_0/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("country", "phonetype").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("1")
      store.createCarbonStore(model)
      FileFactory
        .deleteAllFilesOfDir(new File(warehouse + "/testdb/testtable/Fact/Part0/Segment_1/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("date").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("2")
      store.createCarbonStore(model)
      FileFactory
        .deleteAllFilesOfDir(new File(warehouse + "/testdb/testtable/Fact/Part0/Segment_2/0"))
      store.setSortColumns(new util.ArrayList[String](Seq("serialname").asJava))
      model = store.createTableAndLoadModel(false)
      model.setSegmentId("3")
      store.createCarbonStore(model)
      FileFactory
        .deleteAllFilesOfDir(new File(warehouse + "/testdb/testtable/Fact/Part0/Segment_3/0"))
      sql(s"create table old_comp(id int, date string, country string, name string, phonetype " +
          s"string, serialname string, salary int) using carbon options" +
          s"(path='$warehouse/testdb/testtable/Fact/Part0/', 'sort_columns'='name')")

      assert(sql("select * from old_comp where country='china'").count() == 3396)
      assert(sql("select * from old_comp ").count() == 4000)
      sql("drop table if exists old_comp")

      sql(s"create table old_comp1 using carbon options" +
          s"(path='$warehouse/testdb/testtable/Fact/Part0/')")
      assert(sql("select * from old_comp1 where country='china'").count() == 3396)
      assert(sql("select * from old_comp1 ").count() == 4000)
      sql("drop table if exists old_comp1")
      FileFactory.deleteAllFilesOfDir(new File(warehouse + "/testdb"))
    }
  }


  test("test write sdk and read with spark using different sort order data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse + "/sdk")
    sql(s"create table sdkout using carbon options(path='$warehouse/sdk')")
    assert(sql("select * from sdkout").collect().length == 5)
    buildTestDataOtherDataType(5, Array("name", "salary"), warehouse + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name = 'name1'").collect().length == 2)
    assert(sql("select * from sdkout where salary=100").collect().length == 2)
    buildTestDataOtherDataType(5, Array("name", "age"), warehouse + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name='name0'").collect().length == 3)
    assert(sql("select * from sdkout").collect().length == 15)
    assert(sql("select * from sdkout where salary=100").collect().length == 3)
    assert(sql("select * from sdkout where address='address1'").collect().length == 3)
    buildTestDataOtherDataType(5, Array("name", "salary"), warehouse + "/sdk")
    sql("refresh table sdkout")
    assert(sql("select * from sdkout where name='name0'").collect().length == 4)
    assert(sql("select * from sdkout").collect().length == 20)
    assert(sql("select * from sdkout where salary=100").collect().length == 4)
    assert(sql("select * from sdkout where address='address1'").collect().length == 4)
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk"))
  }

  test("test write sdk with different schema and read with spark") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout using carbon options(path='$warehouse/sdk1')")
    assert(sql("select * from sdkout").collect().length == 5)
    buildTestDataOtherDataType(5, null, warehouse + "/sdk1", 2)
    sql("refresh table sdkout")
    assert(sql("select * from sdkout").count() == 10)
    assert(sql("select * from sdkout where salary=100").count() == 1)
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
  }

  test("test Float data type by giving schema explicitly and desc formatted") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(5, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse/sdk1')")
    assert(sql("desc formatted sdkout").collect().take(7).reverse.head.get(1).equals("float"))
    assert(sql("desc formatted sdkout").collect().take(8).reverse.head.get(1).equals
    ("tinyint"))
  }

  test("test select * on table with float data type") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse/sdk1')")
    checkAnswer(sql("select * from par_table"), sql("select * from sdkout"))
    checkAnswer(sql("select floatfield from par_table"),
      sql("select floatfield from sdkout"))
  }

  test("test various filters on float data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse/sdk1')")
    checkAnswer(sql("select * from par_table where floatfield < 10"),
      sql("select * from sdkout where floatfield < 10"))
    checkAnswer(sql("select * from par_table where floatfield > 5.3"),
      sql("select * from sdkout where floatfield > 5.3"))
    checkAnswer(sql("select * from par_table where floatfield >= 4.1"),
      sql("select * from sdkout where floatfield >= 4.1"))
    checkAnswer(sql("select * from par_table where floatfield != 5.5"),
      sql("select * from sdkout where floatfield != 5.5"))
    checkAnswer(sql("select * from par_table where floatfield <= 5"),
      sql("select * from sdkout where floatfield <= 5"))
    checkAnswer(sql("select * from par_table where floatfield >= 5"),
      sql("select * from sdkout where floatfield >= 5"))
    checkAnswer(sql("select * from par_table where floatfield IN ('5.5','6.6')"),
      sql("select * from sdkout where floatfield IN ('5.5','6.6')"))
    checkAnswer(sql("select * from par_table where floatfield NOT IN ('5.5','6.6')"),
      sql("select * from sdkout where floatfield NOT IN ('5.5','6.6')"))
    checkAnswer(sql("select * from par_table where floatfield = cast('6.6' as float)"),
      sql("select * from sdkout where floatfield = cast('6.6' as float)"))
  }

  test("test select * on table with byte data type") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse/sdk1')")
    checkAnswer(sql("select * from par_table"), sql("select * from sdkout"))
    checkAnswer(sql("select byteField from par_table"),
      sql("select bytefield from sdkout"))
  }

  test("test various filters on byte data") {
    sql("drop table if exists sdkout")
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    buildTestDataOtherDataType(11, Array("age", "address"), warehouse + "/sdk1")
    sql(s"create table sdkout(male boolean, age int, height double, name string, address " +
        s"string," +
        s"salary long, floatField float, bytefield byte) using carbon options " +
        s"(path='$warehouse/sdk1')")
    checkAnswer(sql("select * from par_table where bytefield < 10"),
      sql("select * from sdkout where bytefield < 10"))
    checkAnswer(sql("select * from par_table where bytefield > 5"),
      sql("select * from sdkout where bytefield > 5"))
    checkAnswer(sql("select * from par_table where bytefield >= 4"),
      sql("select * from sdkout where bytefield >= 4"))
    checkAnswer(sql("select * from par_table where bytefield != 5"),
      sql("select * from sdkout where bytefield != 5"))
    checkAnswer(sql("select * from par_table where bytefield <= 5"),
      sql("select * from sdkout where bytefield <= 5"))
    checkAnswer(sql("select * from par_table where bytefield >= 5"),
      sql("select * from sdkout where bytefield >= 5"))
    checkAnswer(sql("select * from par_table where bytefield IN ('5','6')"),
      sql("select * from sdkout where bytefield IN ('5','6')"))
    checkAnswer(sql("select * from par_table where bytefield NOT IN ('5','6')"),
      sql("select * from sdkout where bytefield NOT IN ('5','6')"))
  }

  test("test struct of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = FileFactory.getPath(warehouse + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    sql("drop table if exists complextable")
    val fields = List(new StructField
    ("byteField", DataTypes.BYTE), new StructField("floatField", DataTypes.FLOAT))
    val structType = Array(new Field("stringfield", DataTypes.STRING), new Field
    ("structField", "struct", fields.asJava))


    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(structType)).writtenBy("SparkCarbonDataSourceTest").build()

      var i = 0
      while (i < 11) {
        val array = Array[String](s"name$i", s"$i" + "\001" + s"$i.${ i }12")
        writer.write(array)
        i += 1
      }
      writer.close()
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        if (!FileFactory.isFileExist(path)) {
          FileFactory.createDirectoryAndSetPermission(path,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
        }
        sql("create table complextable (stringfield string, structfield struct<bytefield: " +
            "byte, floatfield: float>) " +
            s"using carbon options(path '$path')")
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        sql("create table complextable (stringfield string, structfield struct<bytefield: " +
            "byte, floatfield: float>) " +
            s"using carbon location '$path'")
      }
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
    checkAnswer(sql("select * from complextable limit 1"), Seq(Row("name0", Row(0
      .asInstanceOf[Byte], 0.012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.bytefield > 9"), Seq(Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.floatfield > 9.912"), Seq
    (Row
    ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
    checkAnswer(sql("select * from complextable where structfield.floatfield > 9.912 and " +
                    "structfield.bytefield < 11"),
      Seq(Row("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
  }

  test("test bytefield as sort column") {
    val path = FileFactory.getPath(warehouse + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("age", DataTypes.INT)
    fields(1) = new Field("height", DataTypes.DOUBLE)
    fields(2) = new Field("name", DataTypes.STRING)
    fields(3) = new Field("address", DataTypes.STRING)
    fields(4) = new Field("salary", DataTypes.LONG)
    fields(5) = new Field("bytefield", DataTypes.BYTE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).sortBy(Array("bytefield"))
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTest").build()

      var i = 0
      while (i < 11) {
        val array = Array[String](
          String.valueOf(i),
          String.valueOf(i.toDouble / 2),
          "name" + i,
          "address" + i,
          (i * 100).toString,
          s"${ 10 - i }")
        writer.write(array)
        i += 1
      }
      writer.close()
      sql("drop table if exists sorted_par")
      sql("drop table if exists sort_table")
      val path2 = s"$warehouse/../warehouse2";
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        if (!FileFactory.isFileExist(path)) {
          FileFactory.createDirectoryAndSetPermission(path,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
        }
        sql(s"create table sort_table (age int, height double, name string, address string," +
            s" salary long, bytefield byte) using carbon  options(path '$path')")
        FileFactory
          .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$warehouse/../warehouse2"))
        if (!FileFactory.isFileExist(path2)) {
          FileFactory.createDirectoryAndSetPermission(path2,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
        }
        sql(s"create table sorted_par(age int, height double, name string, address " +
            s"string," +
            s"salary long, bytefield byte) using parquet options(path " +
            s"'$path2')")
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        sql(s"create table sort_table (age int, height double, name string, address string," +
            s" salary long, bytefield byte) using carbon location '$path'")
        FileFactory
          .deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$warehouse/../warehouse2"))
        sql(s"create table sorted_par(age int, height double, name string, address " +
            s"string," +
            s"salary long, bytefield byte) using parquet location " +
            s"'$warehouse/../warehouse2'")
      }

      (0 to 10).foreach {
        i =>
          sql(s"insert into sorted_par select '$i', ${ i.toDouble / 2 }, 'name$i', " +
              s"'address$i', ${ i * 100 }, '${ 10 - i }'")
      }
      checkAnswer(sql("select * from sorted_par order by bytefield"),
        sql("select * from sort_table"))
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
  }

  test("test array of float type and byte type") {
    import scala.collection.JavaConverters._
    val path = FileFactory.getPath(warehouse + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    sql("drop table if exists complextable")
    val structType =
      Array(new Field("stringfield", DataTypes.STRING),
        new Field("bytearray", "array", List(new StructField("byteField", DataTypes.BYTE))
          .asJava),
        new Field("floatarray", "array", List(new StructField("floatfield", DataTypes.FLOAT))
          .asJava))

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(structType)).writtenBy("SparkCarbonDataSourceTest").build()

      var i = 0
      while (i < 10) {
        val array = Array[String](s"name$i",
          s"$i" + "\001" + s"${ i * 2 }",
          s"${ i / 2 }" + "\001" + s"${ i / 3 }")
        writer.write(array)
        i += 1
      }
      writer.close()
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        if (!FileFactory.isFileExist(path)) {
          FileFactory.createDirectoryAndSetPermission(path,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
        }
        sql(s"create table complextable (stringfield string, bytearray " +
            s"array<byte>, floatarray array<float>) using carbon " +
            s"options( path " +
            s"'$path')")
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        sql(s"create table complextable (stringfield string, bytearray " +
            s"array<byte>, floatarray array<float>) using carbon " +
            s"location " +
            s"'$path'")
      }
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
    checkAnswer(sql("select * from complextable limit 1"), Seq(Row("name0", mutable
      .WrappedArray.make(Array[Byte](0, 0)), mutable.WrappedArray.make(Array[Float](0.0f, 0.0f)))))
    checkAnswer(sql("select * from complextable where bytearray[0] = 1"), Seq(Row("name1",
      mutable.WrappedArray.make(Array[Byte](1, 2)), mutable.WrappedArray.make(Array[Float](0.0f,
        0.0f)))))
    checkAnswer(sql("select * from complextable where bytearray[0] > 8"), Seq(Row("name9",
      mutable.WrappedArray.make(Array[Byte](9, 18)), mutable.WrappedArray.make(Array[Float](4.0f,
        3.0f)))))
    checkAnswer(sql(
      "select * from complextable where floatarray[0] IN (4.0) and stringfield = 'name8'"),
      Seq(Row
      ("name8",
        mutable.WrappedArray.make(Array[Byte](8, 16)), mutable.WrappedArray.make(Array[Float](4.0f,
        2.0f)))))
  }

  private def createParquetTable {
    val path = FileFactory.getUpdatedFilePath(s"$warehouse/../warehouse2")
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(s"$path"))
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      if (!FileFactory.isFileExist(path)) {
        FileFactory.createDirectoryAndSetPermission(path,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
      }
      sql(s"create table par_table(male boolean, age int, height double, name string, address " +
          s"string," +
          s"salary long, floatField float, bytefield byte) using parquet options(path '$path')")
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql(s"create table par_table(male boolean, age int, height double, name string, address " +
          s"string," +
          s"salary long, floatField float, bytefield byte) using parquet location '$path'")
    }

    (0 to 10).foreach {
      i =>
        sql(s"insert into par_table select 'true','$i', ${ i.toDouble / 2 }, 'name$i', " +
            s"'address$i', ${ i * 100 }, $i.$i, '$i'")
    }
  }

  // prepare sdk writer output with other schema
  def buildTestDataOtherDataType(rows: Int,
      sortColumns: Array[String],
      writerPath: String,
      colCount: Int = -1): Any = {
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("male", DataTypes.BOOLEAN)
    fields(1) = new Field("age", DataTypes.INT)
    fields(2) = new Field("height", DataTypes.DOUBLE)
    fields(3) = new Field("name", DataTypes.STRING)
    fields(4) = new Field("address", DataTypes.STRING)
    fields(5) = new Field("salary", DataTypes.LONG)
    fields(6) = new Field("floatField", DataTypes.FLOAT)
    fields(7) = new Field("bytefield", DataTypes.BYTE)

    if (colCount > 0) {
      val fieldsToWrite: Array[Field] = new Array[Field](colCount)
      var i = 0
      while (i < colCount) {
        fieldsToWrite(i) = fields(i)
        i += 1
      }
      fields = fieldsToWrite
    }

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2).sortBy(sortColumns)
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTest").build()

      var i = 0
      while (i < rows) {
        val array = Array[String]("true",
          String.valueOf(i),
          String.valueOf(i.toDouble / 2),
          "name" + i,
          "address" + i,
          (i * 100).toString,
          s"$i.$i", s"$i")
        if (colCount > 0) {
          writer.write(array.slice(0, colCount))
        } else {
          writer.write(array)
        }
        i += 1
      }
      writer.close()
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
      case _: Throwable => None
    }
  }

  def buildStructSchemaWithNestedArrayOfMapTypeAsValue(writerPath: String, rows: Int): Unit = {
    FileFactory.deleteAllFilesOfDir(new File(writerPath))
    val mySchema =
      """
        |{
        |  "name": "address",
        |  "type": "record",
        |  "fields": [
        |    {
        |      "name": "name",
        |      "type": "string"
        |    },
        |    {
        |      "name": "age",
        |      "type": "int"
        |    },
        |    {
        |      "name": "structRecord",
        |      "type": {
        |        "type": "record",
        |        "name": "my_address",
        |        "fields": [
        |          {
        |            "name": "street",
        |            "type": "string"
        |          },
        |          {
        |            "name": "houseDetails",
        |            "type": {
        |               "type": "array",
        |               "items": {
        |                   "name": "memberDetails",
        |                   "type": "map",
        |                   "values": "string"
        |                }
        |             }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin
    val json =
      """ {"name":"bob", "age":10, "structRecord": {"street":"street1",
        |"houseDetails": [{"101": "Rahul", "102": "Pawan"}]}} """
        .stripMargin
    writeFilesWithAvroWriter(writerPath, rows, mySchema, json)
  }

  def writeFilesWithAvroWriter(writerPath: String,
      rows: Int,
      mySchema: String,
      json: String) = {
    // conversion to GenericData.Record
    val nn = new avro.Schema.Parser().parse(mySchema)
    val record = jsonToAvro(json, mySchema)
    try {
      val writer = CarbonWriter.builder
        .outputPath(writerPath)
        .uniqueIdentifier(System.currentTimeMillis()).withAvroInput(nn).writtenBy("DataSource").build()
      var i = 0
      while (i < rows) {
        writer.write(record)
        i = i + 1
      }
      writer.close()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }

  private def jsonToAvro(json: String, avroSchema: String): GenericRecord = {
    var input: InputStream = null
    var writer: DataFileWriter[GenericRecord] = null
    var encoder: Encoder = null
    var output: ByteArrayOutputStream = null
    try {
      val schema = new org.apache.avro.Schema.Parser().parse(avroSchema)
      val reader = new GenericDatumReader[GenericRecord](schema)
      input = new ByteArrayInputStream(json.getBytes())
      output = new ByteArrayOutputStream()
      val din = new DataInputStream(input)
      writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
      writer.create(schema, output)
      val decoder = DecoderFactory.get().jsonDecoder(schema, din)
      var datum: GenericRecord = reader.read(null, decoder)
      return datum
    } finally {
      input.close()
      writer.close()
    }
  }

  test("test external table with struct type with value as nested struct<array<map>> type") {
    val writerPath: String = FileFactory.getUpdatedFilePath(warehouse + "/sdk1")
    val rowCount = 3
    buildStructSchemaWithNestedArrayOfMapTypeAsValue(writerPath, rowCount)
    sql("drop table if exists carbon_external")
    if (SparkUtil.isSparkVersionEqualTo("2.1")) {
      if (!FileFactory.isFileExist(writerPath)) {
        FileFactory.createDirectoryAndSetPermission(writerPath,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
      }
      sql(s"create table carbon_external using carbon options(path '$writerPath')")
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql(s"create table carbon_external using carbon location '$writerPath'")
    }
    assert(sql("select * from carbon_external").count() == rowCount)
    sql("drop table if exists carbon_external")
  }

  test("test byte and float for multiple pages") {
    val path = FileFactory.getPath(warehouse + "/sdk1").toString
    FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    sql("drop table if exists multi_page")
    var fields: Array[Field] = new Array[Field](8)
    // same column name, but name as boolean type
    fields(0) = new Field("a", DataTypes.STRING)
    fields(1) = new Field("b", DataTypes.FLOAT)
    fields(2) = new Field("c", DataTypes.BYTE)

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(path)
          .uniqueIdentifier(System.nanoTime()).withBlockSize(2)
          .withCsvInput(new Schema(fields)).writtenBy("SparkCarbonDataSourceTest").build()

      var i = 0
      while (i < 33000) {
        val array = Array[String](
          String.valueOf(i),
          s"$i.3200", "32")
        writer.write(array)
        i += 1
      }
      writer.close()
      if (SparkUtil.isSparkVersionEqualTo("2.1")) {
        if (!FileFactory.isFileExist(path)) {
          FileFactory.createDirectoryAndSetPermission(path,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
        }
        sql(s"create table multi_page (a string, b float, c byte) using carbon options(path " +
            s"'$path')")
      } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
        sql(s"create table multi_page (a string, b float, c byte) using carbon location " +
            s"'$path'")
      }
      assert(sql("select * from multi_page").count() == 33000)
    } catch {
      case ex: Exception => throw new RuntimeException(ex)
    } finally {
      FileFactory.deleteAllFilesOfDir(new File(warehouse + "/sdk1"))
    }
  }

  test("test partition issue with add location") {
    sql("drop table if exists partitionTable_obs")
    sql("drop table if exists partitionTable_obs_par")
    sql(s"create table partitionTable_obs (id int,name String,email String) using carbon " +
        s"partitioned by(email) ")
    sql(s"create table partitionTable_obs_par (id int,name String,email String) using parquet " +
        s"partitioned by(email) ")
    sql("insert into partitionTable_obs select 1,'huawei','abc'")
    sql("insert into partitionTable_obs select 1,'huawei','bcd'")
    sql(s"alter table partitionTable_obs add partition (email='def') location " +
        s"'$warehouse/test_folder121/'")
    sql("insert into partitionTable_obs select 1,'huawei','def'")

    sql("insert into partitionTable_obs_par select 1,'huawei','abc'")
    sql("insert into partitionTable_obs_par select 1,'huawei','bcd'")
    sql(s"alter table partitionTable_obs_par add partition (email='def') location " +
        s"'$warehouse/test_folder122/'")
    sql("insert into partitionTable_obs_par select 1,'huawei','def'")

    checkAnswer(sql("select * from partitionTable_obs"),
      sql("select * from partitionTable_obs_par"))
    sql("drop table if exists partitionTable_obs")
    sql("drop table if exists partitionTable_obs_par")
  }

  test("test multiple partition  select issue") {
    sql("drop table if exists t_carbn01b_hive")
    sql(s"drop table if exists t_carbn01b")
    sql(
      "create table t_carbn01b_hive(Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep" +
      " DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String," +
      "Create_date String,Active_status String,Item_type_cd INT, Update_time TIMESTAMP, " +
      "Discount_price DOUBLE)  using parquet partitioned by (Active_status,Item_type_cd, " +
      "Update_time, Discount_price)")
    sql(
      "create table t_carbn01b(Qty_day_avg INT,Qty_total INT,Sell_price BIGINT,Sell_pricep " +
      "DOUBLE,Profit DECIMAL(3,2),Item_code String,Item_name String,Outlet_name String," +
      "Create_date String,Active_status String,Item_type_cd INT, Update_time TIMESTAMP, " +
      "Discount_price DOUBLE)  using carbon partitioned by (Active_status,Item_type_cd, " +
      "Update_time, Discount_price)")
    sql(
      "insert into t_carbn01b partition(Active_status, Item_type_cd,Update_time,Discount_price)" +
      " select * from t_carbn01b_hive")
    sql(
      "alter table t_carbn01b add partition (active_status='xyz',Item_type_cd=12," +
      "Update_time=NULL,Discount_price='3000')")
    sql(
      "insert overwrite table t_carbn01b select 'xyz', 12, 74,3000,20000000,121.5,4.99,2.44," +
      "'RE3423ee','dddd', 'ssss','2012-01-02 23:04:05.12', '2012-01-20'")
    sql(
      "insert overwrite table t_carbn01b_hive select 'xyz', 12, 74,3000,20000000,121.5,4.99," +
      "2.44,'RE3423ee','dddd', 'ssss','2012-01-02 23:04:05.12', '2012-01-20'")
    checkAnswer(sql("select * from t_carbn01b_hive"), sql("select * from t_carbn01b"))
    sql("drop table if exists t_carbn01b_hive")
    sql(s"drop table if exists t_carbn01b")
  }

  test("Test Float value by having negative exponents") {
    sql("DROP TABLE IF EXISTS float_p")
    sql("DROP TABLE IF EXISTS float_c")
    sql("CREATE TABLE float_p(f float) using parquet")
    sql("CREATE TABLE float_c(f float) using carbon")
    sql("INSERT INTO float_p select \"1.4E-3\"")
    sql("INSERT INTO float_p select \"1.4E-38\"")
    sql("INSERT INTO float_c select \"1.4E-3\"")
    sql("INSERT INTO float_c select \"1.4E-38\"")
    checkAnswer(sql("SELECT * FROM float_p"),
      sql("SELECT * FROM float_c"))
    sql("DROP TABLE float_p")
    sql("DROP TABLE float_c")
  }

  test("test fileformat flow with drop and query on same table") {
    sql("drop table if exists fileformat_drop")
    sql("drop table if exists fileformat_drop_hive")
    sql(
      "create table fileformat_drop (imei string,AMSize string,channelsId string,ActiveCountry " +
      "string, Activecity string,gamePointId double,deviceInformationId double,productionDate " +
      "Timestamp,deliveryDate timestamp,deliverycharge double) using carbon options" +
      "('table_blocksize'='1','LOCAL_DICTIONARY_ENABLE'='TRUE'," +
      "'LOCAL_DICTIONARY_THRESHOLD'='1000')")
    sql(
      "create table fileformat_drop_hive(imei string,deviceInformationId double,AMSize string," +
      "channelsId string,ActiveCountry string,Activecity string,gamePointId double," +
      "productionDate Timestamp,deliveryDate timestamp,deliverycharge double)row format " +
      "delimited FIELDS terminated by ',' LINES terminated by '\n' stored as textfile")
    val sourceFile = FileFactory.getPath(s"$resourcesPath/vardhandaterestruct.csv").toString
    sql(s"load data local inpath '$sourceFile' into table fileformat_drop_hive")
    sql(
      "insert into fileformat_drop select imei ,deviceInformationId ,AMSize ,channelsId ," +
      "ActiveCountry ,Activecity ,gamePointId ,productionDate ,deliveryDate ,deliverycharge " +
      "from fileformat_drop_hive")
    assert(
      sql("select count(*) from fileformat_drop where imei='1AA10000'").collect().length == 1)

    sql("drop table if exists fileformat_drop")
    sql(
      "create table fileformat_drop (imei string,deviceInformationId double,AMSize string," +
      "channelsId string,ActiveCountry string,Activecity string,gamePointId float," +
      "productionDate timestamp,deliveryDate timestamp,deliverycharge decimal(10,2)) using " +
      "carbon options('table_blocksize'='1','LOCAL_DICTIONARY_ENABLE'='true'," +
      "'local_dictionary_threshold'='1000')")
    sql(
      "insert into fileformat_drop select imei ,deviceInformationId ,AMSize ,channelsId ," +
      "ActiveCountry ,Activecity ,gamePointId ,productionDate ,deliveryDate ,deliverycharge " +
      "from fileformat_drop_hive")
    assert(
      sql("select count(*) from fileformat_drop where imei='1AA10000'").collect().length == 1)
    sql("drop table if exists fileformat_drop")
    sql("drop table if exists fileformat_drop_hive")
  }

  test("test complexdatype for date and timestamp datatype") {
    sql("drop table if exists fileformat_date")
    sql("drop table if exists fileformat_date_hive")
    sql(
      "create table fileformat_date_hive(name string, age int, dob array<date>, joinTime " +
      "array<timestamp>) using parquet")
    sql(
      "create table fileformat_date(name string, age int, dob array<date>, joinTime " +
      "array<timestamp>) using carbon")
    sql(
      "insert into fileformat_date_hive select 'joey', 32, array('1994-04-06','1887-05-06'), " +
      "array('1994-04-06 00:00:05','1887-05-06 00:00:08')")
    sql(
      "insert into fileformat_date select 'joey', 32, array('1994-04-06','1887-05-06'), array" +
      "('1994-04-06 00:00:05','1887-05-06 00:00:08')")
    checkAnswer(sql("select * from fileformat_date_hive"),
      sql("select * from fileformat_date"))
  }

  test("validate the columns not present in schema") {
    sql("drop table if exists validate")
    sql(
      "create table validate (name string, age int, address string) using carbon options" +
      "('inverted_index'='abc')")
    val ex = intercept[Exception] {
      sql("insert into validate select 'abc',4,'def'")
    }
    assert(ex.getMessage
      .contains("column: abc specified in inverted index columns does not exist in schema"))
  }

  var writerPath = s"$target/SparkCarbonFileFormat/WriterOutput/"

  test("Don't support load for datasource") {
    sql("DROP TABLE IF EXISTS binaryCarbon")
    if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      sql(
        s"""
           | CREATE TABLE binaryCarbon(
           |    binaryId INT,
           |    binaryName STRING,
           |    binary BINARY,
           |    labelName STRING,
           |    labelContent STRING
           |) USING CARBON  """.stripMargin)

      val exception = intercept[Exception] {
        sql(s"load data local inpath '$writerPath' into table binaryCarbon")
      }
      assert(exception.getMessage.contains("LOAD DATA is not supported for datasource tables"))
    }
    sql("DROP TABLE IF EXISTS binaryCarbon")
  }

  test("test load data with binary_decoder in df") {
    try {
      sql("DROP TABLE IF EXISTS carbon_table")
      val rdd = sqlContext.sparkContext.parallelize(1 to 3)
        .map(x => Row("a" + x % 10, "b", x, "YWJj".getBytes()))
      val customSchema = StructType(Array(
        SparkStructField("c1", StringType),
        SparkStructField("c2", StringType),
        SparkStructField("number", IntegerType),
        SparkStructField("c4", BinaryType)))

      val df = sqlContext.sparkSession.createDataFrame(rdd, customSchema);
      // Saves dataFrame to carbon file
      df.write.format("carbon")
        .option("binary_decoder", "base64")
        .saveAsTable("carbon_table")
      val path = warehouse + "/carbon_table"

      val carbonDF = sqlContext.sparkSession.read
        .format("carbon")
        .option("tablename", "carbon_table")
        .schema(customSchema)
        .load(path) // TODO: check why can not read when without path
      assert(carbonDF.schema.map(_.name) === Seq("c1", "c2", "number", "c4"))
      // "YWJj" is base64 decode data of "abc" string,
      // but spark doesn't support string for binary, so we use byte[] and
      // carbon will not decode for byte
      checkAnswer(carbonDF, Seq(Row("a1", "b", 1, "YWJj".getBytes()),
        Row("a2", "b", 2, "YWJj".getBytes()),
        Row("a3", "b", 3, "YWJj".getBytes())))

      val carbonDF2 = carbonDF.drop("c1")
      assert(carbonDF2.schema.map(_.name) === Seq("c2", "number", "c4"))
      checkAnswer(sql(s"select * from carbon.`$path`"),
        Seq(Row("a1", "b", 1, "YWJj".getBytes()),
          Row("a2", "b", 2, "YWJj".getBytes()),
          Row("a3", "b", 3, "YWJj".getBytes())))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        assert(false)
    } finally {
      sql("DROP TABLE IF EXISTS carbon_table")
    }
  }

  test("test spark doesn't support input string value for binary data type") {
    val rdd = sqlContext.sparkContext.parallelize(1 to 3)
      .map(x => Row("a" + x % 10, "b", x, "YWJj".getBytes()))
    val customSchema = StructType(Array(
      SparkStructField("c1", StringType),
      SparkStructField("c2", StringType),
      SparkStructField("number", IntegerType),
      SparkStructField("c4", BinaryType)))

    try {
      sqlContext.sparkSession.createDataFrame(rdd, customSchema);
    } catch {
      case e: RuntimeException => e.getMessage.contains(
        "java.lang.String is not a valid external type for schema of binary")
    }
  }

  override protected def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .removeProperty(CarbonCommonConstants.LOAD_SORT_SCOPE)
    drop
    createParquetTable
  }

  override def afterAll(): Unit = {
    drop
  }

  private def drop = {
    sql("drop table if exists testformat")
    sql("drop table if exists carbon_table")
    sql("drop table if exists testparquet")
    sql("drop table if exists par_table")
    sql("drop table if exists sdkout")
    sql("drop table if exists validate")
    sql("drop table if exists fileformat_date")
  }
}

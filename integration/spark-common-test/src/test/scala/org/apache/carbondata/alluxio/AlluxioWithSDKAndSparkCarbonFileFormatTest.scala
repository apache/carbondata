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

package org.apache.carbondata.alluxio

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.{Timestamp, Date => sqlData}

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.Row
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll
import scala.collection.mutable

import org.apache.carbondata.alluxio.util.AlluxioUtilTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.{CarbonReader, CarbonWriter, Field, Schema}

class AlluxioWithSDKAndSparkCarbonFileFormatTest extends AlluxioUtilTest with BeforeAndAfterAll {
    var remoteFile = ""
    var allDataTypeRemote = ""
    var storeLocationOriginal = ""
    val carbonAndAlluxio = "/CarbonAndAlluxio"
    var SDKPath = "SDK"

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        storeLocationOriginal = CarbonProperties.getInstance().
                getProperty(CarbonCommonConstants.STORE_LOCATION)
        val alluxioStoreLocation = localAlluxioCluster.getMasterURI + carbonAndAlluxio
        CarbonProperties.getInstance().
                addProperty(CarbonCommonConstants.STORE_LOCATION, alluxioStoreLocation)
        val rootPath = new File(this.getClass.getResource("/").getPath
                + "../../../..").getCanonicalPath

        val time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
        val localFile = rootPath + "/hadoop/src/test/resources/data.csv"
        remoteFile = "/carbon_alluxio" + time + ".csv"
        fileSystemShell.run("copyFromLocal", localFile, remoteFile)

        SDKPath = "/SDK" + time
        val allDataTypeLocal = resourcesPath + "/alldatatypeforpartition.csv"
        allDataTypeRemote = "/alldatatype" + time + ".csv"
        fileSystemShell.run("copyFromLocal", allDataTypeLocal, allDataTypeRemote)
        println(alluxioStoreLocation)
        fileSystemShell.run("ls", allDataTypeRemote)
    }

    test("Write and read data by SDK") {
        val path = localAlluxioCluster.getMasterURI + SDKPath

        fileSystemShell.run("mkdir", path)
        fileSystemShell.run("ls", path)

        try {
            import scala.collection.JavaConverters._
            val fields = List(new StructField("byteField", DataTypes.BYTE),
                new StructField("floatField", DataTypes.FLOAT))
            val structType = Array(new Field("stringfield", DataTypes.STRING),
                new Field("structField", "struct", fields.asJava))

            val builder = CarbonWriter.builder()
            val writer = builder.outputPath(path)
                    .uniqueIdentifier(System.nanoTime())
                    .withBlockSize(2)
                    .withCsvInput(new Schema(structType))
                    .writtenBy("SparkCarbonDataSourceTest")
                    .build()

            var i = 0
            while (i < 11) {
                val array = Array[String](s"name$i", s"$i" + "\001" + s"$i.${i}12")
                writer.write(array)
                i += 1
            }
            writer.close()

            val reader = CarbonReader.builder(path, "_temp").build

            i = 0
            while (i < 20 && reader.hasNext) {
                val row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
                val array = row(1).asInstanceOf[Array[Object]]
                assert(("name" + i).equalsIgnoreCase(row(0).toString))
                assert((i.toString).equalsIgnoreCase(array(0).toString))
                assert((s"$i.${i}12".toString).equalsIgnoreCase(array(1).toString))
                i += 1
            }
            reader.close()
            println("SDK WritePath:")
            fileSystemShell.run("ls", path)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            fileSystemShell.run("rm", "-R", path)
            fileSystemShell.run("ls", "/")
        }
    }

    test("Write data by SDK and read data by using carbon") {
        val path = localAlluxioCluster.getMasterURI + SDKPath

        fileSystemShell.run("mkdir", path)
        fileSystemShell.run("ls", path)

        try {
            import scala.collection.JavaConverters._
            val fields = List(new StructField("byteField", DataTypes.BYTE),
                new StructField("floatField", DataTypes.FLOAT))
            val structType = Array(new Field("stringfield", DataTypes.STRING),
                new Field("structField", "struct", fields.asJava))

            val builder = CarbonWriter.builder()
            val writer = builder.outputPath(path)
                    .uniqueIdentifier(System.nanoTime())
                    .withBlockSize(2)
                    .withCsvInput(new Schema(structType))
                    .writtenBy("SparkCarbonDataSourceTest")
                    .build()

            var i = 0
            while (i < 11) {
                val array = Array[String](s"name$i", s"$i" + "\001" + s"$i.${i}12")
                writer.write(array)
                i += 1
            }
            writer.close()

            val reader = CarbonReader.builder(path, "_temp")
                    .build

            i = 0
            while (i < 20 && reader.hasNext) {
                val row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
                val array = row(1).asInstanceOf[Array[Object]]
                assert(("name" + i).equalsIgnoreCase(row(0).toString))
                assert((i.toString).equalsIgnoreCase(array(0).toString))
                assert((s"$i.${i}12".toString).equalsIgnoreCase(array(1).toString))
                i += 1
            }
            reader.close()
            sql("DROP TABLE IF EXISTS complex_table")
            if (SparkUtil.isSparkVersionEqualTo("2.1")) {
                if (!FileFactory.isFileExist(path)) {
                    FileFactory.createDirectoryAndSetPermission(path,
                        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
                }
                sql(
                    s"""
                       | CREATE TABLE complex_table(
                       |    stringfield STRING,
                       |    structfield STRUCT<bytefield: BYTE, floatfield: FLOAT>) "
                       | USING carbon OPTIONS(PATH '$path')
                     """.stripMargin)
            } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
                sql(
                    s"""
                       | CREATE TABLE complex_table(
                       |    stringfield STRING,
                       |    structfield STRUCT<bytefield: BYTE, floatfield: FLOAT>)
                       | USING carbon LOCATION '$path'
                     """.stripMargin)
            }

            checkAnswer(sql("SELECT * FROM complex_table limit 1"), Seq(Row("name0", Row(0
                    .asInstanceOf[Byte], 0.012.asInstanceOf[Float]))))
            checkAnswer(sql("SELECT * FROM complex_table WHERE structfield.bytefield > 9"), Seq(Row
            ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
            checkAnswer(sql("SELECT * FROM complex_table WHERE structfield.bytefield > 9"), Seq(Row
            ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
            checkAnswer(sql("SELECT * FROM complex_table WHERE structfield.floatfield > 9.912"), Seq
            (Row
            ("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
            checkAnswer(sql("SELECT * FROM complex_table WHERE structfield.floatfield > 9.912 AND structfield.bytefield < 11"),
                Seq(Row("name10", Row(10.asInstanceOf[Byte], 10.1012.asInstanceOf[Float]))))
            println("SDK WritePath:")
            fileSystemShell.run("ls", path)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            sql("drop table if exists complex_table")
            fileSystemShell.run("rm", "-R", path)
            fileSystemShell.run("ls", "/")
        }
    }

    test("test alluxio with all data type and Create/Load/Select/Insert/Update/Delete") {
        val tableNameForAllTypeOriginal = "alluxio_table_all_type_original"
        val tableNameForAllType = "alluxio_table_all_type"
        try {
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
                    .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

            val path = localAlluxioCluster.getMasterURI + allDataTypeRemote
            val alluxioSDKPath = localAlluxioCluster.getMasterURI + SDKPath

            fileSystemShell.run("mkdir", alluxioSDKPath)
            fileSystemShell.run("ls", alluxioSDKPath)
            val df = sqlContext.read.textFile(path)
            println(path)

            import sqlContext.implicits._
            val arrayString = df.filter { each =>
                !each.startsWith("smallIntField")
            }.map { each: String =>
                each.split(',')
            }.collect()

            import scala.collection.JavaConverters._

            val fields = List(
                new StructField("stringField1", DataTypes.STRING),
                new StructField("stringField2", DataTypes.STRING),
                new StructField("stringField3", DataTypes.STRING))
            val structType = Array(
                new Field("smallIntField", DataTypes.SHORT),
                new Field("intField", DataTypes.INT),
                new Field("bigIntField", DataTypes.LONG),
                new Field("floatField", DataTypes.DOUBLE),
                new Field("doubleField", DataTypes.DOUBLE),
                new Field("decimalField", DataTypes.createDecimalType(25, 4)),
                new Field("timestampField", DataTypes.TIMESTAMP),
                new Field("dateField", DataTypes.DATE),
                new Field("stringField", DataTypes.STRING),
                new Field("varcharField", DataTypes.STRING),
                new Field("charField", DataTypes.STRING),
                new Field("arrayField", DataTypes.createArrayType(DataTypes.STRING)),
                new Field("structField", "struct", fields.asJava),
                new Field("booleanField", DataTypes.BOOLEAN))

            val builder = CarbonWriter.builder()
            val writer = builder.outputPath(alluxioSDKPath)
                    .uniqueIdentifier(System.nanoTime())
                    .withBlockSize(2)
                    .withCsvInput(new Schema(structType))
                    .writtenBy("SparkCarbonDataSourceTest")
                    .withLoadOption("COMPLEX_DELIMITER_LEVEL_1", "$")
                    .withLoadOption("COMPLEX_DELIMITER_LEVEL_2", ":")
                    .build()

            var i = 0
            while (i < 3) {
                val array = arrayString(i)
                writer.write(array)
                i += 1
            }
            writer.close()

            val reader = CarbonReader.builder(alluxioSDKPath, "_temp").build

            i = 0
            while (i < 20 && reader.hasNext) {
                val row = reader.readNextRow.asInstanceOf[Array[AnyRef]]
                assert(14 == row.length)
                i += 1
            }
            reader.close()

            sql("DROP TABLE IF EXISTS " + tableNameForAllType)
            sql("DROP TABLE IF EXISTS " + tableNameForAllTypeOriginal)
            sql(
                s"""
                   |create table $tableNameForAllTypeOriginal(
                   | smallIntField SMALLINT,
                   | intField INT,
                   | bigIntField BIGINT,
                   | floatField FLOAT,
                   | doubleField DOUBLE,
                   | decimalField DECIMAL(25, 4),
                   | timestampField TIMESTAMP,
                   | dateField DATE,
                   | stringField STRING,
                   | varcharField VARCHAR(10),
                   | charField CHAR(10),
                   | arrayField ARRAY < string >,
                   | structField STRUCT < col1: STRING, col2: STRING, col3: STRING >,
                   | booleanField BOOLEAN)
                   | stored by 'carbondata'
                 """.stripMargin)


            sql(s"LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableNameForAllTypeOriginal " +
                    "options('COMPLEX_DELIMITER_LEVEL_1'='$','COMPLEX_DELIMITER_LEVEL_2'=':')")

            fileSystemShell.run("ls", carbonAndAlluxio + "/default")
            val externalTablePath = localAlluxioCluster.getMasterURI + carbonAndAlluxio + "/default/" + tableNameForAllTypeOriginal
            sql(s"CREATE TABLE $tableNameForAllType using carbon" +
                    s" LOCATION '$alluxioSDKPath'")

            // Count
            checkAnswer(sql(s"SELECT count(*) FROM $tableNameForAllType"), Seq(Row(3)))

            // Select
            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = -32768"),
                Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), sqlData.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"), true)))

            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 128"),
                Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), sqlData.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"), false)))

            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 32767"),
                Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), sqlData.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"), true)))

            // TODO: support it
            // AlluxioCommonTest.testAllDataType(tableNameForAllType)

            fileSystemShell.run("ls", carbonAndAlluxio + "/default")
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            sql("DROP TABLE IF EXISTS " + tableNameForAllType)
            sql("DROP TABLE IF EXISTS " + tableNameForAllTypeOriginal)
        }
    }

    override protected def afterAll(): Unit = {
        // fileSystemShell.run("rm", remoteFile)
        // fileSystemShell.run("rm", allDataTypeRemote)
        super.afterAll()
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocationOriginal)
                .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                    CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    }
}

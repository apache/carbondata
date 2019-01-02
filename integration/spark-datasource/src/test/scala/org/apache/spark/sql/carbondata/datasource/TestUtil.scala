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

import scala.collection.JavaConverters._

import org.apache.avro
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, Encoder}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonFileIndexReplaceRule
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.sideBySide
import org.junit.Assert

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.file.CarbonWriter

object TestUtil {

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val warehouse1 = FileFactory.getPath(s"$rootPath/integration/spark-datasource/target/warehouse").toString
  val resource = s"$rootPath/integration/spark-datasource/src/test/resources"
  val metaStoreDB1 = s"$rootPath/integration/spark-datasource/target"
  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("local")
    .config("spark.sql.warehouse.dir", warehouse1)
    .config("spark.driver.host", "localhost")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  if (!spark.sparkContext.version.startsWith("2.1")) {
    spark.experimental.extraOptimizations = Seq(new CarbonFileIndexReplaceRule)
  }
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_MINMAX_ALLOWED_BYTE_COUNT, "40")

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]):Unit = {
    checkAnswer(df, expectedAnswer.asScala)
  }

  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case d : Double =>
            if (!d.isInfinite && !d.isNaN) {
              var bd = BigDecimal(d)
              bd = bd.setScale(5, BigDecimal.RoundingMode.UP)
              bd.doubleValue()
            }
            else {
              d
            }
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           |Results do not match for query:
           |${df.queryExecution}
           |== Results ==
           |${
          sideBySide(
            s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")
        }
      """.stripMargin
      assert(false, errorMessage)
    }
  }

  def WriteFilesWithAvroWriter(writerPath: String,
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

}

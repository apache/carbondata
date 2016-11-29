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

package com.databricks.spark.csv.newapi

import com.databricks.spark.csv.{CarbonCsvRelation, CsvSchemaRDD}
import com.databricks.spark.csv.util.{ParserLibs, TextFile, TypeCast}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Provides access to CSV data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
    extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
  }

  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {
    val path = checkPath(parameters)
    val delimiter = TypeCast.toChar(parameters.getOrElse("delimiter", ","))

    val quote = parameters.getOrElse("quote", "\"")
    val quoteChar = if (quote.length == 1) {
      quote.charAt(0)
    } else {
      throw new Exception("Quotation cannot be more than one character.")
    }

    val escape = parameters.getOrElse("escape", null)
    val escapeChar: Character = if (escape == null || (escape.length == 0)) {
      null
    } else if (escape.length == 1) {
      escape.charAt(0)
    } else {
      throw new Exception("Escape character cannot be more than one character.")
    }

    val comment = parameters.getOrElse("comment", "#")
    val commentChar: Character = if (comment == null) {
      null
    } else if (comment.length == 1) {
      comment.charAt(0)
    } else {
      throw new Exception("Comment marker cannot be more than one character.")
    }

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val useHeader = parameters.getOrElse("header", "false")
    val headerFlag = if (useHeader == "true") {
      true
    } else if (useHeader == "false") {
      false
    } else {
      throw new Exception("Header flag can be true or false")
    }

    val parserLib = parameters.getOrElse("parserLib", ParserLibs.DEFAULT)
    val ignoreLeadingWhiteSpace = parameters.getOrElse("ignoreLeadingWhiteSpace", "false")
    val ignoreLeadingWhiteSpaceFlag = if (ignoreLeadingWhiteSpace == "false") {
      false
    } else if (ignoreLeadingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitesspace supported for Univocity parser only")
      }
      true
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }
    val ignoreTrailingWhiteSpace = parameters.getOrElse("ignoreTrailingWhiteSpace", "false")
    val ignoreTrailingWhiteSpaceFlag = if (ignoreTrailingWhiteSpace == "false") {
      false
    } else if (ignoreTrailingWhiteSpace == "true") {
      if (!ParserLibs.isUnivocityLib(parserLib)) {
        throw new Exception("Ignore whitespace supported for the Univocity parser only")
      }
      true
    } else {
      throw new Exception("Ignore white space flag can be true or false")
    }

    val charset = parameters.getOrElse("charset", TextFile.DEFAULT_CHARSET.name())
    // TODO validate charset?

    val inferSchema = parameters.getOrElse("inferSchema", "false")
    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }

    CarbonCsvRelation(path,
      headerFlag,
      delimiter,
      quoteChar,
      escapeChar,
      commentChar,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpaceFlag,
      ignoreTrailingWhiteSpaceFlag,
      schema,
      charset,
      inferSchemaFlag)(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }

    val codec: Class[_ <: CompressionCodec] =
      parameters.getOrElse("codec", "none") match {
        case "gzip" => classOf[GzipCodec]
        case _ => null
      }

    if (doSave) {
      // Only save data when the save mode is not ignore.
      data.saveAsCsvFile(path, parameters, codec)
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}


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
package org.apache.carbondata.streamer

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.command.mutation.merge.CarbonMergeDataSetUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.util.SparkStreamingUtil

/**
 * Abstract class Source which will be extended based on the source types of KAFKA, DFS etc
 */
abstract class Source {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  // avro schema, which is basically the read schema for the incoming data from sources like
  // Kafka, DFS etc
  protected var schema: Schema = _

  protected lazy val schemaSource: SchemaSource = try {
    schemaProviderClass match {
      case "SchemaRegistry" | "org.apache.carbondata.streamer.SchemaRegistry" => SchemaRegistry
        .getClass
        .getClassLoader
        .loadClass("org.apache.carbondata.streamer.SchemaRegistry")
        .newInstance()
        .asInstanceOf[SchemaRegistry]
      case "FileSchema" | "org.apache.carbondata.streamer.FileSchema" => FileSchema
        .getClass
        .getClassLoader
        .loadClass("org.apache.carbondata.streamer.FileSchema")
        .newInstance()
        .asInstanceOf[FileSchema]
      case _ => throw new UnsupportedOperationException(
        "Schema provider other than SchemaRegistry and FileSchema are not supported. Please " +
        "configure the proper value.")
    }
  } catch {
    case ex: ClassNotFoundException =>
      LOGGER.error("Schema provider class is configured wrongly. Please configure and retry.", ex)
      throw new CarbonDataStreamerException(
        "Schema provider class is configured wrongly. Please configure and retry.",
        ex)
  }

  // join key column
  protected val keyColumn: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_STREAMER_KEY_FIELD)

  protected val mergeOperationType: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_STREAMER_MERGE_OPERATION_TYPE)

  val schemaProviderClass: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER,
      CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER_DEFAULT)

  /**
   * This method will load the class based on the schema source provider configured by user and
   * initializes the read schema.
   */
  def loadSchemaBasedOnConfiguredClass(): Unit = {
    schema = schemaSource.getSchema
  }

  /**
   * This method returns the Row object for each incoming GenericRecord.
   * @param record incoming generic record read from kafka or DFS.
   * @param sqlType the reader schema to convert to Row.
   * @return Spark Row
   */
  def genericRecordToRow(record: GenericRecord, sqlType: StructType): Row = {
    val values: scala.collection.mutable.Buffer[Object] = scala.collection.mutable.Buffer.empty
    record.getSchema.getFields.asScala.foreach { field =>
      var value = record.get(field.name())
      // if the field type is union, assuming the first type will be null type.
      val fieldType = if (field.schema().getType.equals(Type.UNION)) {
        val fieldTypesInUnion = field.schema().getTypes
        if (fieldTypesInUnion.get(0).getType.equals(Type.NULL)) {
          fieldTypesInUnion.get(1).getType
        } else {
          fieldTypesInUnion.get(0).getType
        }
      } else {
        field.schema().getType
      }
      fieldType match {
        case Type.STRING if value != null =>
          // Avro returns Utf8s for strings, which Spark SQL doesn't know how to use.
          value = value.toString
        case Type.BYTES =>
          // Avro returns binary as a ByteBuffer, but Spark SQL wants a byte[].
          value = value.asInstanceOf[ByteBuffer].array()
        case _ =>
      }
      values += value
    }
    new GenericRowWithSchema(values.toArray, sqlType)
  }

  /**
   * This method prepares the dataset for the avro source and calls to perform the specified
   * merge operation.
   * @param inputStream The wrapper object which contains the spark's DStream to read the data.
   * @param carbonTable target carbondata table object.
   */
  def prepareDSForAvroSourceAndMerge(
      inputStream: CarbonDStream,
      carbonTable: CarbonTable): Unit = {
    inputStream.inputDStream.asInstanceOf[DStream[GenericRecord]].foreachRDD { rdd =>
      // get updated schema for each rdd to capture any schema changes
      val schema = schemaSource.getSchema
      val schemaString = schema.toString()
      val sparkDataTypes = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val rowRDD = rdd.mapPartitions { iterator =>
        // avro schema can have the default values in the schema, but the spark Avro Deserializer do
        // not take care to prepare the internal row based on this. So here get the fields where the
        // default value is mentioned for the fields and also get the corresponding index. Based on
        // this index in the internal row object update with the default value if the value at that
        // specific index in the internal row is null.
        val stringToAvroSchema = new Schema.Parser().parse(schemaString)
        val filteredSchemaFields = stringToAvroSchema.getFields
          .asScala
          .zipWithIndex
          .filter { case (field, _) =>
            field.defaultVal() != null
          }.map { case (field, i) =>
          (field.name(), field.defaultVal(), i)
        }
        val encoder = RowEncoder.apply(sparkDataTypes).resolveAndBind()
        new Iterator[Row] {
          override def hasNext: Boolean = {
            iterator.hasNext
          }

          override def next(): Row = {
            if (!hasNext) {
              throw new NoSuchElementException("next on empty iterator")
            }
            val record = iterator.next()
            val avroWriteSchema = record.getSchema
            var sparkAvroDeserializer = new AvroDeserializer(avroWriteSchema, sparkDataTypes)
            val internalRow = sparkAvroDeserializer.deserialize(record).asInstanceOf[InternalRow]
            // update with the default values if the value is null
            if (avroWriteSchema.getFields.size() != sparkDataTypes.fields.length) {
              val missingFieldsWithReadSchema = sparkDataTypes.fields
                .map(_.name.toLowerCase())
                .diff(avroWriteSchema.getFields.asScala.map(_.name.toLowerCase()))
              filteredSchemaFields.foreach { case (fieldName, defaultValue, i) =>
                if (missingFieldsWithReadSchema.contains(fieldName.toLowerCase()) &&
                    internalRow.isNullAt(i)) {
                  internalRow.update(i, defaultValue)
                }
              }
            }
            val row = SparkStreamingUtil.convertInternalRowToRow(encoder)(internalRow)
            sparkAvroDeserializer = null
            row
          }
        }
      }
      // TODO: check without alias and remove alias
      val targetDs = spark
        .sql(s"select * from ${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
        .as("A")
      val sourceDS = spark.createDataFrame(rowRDD, sparkDataTypes).as("B")
      CarbonMergeDataSetUtil.handleSchemaEvolutionForCarbonStreamer(targetDs, sourceDS, spark)
      val updatedTargetDs = spark
        .sql(s"select * from ${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
        .as("A")
      val updatedCarbonTable = CarbonEnv.getCarbonTable(Some(carbonTable.getDatabaseName),
        carbonTable.getTableName)(spark)
      val tableCols =
        updatedCarbonTable.getCreateOrderColumn.asScala.map(_.getColName).
          filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
      inputStream.performMergeOperation(updatedTargetDs,
        sourceDS.select(tableCols.map(col): _*).as("B"),
        keyColumn,
        mergeOperationType)
    }
  }

  /**
   * This method prepares the wrapper object containing the DStream. The DStream object prepared
   * based on the input source type of Kafka or DFS.
   * @param ssc Spark streaming context to prepare the DStream.
   * @param sparkSession Spark Session.
   * @return Wrapper object of CarbonDStream containing DStream.
   */
  def getStream(
      ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream

  /**
   * This prepared the Dataset with the stream provided and call to perform the specified merge
   * operation.
   * @param inputStream Input CarbonDStream.
   */
  def prepareDFAndMerge(inputStream: CarbonDStream)
}

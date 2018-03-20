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

package org.apache.carbondata.hive

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener
import org.apache.hadoop.hive.metastore.api.{FieldSchema, MetaException}
import org.apache.hadoop.hive.metastore.events._
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType._
import org.apache.spark.SparkException
import org.apache.spark.sql.CarbonSource
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.execution.command.TableNewProcessor
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.writer.ThriftWriter

class CarbonHiveMetastoreListener(conf: Configuration) extends MetaStorePreEventListener(conf) {

  override def onEvent(preEventContext: PreEventContext): Unit = {
    preEventContext.getEventType match {
      case CREATE_TABLE =>
        val table = preEventContext.asInstanceOf[PreCreateTableEvent].getTable
        val tableProps = table.getParameters
        if (tableProps.get("spark.sql.sources.provider") == "org.apache.spark.sql.CarbonSource") {
          val numSchemaParts = tableProps.get("spark.sql.sources.schema.numParts")
          if (numSchemaParts != null && !numSchemaParts.isEmpty) {
            val parts = (0 until numSchemaParts.toInt).map { index =>
              val part = tableProps.get(s"spark.sql.sources.schema.part.${index}")
              if (part == null) {
                throw new MetaException(s"spark.sql.sources.schema.part.${index} is missing!")
              }
              part
            }
            // Stick all parts back to a single schema string.
            val schema = DataType.fromJson(parts.mkString).asInstanceOf[StructType]
            val hiveSchema = schema.map(toHiveColumn).asJava
            table.getSd.setCols(hiveSchema)
            table.getSd.setInputFormat("org.apache.carbondata.hive.MapredCarbonInputFormat")
            table.getSd.setOutputFormat("org.apache.carbondata.hive.MapredCarbonOutputFormat")
            val serdeInfo = table.getSd.getSerdeInfo
            serdeInfo.setSerializationLib("org.apache.carbondata.hive.CarbonHiveSerDe")
            val tablePath = serdeInfo.getParameters.get("tablePath")
            if (tablePath != null) {
              table.getSd.setLocation(tablePath)
            }
          }
        } else if (table.getSd
          .getInputFormat.equals("org.apache.carbondata.hive.MapredCarbonInputFormat")) {
          tableProps.put("spark.sql.sources.provider", "org.apache.spark.sql.CarbonSource")
          val dbName = table.getDbName
          val tableName = table.getTableName
          val schema = new StructType()
          val cols = table.getSd.getCols.asScala
          cols.foreach { f =>
            schema.add(fromHiveColumn(f))
          }
          val cm = CarbonSource.createTableInfoFromParams(tableProps.asScala.toMap,
            schema, dbName, tableName)

          val tableInfo = TableNewProcessor(cm)
          val hiveSchemaStore = conf.get(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
            CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT).toBoolean

          if (hiveSchemaStore) {
            val schemaParts = CarbonUtil.convertToMultiGsonStrings(tableInfo, "=", "'", "")
            val schemaPartSeq = schemaParts.split(",")
            schemaPartSeq.foreach { p =>
              val parts = p.split(",")
              table.getSd.getSerdeInfo.getParameters.put(parts(0).substring(1, parts(0).length - 1),
                parts(1).substring(1, parts(1).length - 1))
            }
          } else {
            val schemaFilePath = table.getSd.getLocation + "/Metadata"
            tableInfo.setMetaDataFilepath(schemaFilePath)
            val schemaConverter = new ThriftWrapperSchemaConverterImpl
            val thriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(tableInfo,
              tableInfo.getDatabaseName, tableInfo.getFactTable.getTableName)
            val fileType = FileFactory.getFileType(schemaFilePath)
            if (!FileFactory.isFileExist(schemaFilePath, fileType)) {
              FileFactory.mkdirs(schemaFilePath, fileType)
            }
            val thriftWriter = new ThriftWriter(schemaFilePath, false)
            thriftWriter.open()
            thriftWriter.write(thriftTableInfo)
            thriftWriter.close()
          }
          CarbonMetadata.getInstance.loadTableMetadata(tableInfo)

        }
      case ALTER_TABLE =>
        val table = preEventContext.asInstanceOf[PreAlterTableEvent].getNewTable
        val tableProps = table.getParameters
        if (tableProps != null &&
            tableProps.get("spark.sql.sources.provider") == "org.apache.spark.sql.CarbonSource") {
          val numSchemaParts = tableProps.get("spark.sql.sources.schema.numParts")
          if (numSchemaParts != null && !numSchemaParts.isEmpty) {
            val schemaParts = (0 until numSchemaParts.toInt).map { index =>
              val schemaPart = tableProps.get(s"spark.sql.sources.schema.part.$index")
              if (schemaPart == null) {
                throw new MetaException(s"spark.sql.sources.schema.part.$index is missing!")
              }
              schemaPart
            }
            // Stick all schemaParts back to a single schema string.
            val schema = DataType.fromJson(schemaParts.mkString).asInstanceOf[StructType]
            val hiveSchema = schema.map(toHiveColumn).asJava
            table.getSd.setCols(hiveSchema)
          }
        }
      case _ =>
      // do nothing
    }
  }

  private def fromHiveColumn(hc: FieldSchema): StructField = {
    val columnType = try {
      CatalystSqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }

    val metadata = new MetadataBuilder().putString("HIVE_TYPE_STRING", hc.getType).build()
    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  private def toHiveColumn(c: StructField): FieldSchema = {
    val typeString = if (c.metadata.contains("HIVE_TYPE_STRING")) {
      c.metadata.getString("HIVE_TYPE_STRING")
    } else {
      c.dataType.catalogString
    }
    new FieldSchema(c.name, typeString, c.getComment().orNull)
  }
}

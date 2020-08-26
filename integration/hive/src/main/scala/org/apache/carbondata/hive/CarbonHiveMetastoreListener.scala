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
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class CarbonHiveMetastoreListener(conf: Configuration) extends MetaStorePreEventListener(conf) {

  override def onEvent(preEventContext: PreEventContext): Unit = {
    preEventContext.getEventType match {
      case CREATE_TABLE =>
        val table = preEventContext.asInstanceOf[PreCreateTableEvent].getTable
        val tableProps = table.getParameters
        if (tableProps != null
          && (tableProps.get("spark.sql.sources.provider") == "org.apache.spark.sql.CarbonSource"
          || tableProps.get("spark.sql.sources.provider").equalsIgnoreCase("carbondata"))) {
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
            table.getParameters
              .put("storage_handler", "org.apache.carbondata.hive.CarbonStorageHandler")
            val serdeInfo = table.getSd.getSerdeInfo
            serdeInfo.setSerializationLib("org.apache.carbondata.hive.CarbonHiveSerDe")
            val tablePath = serdeInfo.getParameters.get("tablePath")
            if (tablePath != null) {
              table.getSd.setLocation(tablePath)
            }
          }
        }
      case ALTER_TABLE =>
        val table = preEventContext.asInstanceOf[PreAlterTableEvent].getNewTable
        val tableProps = table.getParameters
        if (tableProps != null
          && (tableProps.get("spark.sql.sources.provider") == "org.apache.spark.sql.CarbonSource"
          || tableProps.get("spark.sql.sources.provider").equalsIgnoreCase("carbondata"))) {
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

  private def toHiveColumn(c: StructField): FieldSchema = {
    val typeString = if (c.metadata.contains("HIVE_TYPE_STRING")) {
      c.metadata.getString("HIVE_TYPE_STRING")
    } else {
      c.dataType.catalogString
    }
    new FieldSchema(c.name, typeString, c.getComment().orNull)
  }
}

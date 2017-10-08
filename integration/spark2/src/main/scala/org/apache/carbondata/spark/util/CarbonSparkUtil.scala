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

package org.apache.carbondata.spark.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.hive.{CarbonMetaData, CarbonRelation, DictionaryMap}

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.processing.merger.TableMeta

case class TransformHolder(rdd: Any, mataData: CarbonMetaData)

object CarbonSparkUtil {

  def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
    val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
        .asScala.map(x => x.getColName) // wf : may be problem
    val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
        .asScala.map(x => x.getColName)
    val dictionary =
      carbonTable.getDimensionByTableName(carbonTable.getFactTableName).asScala.map { f =>
        (f.getColName.toLowerCase,
            f.hasEncoding(Encoding.DICTIONARY) && !f.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
                !f.getDataType.isComplexType)
      }
    CarbonMetaData(dimensionsAttr, measureAttr, carbonTable, DictionaryMap(dictionary.toMap))
  }

  def createCarbonRelation(tableInfo: TableInfo, tablePath: String): CarbonRelation = {
    val identifier = AbsoluteTableIdentifier.fromTablePath(tablePath)
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    val meta = new TableMeta(identifier.getCarbonTableIdentifier,
      identifier.getStorePath, tablePath, table)
    CarbonRelation(tableInfo.getDatabaseName, tableInfo.getFactTable.getTableName,
      CarbonSparkUtil.createSparkMeta(table), meta)
  }

}

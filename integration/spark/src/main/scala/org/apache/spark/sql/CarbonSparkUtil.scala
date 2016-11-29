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

package org.apache.spark.sql

import scala.collection.JavaConverters._

import org.apache.spark.sql.hive.{CarbonMetaData, DictionaryMap}

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil

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
                !CarbonUtil.hasComplexDataType(f.getDataType))
      }
    CarbonMetaData(dimensionsAttr, measureAttr, carbonTable, DictionaryMap(dictionary.toMap))
  }
}

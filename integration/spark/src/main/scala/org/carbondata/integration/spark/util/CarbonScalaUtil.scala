/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.util

import org.apache.spark.sql.cubemodel.Level
import org.apache.spark.sql.hive.CarbonMetaData
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CarbonEnv, CarbonContext, CarbonRelation, SchemaRDD}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.query.expression.{DataType => CarbonDataType}
import scala.collection.JavaConversions._
import org.apache.spark.sql.catalyst.plans.logical.Cube
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(dataType: org.apache.spark.sql.types.DataType): CarbonDataType =
    dataType match {

      case StringType => CarbonDataType.StringType
      case IntegerType => CarbonDataType.IntegerType
      case LongType => CarbonDataType.LongType
      case DoubleType => CarbonDataType.DoubleType
      case FloatType => CarbonDataType.FloatType
      case DateType => CarbonDataType.DateType
      case BooleanType => CarbonDataType.BooleanType
      case TimestampType => CarbonDataType.TimestampType
      case ArrayType(_, _) => CarbonDataType.ArrayType
      case StructType(_) => CarbonDataType.StructType
      case NullType => CarbonDataType.NullType
      case _ => CarbonDataType.DecimalType
    }

  def convertSparkToCarbonSchemaDataType(dataType: String): String =

    dataType match {
      case CarbonCommonConstants.STRING_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.INTEGER_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.BYTE_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.SHORT_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.LONG_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DOUBLE_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.FLOAT_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DECIMAL_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DATE_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.BOOLEAN_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.TIMESTAMP_TYPE => CarbonCommonConstants.TIMESTAMP
      case anyType => anyType
    }

  def convertSparkColumnToCarbonLevel(field: (String, String)): Seq[Level] =
    field._2 match {
      case CarbonCommonConstants.STRING_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.INTEGER_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.BYTE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.SHORT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.LONG_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DOUBLE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.FLOAT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DECIMAL_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DATE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.BOOLEAN_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.TIMESTAMP_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.TIMESTAMP))
    }


  case class TransformHolder(rdd: Any, mataData: CarbonMetaData)

  object CarbonSparkUtil {
    def createBaseRDD(carbonContext: CarbonContext, carbonTable: CarbonTable): TransformHolder = {
      val relation = CarbonEnv.getInstance(carbonContext).carbonCatalog.lookupRelation1(Option(carbonTable.getDatabaseName),
        carbonTable.getFactTableName, None)(carbonContext).asInstanceOf[CarbonRelation]
      var rdd = new SchemaRDD(carbonContext, relation)
      rdd.registerTempTable(carbonTable.getFactTableName)
      TransformHolder(rdd, createSparkMeta(carbonTable))
    }

    def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
      val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getFactTableName).map(x => x.getColName) // wf : may be problem
      val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getFactTableName).map(x => x.getColName)
      CarbonMetaData(dimensionsAttr, measureAttr, carbonTable)
    }

  }

}
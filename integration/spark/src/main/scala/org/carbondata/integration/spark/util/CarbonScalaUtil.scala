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
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.metadata.CarbonMetadata.Cube
import org.carbondata.query.expression.{DataType => MolapDataType}

import scala.collection.JavaConversions._

object CarbonScalaUtil {
  def convertSparkToMolapDataType(dataType: org.apache.spark.sql.types.DataType): MolapDataType =
    dataType match {

      case StringType => MolapDataType.StringType
      case IntegerType => MolapDataType.IntegerType
      case LongType => MolapDataType.LongType
      case DoubleType => MolapDataType.DoubleType
      case FloatType => MolapDataType.FloatType
      case DateType => MolapDataType.DateType
      case BooleanType => MolapDataType.BooleanType
      case TimestampType => MolapDataType.TimestampType
      case ArrayType(_, _) => MolapDataType.ArrayType
      case StructType(_) => MolapDataType.StructType
      case NullType => MolapDataType.NullType
      case _ => MolapDataType.DecimalType
    }

  def convertSparkToMolapSchemaDataType(dataType: String): String =

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

  def convertSparkColumnToMolapLevel(field: (String, String)): Seq[Level] =
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

  object OlapUtil {
    def createBaseRDD(olapContext: CarbonContext, cube: Cube): TransformHolder = {
      val olapCube = CarbonMetadata.getInstance().getCube(cube.getCubeName())
      val relation = CarbonEnv.getInstance(olapContext).carbonCatalog.lookupRelation1(Option(cube.getSchemaName()),
        cube.getOnlyCubeName(), None)(olapContext).asInstanceOf[CarbonRelation]
      var rdd = new SchemaRDD(olapContext, relation)
      rdd.registerTempTable(cube.getOnlyCubeName())
      TransformHolder(rdd, createSparkMeta(cube))
    }

    def createSparkMeta(cube: Cube): CarbonMetaData = {
      val dimensionsAttr = cube.getDimensions(cube.getFactTableName).map(x => x.getName()) // wf : may be problem
      val measureAttr = cube.getMeasures(cube.getFactTableName).map(x => x.getName())
      CarbonMetaData(dimensionsAttr, measureAttr, cube)
    }

  }

}
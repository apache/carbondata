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
import org.apache.spark.sql.hive.OlapMetaData
import org.apache.spark.sql.types._
import org.apache.spark.sql.{CarbonEnv, OlapContext, OlapRelation, SchemaRDD}
import org.carbondata.core.constants.MolapCommonConstants
import org.carbondata.core.metadata.MolapMetadata
import org.carbondata.core.metadata.MolapMetadata.Cube
import org.carbondata.query.expression.{DataType => MolapDataType}

import scala.collection.JavaConversions._

object MolapScalaUtil {
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
      case MolapCommonConstants.STRING_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.INTEGER_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.BYTE_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.SHORT_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.LONG_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DOUBLE_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.FLOAT_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DECIMAL_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DATE_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.BOOLEAN_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.TIMESTAMP_TYPE => MolapCommonConstants.TIMESTAMP
      case anyType => anyType
    }

  def convertSparkColumnToMolapLevel(field: (String, String)): Seq[Level] =
    field._2 match {
      case MolapCommonConstants.STRING_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.INTEGER_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.BYTE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.SHORT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.LONG_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DOUBLE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.FLOAT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DECIMAL_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DATE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.BOOLEAN_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.TIMESTAMP_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.TIMESTAMP))
    }


  case class TransformHolder(rdd: Any, mataData: OlapMetaData)

  object OlapUtil {
    def createBaseRDD(olapContext: OlapContext, cube: Cube): TransformHolder = {
      val olapCube = MolapMetadata.getInstance().getCube(cube.getCubeName())
      val relation = CarbonEnv.getInstance(olapContext).carbonCatalog.lookupRelation1(Option(cube.getSchemaName()),
        cube.getOnlyCubeName(), None)(olapContext).asInstanceOf[OlapRelation]
      var rdd = new SchemaRDD(olapContext, relation)
      rdd.registerTempTable(cube.getOnlyCubeName())
      TransformHolder(rdd, createSparkMeta(cube))
    }

    def createSparkMeta(cube: Cube): OlapMetaData = {
      val dimensionsAttr = cube.getDimensions(cube.getFactTableName).map(x => x.getName()) // wf : may be problem
      val measureAttr = cube.getMeasures(cube.getFactTableName).map(x => x.getName())
      OlapMetaData(dimensionsAttr, measureAttr, cube)
    }

  }

}
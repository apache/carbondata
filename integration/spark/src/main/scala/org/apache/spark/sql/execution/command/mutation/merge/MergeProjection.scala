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
package org.apache.spark.sql.execution.command.mutation.merge

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, GenericRowWithSchema, InterpretedMutableProjection, Projection}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DateType, TimestampType}

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Creates the projection for each action like update,delete or insert.
 */
case class MergeProjection(
    @transient tableCols: Seq[String],
    @transient ds: Dataset[Row],
    @transient rltn: CarbonDatasourceHadoopRelation,
    @transient sparkSession: SparkSession,
    @transient mergeAction: MergeAction) {

  private val cutOffDate = Integer.MAX_VALUE >> 1

  val isUpdate = mergeAction.isInstanceOf[UpdateAction]
  val isDelete = mergeAction.isInstanceOf[DeleteAction]

  def apply(row: GenericRowWithSchema): Array[Object] = {
    // TODO we can avoid these multiple conversions if this is added as a SparkPlan node.
    val values = row.toSeq.map {
      case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case d: java.math.BigDecimal => org.apache.spark.sql.types.Decimal.apply(d)
      case b: Array[Byte] => org.apache.spark.unsafe.types.UTF8String.fromBytes(b)
      case d: Date => DateTimeUtils.fromJavaDate(d)
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t)
      case value => value
    }

    val outputRow = projection(new GenericInternalRow(values.toArray))
      .asInstanceOf[GenericInternalRow]

    val array = outputRow.values.clone()
    var i = 0
    while (i < array.length) {
      output(i).dataType match {
        case d: DateType =>
          if (array(i) == null) {
            array(i) = CarbonCommonConstants.DIRECT_DICT_VALUE_NULL
          } else {
            array(i) = (array(i).asInstanceOf[Int] + cutOffDate)
          }
        case d: TimestampType =>
          if (array(i) == null) {
            array(i) = CarbonCommonConstants.DIRECT_DICT_VALUE_NULL
          } else {
            array(i) = (array(i).asInstanceOf[Long] / 1000)
          }

        case _ =>
      }
      i += 1
    }
    array.asInstanceOf[Array[Object]]
  }

  val (projection, output) = generateProjection

  private def generateProjection: (Projection, Array[Expression]) = {
    val existingDsOutput = rltn.carbonRelation.schema.toAttributes
    val colsMap = mergeAction match {
      case UpdateAction(updateMap) => updateMap
      case InsertAction(insertMap) => insertMap
      case _ => null
    }
    if (colsMap != null) {
      val output = new Array[Expression](tableCols.length)
      val expecOutput = new Array[Expression](tableCols.length)
      colsMap.foreach { case (k, v) =>
        val tableIndex = tableCols.indexOf(k.toString().toLowerCase)
        if (tableIndex < 0) {
          throw new CarbonMergeDataSetException(s"Mapping is wrong $colsMap")
        }
        output(tableIndex) = v.expr.transform {
          case a: Attribute if !a.resolved =>
            ds.queryExecution.analyzed.resolveQuoted(a.name,
              sparkSession.sessionState.analyzer.resolver).get
        }
        expecOutput(tableIndex) =
          existingDsOutput.find(_.name.equalsIgnoreCase(tableCols(tableIndex))).get
      }
      if (output.contains(null)) {
        throw new CarbonMergeDataSetException(s"Not all columns are mapped")
      }
      (new InterpretedMutableProjection(output, ds.queryExecution.analyzed.output), expecOutput)
    } else {
      (null, null)
    }
  }
}

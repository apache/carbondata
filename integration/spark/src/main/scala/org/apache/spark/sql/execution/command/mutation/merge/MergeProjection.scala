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

import scala.collection.mutable

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonToSparkAdapter, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.types.StructType

/**
 * Creates the projection for each action like update,delete or insert.
 */
case class MergeProjection(
    @transient tableCols: Seq[String],
    @transient ds: Dataset[Row],
    @transient rltn: CarbonDatasourceHadoopRelation,
    @transient sparkSession: SparkSession,
    @transient mergeAction: MergeAction) {

  val isUpdate: Boolean = mergeAction.isInstanceOf[UpdateAction]
  val isDelete: Boolean = mergeAction.isInstanceOf[DeleteAction]

  val schema: StructType = ds.schema

  val outputListOfDataset: Seq[Attribute] = ds.queryExecution.logical.output

  val targetTableAttributes = rltn.carbonRelation.output

  val indexesToFetch: Seq[(Expression, Int)] = {
    val existingDsOutput = rltn.carbonRelation.schema.toAttributes
    val literalToAttributeMap: collection.mutable.Map[Expression, Attribute] =
      collection.mutable.Map.empty[Expression, Attribute]
    val colsMap = mergeAction match {
      case UpdateAction(updateMap, _: Boolean) => updateMap
      case InsertAction(insertMap, _: Boolean) => insertMap
      case _ => null
    }
    if (colsMap != null) {
      val output = new Array[Expression](tableCols.length)
      val expectOutput = new Array[Expression](tableCols.length)
      colsMap.foreach { case (k, v) =>
        val tableIndex = tableCols.indexOf(k.toString().toLowerCase)
        if (tableIndex < 0) {
          throw new CarbonMergeDataSetException(s"Mapping is wrong $colsMap")
        }
        val resolvedValue = v.expr.transform {
          case a: Attribute if !a.resolved =>
            ds.queryExecution.analyzed.resolveQuoted(a.name,
              sparkSession.sessionState.analyzer.resolver).get
        }
        output(tableIndex) = resolvedValue
        val attributesInResolvedVal = resolvedValue.collect {
          case attribute: Attribute => attribute
        }
        if (attributesInResolvedVal.isEmpty) {
          val resolvedKey = k.expr.collect {
            case a: Attribute if !a.resolved =>
              targetTableAttributes.find(col => col.name
                .equalsIgnoreCase(a.name))
          }.head.get
          literalToAttributeMap += ((resolvedValue -> resolvedKey.asInstanceOf[Attribute]))
        }
        expectOutput(tableIndex) =
          existingDsOutput.find(_.name.equalsIgnoreCase(tableCols(tableIndex))).get
      }
      if (output.contains(null)) {
        throw new CarbonMergeDataSetException(s"Not all columns are mapped")
      }
      var exprToIndexMapping: scala.collection.mutable.Buffer[(Expression, Int)] =
        collection.mutable.Buffer.empty
      (ds.queryExecution.logical.output ++ targetTableAttributes).distinct.zipWithIndex.collect {
        case (attribute, index) =>
          output.map { exp =>
            val attributeInExpression = exp.collect {
              case attribute: Attribute => attribute
            }
            if (attributeInExpression.isEmpty) {
              if (literalToAttributeMap(exp).semanticEquals(attribute)) {
                exprToIndexMapping += ((exp, index))
              } else if (literalToAttributeMap(exp).name.equals(attribute.name)) {
                exprToIndexMapping += ((exp, index))
              }
            } else {
              if (attributeInExpression.nonEmpty &&
                  attributeInExpression.head.semanticEquals(attribute)) {
                exprToIndexMapping += ((exp, index))
              }
            }
          }
      }
      output zip output.map(exprToIndexMapping.toMap)
    } else {
      Seq.empty
    }
  }

  def getInternalRowFromIndex(row: InternalRow, status_on_mergeds: Int): InternalRow = {
    val rowValues = row.toSeq(schema)
    val requiredOutput = indexesToFetch.map { case (expr, index) =>
      if (expr.isInstanceOf[Attribute]) {
        rowValues(index)
      } else {
        CarbonToSparkAdapter.evaluateWithPredicate(expr, outputListOfDataset, row)
      }
    }
    InternalRow.fromSeq(requiredOutput ++ Seq(status_on_mergeds))
  }

}

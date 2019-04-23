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

package org.apache.carbondata.mv.datamap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{ColumnTableRelation, DataMapField, Field}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility class for keeping all the utility method for mv datamap
 */
object MVUtil {

  /**
   * Below method will be used to validate and get the required fields from select plan
   */
  def getFieldsAndDataMapFieldsFromPlan(plan: LogicalPlan,
      selectStmt: String,
      sparkSession: SparkSession): scala.collection.mutable.LinkedHashMap[Field, DataMapField] = {
    plan match {
      case Project(projectList, child: Sort) =>
        getFieldsFromProject(projectList, plan, child)
      case Project(projectList, _) =>
        getFieldsFromProject(projectList, plan)
      case Aggregate(groupByExp, aggExp, _) =>
        getFieldsFromAggregate(groupByExp, aggExp, plan)
    }
  }

  def getFieldsFromProject(projectList: Seq[NamedExpression],
      plan: LogicalPlan, sort: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    sort.transformDown {
      case agg@Aggregate(groupByExp, aggExp, _) =>
        fieldToDataMapFieldMap ++== getFieldsFromAggregate(groupByExp, aggExp, plan)
        agg
    }
    fieldToDataMapFieldMap ++== getFieldsFromProject(projectList, plan)
    fieldToDataMapFieldMap
  }

  def getFieldsFromProject(projectList: Seq[NamedExpression],
      plan: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    val logicalRelation =
      plan.collect {
        case lr: LogicalRelation =>
          lr
      }
    projectList.map {
      case attr: AttributeReference =>
        val carbonTable = getCarbonTable(logicalRelation, attr)
        if (null != carbonTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(attr.name,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
            carbonTable)
          if (null != relation) {
            arrayBuffer += relation
          }
          fieldToDataMapFieldMap +=
          getFieldToDataMapFields(attr.name,
            attr.dataType,
            attr.qualifier,
            "",
            arrayBuffer,
            carbonTable.getTableName)
        }
      case Alias(attr: AttributeReference, name) =>
        val carbonTable = getCarbonTable(logicalRelation, attr)
        if (null != carbonTable) {
          val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
          val relation = getColumnRelation(attr.name,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
            carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
            carbonTable)
          if (null != relation) {
            arrayBuffer += relation
          }
          fieldToDataMapFieldMap +=
          getFieldToDataMapFields(name, attr.dataType, attr.qualifier, "", arrayBuffer, "")
        }
    }
    fieldToDataMapFieldMap
  }

  def getFieldsFromAggregate(groupByExp: Seq[Expression],
      aggExp: Seq[NamedExpression],
      plan: LogicalPlan): mutable.LinkedHashMap[Field, DataMapField] = {
    var fieldToDataMapFieldMap = scala.collection.mutable.LinkedHashMap.empty[Field, DataMapField]
    val logicalRelation =
      plan.collect {
        case lr: LogicalRelation =>
          lr
      }
    aggExp.map { agg =>
      var aggregateType: String = ""
      val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new ArrayBuffer[ColumnTableRelation]()
      agg.collect {
        case Alias(attr: AggregateExpression, name) =>
          if (attr.aggregateFunction.isInstanceOf[Count]) {
            fieldToDataMapFieldMap +=
            getFieldToDataMapFields(name,
              attr.aggregateFunction.dataType,
              None,
              attr.aggregateFunction.nodeName,
              arrayBuffer,
              "")
          } else {
            aggregateType = attr.aggregateFunction.nodeName
          }
        case Alias(_, name) =>
          // In case of arithmetic expressions like sum(a)+sum(b)
          aggregateType = "arithmetic"
      }
      agg.collect {
        case attr: AttributeReference =>
          val carbonTable: CarbonTable = getCarbonTable(logicalRelation, attr)
          if (null != carbonTable) {
            val relation = getColumnRelation(attr.name,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
              carbonTable)
            if (null != relation) {
              arrayBuffer += relation
            }
            if (aggregateType.isEmpty && arrayBuffer.nonEmpty) {
              val tableName = carbonTable.getTableName
              fieldToDataMapFieldMap +=
              getFieldToDataMapFields(agg.name,
                agg.dataType,
                attr.qualifier,
                aggregateType,
                arrayBuffer,
                tableName)
            }
          }
      }
      if (!aggregateType.isEmpty && arrayBuffer.nonEmpty) {
        fieldToDataMapFieldMap +=
        getFieldToDataMapFields(agg.name,
          agg.dataType,
          agg.qualifier,
          aggregateType,
          arrayBuffer,
          "")
      }
    }
    groupByExp map { grp =>
      grp.collect {
        case attr: AttributeReference =>
          val carbonTable: CarbonTable = getCarbonTable(logicalRelation, attr)
          if (null != carbonTable) {
            val arrayBuffer: ArrayBuffer[ColumnTableRelation] = new
                ArrayBuffer[ColumnTableRelation]()
            arrayBuffer += getColumnRelation(attr.name,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
              carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
              carbonTable)
            fieldToDataMapFieldMap +=
            getFieldToDataMapFields(attr.name,
              attr.dataType,
              attr.qualifier,
              "",
              arrayBuffer,
              carbonTable.getTableName)
          }
      }
    }
    fieldToDataMapFieldMap
  }

  /**
   * Below method will be used to get the column relation with the parent column
   */
  def getColumnRelation(parentColumnName: String,
      parentTableId: String,
      parentTableName: String,
      parentDatabaseName: String,
      carbonTable: CarbonTable): ColumnTableRelation = {
    val parentColumn = carbonTable.getColumnByName(parentTableName, parentColumnName)
    var columnTableRelation: ColumnTableRelation = null
    if (null != parentColumn) {
      val parentColumnId = parentColumn.getColumnId
      columnTableRelation = ColumnTableRelation(parentColumnName = parentColumnName,
        parentColumnId = parentColumnId,
        parentTableName = parentTableName,
        parentDatabaseName = parentDatabaseName, parentTableId = parentTableId)
      columnTableRelation
    } else {
      columnTableRelation
    }
  }

  /**
   * This method is used to get carbon table for corresponding attribute reference
   * from logical relation
   */
  private def getCarbonTable(logicalRelation: Seq[LogicalRelation],
      attr: AttributeReference) = {
    val relations = logicalRelation
      .filter(lr => lr.output
        .exists(attrRef => attrRef.name.equalsIgnoreCase(attr.name) &&
                           attrRef.exprId.equals(attr.exprId)))
    if (relations.nonEmpty) {
      relations
        .head.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        .metaData.carbonTable
    } else {
      null
    }
  }

  /**
   * Below method will be used to get the fields object for mv table
   */
  private def getFieldToDataMapFields(name: String,
      dataType: DataType,
      qualifier: Option[String],
      aggregateType: String,
      columnTableRelationList: ArrayBuffer[ColumnTableRelation],
      parenTableName: String) = {
    var actualColumnName =
      name.replace("(", "_")
        .replace(")", "")
        .replace(" ", "_")
        .replace("=", "")
        .replace(",", "")
    if (qualifier.isDefined) {
      actualColumnName = qualifier.map(qualifier => qualifier + "_" + name)
        .getOrElse(actualColumnName)
    }
    if (qualifier.isEmpty) {
      if (aggregateType.isEmpty && !parenTableName.isEmpty) {
        actualColumnName = parenTableName + "_" + actualColumnName
      }
    }
    val rawSchema = '`' + actualColumnName + '`' + ' ' + dataType.typeName
    val dataMapField = DataMapField(aggregateType, Some(columnTableRelationList))
    if (dataType.typeName.startsWith("decimal")) {
      val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType.catalogString)
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        precision = precision,
        scale = scale,
        rawSchema = rawSchema), dataMapField)
    } else {
      (Field(column = actualColumnName,
        dataType = Some(dataType.typeName),
        name = Some(actualColumnName),
        children = None,
        rawSchema = rawSchema), dataMapField)
    }
  }
}

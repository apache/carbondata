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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation, InsertIntoCarbonTable, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Divide, Expression, NamedExpression, PredicateSubquery, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.{AggregationDataMapSchema, CarbonTable, DataMapSchema}
import org.apache.carbondata.core.preagg.{AggregateTableSelector, QueryColumn, QueryPlan}
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
 * Class for applying Pre Aggregate rules
 * Responsibility.
 * 1. Check plan is valid plan for updating the parent table plan with child table
 * 2. Updated the plan based on child schema
 *
 * Rules for Upadating the plan
 * 1. Grouping expression rules
 *    1.1 Change the parent attribute reference for of group expression
 * to child attribute reference
 *
 * 2. Aggregate expression rules
 *    2.1 Change the parent attribute reference for of group expression to
 * child attribute reference
 *    2.2 Change the count AggregateExpression to Sum as count
 * is already calculated so in case of aggregate table
 * we need to apply sum to get the count
 *    2.2 In case of average aggregate function select 2 columns from aggregate table with
 * aggregation
 * sum and count. Then add divide(sum(column with sum), sum(column with count)).
 * Note: During aggregate table creation for average table will be created with two columns
 * one for sum(column) and count(column) to support rollup
 *
 * 3. Filter Expression rules.
 *    3.1 Updated filter expression attributes with child table attributes
 * 4. Update the Parent Logical relation with child Logical relation
 *
 * @param sparkSession
 * spark session
 */
case class CarbonPreAggregateQueryRules(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    var needAnalysis = true
    plan.transformExpressions {
      // first check if any preAgg scala function is applied it is present is in plan
      // then call is from create preaggregate table class so no need to transform the query plan
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") =>
        needAnalysis = false
        al
      // in case of query if any unresolve alias is present then wait for plan to be resolved
      // return the same plan as we can tranform the plan only when everything is resolved
      case unresolveAlias@UnresolvedAlias(_, _) =>
        needAnalysis = false
        unresolveAlias
      case attr@UnresolvedAttribute(_) =>
        needAnalysis = false
        attr
    }
    // if plan is not valid for transformation then return same plan
    if (!needAnalysis) {
      plan
    } else {
      // create buffer to collect all the column and its metadata information
      val list = scala.collection.mutable.ListBuffer.empty[QueryColumn]
      var isValidPlan = true
      val carbonTable = plan match {
        // matching the plan based on supported plan
        // if plan is matches with any case it will validate and get all
        // information required for transforming the plan

        // When plan has grouping expression, aggregate expression
        // subquery
        case Aggregate(groupingExp,
        aggregateExp,
        SubqueryAlias(_, logicalRelation: LogicalRelation, _))
          // only carbon query plan is supported checking whether logical relation is
          // is for carbon
          if logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]   &&
             logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
               .hasDataMapSchema =>
          val (carbonTable, tableName) = getCarbonTableAndTableName(logicalRelation)
          // if it is valid plan then extract the query columns
          isValidPlan = extractQueryColumnsFromAggExpression(groupingExp,
            aggregateExp,
            carbonTable,
            tableName,
            list)
          carbonTable

        // below case for handling filter query
        // When plan has grouping expression, aggregate expression
        // filter expression
        case Aggregate(groupingExp, aggregateExp,
        Filter(filterExp,
        SubqueryAlias(_, logicalRelation: LogicalRelation, _)))
          // only carbon query plan is supported checking whether logical relation is
          // is for carbon
          if logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]   &&
             logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
               .hasDataMapSchema =>
          val (carbonTable, tableName) = getCarbonTableAndTableName(logicalRelation)
          // if it is valid plan then extract the query columns
          isValidPlan = extractQueryColumnsFromAggExpression(groupingExp,
            aggregateExp,
            carbonTable,
            tableName,
            list)
          // TODO need to handle filter predicate subquery scenario
          isValidPlan = !PredicateSubquery.hasPredicateSubquery(filterExp)
          // getting the columns from filter expression
          if(isValidPlan) {
            filterExp.transform {
              case attr: AttributeReference =>
                list += getQueryColumn(attr.name, carbonTable, tableName, isFilterColumn = true)
                attr
            }
          }
          carbonTable

        // When plan has grouping expression, aggregate expression
        // logical relation
        case Aggregate(groupingExp, aggregateExp, logicalRelation: LogicalRelation)
          // only carbon query plan is supported checking whether logical relation is
          // is for carbon
          if logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
             logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
               .hasDataMapSchema =>
          val (carbonTable, tableName) = getCarbonTableAndTableName(logicalRelation)
          // if it is valid plan then extract the query columns
          isValidPlan = extractQueryColumnsFromAggExpression(groupingExp,
            aggregateExp,
            carbonTable,
            tableName,
            list)
          carbonTable
        case _ =>
          isValidPlan = false
          null
      }
      // if plan is valid then update the plan with child attributes
      if (isValidPlan) {
        // getting all the projection columns
        val listProjectionColumn = list
          .filter(queryColumn => queryColumn.getAggFunction.isEmpty && !queryColumn.isFilterColumn)
        // getting all the filter columns
        val listFilterColumn = list
          .filter(queryColumn => queryColumn.getAggFunction.isEmpty && queryColumn.isFilterColumn)
        // getting all the aggregation columns
        val listAggregationColumn = list.filter(queryColumn => !queryColumn.getAggFunction.isEmpty)
        // create a query plan object which will be used to select the list of pre aggregate tables
        // matches with this plan
        val queryPlan = new QueryPlan(listProjectionColumn.asJava,
          listAggregationColumn.asJava,
          listFilterColumn.asJava)
        // create aggregate table selector object
        val aggregateTableSelector = new AggregateTableSelector(queryPlan, carbonTable)
        // select the list of valid child tables
        val selectedDataMapSchemas = aggregateTableSelector.selectPreAggDataMapSchema()
        // if it doesnot match with any pre aggregate table return the same plan
        if (!selectedDataMapSchemas.isEmpty) {
          // sort the selected child schema based on size to select smallest pre aggregate table
          val (aggDataMapSchema, carbonRelation) =
            selectedDataMapSchemas.asScala.map { selectedDataMapSchema =>
              val catalog = sparkSession.sessionState.catalog
              val carbonRelation = catalog
                .lookupRelation(TableIdentifier(selectedDataMapSchema.getRelationIdentifier
                  .getTableName,
                  Some(selectedDataMapSchema.getRelationIdentifier
                    .getDatabaseName))).asInstanceOf[SubqueryAlias].child
                .asInstanceOf[LogicalRelation]
              (selectedDataMapSchema, carbonRelation)
            }.minBy(f => f._2.relation.asInstanceOf[CarbonDatasourceHadoopRelation].sizeInBytes)
          // transform the query plan based on selected child schema
          transformPreAggQueryPlan(plan, aggDataMapSchema, carbonRelation)
        } else {
          plan
        }
      } else {
        plan
      }
    }
  }

  /**
   * Below method will be used to get the child attribute reference
   * based on parent name
   *
   * @param dataMapSchema
   * child schema
   * @param attributeReference
   * parent attribute reference
   * @param childCarbonRelation
   * child logical relation
   * @param aggFunction
   * aggregation function applied on child
   * @return child attribute reference
   */
  def getChildAttributeReference(dataMapSchema: DataMapSchema,
      attributeReference: AttributeReference,
      childCarbonRelation: LogicalRelation,
      aggFunction: String = ""): AttributeReference = {
    val aggregationDataMapSchema = dataMapSchema.asInstanceOf[AggregationDataMapSchema];
    val columnSchema = if (aggFunction.isEmpty) {
      aggregationDataMapSchema.getChildColByParentColName(attributeReference.name)
    } else {
      aggregationDataMapSchema.getAggChildColByParent(attributeReference.name, aggFunction)
    }
    // here column schema cannot be null, if it is null then aggregate table selection
    // logic has some problem
    if (null == columnSchema) {
      throw new AnalysisException("Column doesnot exists in Pre Aggregate table")
    }
    // finding the child attribute from child logical relation
    childCarbonRelation.attributeMap.find(p => p._2.name.equals(columnSchema.getColumnName)).get._2
  }

  /**
   * Below method will be used to transform the main table plan to child table plan
   * rules for transformming is as below.
   * 1. Grouping expression rules
   *    1.1 Change the parent attribute reference for of group expression
   * to child attribute reference
   *
   * 2. Aggregate expression rules
   *    2.1 Change the parent attribute reference for of group expression to
   * child attribute reference
   *    2.2 Change the count AggregateExpression to Sum as count
   * is already calculated so in case of aggregate table
   * we need to apply sum to get the count
   *    2.2 In case of average aggregate function select 2 columns from aggregate table with
   * aggregation sum and count. Then add divide(sum(column with sum), sum(column with count)).
   * Note: During aggregate table creation for average table will be created with two columns
   * one for sum(column) and count(column) to support rollup
   * 3. Filter Expression rules.
   *    3.1 Updated filter expression attributes with child table attributes
   * 4. Update the Parent Logical relation with child Logical relation
   *
   * @param logicalPlan
   * parent logical plan
   * @param aggDataMapSchema
   * select data map schema
   * @param childCarbonRelation
   * child carbon table relation
   * @return transformed plan
   */
  def transformPreAggQueryPlan(logicalPlan: LogicalPlan,
      aggDataMapSchema: DataMapSchema, childCarbonRelation: LogicalRelation): LogicalPlan = {
    logicalPlan.transform {
      case Aggregate(grExp, aggExp, child@SubqueryAlias(_, l: LogicalRelation, _))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.hasDataMapSchema =>
        val (updatedGroupExp, updatedAggExp, newChild, None) =
          getUpdatedExpressions(grExp,
            aggExp,
            child,
            None,
            aggDataMapSchema,
            childCarbonRelation)
        Aggregate(updatedGroupExp,
          updatedAggExp,
          newChild)
      case Aggregate(grExp,
      aggExp,
      Filter(expression, child@SubqueryAlias(_, l: LogicalRelation, _)))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.hasDataMapSchema =>
        val (updatedGroupExp, updatedAggExp, newChild, updatedFilterExpression) =
          getUpdatedExpressions(grExp,
            aggExp,
            child,
            Some(expression),
            aggDataMapSchema,
            childCarbonRelation)
        Aggregate(updatedGroupExp,
          updatedAggExp,
          Filter(updatedFilterExpression.get,
            newChild))
      case Aggregate(grExp, aggExp, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.hasDataMapSchema =>
        val (updatedGroupExp, updatedAggExp, newChild, None) =
          getUpdatedExpressions(grExp,
            aggExp,
            l,
            None,
            aggDataMapSchema,
            childCarbonRelation)
        Aggregate(updatedGroupExp,
          updatedAggExp,
          newChild)
    }
  }

  /**
   * Below method will be used to get the updated expression for pre aggregated table.
   * It will replace the attribute of actual plan with child table attributes.
   * Updation will be done for below expression.
   * 1. Grouping expression
   * 2. aggregate expression
   * 3. child logical plan
   * 4. filter expression if present
   *
   * @param groupingExpressions
   * actual plan grouping expression
   * @param aggregateExpressions
   * actual plan aggregate expression
   * @param child
   * child logical plan
   * @param filterExpression
   * filter expression
   * @param aggDataMapSchema
   * pre aggregate table schema
   * @param childCarbonRelation
   * pre aggregate table logical relation
   * @return tuple of(updated grouping expression,
   *         updated aggregate expression,
   *         updated child logical plan,
   *         updated filter expression if present in actual plan)
   */
  def getUpdatedExpressions(groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: LogicalPlan, filterExpression: Option[Expression] = None,
      aggDataMapSchema: DataMapSchema,
      childCarbonRelation: LogicalRelation): (Seq[Expression], Seq[NamedExpression], LogicalPlan,
    Option[Expression]) = {
    // transforming the group by expression attributes with child attributes
    val updatedGroupExp = groupingExpressions.map { exp =>
      exp.transform {
        case attr: AttributeReference =>
          getChildAttributeReference(aggDataMapSchema, attr, childCarbonRelation)
      }
    }
    // below code is for updating the aggregate expression.
    // Note: In case of aggregate expression updation we need to return alias as
    //       while showing the final result we need to show based on actual query
    //       for example: If query is "select name from table group by name"
    //       if we only update the attributes it will show child table column name in final output
    //       so for handling this if attributes does not have alias we need to return alias of
    // parent
    //       table column name
    // Rules for updating aggregate expression.
    // 1. If it matches with attribute reference return alias of child attribute reference
    // 2. If it matches with alias return same alias with child attribute reference
    // 3. If it matches with alias of any supported aggregate function return aggregate function
    // with child attribute reference. Please check class level documentation how when aggregate
    // function will be updated

    val updatedAggExp = aggregateExpressions.map {
      // case for attribute reference
      case attr: AttributeReference =>
        val childAttributeReference = getChildAttributeReference(aggDataMapSchema,
          attr,
          childCarbonRelation)
        // returning the alias to show proper column name in output
        Alias(childAttributeReference,
          attr.name)(NamedExpression.newExprId,
          childAttributeReference.qualifier).asInstanceOf[NamedExpression]
      // case for alias
      case Alias(attr: AttributeReference, name) =>
        val childAttributeReference = getChildAttributeReference(aggDataMapSchema,
          attr,
          childCarbonRelation)
        // returning alias with child attribute reference
        Alias(childAttributeReference,
          name)(NamedExpression.newExprId,
          childAttributeReference.qualifier).asInstanceOf[NamedExpression]
      // for aggregate function case
      case alias@Alias(attr: AggregateExpression, name) =>
        // get the updated aggregate aggregate function
        val aggExp = getUpdatedAggregateExpressionForChild(attr,
          aggDataMapSchema,
          childCarbonRelation)
        // returning alias with child attribute reference
        Alias(aggExp,
          name)(NamedExpression.newExprId,
          alias.qualifier).asInstanceOf[NamedExpression]
    }
    // transformaing the logical relation
    val newChild = child.transform {
      case _: LogicalRelation =>
        childCarbonRelation
      case _: SubqueryAlias =>
        childCarbonRelation
    }
    // updating the filter expression if present
    val updatedFilterExpression = if (filterExpression.isDefined) {
      val filterExp = filterExpression.get
      Some(filterExp.transform {
        case attr: AttributeReference =>
          getChildAttributeReference(aggDataMapSchema, attr, childCarbonRelation)
      })
    } else {
      None
    }
    (updatedGroupExp, updatedAggExp, newChild, updatedFilterExpression)
  }

  /**
   * Below method will be used to get the aggregate expression based on match
   * Aggregate expression updation rules
   * 1 Change the count AggregateExpression to Sum as count
   * is already calculated so in case of aggregate table
   * we need to apply sum to get the count
   * 2 In case of average aggregate function select 2 columns from aggregate table
   * with aggregation sum and count.
   * Then add divide(sum(column with sum), sum(column with count)).
   * Note: During aggregate table creation for average aggregation function
   * table will be created with two columns one for sum(column) and count(column)
   * to support rollup
   *
   * @param aggExp
   * aggregate expression
   * @param dataMapSchema
   * child data map schema
   * @param childCarbonRelation
   * child logical relation
   * @return updated expression
   */
  def getUpdatedAggregateExpressionForChild(aggExp: AggregateExpression,
      dataMapSchema: DataMapSchema,
      childCarbonRelation: LogicalRelation):
  Expression = {
    aggExp.aggregateFunction match {
      // Change the count AggregateExpression to Sum as count
      // is already calculated so in case of aggregate table
      // we need to apply sum to get the count
      case count@Count(Seq(attr: AttributeReference)) =>
        AggregateExpression(Sum(Cast(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          count.prettyName),
          LongType)),
          aggExp.mode,
          isDistinct = false)
      case sum@Sum(attr: AttributeReference) =>
        AggregateExpression(Sum(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          sum.prettyName)),
          aggExp.mode,
          isDistinct = false)
      case max@Max(attr: AttributeReference) =>
        AggregateExpression(Max(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          max.prettyName)),
          aggExp.mode,
          isDistinct = false)
      case min@Min(attr: AttributeReference) =>
        AggregateExpression(Min(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          min.prettyName)),
          aggExp.mode,
          isDistinct = false)
      case sum@Sum(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        AggregateExpression(Sum(Cast(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          sum.prettyName),
          changeDataType)),
          aggExp.mode,
          isDistinct = false)
      case min@Min(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        AggregateExpression(Min(Cast(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          min.prettyName),
          changeDataType)),
          aggExp.mode,
          isDistinct = false)
      case max@Max(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        AggregateExpression(Max(Cast(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          max.prettyName),
          changeDataType)),
          aggExp.mode,
          isDistinct = false)

      // In case of average aggregate function select 2 columns from aggregate table
      // with aggregation sum and count.
      // Then add divide(sum(column with sum), sum(column with count)).
      case Average(attr: AttributeReference) =>
        Divide(AggregateExpression(Sum(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          "sum")),
          aggExp.mode,
          isDistinct = false),
          AggregateExpression(Sum(Cast(getChildAttributeReference(dataMapSchema,
            attr,
            childCarbonRelation,
            "count"),
            LongType)),
            aggExp.mode,
            isDistinct = false))
      // In case of average aggregate function select 2 columns from aggregate table
      // with aggregation sum and count.
      // Then add divide(sum(column with sum), sum(column with count)).
      case Average(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        Divide(AggregateExpression(Sum(Cast(getChildAttributeReference(dataMapSchema,
          attr,
          childCarbonRelation,
          "sum"),
          changeDataType)),
          aggExp.mode,
          isDistinct = false),
          AggregateExpression(Sum(Cast(getChildAttributeReference(dataMapSchema,
            attr,
            childCarbonRelation,
            "count"),
            LongType)),
            aggExp.mode,
            isDistinct = false))
    }
  }

  /**
   * Method to get the carbon table and table name
   *
   * @param parentLogicalRelation
   * parent table relation
   * @return tuple of carbon table and table name
   */
  def getCarbonTableAndTableName(parentLogicalRelation: LogicalRelation): (CarbonTable, String) = {
    val carbonTable = parentLogicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      .carbonRelation
      .metaData.carbonTable
    val tableName = carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
      .getTableName
    (carbonTable, tableName)
  }

  /**
   * Below method will be used to get the query columns from plan
   *
   * @param groupByExpression
   * group by expression
   * @param aggregateExpressions
   * aggregate expression
   * @param carbonTable
   * parent carbon table
   * @param tableName
   * parent table name
   * @param list
   * list of attributes
   * @return plan is valid
   */
  def extractQueryColumnsFromAggExpression(groupByExpression: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      carbonTable: CarbonTable, tableName: String,
      list: scala.collection.mutable.ListBuffer[QueryColumn]): Boolean = {
    aggregateExpressions.map {
      case attr: AttributeReference =>
        list += getQueryColumn(attr.name,
          carbonTable,
          tableName);
      case Alias(attr: AttributeReference, _) =>
        list += getQueryColumn(attr.name,
          carbonTable,
          tableName);
      case Alias(attr: AggregateExpression, _) =>
        if (attr.isDistinct) {
          return false
        }
        val queryColumn = validateAggregateFunctionAndGetFields(carbonTable,
          attr.aggregateFunction,
          tableName)
        if (queryColumn.nonEmpty) {
          list ++= queryColumn
        } else {
          return false
        }
    }
    true
  }

  /**
   * Below method will be used to validate aggregate function and get the attribute information
   * which is applied on select query.
   * Currently sum, max, min, count, avg is supported
   * in case of any other aggregate function it will return empty sequence
   * In case of avg it will return two fields one for count
   * and other of sum of that column to support rollup
   *
   * @param carbonTable
   * parent table
   * @param aggFunctions
   * aggregation function
   * @param tableName
   * parent table name
   * @return list of fields
   */
  def validateAggregateFunctionAndGetFields(carbonTable: CarbonTable,
      aggFunctions: AggregateFunction,
      tableName: String
  ): Seq[QueryColumn] = {
    val changedDataType = true
    aggFunctions match {
      case sum@Sum(attr: AttributeReference) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          sum.prettyName))
      case sum@Sum(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          sum.prettyName,
          changeDataType.typeName,
          changedDataType))
      case count@Count(Seq(attr: AttributeReference)) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          count.prettyName))
      case min@Min(attr: AttributeReference) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          min.prettyName))
      case min@Min(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          min.prettyName,
          changeDataType.typeName,
          changedDataType))
      case max@Max(attr: AttributeReference) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          max.prettyName))
      case max@Max(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          max.prettyName,
          changeDataType.typeName,
          changedDataType))
      // in case of average need to return two columns
      // sum and count of the column to added during table creation to support rollup
      case Average(attr: AttributeReference) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          "sum"
        ), getQueryColumn(attr.name,
          carbonTable,
          tableName,
          "count"
        ))
      // in case of average need to return two columns
      // sum and count of the column to added during table creation to support rollup
      case Average(Cast(attr: AttributeReference, changeDataType: DataType)) =>
        Seq(getQueryColumn(attr.name,
          carbonTable,
          tableName,
          "sum",
          changeDataType.typeName,
          changedDataType), getQueryColumn(attr.name,
          carbonTable,
          tableName,
          "count",
          changeDataType.typeName,
          changedDataType))
      case _ =>
        Seq.empty
    }
  }

  /**
   * Below method will be used to get the query column object which
   * will have details of the column and its property
   *
   * @param columnName
   * parent column name
   * @param carbonTable
   * parent carbon table
   * @param tableName
   * parent table name
   * @param aggFunction
   * aggregate function applied
   * @param dataType
   * data type of the column
   * @param isChangedDataType
   * is cast is applied on column
   * @param isFilterColumn
   * is filter is applied on column
   * @return query column
   */
  def getQueryColumn(columnName: String,
      carbonTable: CarbonTable,
      tableName: String,
      aggFunction: String = "",
      dataType: String = "",
      isChangedDataType: Boolean = false,
      isFilterColumn: Boolean = false): QueryColumn = {
    val columnSchema = carbonTable.getColumnByName(tableName, columnName).getColumnSchema
    if (isChangedDataType) {
      new QueryColumn(columnSchema, columnSchema.getDataType.getName, aggFunction, isFilterColumn)
    } else {
      new QueryColumn(columnSchema,
        CarbonScalaUtil.convertSparkToCarbonSchemaDataType(dataType),
        aggFunction, isFilterColumn)
    }
  }
}

/**
 * Insert into carbon table from other source
 */
object CarbonPreInsertionCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p@InsertIntoTable(relation: LogicalRelation, _, child, _, _)
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        castChildOutput(p, relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation], child)
    }
  }

  def castChildOutput(p: InsertIntoTable,
      relation: CarbonDatasourceHadoopRelation,
      child: LogicalPlan)
  : LogicalPlan = {
    if (relation.carbonRelation.output.size > CarbonCommonConstants
      .DEFAULT_MAX_NUMBER_OF_COLUMNS) {
      sys
        .error("Maximum supported column by carbon is:" + CarbonCommonConstants
          .DEFAULT_MAX_NUMBER_OF_COLUMNS
        )
    }
    val isAggregateTable = !relation.carbonRelation.carbonTable.getTableInfo
      .getParentRelationIdentifiers.isEmpty
    // transform logical plan if the load is for aggregate table.
    val childPlan = if (isAggregateTable) {
      transformAggregatePlan(child)
    } else {
      child
    }
    if (childPlan.output.size >= relation.carbonRelation.output.size) {
      val newChildOutput = childPlan.output.zipWithIndex.map { columnWithIndex =>
        columnWithIndex._1 match {
          case attr: Alias =>
            Alias(attr.child, s"col${ columnWithIndex._2 }")(attr.exprId)
          case attr: Attribute =>
            Alias(attr, s"col${ columnWithIndex._2 }")(NamedExpression.newExprId)
          case attr => attr
        }
      }
      val newChild: LogicalPlan = if (newChildOutput == childPlan.output) {
        p.child
      } else {
        Project(newChildOutput, childPlan)
      }
      InsertIntoCarbonTable(relation, p.partition, newChild, p.overwrite, p.ifNotExists)
    } else {
      sys.error("Cannot insert into target table because column number are different")
    }
  }

  /**
   * Transform the logical plan with average(col1) aggregation type to sum(col1) and count(col1).
   *
   * @param logicalPlan
   * @return
   */
  private def transformAggregatePlan(logicalPlan: LogicalPlan): LogicalPlan = {
    logicalPlan transform {
      case aggregate@Aggregate(_, aExp, _) =>
        val newExpressions = aExp.flatMap {
          case alias@Alias(attrExpression: AggregateExpression, _) =>
            attrExpression.aggregateFunction match {
              case Average(attr: AttributeReference) =>
                Seq(Alias(attrExpression
                  .copy(aggregateFunction = Sum(attr),
                    resultId = NamedExpression.newExprId), attr.name + "_sum")(),
                  Alias(attrExpression
                    .copy(aggregateFunction = Count(attr),
                      resultId = NamedExpression.newExprId), attr.name + "_count")())
              case Average(cast@Cast(attr: AttributeReference, _)) =>
                Seq(Alias(attrExpression
                  .copy(aggregateFunction = Sum(cast),
                    resultId = NamedExpression.newExprId),
                  attr.name + "_sum")(),
                  Alias(attrExpression
                    .copy(aggregateFunction = Count(cast),
                      resultId = NamedExpression.newExprId), attr.name + "_count")())
              case _ => Seq(alias)
            }
          case namedExpr: NamedExpression => Seq(namedExpr)
        }
        aggregate.copy(aggregateExpressions = newExpressions.asInstanceOf[Seq[NamedExpression]])
      case plan: LogicalPlan => plan
    }
  }
}


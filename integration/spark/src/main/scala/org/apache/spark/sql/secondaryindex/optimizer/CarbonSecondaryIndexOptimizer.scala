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

package org.apache.spark.sql.secondaryindex.optimizer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonRelation}
import org.apache.spark.sql.secondaryindex.optimizer
import org.apache.spark.sql.secondaryindex.optimizer.NodeType.NodeType
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

class SIFilterPushDownOperation(nodeType: NodeType)

case class SIBinaryFilterPushDownOperation(nodeType: NodeType,
    leftOperation: SIFilterPushDownOperation,
    rightOperation: SIFilterPushDownOperation) extends SIFilterPushDownOperation(nodeType)

case class SIUnaryFilterPushDownOperation(tableName: String, filterCondition: Expression)
  extends SIFilterPushDownOperation(nodeType = null)

object NodeType extends Enumeration {
  type NodeType = Value
  val Or: optimizer.NodeType.Value = Value("or")
  val And: optimizer.NodeType.Value = Value("and")
}

/**
 * Carbon Optimizer to add dictionary decoder.
 */
class CarbonSecondaryIndexOptimizer(sparkSession: SparkSession) {

  /**
   * This method will identify whether provided filter column have any index table
   * if exist, it will transform the filter with join plan as follows
   *
   * eg., select * from  a where dt='20261201' and age='10' limit 10
   *
   * Assume 2 index tables
   * 1)index1 indexed on column(dt)
   * 2)index2 indexed on column(age)
   *
   * Plan will be transformed as follows
   *
   * with i1 as (select positionReference from index1 where dt='20261201'
   * group by positionReference),
   * with i2 as (select positionReference from index2 where age='10'),
   * with indexJion as (select positionReference from i1 join i2 on
   * i1.positionReference = i2.positionReference limit 10),
   * with index as (select positionReference from indexJoin group by positionReference)
   * select * from a join index on a.positionId = index.positionReference limit 10
   *
   * @return transformed logical plan
   */
  private def rewritePlanForSecondaryIndex(filter: Filter,
      indexableRelation: CarbonDatasourceHadoopRelation, dbName: String,
      cols: Seq[NamedExpression] = null, limitLiteral: Literal = null): LogicalPlan = {
    var originalFilterAttributes: Set[String] = Set.empty
    var filterAttributes: Set[String] = Set.empty
    var matchingIndexTables: Seq[String] = Seq.empty
    // all filter attributes are retrived
    filter.condition collect {
      case attr: AttributeReference =>
        originalFilterAttributes = originalFilterAttributes. +(attr.name.toLowerCase)
    }
    // Removed is Not Null filter from all filters and other attributes are selected
    // isNotNull filter will return all the unique values except null from table,
    // For High Cardinality columns, this filter is of no use, hence skipping it.
    removeIsNotNullAttribute(filter.condition) collect {
      case attr: AttributeReference =>
        filterAttributes = filterAttributes. +(attr.name.toLowerCase)
    }

    matchingIndexTables = CarbonCostBasedOptimizer.identifyRequiredTables(
      filterAttributes.asJava,
      CarbonInternalScalaUtil.getIndexes(indexableRelation).mapValues(_.toList.asJava).asJava)
      .asScala

    // filter out all the index tables which are disabled
    val enabledMatchingIndexTables = matchingIndexTables
      .filter(table => sparkSession.sessionState.catalog
        .getTableMetadata(TableIdentifier(table, Some(dbName))).storage.properties
        .getOrElse("isSITableEnabled", "true").equalsIgnoreCase("true"))

    if (enabledMatchingIndexTables.isEmpty) {
      filter
    } else {
      var isPositionIDPresent = false
      val project: Seq[NamedExpression] = if (cols != null) {
        cols.foreach {
          case a@Alias(s: ScalaUDF, name)
            if name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID) =>
            isPositionIDPresent = true
          case _ =>
        }
        cols
      } else {
        filter.output
      }
      var mainTableDf = createDF(sparkSession, Project(project, filter))

      // If the createDF creates DF from MV flow by hitting MV datamap, no need to consider this DF
      // as main table DF and then do join with SI table and go ahead, So here checking whether the
      // DF is from child table, if it is, just return the filter as it is without rewriting
      val tableRelation = mainTableDf.logicalPlan collect {
        case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
          l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      }
      if (tableRelation.nonEmpty && tableRelation.head.carbonTable.isChildTableForMV) {
        return filter
      }

      if (!isPositionIDPresent) {
        mainTableDf = mainTableDf.selectExpr("getPositionId() as positionId", "*")
      } else {
        // if the user has requested for the positionID column, in that case we are
        // adding this table property. This is used later to check whether to
        // remove the positionId column from the projection list.
        // This property will be reflected across sessions as it is directly added to tblproperties.
        // So concurrent query run with getPositionID() UDF will have issue.
        // But getPositionID() UDF is restricted to testing purpose.
        indexableRelation.carbonTable.getTableInfo.getFactTable.getTableProperties
          .put("isPositionIDRequested", "true")
      }

      // map for index table name to its column name mapping
      val indexTableToColumnsMapping: mutable.Map[String, Set[String]] =
        new mutable.HashMap[String, Set[String]]()
      // map for index table to logical relation mapping
      val indexTableToLogicalRelationMapping: mutable.Map[String, LogicalPlan] =
        new mutable.HashMap[String, LogicalPlan]()
      // map for index table to attributeMap mapping. AttributeMap is a mapping of columnName
      // to its attribute reference
      val indexTableAttributeMap: mutable.Map[String, Map[String, AttributeReference]] =
        new mutable.HashMap[String, Map[String, AttributeReference]]()
      // mapping of all the index tables and its columns created on the main table
      val allIndexTableToColumnMapping = CarbonInternalScalaUtil.getIndexes(indexableRelation)

      enabledMatchingIndexTables.foreach { matchedTable =>
        // create index table to index column mapping
        val indexTableColumns = allIndexTableToColumnMapping.getOrElse(matchedTable, Array())
        indexTableToColumnsMapping.put(matchedTable, indexTableColumns.toSet)

        // create index table to logical plan mapping
        val indexTableLogicalPlan = retrievePlan(sparkSession.sessionState.catalog
            .lookupRelation(TableIdentifier(matchedTable, Some(dbName))))(sparkSession)
        indexTableToLogicalRelationMapping.put(matchedTable, indexTableLogicalPlan)

        // collect index table columns
        indexTableLogicalPlan collect {
          case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
            indexTableAttributeMap
              .put(matchedTable, l.output.map { attr => (attr.name.toLowerCase -> attr) }.toMap)
        }
      }

      val filterTree: SIFilterPushDownOperation = null
      val newSIFilterTree = createIndexTableFilterCondition(
          filterTree,
          filter.copy(filter.condition, filter.child).condition,
          indexTableToColumnsMapping)
      val indexTablesDF: DataFrame = newSIFilterTree._3 match {
        case Some(tableName) =>
          // flag to check whether apply limit literal on the filter push down condition or not
          // if it satisfies the limit push down scenario. it will be true only if the complete
          // tree has only one node which is of unary type
          val checkAndApplyLimitLiteral = newSIFilterTree._1 match {
            case SIUnaryFilterPushDownOperation(tableName, filterCondition) => true
            case _ => false
          }
          val dataFrameWithAttributes = createIndexFilterDataFrame(
            newSIFilterTree._1,
            indexTableAttributeMap,
            Set.empty,
            indexTableToLogicalRelationMapping,
            originalFilterAttributes,
            limitLiteral,
            checkAndApplyLimitLiteral)
          dataFrameWithAttributes._1
        case _ =>
          null
        // don't do anything
      }
      // In case column1 > column2, index table have only 1 column1,
      // then index join should not happen if only 1 index table is selected.
      if (indexTablesDF != null) {
        mainTableDf = mainTableDf.join(indexTablesDF,
          mainTableDf(CarbonCommonConstants.POSITION_ID) ===
          indexTablesDF(CarbonCommonConstants.POSITION_REFERENCE))
        mainTableDf.queryExecution.analyzed
      } else {
        filter
      }
    }
  }

  def retrievePlan(plan: LogicalPlan)(sparkSession: SparkSession):
  LogicalRelation = {
    plan match {
      case SubqueryAlias(alias, l: UnresolvedCatalogRelation) =>
        val logicalPlan = new FindDataSourceTable(sparkSession).apply(l).collect {
          case lr: LogicalRelation => lr
        }
        if (logicalPlan.head.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          logicalPlan.head
        } else {
          null
        }
      case SubqueryAlias(alias, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case _ => null
    }
  }

  private def createDF(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    new Dataset[Row](sparkSession, logicalPlan, RowEncoder(logicalPlan.schema))
  }

  /**
   * This method will traverse the filter push down tree and prepare the data frame based on the
   * NodeType. if nodeType is union then it will perform union on 2 tables else it will
   * perform join on 2 tables
   *
   */
  private def createIndexFilterDataFrame(
      siFilterPushDownTree: SIFilterPushDownOperation,
      indexTableAttributeMap: mutable.Map[String, Map[String, AttributeReference]],
      indexJoinedFilterAttributes: Set[String],
      indexTableToLogicalRelationMapping: mutable.Map[String, LogicalPlan],
      originalFilterAttributes: Set[String],
      limitLiteral: Literal,
      checkAndAddLimitLiteral: Boolean = false): (DataFrame, Set[String]) = {
    siFilterPushDownTree match {
      case SIUnaryFilterPushDownOperation(tableName, filterCondition) =>
        val attributeMap = indexTableAttributeMap.get(tableName).get
        var filterAttributes = indexJoinedFilterAttributes
        val indexTableFilter = filterCondition transformDown {
          case attr: AttributeReference =>
            val attrNew = attributeMap.get(attr.name.toLowerCase()).get
            filterAttributes += attr.name.toLowerCase
            attrNew
        }
        val positionReference =
          Seq(attributeMap(CarbonCommonConstants.POSITION_REFERENCE.toLowerCase()))
        // Add Filter on logicalRelation
        var planTransform: LogicalPlan = Filter(indexTableFilter,
          indexTableToLogicalRelationMapping(tableName))
        // Add PositionReference Projection on Filter
        planTransform = Project(positionReference, planTransform)
        var indexTableDf = createDF(sparkSession, planTransform)
        // When all the filter columns are joined from index table,
        // limit can be pushed down before grouping last index table as the
        // number of records selected will definitely return at least 1 record
        // NOTE: flag checkAndAddLimitLiteral will be true only when the complete filter tree
        // contains only one node which is a unary node
        val indexLogicalPlan = if (checkAndAddLimitLiteral) {
          if (limitLiteral != null &&
              filterAttributes.intersect(originalFilterAttributes)
                .size == originalFilterAttributes.size) {
            Limit(limitLiteral, indexTableDf.logicalPlan)
          } else {
            indexTableDf.logicalPlan
          }
        } else {
          indexTableDf.logicalPlan
        }
        // Add Group By on PositionReference after join
        indexTableDf = createDF(sparkSession,
          Aggregate(positionReference, positionReference, indexLogicalPlan))
        // return the data frame
        (indexTableDf, filterAttributes)
      case SIBinaryFilterPushDownOperation(nodeType, leftOperation, rightOperation) =>
        val (leftOperationDataFrame, indexFilterAttributesLeft) = createIndexFilterDataFrame(
          leftOperation,
          indexTableAttributeMap,
          indexJoinedFilterAttributes,
          indexTableToLogicalRelationMapping,
          originalFilterAttributes,
          limitLiteral)
        val (rightOperationDataFrame, indexFilterAttributesRight) = createIndexFilterDataFrame(
          rightOperation,
          indexTableAttributeMap,
          indexFilterAttributesLeft,
          indexTableToLogicalRelationMapping,
          originalFilterAttributes,
          limitLiteral)

        // create new data frame by applying join or union based on nodeType
        val newDFAfterUnionOrJoin = applyUnionOrJoinOnDataFrames(nodeType,
          leftOperationDataFrame,
          rightOperationDataFrame,
          indexFilterAttributesRight,
          originalFilterAttributes,
          limitLiteral)
        (newDFAfterUnionOrJoin, indexFilterAttributesRight)
    }
  }

  /**
   * This method will combine 2 dataframes by applying union or join based on the nodeType and
   * create a new dataframe
   *
   */
  private def applyUnionOrJoinOnDataFrames(nodeType: NodeType,
      leftConditionDataFrame: DataFrame,
      rightConditionDataFrame: DataFrame,
      indexJoinedFilterAttributes: Set[String],
      originalFilterAttributes: Set[String],
      limitLiteral: Literal): DataFrame = {
    // For multiple index table selection,
    // all index tables are joined before joining with main table
    var allIndexTablesDF = nodeType match {
      case NodeType.Or =>
        rightConditionDataFrame.union(leftConditionDataFrame)
      case _ =>
        leftConditionDataFrame.join(rightConditionDataFrame,
          leftConditionDataFrame(CarbonCommonConstants.POSITION_REFERENCE) ===
          rightConditionDataFrame(CarbonCommonConstants.POSITION_REFERENCE))
    }
    // When all the filter columns are joined from index table,
    // limit can be pushed down before grouping last index table as the
    // number of records selected will definitely return at least 1 record
    val indexLogicalPlan = if (limitLiteral != null &&
                               indexJoinedFilterAttributes.intersect(originalFilterAttributes)
                                 .size == originalFilterAttributes.size) {
      Limit(limitLiteral, allIndexTablesDF.logicalPlan)
    } else {
      allIndexTablesDF.logicalPlan
    }
    // in case of same table join position reference taken from the table relation will always
    // return the same positionReference which can result in Node Binding exception.
    // To avoid this take the positionReference from logical plan of dataFrame in which right
    // column will always be the projection column in join condition
    var positionReferenceFromLogicalPlan: Seq[AttributeReference] = Seq.empty
    indexLogicalPlan transform {
      case join: Join =>
        // this check is required as we need the right condition positionReference only for
        // the topmost node
        if (positionReferenceFromLogicalPlan.isEmpty) {
          // take the right attribute reference as new data frame positionReference is always
          // put in the right above
          positionReferenceFromLogicalPlan =
            Seq(join.condition.get.asInstanceOf[EqualTo].right
              .asInstanceOf[AttributeReference])
        }
        join
      case project: Project =>
        if (positionReferenceFromLogicalPlan.isEmpty) {
          positionReferenceFromLogicalPlan =
            Seq(project.projectList.head.asInstanceOf[AttributeReference])
        }
        project
    }
    // Add Group By on PositionReference after join
    allIndexTablesDF = createDF(sparkSession,
      Aggregate(positionReferenceFromLogicalPlan,
        positionReferenceFromLogicalPlan,
        indexLogicalPlan))
    // return the data frame
    allIndexTablesDF
  }

  private def removeIsNotNullAttribute(condition: Expression): Expression = {
    val isPartialStringEnabled = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
        CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT)
      .equalsIgnoreCase("true")
    condition transform {
      case IsNotNull(child: AttributeReference) => Literal(true)
      // Like is possible only if user provides _ in between the string
      // _ in like means any single character wild card check.
      case plan if (CarbonHiveMetadataUtil.checkNIUDF(plan)) => Literal(true)
      case Like(left: AttributeReference, right: Literal) if (!isPartialStringEnabled) => Literal(
        true)
      case EndsWith(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
      case Contains(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
    }
  }

  private def conditionsHasStartWith(condition: Expression): Boolean = {
    condition match {
      case or@Or(left, right) =>
        val isIndexColumnUsedInLeft = conditionsHasStartWith(left)
        val isIndexColumnUsedInRight = conditionsHasStartWith(right)

        isIndexColumnUsedInLeft || isIndexColumnUsedInRight

      case and@And(left, right) =>
        val isIndexColumnUsedInLeft = conditionsHasStartWith(left)
        val isIndexColumnUsedInRight = conditionsHasStartWith(right)

        isIndexColumnUsedInLeft || isIndexColumnUsedInRight

      case _ => hasStartsWith(condition)
    }
  }

  private def hasStartsWith(condition: Expression): Boolean = {
    condition match {
      case Like(left: AttributeReference, right: Literal) => false
      case EndsWith(left: AttributeReference, right: Literal) => false
      case Contains(left: AttributeReference, right: Literal) => false
      case _ => true
    }
  }

  /**
   * This method will check whether the condition is valid for SI pushdown. If yes then return the
   * tableName which contains this condition
   *
   * @param condition
   * @param indexTableColumnsToTableMapping
   * @param pushDownRequired
   * @return
   */
  private def isConditionColumnInIndexTable(condition: Expression,
      indexTableColumnsToTableMapping: mutable.Map[String, Set[String]],
      pushDownRequired: Boolean): Option[String] = {
    // In case of Like Filter in OR, both the conditions should not be transformed
    // Incase of like filter in And, only like filter should be removed and
    // other filter should be transformed with index table

    // Incase NI condition with and, eg., NI(col1 = 'a') && col1 = 'b',
    // only col1 = 'b' should be pushed to index table.
    // Incase NI condition with or, eg., NI(col1 = 'a') || col1 = 'b',
    // both the condition should not be pushed to index table.

    var tableName: Option[String] = None
    val doNotPushToSI = condition match {
      case Not(EqualTo(left: AttributeReference, right: Literal)) => true
      case Not(Like(left: AttributeReference, right: Literal)) => true
      case Not(In(left: AttributeReference, right: Seq[Expression])) => true
      case IsNotNull(child: AttributeReference) => true
      case Like(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case EndsWith(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case Contains(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case plan if (CarbonHiveMetadataUtil.checkNIUDF(plan)) => true
      case _ => false
    }
    if (!doNotPushToSI) {
      val attributes = condition collect {
        case attributeRef: AttributeReference => attributeRef
      }
      var isColumnExistsInSITable = false
      breakable {
        indexTableColumnsToTableMapping.foreach { tableAndIndexColumn =>
          isColumnExistsInSITable = attributes
            .forall { attributeRef => tableAndIndexColumn._2
              .contains(attributeRef.name.toLowerCase)
            }
          if (isColumnExistsInSITable) {
            tableName = Some(tableAndIndexColumn._1)
            break
          }
        }
      }
    }
    tableName
  }

  /**
   * This method will evaluate the filter tree and return new filter tree with SI push down
   * operation and flag
   * 1) In case of or condition, all the columns in left & right are existing in the index tables,
   * then the condition will be pushed to index tables as union and joined with main table
   * 2) In case of and condition, if any of the left or right condition column matches with
   * index table column, then that particular condition will be pushed to index table.
   *
   * @param filterTree
   * @param condition
   * @param indexTableToColumnsMapping
   * @return newSIFilterCondition can be pushed to index table & boolean to join with index table
   */
  private def createIndexTableFilterCondition(filterTree: SIFilterPushDownOperation,
      condition: Expression,
      indexTableToColumnsMapping: mutable.Map[String, Set[String]]):
  (SIFilterPushDownOperation, Expression, Option[String]) = {
    condition match {
      case or@Or(left, right) =>
        val (newSIFilterTreeLeft, newLeft, tableNameLeft) =
          createIndexTableFilterCondition(
            filterTree,
            left,
            indexTableToColumnsMapping)
        val (newSIFilterTreeRight, newRight, tableNameRight) =
          createIndexTableFilterCondition(
            filterTree,
            right,
            indexTableToColumnsMapping)

        (tableNameLeft, tableNameRight) match {
          case (Some(tableLeft), Some(tableRight)) =>
            // In case of OR filter when both right and left filter exists in the
            // index table (same or different), then only push down the condition to index tables
            // e.g name='xyz' or city='c1', then if both name and city have SI tables created on
            // them, then push down the condition to SI tables.
            // 1. If both the columns are from same index table then then the condition can
            // directly be joined to main table
            // 2. If both the columns are from different index table then first union operation
            // need to be performed between the 2 index tables and then joined with main table
            val newFilterCondition = or.copy(newLeft, newRight)
            // Points to be noted for passing the table name to next level: applicable for both
            // AND and OR filter case
            // 1. If both left and right condition are from same table then Unary node is created.
            // When it is an Unary node both left and right table name will be same so does not
            // matter which table name you are passing to next level.
            // 2. If left and right condition are from different table then binary node is
            // created. In case of binary node table name is not used for comparison. So it does
            // not matter which table name you pass to next level.
            (createSIFilterPushDownNode(
              newSIFilterCondition = newFilterCondition,
              leftOperation = newSIFilterTreeLeft,
              leftNodeTableName = tableLeft,
              rightOperation = newSIFilterTreeRight,
              rightNodeTableName = tableRight,
              nodeType = NodeType.Or), newFilterCondition, tableNameRight)
          case _ =>
            (filterTree, condition, None)
        }
      case and@And(left, right) =>
        val (newSIFilterTreeLeft, newLeft, tableNameLeft) =
          createIndexTableFilterCondition(
            filterTree,
            left,
            indexTableToColumnsMapping)
        val (newSIFilterTreeRight, newRight, tableNameRight) =
          createIndexTableFilterCondition(
            filterTree,
            right,
            indexTableToColumnsMapping)
        (tableNameLeft, tableNameRight) match {
          case (Some(tableLeft), Some(tableRight)) =>
            // push down both left and right condition if both left and right columns have index
            // table created on them
            val newFilterCondition = and.copy(newLeft, newRight)
            (createSIFilterPushDownNode(
              newSIFilterCondition = newFilterCondition,
              leftOperation = newSIFilterTreeLeft,
              leftNodeTableName = tableLeft,
              rightOperation = newSIFilterTreeRight,
              rightNodeTableName = tableRight,
              nodeType = NodeType.And), newFilterCondition, tableNameRight)
          case (Some(tableLeft), None) =>
            // return the left node
            (newSIFilterTreeLeft, newLeft, tableNameLeft)
          case (None, Some(tableRight)) =>
            // return the right node
            (newSIFilterTreeRight, newRight, tableNameRight)
          case _ =>
            (filterTree, condition, None)
        }
      case _ =>
        // check whether the filter column exists in SI table and can it be pushDown
        var isPartialStringEnabled = CarbonProperties.getInstance
          .getProperty(CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING,
            CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT)
          .equalsIgnoreCase("true")
        // When carbon.si.lookup.partialstring set to FALSE, if filter has startsWith then SI is
        // used even though combination of other filters like endsWith or Contains
        if (!isPartialStringEnabled) {
          isPartialStringEnabled = conditionsHasStartWith(condition)
        }
        val tableName = isConditionColumnInIndexTable(condition,
          indexTableToColumnsMapping,
          isPartialStringEnabled)
        // create a node if condition can be pushed down else return the same filterTree
        val newFilterTree = tableName match {
          case Some(table) =>
            SIUnaryFilterPushDownOperation(table, condition)
          case None =>
            filterTree
        }
        (newFilterTree, condition, tableName)
    }
  }

  /**
   * This method will create a new node for the filter push down tree.
   * a. If both left and right condition are from same index table then merge both the conditions
   * and create a unary operation root node
   * b. If left and right condition are from different table then create a binary node with
   * separate left and right operation
   *
   * @param newSIFilterCondition
   * @param leftOperation
   * @param leftNodeTableName
   * @param rightOperation
   * @param rightNodeTableName
   * @param nodeType
   * @return
   */
  private def createSIFilterPushDownNode(
      newSIFilterCondition: Expression,
      leftOperation: SIFilterPushDownOperation,
      leftNodeTableName: String,
      rightOperation: SIFilterPushDownOperation,
      rightNodeTableName: String,
      nodeType: NodeType): SIFilterPushDownOperation = {
    // flag to check whether there exist a binary node in left or right operation
    var isLeftOrRightOperationBinaryNode = false
    leftOperation match {
      case SIBinaryFilterPushDownOperation(nodeType, left, right) =>
        isLeftOrRightOperationBinaryNode = true
      case _ =>
      // don't do anything as flag for checking binary is already false
    }
    // if flag is till false at this point then only check for binary node in right operation
    if (!isLeftOrRightOperationBinaryNode) {
      rightOperation match {
        case SIBinaryFilterPushDownOperation(nodeType, left, right) =>
          isLeftOrRightOperationBinaryNode = true
        case _ =>
        // don't do anything as flag for checking binary is already false
      }
    }
    // if left or right node is binary then unary node cannot be created even though left and right
    // table names are same. In this case only a new binary node can be created
    if (isLeftOrRightOperationBinaryNode) {
      SIBinaryFilterPushDownOperation(nodeType, leftOperation, rightOperation)
    } else {
      // If left and right table name is same then merge the 2 conditions
      if (leftNodeTableName == rightNodeTableName) {
        SIUnaryFilterPushDownOperation(leftNodeTableName, newSIFilterCondition)
      } else {
        SIBinaryFilterPushDownOperation(nodeType, leftOperation, rightOperation)
      }
    }
  }

  /**
   * This method is used to determn whether limit has to be pushed down to secondary index or not.
   *
   * @param relation
   * @return false if carbon table is not an index table and update status file exists because
   *         we know delete has happened on table and there is no need to push down the filter.
   *         Otherwise true
   */
  private def isLimitPushDownRequired(relation: CarbonRelation): Boolean = {
    val carbonTable = relation.carbonTable
    lazy val updateStatusFileExists = FileFactory.getCarbonFile(carbonTable.getMetadataPath)
      .listFiles()
      .exists(file => file.getName.startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME))
    (!carbonTable.isIndexTable && !updateStatusFileExists)
  }

  def transformFilterToJoin(plan: LogicalPlan, needProjection: Boolean): LogicalPlan = {
    val isRowDeletedInTableMap = scala.collection.mutable.Map.empty[String, Boolean]
    // if the join pushdown is enabled, then no need to add projection list to the logical plan as
    // we can directly map the join output with the required projections
    // if it is false then the join will not be pushed down to carbon and
    // there it is required to add projection list to map the output from the join
    val pushDownJoinEnabled = sparkSession.sparkContext.getConf
      .getBoolean("spark.carbon.pushdown.join.as.filter", defaultValue = true)
    val transformChild = false
    var addProjection = needProjection
    val transformedPlan = transformPlan(plan, {
      case union@Union(children) =>
        // In case of Union, Extra Project has to be added to the Plan. Because if left table is
        // pushed to SI and right table is not pushed, then Output Attribute mismatch will happen
        addProjection = true
        (union, true)
      case sort@Sort(order, global, plan) =>
        addProjection = true
        (sort, true)
      case filter@Filter(condition, logicalRelation@MatchIndexableRelation(indexableRelation))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty =>
        val reWrittenPlan = rewritePlanForSecondaryIndex(filter, indexableRelation,
          filter.child.asInstanceOf[LogicalRelation].relation
            .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.databaseName)
        if (reWrittenPlan.isInstanceOf[Join]) {
          if (pushDownJoinEnabled && !addProjection) {
            (reWrittenPlan, transformChild)
          } else {
            (Project(filter.output, reWrittenPlan), transformChild)
          }
        } else {
          (filter, transformChild)
        }
      case projection@Project(cols, filter@Filter(condition,
      logicalRelation@MatchIndexableRelation(indexableRelation)))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty =>
        val reWrittenPlan = rewritePlanForSecondaryIndex(filter, indexableRelation,
          filter.child.asInstanceOf[LogicalRelation].relation
            .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.databaseName, cols)
        // If Index table is matched, join plan will be returned.
        // Adding projection over join to return only selected columns from query.
        // Else all columns from left & right table will be returned in output columns
        if (reWrittenPlan.isInstanceOf[Join]) {
          if (pushDownJoinEnabled && !addProjection) {
            (reWrittenPlan, transformChild)
          } else {
            (Project(projection.output, reWrittenPlan), transformChild)
          }
        } else {
          (projection, transformChild)
        }
      // When limit is provided in query, this limit literal can be pushed down to index table
      // if all the filter columns have index table, then limit can be pushed down before grouping
      // last index table, as number of records returned after join where unique and it will
      // definitely return atleast 1 record.
      case limit@Limit(literal: Literal,
      filter@Filter(condition, logicalRelation@MatchIndexableRelation(indexableRelation)))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty =>
        val carbonRelation = filter.child.asInstanceOf[LogicalRelation].relation
          .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        val uniqueTableName = s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }"
        if (!isRowDeletedInTableMap
          .contains(s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }")) {
          isRowDeletedInTableMap.put(uniqueTableName, isLimitPushDownRequired(carbonRelation))
        }
        val reWrittenPlan = if (isRowDeletedInTableMap(uniqueTableName)) {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, limitLiteral = literal)
        } else {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName)
        }
        if (reWrittenPlan.isInstanceOf[Join]) {
          if (pushDownJoinEnabled && !addProjection) {
            (Limit(literal, reWrittenPlan), transformChild)
          } else {
            (Limit(literal, Project(limit.output, reWrittenPlan)), transformChild)
          }
        } else {
          (limit, transformChild)
        }
      case limit@Limit(literal: Literal, projection@Project(cols, filter@Filter(condition,
      logicalRelation@MatchIndexableRelation(indexableRelation))))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty =>
        val carbonRelation = filter.child.asInstanceOf[LogicalRelation].relation
          .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        val uniqueTableName = s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }"
        if (!isRowDeletedInTableMap
          .contains(s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }")) {
          isRowDeletedInTableMap.put(uniqueTableName, isLimitPushDownRequired(carbonRelation))
        }
        val reWrittenPlan = if (isRowDeletedInTableMap(uniqueTableName)) {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, cols, limitLiteral = literal)
        } else {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, cols)
        }
        if (reWrittenPlan.isInstanceOf[Join]) {
          if (pushDownJoinEnabled && !addProjection) {
            (Limit(literal, reWrittenPlan), transformChild)
          } else {
            (Limit(literal, Project(projection.output, reWrittenPlan)), transformChild)
          }
        } else {
          (limit, transformChild)
        }
    })
    val transformedPlanWithoutNIUdf = transformedPlan.transform {
      case filter: Filter =>
        Filter(CarbonHiveMetadataUtil.transformToRemoveNI(filter.condition), filter.child)
    }
    transformedPlanWithoutNIUdf
  }

  /**
   * Returns a copy of this node where `rule` has been applied to the tree and all of
   * its children (pre-order). When `rule` does not apply to a given node it is left unchanged. If
   * rule is already applied to the node, then boolean value 'transformChild' decides whether to
   * apply rule to its children nodes or not.
   *
   * @param plan
   * @param rule the function used to transform this nodes children. Boolean value
   *             decides if need to traverse children nodes or not
   */
  def transformPlan(plan: LogicalPlan,
      rule: PartialFunction[LogicalPlan, (LogicalPlan, Boolean)]): LogicalPlan = {
    val func: LogicalPlan => (LogicalPlan, Boolean) = {
      a => (a, true)
    }
    val (afterRule, transformChild) = CurrentOrigin.withOrigin(CurrentOrigin.get) {
      rule.applyOrElse(plan, func)
    }
    if (plan fastEquals afterRule) {
      plan.mapChildren(transformPlan(_, rule))
    } else {
      // If node is not changed, then traverse the children nodes to transform the plan. Else
      // return the changed plan
      if (transformChild) {
        afterRule.mapChildren(transformPlan(_, rule))
      } else {
        afterRule
      }
    }
  }

}

object MatchIndexableRelation {

  type ReturnType = (CarbonDatasourceHadoopRelation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case l: LogicalRelation if (l.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        Some(l.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
      case _ => None
    }
  }
}

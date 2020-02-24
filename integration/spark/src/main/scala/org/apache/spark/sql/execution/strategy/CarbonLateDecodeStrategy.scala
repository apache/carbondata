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

package org.apache.spark.sql.execution.strategy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.CarbonExpressions.{MatchCast => Cast}
import org.apache.spark.sql.carbondata.execution.datasources.{CarbonFileIndex, CarbonSparkDataSourceUtil}
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, _}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.hive.MatchLogicalRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.schema.BucketingInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.{TextMatch, TextMatchLimit, TextMatchMaxDocUDF, TextMatchUDF}
import org.apache.carbondata.geo.{InPolygon, InPolygonUDF}
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * Carbon specific optimization
 * 1. filter push down
 * 2. count star
 */
private[sql] class CarbonLateDecodeStrategy extends SparkStrategy {
  val PUSHED_FILTERS = "PushedFilters"
  val READ_SCHEMA = "ReadSchema"

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  /*
  Spark 2.3.1 plan there can be case of multiple projections like below
  Project [substring(name, 1, 2)#124, name#123, tupleId#117, cast(rand(-6778822102499951904)#125
  as string) AS rand(-6778822102499951904)#137]
   +- Project [substring(name#123, 1, 2) AS substring(name, 1, 2)#124, name#123, UDF:getTupleId()
    AS tupleId#117, (rand(-6778822102499951904) AS rand(-6778822102499951904)#125]
   +- Relation[imei#118,age#119,task#120L,num#121,level#122,name#123]
   CarbonDatasourceHadoopRelation []
 */
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val transformedPlan = makeDeterministic(plan)
    transformedPlan match {
      case PhysicalOperation(projects, filters, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        // In Spark 2.3.1 there is case of multiple projections like below
        // if 1 projection is failed then need to continue to other
        try {
          pruneFilterProject(
            l,
            projects.filterNot(_.name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)),
            filters,
            (a, f, p) =>
              setVectorReadSupport(
                l,
                a,
                relation.buildScan(a.map(_.name).toArray, filters, projects, f, p))
          ) :: Nil
        } catch {
          case e: CarbonPhysicalPlanException => Nil
        }
      case CountStarPlan(colAttr, PhysicalOperation(projectList, predicates, l: LogicalRelation))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] && driverSideCountStar(l) =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        CarbonCountStar(colAttr, relation.carbonTable, SparkSession.getActiveSession.get) :: Nil
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition,
      left, right)
        if isCarbonPlan(left) && CarbonInternalScalaUtil.checkIsIndexTable(right) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
        val carbon = apply(left).head
        // in case of SI Filter push join remove projection list from the physical plan
        // no need to have the project list in the main table physical plan execution
        // only join uses the projection list
        var carbonChild = carbon match {
          case projectExec: ProjectExec =>
            projectExec.child
          case _ =>
            carbon
        }
        // check if the outer and the inner project are matching, only then remove project
        if (left.isInstanceOf[Project]) {
          val leftOutput = left.output
            .filterNot(attr => attr.name
              .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
            .map(c => (c.name.toLowerCase, c.dataType))
          val childOutput = carbonChild.output
            .filterNot(attr => attr.name
              .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
            .map(c => (c.name.toLowerCase, c.dataType))
          if (!leftOutput.equals(childOutput)) {
            // if the projection list and the scan list are different(in case of alias)
            // we should not skip the project, so we are taking the original plan with project
            carbonChild = carbon
          }
        }
        val pushedDownJoin = BroadCastSIFilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          Inner,
          BuildRight,
          carbonChild,
          planLater(right),
          condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left,
      right)
        if isCarbonPlan(right) && CarbonInternalScalaUtil.checkIsIndexTable(left) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:left")
        val carbon = planLater(right)

        val pushedDownJoin =
          BroadCastSIFilterPushJoin(
            leftKeys: Seq[Expression],
            rightKeys: Seq[Expression],
            Inner,
            BuildLeft,
            planLater(left),
            carbon,
            condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition,
      left, right)
        if isLeftSemiExistPushDownEnabled &&
            isAllCarbonPlan(left) && isAllCarbonPlan(right) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeysLeftSemiExist:right")
        val carbon = planLater(left)
        val pushedDownJoin = BroadCastSIFilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          LeftSemi,
          BuildRight,
          carbon,
          planLater(right),
          condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case _ => Nil
    }
  }

  private def isAllCarbonPlan(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.forall(x => x.relation.isInstanceOf[CarbonDatasourceHadoopRelation])
  }

  private def isCarbonPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _,
      MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case LogicalFilter(_, MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case _ => false
    }
  }

  private def isLeftSemiExistPushDownEnabled: Boolean = {
    CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER,
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT).toBoolean
  }

  /**
   * Convert all Expression to deterministic Expression
   */
  private def makeDeterministic(plan: LogicalPlan): LogicalPlan = {
    val transformedPlan = plan transform {
      case p@Project(projectList: Seq[NamedExpression], cd) =>
        if (cd.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Filter] ||
            cd.isInstanceOf[LogicalRelation]) {
          p.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CarbonToSparkAdapter.createAliasRef(
                CustomDeterministicExpression(exp),
                a.name,
                a.exprId,
                a.qualifier,
                a.explicitMetadata,
                Some(a))
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CustomDeterministicExpression(exp)
          }
        } else {
          p
        }
      case f@org.apache.spark.sql.catalyst.plans.logical.Filter(condition: Expression, cd) =>
        if (cd.isInstanceOf[Project] || cd.isInstanceOf[LogicalRelation]) {
          f.transformAllExpressions {
            case a@Alias(exp, _)
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CarbonToSparkAdapter.createAliasRef(
                CustomDeterministicExpression(exp),
                a.name,
                a.exprId,
                a.qualifier,
                a.explicitMetadata,
                Some(a))
            case exp: NamedExpression
              if !exp.deterministic && !exp.isInstanceOf[CustomDeterministicExpression] =>
              CustomDeterministicExpression(exp)
          }
        } else {
          f
        }
    }
    transformedPlan
  }

  /**
   * Return true if driver-side count star optimization can be used.
   * Following case can't use driver-side count star:
   * 1. There is data update and delete
   * 2. It is streaming table
   */
  private def driverSideCountStar(logicalRelation: LogicalRelation): Boolean = {
    val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val segmentUpdateStatusManager = new SegmentUpdateStatusManager(
      relation.carbonRelation.carbonTable)
    val updateDeltaMetadata = segmentUpdateStatusManager.readLoadMetadata()
    val hasNonCarbonSegment =
      segmentUpdateStatusManager.getLoadMetadataDetails.exists(!_.isCarbonFormat)
    if (hasNonCarbonSegment || updateDeltaMetadata != null && updateDeltaMetadata.nonEmpty) {
      false
    } else if (relation.carbonTable.isStreamingSink) {
      false
    } else {
      true
    }
  }

  /**
   * Converts to physical RDD of carbon after pushing down applicable filters.
   * @return
   */
  def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter], Seq[PartitionSpec]) => RDD[InternalRow])
  : CodegenSupport = {
    val names = relation.catalogTable match {
      case Some(table) => table.partitionColumnNames
      case _ => Seq.empty
    }
    // Get the current partitions from table.
    var partitions: Seq[PartitionSpec] = null
    if (names.nonEmpty) {
      val partitionSet = AttributeSet(names
        .map(p => relation.output.find(_.name.equalsIgnoreCase(p)).get))
      val partitionKeyFilters = CarbonToSparkAdapter
        .getPartitionKeyFilter(partitionSet, filterPredicates)
      // Update the name with lower case as it is case sensitive while getting partition info.
      val updatedPartitionFilters = partitionKeyFilters.map { exp =>
        exp.transform {
          case attr: AttributeReference =>
            CarbonToSparkAdapter.createAttributeReference(
              attr.name.toLowerCase,
              attr.dataType,
              attr.nullable,
              attr.metadata,
              attr.exprId,
              attr.qualifier)
        }
      }
      partitions =
        CarbonFilters.getPartitions(
          updatedPartitionFilters.toSeq,
          SparkSession.getActiveSession.get,
          relation.catalogTable.get.identifier).orNull
    }
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      partitions,
      (requestedColumns, _, pushedFilters, p) => {
        scanBuilder(requestedColumns, pushedFilters.toArray, p)
      })
  }

  def setVectorReadSupport(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[InternalRow]): RDD[InternalRow] = {
    rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
      .setVectorReaderSupport(supportBatchedDataSource(relation.relation.sqlContext, output))
    rdd
  }

  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      rawProjects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      partitions: Seq[PartitionSpec],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter], Seq[PartitionSpec])
        => RDD[InternalRow]): CodegenSupport = {
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val extraRdd = MixedFormatHandler.extraRDD(relation, rawProjects, filterPredicates,
      new TableStatusReadCommittedScope(table.identifier, FileFactory.getConfiguration),
      table.identifier)
    val projects = rawProjects.map {p =>
      p.transform {
        case CustomDeterministicExpression(exp) => exp
      }
    }.asInstanceOf[Seq[NamedExpression]]

    // contains the original order of the projection requested
    val projectsAttr = projects.flatMap(_.references)
    val projectSet = AttributeSet(projectsAttr)
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val candidatePredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (unhandledPredicates, pushedFilters, handledFilters ) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      try {
        AttributeSet(handledPredicates.flatMap(_.references)) --
        (projectSet ++ unhandledSet).map(relation.attributeMap)
      } catch {
        case e: Throwable => throw new CarbonPhysicalPlanException
      }
    }
    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)

    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      if (pushedFilters.nonEmpty) {
        pairs += (PUSHED_FILTERS -> pushedFilters.mkString("[", ", ", "]"))
      }
      pairs += (READ_SCHEMA -> projectSet.++(filterSet).toSeq.toStructType.catalogString)
      pairs.toMap
    }

    var vectorPushRowFilters = CarbonProperties.getInstance().isPushRowFiltersForVector

    // Spark cannot filter the rows from the pages in case of polygon query. So, we do the row
    // level filter at carbon and return the rows directly.
    if (candidatePredicates
      .exists(exp => exp.isInstanceOf[ScalaUDF] &&
                     exp.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonUDF])) {
      vectorPushRowFilters = true
    }

    // In case of mixed format, make the vectorPushRowFilters always false as other formats
    // filtering happens in spark layer.
    if (vectorPushRowFilters && extraRdd.isDefined) {
      vectorPushRowFilters = false
    }
    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(relation.attributeMap)
        // Don't request columns that are only referenced by pushed filters.
        .filterNot(handledSet.contains)
      val updateRequestedColumns = requestedColumns

      val updateProject = projects.map { expr =>
        expr.toAttribute.asInstanceOf[AttributeReference]
      }
      val scan = getDataSourceScan(
        relation,
        (updateProject, partitions),
        scanBuilder,
        candidatePredicates,
        pushedFilters,
        handledFilters,
        metadata,
        updateRequestedColumns.asInstanceOf[Seq[Attribute]],
        extraRdd)
      // Check whether spark should handle row filters in case of vector flow.
      if (!vectorPushRowFilters && scan.isInstanceOf[CarbonDataSourceScan]) {
        // Here carbon only do page pruning and row level pruning will be done by spark.
        scan.inputRDDs().head match {
          case rdd: CarbonScanRDD[InternalRow] =>
            rdd.setDirectScanSupport(true)
          case _ =>
        }
        filterPredicates.reduceLeftOption(expressions.And).map(execution.FilterExec(_, scan))
          .getOrElse(scan)
      } else if (extraRdd.isDefined) {
        filterPredicates.reduceLeftOption(expressions.And).map(execution.FilterExec(_, scan))
          .getOrElse(scan)
      } else {
        filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan)
      }
    } else {

      var newProjectList: Seq[Attribute] = Seq.empty
      // In case of implicit exist we should disable vectorPushRowFilters as it goes in IUD flow
      // to get the positionId or tupleID
      var implicitExisted = false
      var updatedProjects = projects.map {
          case a@Alias(s: ScalaUDF, name)
            if name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID) ||
                name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID) =>
            val reference = AttributeReference(name, StringType, true)().withExprId(a.exprId)
            newProjectList :+= reference
            implicitExisted = true
            reference
          case a@Alias(s: ScalaUDF, name)
            if name.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_SEGMENTID) =>
            implicitExisted = true
            val reference =
              AttributeReference(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
                StringType, true)().withExprId(a.exprId)
            newProjectList :+= reference
            a.transform {
              case s: ScalaUDF =>
                CarbonToSparkAdapter.createScalaUDF(s, reference)
            }
          case other => other
      }
      val updatedColumns: (Seq[Attribute], Seq[Expression]) = getRequestedColumns(relation,
        projectsAttr,
        filterSet,
        handledSet,
        newProjectList,
        updatedProjects)
      // Don't request columns that are only referenced by pushed filters.
      val requestedColumns = updatedColumns._1
      updatedProjects = updatedColumns._2

      var updateRequestedColumns =
        if (!vectorPushRowFilters && !implicitExisted) {
          (projectsAttr.to[mutable.LinkedHashSet] ++ filterSet).map(relation.attributeMap).toSeq
        } else {
          requestedColumns
        }
      val supportBatch =
        supportBatchedDataSource(relation.relation.sqlContext,
          updateRequestedColumns) && extraRdd.getOrElse((null, true))._2
      if (!vectorPushRowFilters && !supportBatch && !implicitExisted) {
        // revert for row scan
        updateRequestedColumns = requestedColumns
      }
      val newRequestedColumns = if (!vectorPushRowFilters && extraRdd.isDefined) {
        extractUniqueAttributes(projectsAttr, filterSet.toSeq)
      } else {
        updateRequestedColumns
      }
      val scan = getDataSourceScan(
        relation,
        (newRequestedColumns, partitions),
        scanBuilder,
        candidatePredicates,
        pushedFilters,
        handledFilters,
        metadata,
        newRequestedColumns,
        extraRdd)
      // Check whether spark should handle row filters in case of vector flow.
      if (!vectorPushRowFilters &&
          scan.isInstanceOf[CarbonDataSourceScan] &&
          !implicitExisted) {
        // Here carbon only do page pruning and row level pruning will be done by spark.
        scan.inputRDDs().head match {
          case rdd: CarbonScanRDD[InternalRow] =>
            rdd.setDirectScanSupport(true)
          case _ =>
        }
        execution.ProjectExec(
          updatedProjects.asInstanceOf[Seq[NamedExpression]],
          filterPredicates.reduceLeftOption(expressions.And).map(
            execution.FilterExec(_, scan)).getOrElse(scan))
      } else if (extraRdd.isDefined) {
        execution.ProjectExec(
          updatedProjects.asInstanceOf[Seq[NamedExpression]],
          filterPredicates.reduceLeftOption(expressions.And).map(
            execution.FilterExec(_, scan)).getOrElse(scan))
      } else {
        execution.ProjectExec(
          updatedProjects.asInstanceOf[Seq[NamedExpression]],
          filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
      }

    }
  }

  /*
    This function is used to get the Unique attributes from filter set
    and projection set based on their semantics.
   */
  private def extractUniqueAttributes(projections: Seq[Attribute],
      filter: Seq[Attribute]): Seq[Attribute] = {
    def checkSemanticEquals(filter: Attribute): Option[Attribute] = {
      projections.find(_.semanticEquals(filter))
    }

    filter.toList match {
      case head :: tail =>
        checkSemanticEquals(head) match {
          case Some(_) => extractUniqueAttributes(projections, tail)
          case None => extractUniqueAttributes(projections :+ head, tail)
        }
      case Nil => projections
    }
  }

  protected def getRequestedColumns(relation: LogicalRelation,
      projectsAttr: Seq[Attribute],
      filterSet: AttributeSet,
      handledSet: AttributeSet,
      newProjectList: Seq[Attribute],
      updatedProjects: Seq[Expression]): (Seq[Attribute], Seq[Expression]) = {
    ((projectsAttr.to[mutable.LinkedHashSet] ++ filterSet -- handledSet)
       .map(relation.attributeMap).toSeq ++ newProjectList, updatedProjects)
  }

  private def getDataSourceScan(relation: LogicalRelation,
      outputAndPartitions: (Seq[Attribute], Seq[PartitionSpec]),
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter],
        Seq[PartitionSpec]) => RDD[InternalRow],
      candidatePredicates: Seq[Expression],
      pushedFilters: Seq[Filter], handledFilters: Seq[Filter],
      metadata: Map[String, String],
      updateRequestedColumns: Seq[Attribute],
      extraRDD: Option[(RDD[InternalRow], Boolean)]): DataSourceScanExec = {
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    var rdd = scanBuilder(updateRequestedColumns, candidatePredicates,
      pushedFilters, outputAndPartitions._2)
    if (supportBatchedDataSource(relation.relation.sqlContext, updateRequestedColumns) &&
      extraRDD.getOrElse((rdd, true))._2) {
      rdd = extraRDD.map(_._1.union(rdd)).getOrElse(rdd)
      new CarbonDataSourceScan(
        outputAndPartitions._1,
        rdd,
        createHadoopFSRelation(relation),
        getPartitioning(table.carbonTable, updateRequestedColumns),
        metadata,
        relation.catalogTable.map(_.identifier), relation)
    } else {
      rdd match {
        case cs: CarbonScanRDD[InternalRow] => cs.setVectorReaderSupport(false)
        case _ =>
      }
      rdd = extraRDD.map(_._1.union(rdd)).getOrElse(rdd)
      val partition = getPartitioning(table.carbonTable, updateRequestedColumns)
      CarbonReflectionUtils.getRowDataSourceScanExecObj(relation, outputAndPartitions._1,
        pushedFilters, handledFilters,
        rdd, partition, metadata)
    }
  }

  private def getPartitioning(carbonTable: CarbonTable,
      output: Seq[Attribute]): Partitioning = {
    val info: BucketingInfo = carbonTable.getBucketingInfo()
    if (info != null) {
      val cols = info.getListOfColumns.asScala
      val numBuckets = info.getNumOfRanges
      val bucketColumns = cols.flatMap { n =>
        val attrRef = output.find(_.name.equalsIgnoreCase(n.getColumnName))
        attrRef match {
          case Some(attr: AttributeReference) =>
            Some(AttributeReference(attr.name,
              CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(n.getDataType),
              attr.nullable,
              attr.metadata)(attr.exprId, attr.qualifier))
          case _ => None
        }
      }
      if (bucketColumns.size == cols.size) {
        HashPartitioning(bucketColumns, numBuckets)
      } else {
        UnknownPartitioning(0)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  private def isComplexAttribute(attribute: Attribute) = attribute.dataType match {
    case ArrayType(dataType, _) => true
    case StructType(_) => true
    case MapType(_, _, _) => true
    case _ => false
  }

  protected[sql] def selectFilters(
      relation: BaseRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Filter], Seq[Filter]) = {

    // In case of ComplexType dataTypes no filters should be pushed down. IsNotNull is being
    // explicitly added by spark and pushed. That also has to be handled and pushed back to
    // Spark for handling.
    val predicatesWithoutComplex = predicates.filter(predicate =>
      predicate.collect {
      case a: Attribute if isComplexAttribute(a) => a
    }.size == 0 )

    // For conciseness, all Catalyst filter expressions of type `expressions.Expression` below are
    // called `predicate`s, while all data source filters of type `sources.Filter` are simply called
    // `filter`s. And block filters for lucene with more than one text_match udf
    // Todo: handle when lucene and normal query filter is supported

    var count = 0
    val translated: Seq[(Expression, Filter)] = predicatesWithoutComplex.flatMap {
      predicate =>
        if (predicate.isInstanceOf[ScalaUDF]) {
          predicate match {
            case u: ScalaUDF if u.function.isInstanceOf[TextMatchUDF] ||
                                u.function.isInstanceOf[TextMatchMaxDocUDF] => count = count + 1
            case _ =>
          }
        }
        if (count > 1) {
          throw new MalformedCarbonCommandException(
            "Specify all search filters for Lucene within a single text_match UDF")
        }
        val filter = translateFilter(predicate)
        if (filter.isDefined) {
          Some(predicate, filter.get)
        } else {
          None
        }
    }

    // A map from original Catalyst expressions to corresponding translated data source filters.
    val translatedMap: Map[Expression, Filter] = translated.toMap


    // Catalyst predicate expressions that cannot be translated to data source filters.
    val unrecognizedPredicates = predicates.filterNot(translatedMap.contains)

    // Data source filters that cannot be handled by `relation`. The semantic of a unhandled filter
    // at here is that a data source may not be able to apply this filter to every row
    // of the underlying dataset.
    val unhandledFilters = relation.unhandledFilters(translatedMap.values.toArray).toSet

    val (unhandled, handled) = translated.partition {
      case (predicate, filter) =>
        unhandledFilters.contains(filter)
    }

    // Catalyst predicate expressions that can be translated to data source filters, but cannot be
    // handled by `relation`.
    val (unhandledPredicates, _) = unhandled.unzip

    // Translated data source filters that can be handled by `relation`
    val (_, handledFilters) = handled.unzip

    // translated contains all filters that have been converted to the public Filter interface.
    // We should always push them to the data source no matter whether the data source can apply
    // a filter to every row or not.
    val (_, translatedFilters) = translated.unzip

    (unrecognizedPredicates ++ unhandledPredicates, translatedFilters, handledFilters)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilter(predicate: Expression, or: Boolean = false): Option[Filter] = {
    predicate match {
      case u: ScalaUDF if u.function.isInstanceOf[TextMatchUDF] =>
        if (u.children.size > 1) {
          throw new MalformedCarbonCommandException(
            "TEXT_MATCH UDF syntax: TEXT_MATCH('luceneQuerySyntax')")
        }
        Some(TextMatch(u.children.head.toString()))

      case u: ScalaUDF if u.function.isInstanceOf[TextMatchMaxDocUDF] =>
        if (u.children.size > 2) {
          throw new MalformedCarbonCommandException(
            "TEXT_MATCH UDF syntax: TEXT_MATCH_LIMIT('luceneQuerySyntax')")
        }
        Some(TextMatchLimit(u.children.head.toString(), u.children.last.toString()))

      case u: ScalaUDF if u.function.isInstanceOf[InPolygonUDF] =>
        if (u.children.size > 1) {
          throw new MalformedCarbonCommandException("Expect one string in polygon")
        }
        Some(InPolygon(u.children.head.toString()))

      case or@Or(left, right) =>

        val leftFilter = translateFilter(left, true)
        val rightFilter = translateFilter(right, true)
        if (leftFilter.isDefined && rightFilter.isDefined) {
          Some(sources.Or(leftFilter.get, rightFilter.get))
        } else {
          None
        }

      case And(left, right) =>
        val leftFilter = translateFilter(left, or)
        val rightFilter = translateFilter(right, or)
        if (or) {
          if (leftFilter.isDefined && rightFilter.isDefined) {
            (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)
          } else {
            None
          }
        } else {
          (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)
        }
      case EqualTo(a: Attribute, Literal(v, t)) =>
        Some(sources.EqualTo(a.name, v))
      case EqualTo(l@Literal(v, t), a: Attribute) =>
        Some(sources.EqualTo(a.name, v))
      case c@EqualTo(Cast(a: Attribute, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@EqualTo(Literal(v, t), Cast(a: Attribute, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case Not(EqualTo(a: Attribute, Literal(v, t))) =>
        Some(sources.Not(sources.EqualTo(a.name, v)))
      case Not(EqualTo(Literal(v, t), a: Attribute)) =>
        Some(sources.Not(sources.EqualTo(a.name, v)))
      case c@Not(EqualTo(Cast(a: Attribute, _), Literal(v, t))) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@Not(EqualTo(Literal(v, t), Cast(a: Attribute, _))) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case IsNotNull(a: Attribute) => Some(sources.IsNotNull(a.name))
      case IsNull(a: Attribute) => Some(sources.IsNull(a.name))
      case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        Some(sources.Not(sources.In(a.name, hSet.toArray)))
      case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        Some(sources.In(a.name, hSet.toArray))
      case c@Not(In(Cast(a: Attribute, _), list))
        if !list.exists(!_.isInstanceOf[Literal]) =>
        Some(CastExpr(c))
      case c@In(Cast(a: Attribute, _), list) if !list.exists(!_.isInstanceOf[Literal]) =>
        Some(CastExpr(c))
      case InSet(a: Attribute, set) =>
        Some(sources.In(a.name, set.toArray))
      case Not(InSet(a: Attribute, set)) =>
        Some(sources.Not(sources.In(a.name, set.toArray)))
      case GreaterThan(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThan(a.name, v))
      case GreaterThan(Literal(v, t), a: Attribute) =>
        Some(sources.LessThan(a.name, v))
      case c@GreaterThan(Cast(a: Attribute, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@GreaterThan(Literal(v, t), Cast(a: Attribute, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case LessThan(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThan(a.name, v))
      case LessThan(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThan(a.name, v))
      case c@LessThan(Cast(a: Attribute, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@LessThan(Literal(v, t), Cast(a: Attribute, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case GreaterThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.GreaterThanOrEqual(a.name, v))
      case GreaterThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.LessThanOrEqual(a.name, v))
      case c@GreaterThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@GreaterThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case LessThanOrEqual(a: Attribute, Literal(v, t)) =>
        Some(sources.LessThanOrEqual(a.name, v))
      case LessThanOrEqual(Literal(v, t), a: Attribute) =>
        Some(sources.GreaterThanOrEqual(a.name, v))
      case c@LessThanOrEqual(Cast(a: Attribute, _), Literal(v, t)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case c@LessThanOrEqual(Literal(v, t), Cast(a: Attribute, _)) =>
        CastExpressionOptimization.checkIfCastCanBeRemove(c)
      case s@StartsWith(a: Attribute, Literal(v, t)) =>
        Some(sources.StringStartsWith(a.name, v.toString))
      case c@EndsWith(a: Attribute, Literal(v, t)) =>
        Some(CarbonEndsWith(c))
      case c@Contains(a: Attribute, Literal(v, t)) =>
        Some(CarbonContainsWith(c))
      case c@Literal(v, t) if (v == null) =>
        Some(FalseExpr())
      case others => None
    }
  }

  def supportBatchedDataSource(sqlContext: SQLContext, cols: Seq[Attribute]): Boolean = {
    val vectorizedReader = {
      if (sqlContext.sparkSession.conf.contains(CarbonCommonConstants.ENABLE_VECTOR_READER)) {
        sqlContext.sparkSession.conf.get(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else if (System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER) != null) {
        System.getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER)
      } else {
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ENABLE_VECTOR_READER,
          CarbonCommonConstants.ENABLE_VECTOR_READER_DEFAULT)
      }
    }
    val supportCodegen =
      sqlContext.conf.wholeStageEnabled && sqlContext.conf.wholeStageMaxNumFields >= cols.size
    supportCodegen && vectorizedReader.toBoolean &&
    cols.forall(_.dataType.isInstanceOf[AtomicType])
  }

  private def createHadoopFSRelation(relation: LogicalRelation): HadoopFsRelation = {
    val sparkSession = relation.relation.sqlContext.sparkSession
    relation.catalogTable match {
      case Some(catalogTable) =>
        val fileIndex = new CarbonFileIndex(sparkSession,
          catalogTable.schema,
          catalogTable.storage.properties,
          new CatalogFileIndex(
          sparkSession,
          catalogTable,
          sizeInBytes = relation.relation.sizeInBytes))
        fileIndex.setDummy(true)
        HadoopFsRelation(
          fileIndex,
          catalogTable.partitionSchema,
          catalogTable.schema,
          catalogTable.bucketSpec,
          new SparkCarbonTableFormat,
          catalogTable.storage.properties)(sparkSession)
      case _ =>
        HadoopFsRelation(
          new InMemoryFileIndex(sparkSession, Seq.empty, Map.empty, None),
          new StructType(),
          relation.relation.schema,
          None,
          new SparkCarbonTableFormat,
          null)(sparkSession)
    }
  }
}

class CarbonPhysicalPlanException extends Exception

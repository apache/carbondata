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

import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{expressions, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, _}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper.vectorReaderEnabled
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope
import org.apache.carbondata.core.scan.expression.{Expression => CarbonFilter}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.geo.{GeoHashUtils, GeoIdToGridXyUDF, GeoIdToLatLngUDF, InPolygonListUDF, InPolygonRangeListUDF, InPolygonUDF, InPolylineListUDF, LatLngToGeoIdUDF, ToRangeListUDF, ToUpperLayerGeoIdUDF}
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.index.{TextMatchMaxDocUDF, TextMatchUDF}

private[sql] object CarbonSourceStrategy extends SparkStrategy {
  val PUSHED_FILTERS = "PushedFilters"
  val READ_SCHEMA = "ReadSchema"

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val transformedPlan = CarbonPlanHelper.makeDeterministic(plan)
    transformedPlan match {
      case GlobalLimit(IntegerLiteral(limit), LocalLimit(IntegerLiteral(limitValue),
      _@PhysicalOperation(projects, filters, l: LogicalRelation))) if isCarbonRelation(l) =>
        GlobalLimitExec(limit, LocalLimitExec(limitValue, pruneFilterProject(l,
          projects.filterNot(_.name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)),
          filters))) :: Nil
      case PhysicalOperation(projects, filters, l: LogicalRelation) if isCarbonRelation(l) =>
        try {
          pruneFilterProject(l, projects, filters) :: Nil
        } catch {
          case _: CarbonPhysicalPlanException => Nil
        }
      case Project(projectList, _: OneRowRelation) if validateUdf(projectList) => Nil
      case _ => Nil
    }
  }

  private def validateUdf(projects: Seq[NamedExpression]): Boolean = {
    projects foreach {
      case alias: Alias if alias.child.isInstanceOf[Expression] =>
        alias.child match {
          case Cast(s: ScalaUDF, _, _) => validateGeoUtilUDFs(s)
          case s: ScalaUDF => validateGeoUtilUDFs(s)
          case _ =>
        }
    }
    true
  }

  def validateGeoUtilUDFs(s: ScalaUDF): Boolean = {
    s.function match {
      case _: ToRangeListUDF =>
        val inputNames = List("polygon", "oriLatitude", "gridSize")
        for (i <- s.children.indices) {
          GeoHashUtils.validateUDFInputValue(s.children(i),
            inputNames(i),
            CarbonToSparkAdapter.getTypeName(s.inputTypes(i)))
        }
      case _: LatLngToGeoIdUDF =>
        val inputNames = List("latitude", "longitude", "oriLatitude", "gridSize")
        for (i <- s.children.indices) {
          GeoHashUtils.validateUDFInputValue(s.children(i),
            inputNames(i),
            CarbonToSparkAdapter.getTypeName(s.inputTypes(i)))
        }
      case _: GeoIdToGridXyUDF =>
        GeoHashUtils.validateUDFInputValue(s.children.head,
          "geoId",
          CarbonToSparkAdapter.getTypeName(s.inputTypes.head))
      case _: GeoIdToLatLngUDF =>
        val inputNames = List("geoId", "oriLatitude", "gridSize")
        for (i <- s.children.indices) {
          GeoHashUtils.validateUDFInputValue(s.children(i),
            inputNames(i),
            CarbonToSparkAdapter.getTypeName(s.inputTypes(i)))
        }
      case _: ToUpperLayerGeoIdUDF =>
        GeoHashUtils.validateUDFInputValue(s.children.head,
          "geoId",
          CarbonToSparkAdapter.getTypeName(s.inputTypes.head))
      case _ =>
    }
    true
  }

  private def isCarbonRelation(logicalRelation: LogicalRelation): Boolean = {
    logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation]
  }

  private def getPartitionFilter(relation: LogicalRelation,
      filterPredicates: Seq[Expression], names: Seq[String]): (Seq[Expression], AttributeSet) = {
    // Get the current partitions from table.
    if (names.nonEmpty) {
      val partitionSet = AttributeSet(names
        .map(p => relation.output.find(_.name.equalsIgnoreCase(p)).get))
      // Update the name with lower case as it is case sensitive while getting partition info.
      (CarbonToSparkAdapter
        .getPartitionFilter(partitionSet, filterPredicates)
        .map(CarbonToSparkAdapter.lowerCaseAttribute), partitionSet)
    } else {
      (Seq.empty, AttributeSet.empty)
    }
  }

  /**
   * Converts to physical RDD of carbon after pushing down applicable filters.
   * @return
   */
  private def pruneFilterProject(
      relation: LogicalRelation,
      rawProjects: Seq[NamedExpression],
      allPredicates: Seq[Expression]): SparkPlan = {
    // get partition column names
    val names = relation.catalogTable.map(_.partitionColumnNames).getOrElse(Seq.empty)
    // get partition set from filter expression
    val (partitionsFilter, partitionSet) = getPartitionFilter(relation, allPredicates, names)
    var partitions : (Seq[CatalogTablePartition], Seq[PartitionSpec], Seq[Expression]) =
      (null, null, Seq.empty)
    var filterPredicates = allPredicates
    if(names.nonEmpty && partitionsFilter.nonEmpty) {
      partitions = CarbonFilters.getCatalogTablePartitions(
        partitionsFilter.filterNot(e => e.find(_.isInstanceOf[PlanExpression[_]]).isDefined),
        SparkSession.getActiveSession.get,
        relation.catalogTable.get.identifier
      )
    }
    // remove dynamic partition filter from predicates
    filterPredicates = CarbonToSparkAdapter.getDataFilter(partitionSet,
      allPredicates, partitionsFilter)
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val projects = rawProjects.map {p =>
      p.transform {
        case CustomDeterministicExpression(exp) => exp
      }
    }.asInstanceOf[Seq[NamedExpression]]

    // contains the original order of the projection requested
    val projectsAttr = projects.flatMap(_.references)
    val projectSet = AttributeSet(projectsAttr)
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val relationPredicates = filterPredicates.map {
      _ transform {
        case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
      }
    }

    val (unhandledPredicates, handledPredicates, pushedFilters) =
      selectFilters(table, relationPredicates)

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

    val readCommittedScope =
      new TableStatusReadCommittedScope(table.identifier, FileFactory.getConfiguration)
    val extraSegments = MixedFormatHandler.extraSegments(table.identifier, readCommittedScope)
    val extraRDD = MixedFormatHandler.extraRDD(relation, rawProjects, filterPredicates,
      readCommittedScope, table.identifier, extraSegments, vectorReaderEnabled())
    val vectorPushRowFilters =
      vectorPushRowFiltersEnabled(relationPredicates, extraSegments.nonEmpty)
    var directScanSupport = !vectorPushRowFilters
    val (updatedProjects, output, requiredColumns) = if (projects.map(_.toAttribute) == projects &&
      projectSet.size == projects.size && filterSet.subsetOf(projectSet)) {
      getRequiredColumnsWithoutProject(relation, projects, handledSet)
    } else {
      val tuple4 = getUpdatedProjects(relation, projects, projectsAttr, filterSet, handledSet,
        directScanSupport, extraRDD)
      if (directScanSupport) {
        directScanSupport = tuple4._4
      }
      (tuple4._1, tuple4._2, tuple4._3)
    }
    val inSegmentUDF = allPredicates.filter(e =>
      e.isInstanceOf[ScalaUDF] &&
      e.asInstanceOf[ScalaUDF].udfName.get.equalsIgnoreCase("insegment"))
    val segmentIds: Option[String] = if (!inSegmentUDF.isEmpty) {
      val inSegment = inSegmentUDF.head.asInstanceOf[ScalaUDF]
      Option(inSegment.children.head.asInstanceOf[Literal].value.toString)
    } else {
      None
    }
    // scan
    val scan = CarbonDataSourceScan(
      table,
      output,
      partitionsFilter.filterNot(SubqueryExpression.hasSubquery),
      handledPredicates,
      getCarbonProjection(relationPredicates, requiredColumns, projects),
      pushedFilters,
      directScanSupport,
      extraRDD,
      Some(TableIdentifier(table.identifier.getTableName,
        Option(table.identifier.getDatabaseName))),
      partitions._1,
      partitionsFilter,
      segmentIds
    )
    // filter
    val filterOption = if (directScanSupport && CarbonToSparkAdapter
        .supportsBatchOrColumnar(scan)) {
      filterPredicates.reduceLeftOption(expressions.And)
    } else if (extraSegments.nonEmpty) {
      filterPredicates.reduceLeftOption(expressions.And)
    } else {
      filterCondition
    }
    val scanWithFilter = filterOption.map(execution.FilterExec(_, scan)).getOrElse(scan)
    // project
    updatedProjects.map(execution.ProjectExec(_, scanWithFilter)).getOrElse(scanWithFilter)
  }

  private def getRequiredColumnsWithoutProject(relation: LogicalRelation,
      projects: Seq[NamedExpression],
      handledSet: AttributeSet): (Option[Seq[NamedExpression]], Seq[Attribute], Seq[String]) = {
    val newRequiredColumns = projects
      // Safe due to if above.
      .asInstanceOf[Seq[Attribute]]
      // Match original case of attributes.
      .map(relation.attributeMap)
      // Don't request columns that are only referenced by pushed filters.
      .filterNot(handledSet.contains)

    (None, projects.map(_.toAttribute.asInstanceOf[AttributeReference]),
      newRequiredColumns.map(_.name))
  }

  private def getUpdatedProjects(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      projectsAttr: Seq[Attribute],
      filterSet: AttributeSet,
      handledSet: AttributeSet,
      oldDirectScanSupport: Boolean,
      extraRDD: Option[(RDD[InternalRow], Boolean)]
  ): (Option[Seq[NamedExpression]], Seq[Attribute], Seq[String], Boolean) = {
    var newProjectList: Seq[Attribute] = Seq.empty
    // In case of implicit exist we should disable vectorPushRowFilters as it goes in IUD flow
    // to get the positionId or tupleID
    var implicitExisted = false
    var updatedProjects = projects.map {
      case a@Alias(_: ScalaUDF, name)
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
    }.asInstanceOf[Seq[NamedExpression]]
    val directScanSupport = if (oldDirectScanSupport && implicitExisted) {
      false
    } else {
      oldDirectScanSupport
    }
    val updatedColumns: (Seq[Attribute], Seq[NamedExpression]) = getRequiredColumns(relation,
      projectsAttr, filterSet, handledSet, newProjectList, updatedProjects)
    // Don't request columns that are only referenced by pushed filters.
    val requiredColumns = updatedColumns._1
    updatedProjects = updatedColumns._2
    var updateRequestedColumns = if (directScanSupport) {
      (projectsAttr.to[mutable.LinkedHashSet] ++ filterSet).map(relation.attributeMap).toSeq
    } else {
      requiredColumns
    }
    val supportBatch = CarbonPlanHelper.supportBatchedDataSource(relation.relation.sqlContext,
      updateRequestedColumns, extraRDD)
    if (directScanSupport && !supportBatch && filterSet.nonEmpty &&
      !filterSet.toSeq.exists(_.dataType.isInstanceOf[ArrayType])) {
      // revert for row scan
      updateRequestedColumns = requiredColumns
    }
    val newRequiredColumns = if (oldDirectScanSupport && extraRDD.isDefined) {
      extractUniqueAttributes(projectsAttr, filterSet.toSeq)
    } else {
      updateRequestedColumns
    }
    (Some(updatedProjects), newRequiredColumns, newRequiredColumns.map(_.name), directScanSupport)
  }

  private def getCarbonProjection(scanFilters: Seq[Expression],
      requiredColumns: Seq[String],
      projects: Seq[NamedExpression]): CarbonProjection = {
    val projection = new CarbonProjection

    // As Filter push down for Complex datatype is not supported, if filter is applied on complex
    // column, then Projection push down on Complex Columns will not take effect. Hence, check if
    // filter contains Struct Complex Column.
    val complexFilterExists = scanFilters.map(col =>
      col.map(_.isInstanceOf[GetStructField]))

    if (!complexFilterExists.exists(f => f.contains(true))) {
      PushDownHelper.pushDownProjection(requiredColumns, projects, projection)
    } else {
      requiredColumns.foreach(projection.addColumn)
    }
    projection
  }

  private def vectorPushRowFiltersEnabled(candidatePredicates: Seq[Expression],
      hasExtraSegment: Boolean): Boolean = {
    var vectorPushRowFilters = CarbonProperties.getInstance().isPushRowFiltersForVector

    // Spark cannot filter the rows from the pages in case of polygon query. So, we do the row
    // level filter at carbon and return the rows directly.
    if (candidatePredicates
      .exists(exp => exp.isInstanceOf[ScalaUDF] &&
        (exp.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonUDF] ||
          exp.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonListUDF] ||
          exp.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolylineListUDF] ||
          exp.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonRangeListUDF]))) {
      vectorPushRowFilters = true
    }

    // In case of mixed format, make the vectorPushRowFilters always false as other formats
    // filtering happens in spark layer.
    if (vectorPushRowFilters && hasExtraSegment) {
      vectorPushRowFilters = false
    }
    vectorPushRowFilters
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

  protected def getRequiredColumns(relation: LogicalRelation,
      projectsAttr: Seq[Attribute],
      filterSet: AttributeSet,
      handledSet: AttributeSet,
      newProjectList: Seq[Attribute],
      updatedProjects: Seq[NamedExpression]): (Seq[Attribute], Seq[NamedExpression]) = {
    val sparkSession = SparkSession.getActiveSession.get
    val pushDownJoinEnabled = sparkSession.sparkContext.getConf
      .getBoolean("spark.carbon.pushdown.join.as.filter", defaultValue = true)

    // positionId column can be added in two cases
    // case 1: SI pushdown case, SI rewritten plan adds positionId column
    // case 2: if the user requested positionId column thru getPositionId() UDF
    // positionId column should be removed only in case 1, as it is manually added
    // Below code is added to handle case 2. But getPositionId() UDF is almost used only for testing
    val isPositionIDRequested = relation.catalogTable match {
      case Some(table) =>
        val tblProperties = CarbonEnv.getCarbonTable(table.identifier)(sparkSession).getTableInfo
          .getFactTable
          .getTableProperties
        val isPosIDRequested = if (tblProperties.containsKey("isPositionIDRequested")) {
          java.lang.Boolean.parseBoolean(tblProperties.get("isPositionIDRequested"))
        } else {
          false
        }
        isPosIDRequested
      case _ => false
    }
    // remove positionId col only if pushdown is enabled and
    // positionId col is not requested in the query
    if (pushDownJoinEnabled && !isPositionIDRequested) {
      ((projectsAttr.to[scala.collection.mutable.LinkedHashSet] ++ filterSet -- handledSet)
        .map(relation.attributeMap).toSeq ++ newProjectList
        .filterNot(attr => attr.name
          .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)), updatedProjects
        .filterNot(attr => attr.isInstanceOf[AttributeReference] &&
          attr.asInstanceOf[AttributeReference].name
            .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)))
    } else {
      ((projectsAttr.to[scala.collection.mutable.LinkedHashSet] ++ filterSet -- handledSet)
        .map(relation.attributeMap).toSeq ++ newProjectList, updatedProjects)
    }
  }

  private def isComplexAttribute(attribute: Attribute) = {
    attribute.dataType match {
      case _: ArrayType => true
      case _: StructType => true
      case _: MapType => true
      case _ => false
    }
  }

  protected def selectFilters(relation: CarbonDatasourceHadoopRelation,
      predicates: Seq[Expression]): (Seq[Expression], Seq[Expression], Seq[CarbonFilter]) = {
    // In case of ComplexType dataTypes no filters should be pushed down. IsNotNull is being
    // explicitly added by spark and pushed. That also has to be handled and pushed back to
    // Spark for handling.
    // allow array_contains() push down
    val filteredPredicates = predicates.filter { predicate =>
      predicate.isInstanceOf[ArrayContains] ||
        predicate.collect {
          case a: Attribute if isComplexAttribute(a) => a
        }.isEmpty
    }

    val dataTypeOf = relation.schema.map { f =>
      f.dataType match {
        case arrayType: ArrayType => f.name -> arrayType.elementType
        case _ => f.name -> f.dataType
      }
    }.toMap

    var count = 0
    val translated: Seq[(Expression, CarbonFilter)] = filteredPredicates.flatMap {
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

        val filter = CarbonFilters.translateExpression(relation, predicate, dataTypeOf)
        if (filter.isDefined) {
          Some(predicate, filter.get)
        } else {
          None
        }
    }

    // A map from original Catalyst expressions to corresponding translated data source filters.
    val translatedMap: Map[Expression, CarbonFilter] = translated.toMap

    // Catalyst predicate expressions that cannot be translated to data source filters.
    val unrecognizedPredicates = predicates.filterNot(translatedMap.contains)

    // translated contains all filters that have been converted to the public Filter interface.
    // We should always push them to the data source no matter whether the data source can apply
    // a filter to every row or not.
    val (recognizedPredicates, translatedFilters) = translated.unzip

    (unrecognizedPredicates, recognizedPredicates, translatedFilters)
  }
}

class CarbonPhysicalPlanException extends Exception

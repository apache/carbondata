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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.optimizer.{CarbonDecoderRelation, CarbonFilters}
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.CarbonExpressions.{MatchCast => Cast}
import org.apache.spark.sql.carbondata.execution.datasources.{CarbonFileIndex, CarbonSparkDataSourceUtil}
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.schema.BucketingInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.{TextMatch, TextMatchLimit, TextMatchMaxDocUDF, TextMatchUDF}
import org.apache.carbondata.spark.CarbonAliasDecoderRelation
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * Carbon specific optimization for late decode (convert dictionary key to value as late as
 * possible), which can improve the aggregation performance and reduce memory usage
 */
private[sql] class CarbonLateDecodeStrategy extends SparkStrategy {
  val PUSHED_FILTERS = "PushedFilters"
  val READ_SCHEMA = "ReadSchema"

  /*
  Spark 2.3.1 plan there can be case of multiple projections like below
  Project [substring(name, 1, 2)#124, name#123, tupleId#117, cast(rand(-6778822102499951904)#125
  as string) AS rand(-6778822102499951904)#137]
   +- Project [substring(name#123, 1, 2) AS substring(name, 1, 2)#124, name#123, UDF:getTupleId()
    AS tupleId#117,
       customdeterministicexpression(rand(-6778822102499951904)) AS rand(-6778822102499951904)#125]
   +- Relation[imei#118,age#119,task#120L,num#121,level#122,name#123]
   CarbonDatasourceHadoopRelation []
 */
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case PhysicalOperation(projects, filters, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        // In Spark 2.3.1 there is case of multiple projections like below
        // if 1 projection is failed then need to continue to other
        try {
          pruneFilterProject(
            l,
            projects,
            filters,
            (a, f, needDecoder, p) => toCatalystRDD(l, a, relation.buildScan(
              a.map(_.name).toArray, filters, projects, f, p), needDecoder)) :: Nil
        } catch {
          case e: CarbonPhysicalPlanException => Nil
        }
      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
        if ((profile.isInstanceOf[IncludeProfile] && profile.isEmpty) ||
            !CarbonDictionaryDecoder.
              isRequiredToDecode(CarbonDictionaryDecoder.
                getDictionaryColumnMapping(child.output, relations, profile, aliasMap))) {
          planLater(child) :: Nil
        } else {
          CarbonDictionaryDecoder(relations,
            profile,
            aliasMap,
            planLater(child),
            SparkSession.getActiveSession.get
          ) :: Nil
        }
      case CountStarPlan(colAttr, PhysicalOperation(projectList, predicates, l: LogicalRelation))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] && driverSideCountStar(l) =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        CarbonCountStar(colAttr, relation.carbonTable, SparkSession.getActiveSession.get) :: Nil
      case _ => Nil
    }
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
      relation.carbonRelation.metaData.carbonTable)
    val updateDeltaMetadata = segmentUpdateStatusManager.readLoadMetadata()
    if (updateDeltaMetadata != null && updateDeltaMetadata.nonEmpty) {
      false
    } else if (relation.carbonTable.isStreamingSink) {
      false
    } else {
      true
    }
  }

  def getDecoderRDD(
      logicalRelation: LogicalRelation,
      projectExprsNeedToDecode: ArrayBuffer[AttributeReference],
      rdd: RDD[InternalRow],
      output: Seq[Attribute]): RDD[InternalRow] = {
    val table = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val relation = CarbonDecoderRelation(logicalRelation.attributeMap,
      logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
    val attrs = projectExprsNeedToDecode.map { attr =>
      val newAttr = AttributeReference(attr.name,
        attr.dataType,
        attr.nullable,
        attr.metadata)(attr.exprId, Option(table.carbonRelation.tableName))
      relation.addAttribute(newAttr)
      newAttr
    }

    new CarbonDecoderRDD(
      Seq(relation),
      IncludeProfile(attrs),
      CarbonAliasDecoderRelation(),
      rdd,
      output,
      table.carbonTable.getTableInfo.serialize())
  }

  /**
   * Converts to physical RDD of carbon after pushing down applicable filters.
   * @param relation
   * @param projects
   * @param filterPredicates
   * @param scanBuilder
   * @return
   */
  private def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter],
        ArrayBuffer[AttributeReference], Seq[PartitionSpec]) => RDD[InternalRow]) = {
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
              attr.qualifier,
              attr)
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
      (requestedColumns, _, pushedFilters, a, p) => {
        scanBuilder(requestedColumns, pushedFilters.toArray, a, p)
      })
  }

  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      needDecode: ArrayBuffer[AttributeReference]):
  RDD[InternalRow] = {
    val scanRdd = rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
    if (needDecode.nonEmpty) {
      scanRdd.setVectorReaderSupport(false)
      getDecoderRDD(relation, needDecode, rdd, output)
    } else {
      scanRdd
        .setVectorReaderSupport(supportBatchedDataSource(relation.relation.sqlContext, output))
      rdd
    }
  }

  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      rawProjects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      partitions: Seq[PartitionSpec],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter],
        ArrayBuffer[AttributeReference], Seq[PartitionSpec]) => RDD[InternalRow]) = {
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
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val map = table.carbonRelation.metaData.dictionaryMap

    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      if (pushedFilters.nonEmpty) {
        pairs += (PUSHED_FILTERS -> pushedFilters.mkString("[", ", ", "]"))
      }
      pairs += (READ_SCHEMA -> projectSet.++(filterSet).toSeq.toStructType.catalogString)
      pairs.toMap
    }


    val needDecoder = ArrayBuffer[AttributeReference]()
    filterCondition match {
      case Some(exp: Expression) =>
        exp.references.collect {
          case attr: AttributeReference =>
            val dict = map.get(attr.name)
            if (dict.isDefined && dict.get) {
              needDecoder += attr
            }
        }
      case None =>
    }

    projects.map {
      case attr: AttributeReference =>
      case Alias(attr: AttributeReference, _) =>
      case others =>
        others.references.map { f =>
          val dictionary = map.get(f.name)
          if (dictionary.isDefined && dictionary.get) {
            needDecoder += f.asInstanceOf[AttributeReference]
          }
        }
    }
    // in case of the global dictionary if it has the filter then it needs to decode all data before
    // applying the filter in spark's side. So we should disable vectorPushRowFilters option
    // in case of filters on global dictionary.
    val hasDictionaryFilterCols = hasFilterOnDictionaryColumn(filterSet, table)

    // In case of more dictionary columns spark code gen needs generate lot of code and that slows
    // down the query, so we limit the direct fill in case of more dictionary columns.
    val hasMoreDictionaryCols = hasMoreDictionaryColumnsOnProjection(projectSet, table)
    val vectorPushRowFilters = CarbonProperties.getInstance().isPushRowFiltersForVector
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
      val updateRequestedColumns = updateRequestedColumnsFunc(requestedColumns, table, needDecoder)

      val updateProject = projects.map { expr =>
        var attr = expr.toAttribute.asInstanceOf[AttributeReference]
        if (!needDecoder.exists(_.name.equalsIgnoreCase(attr.name))) {
          val dict = map.get(attr.name)
          if (dict.isDefined && dict.get) {
            attr = AttributeReference(attr.name, IntegerType, attr.nullable, attr.metadata)(attr
              .exprId, attr.qualifier)
          }
        }
        attr
      }
      val scan = getDataSourceScan(relation,
        updateProject,
        partitions,
        scanBuilder,
        candidatePredicates,
        pushedFilters,
        handledFilters,
        metadata,
        needDecoder,
        updateRequestedColumns.asInstanceOf[Seq[Attribute]])
      // Check whether spark should handle row filters in case of vector flow.
      if (!vectorPushRowFilters && scan.isInstanceOf[CarbonDataSourceScan]
          && !hasDictionaryFilterCols && !hasMoreDictionaryCols) {
        // Here carbon only do page pruning and row level pruning will be done by spark.
        scan.inputRDDs().head match {
          case rdd: CarbonScanRDD[InternalRow] =>
            rdd.setDirectScanSupport(true)
          case _ =>
        }
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
                ScalaUDF(s.function, s.dataType, Seq(reference), s.inputTypes)
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
        if (!vectorPushRowFilters && !implicitExisted && !hasDictionaryFilterCols
            && !hasMoreDictionaryCols) {
          updateRequestedColumnsFunc(
            (projectSet ++ filterSet).map(relation.attributeMap).toSeq,
            table,
            needDecoder)
      } else {
        updateRequestedColumnsFunc(requestedColumns, table, needDecoder)
      }
      val supportBatch =
        supportBatchedDataSource(relation.relation.sqlContext,
          updateRequestedColumns.asInstanceOf[Seq[Attribute]]) &&
        needDecoder.isEmpty
      if (!vectorPushRowFilters && !supportBatch && !implicitExisted && !hasDictionaryFilterCols
          && !hasMoreDictionaryCols) {
        // revert for row scan
        updateRequestedColumns = updateRequestedColumnsFunc(requestedColumns, table, needDecoder)
      }
      val scan = getDataSourceScan(relation,
        updateRequestedColumns.asInstanceOf[Seq[Attribute]],
        partitions,
        scanBuilder,
        candidatePredicates,
        pushedFilters,
        handledFilters,
        metadata,
        needDecoder,
        updateRequestedColumns.asInstanceOf[Seq[Attribute]])
      // Check whether spark should handle row filters in case of vector flow.
      if (!vectorPushRowFilters && scan.isInstanceOf[CarbonDataSourceScan]
          && !implicitExisted && !hasDictionaryFilterCols && !hasMoreDictionaryCols) {
        // Here carbon only do page pruning and row level pruning will be done by spark.
        scan.inputRDDs().head match {
          case rdd: CarbonScanRDD[InternalRow] =>
            rdd.setDirectScanSupport(true)
          case _ =>
        }
        execution.ProjectExec(
          updateRequestedColumnsFunc(updatedProjects, table,
            needDecoder).asInstanceOf[Seq[NamedExpression]],
          filterPredicates.reduceLeftOption(expressions.And).map(
            execution.FilterExec(_, scan)).getOrElse(scan))
      } else {
        execution.ProjectExec(
          updateRequestedColumnsFunc(updatedProjects, table,
            needDecoder).asInstanceOf[Seq[NamedExpression]],
          filterCondition.map(execution.FilterExec(_, scan)).getOrElse(scan))
      }

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
      output: Seq[Attribute],
      partitions: Seq[PartitionSpec],
      scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter],
        ArrayBuffer[AttributeReference], Seq[PartitionSpec])
        => RDD[InternalRow],
      candidatePredicates: Seq[Expression],
      pushedFilters: Seq[Filter], handledFilters: Seq[Filter],
      metadata: Map[String, String],
      needDecoder: ArrayBuffer[AttributeReference],
      updateRequestedColumns: Seq[Attribute]): DataSourceScanExec = {
    val table = relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    if (supportBatchedDataSource(relation.relation.sqlContext, updateRequestedColumns) &&
        needDecoder.isEmpty) {
      new CarbonDataSourceScan(
        output,
        scanBuilder(updateRequestedColumns,
          candidatePredicates,
          pushedFilters,
          needDecoder,
          partitions),
        createHadoopFSRelation(relation),
        getPartitioning(table.carbonTable, updateRequestedColumns),
        metadata,
        relation.catalogTable.map(_.identifier), relation)
    } else {
      val partition = getPartitioning(table.carbonTable, updateRequestedColumns)
      val rdd = scanBuilder(updateRequestedColumns, candidatePredicates,
        pushedFilters, needDecoder, partitions)
      CarbonReflectionUtils.getRowDataSourceScanExecObj(relation, output,
        pushedFilters, handledFilters,
        rdd, partition, metadata)
    }
  }


  def updateRequestedColumnsFunc(requestedColumns: Seq[Expression],
      relation: CarbonDatasourceHadoopRelation,
      needDecoder: ArrayBuffer[AttributeReference]): Seq[Expression] = {
    val map = relation.carbonRelation.metaData.dictionaryMap
    requestedColumns.map {
      case attr: AttributeReference =>
        if (needDecoder.exists(_.name.equalsIgnoreCase(attr.name))) {
          attr
        } else {
          val dict = map.get(attr.name)
          if (dict.isDefined && dict.get) {
            AttributeReference(attr.name,
              IntegerType,
              attr.nullable,
              attr.metadata)(attr.exprId, attr.qualifier)
          } else {
            attr
          }
        }
      case alias @ Alias(attr: AttributeReference, name) =>
        if (needDecoder.exists(_.name.equalsIgnoreCase(attr.name))) {
          alias
        } else {
          val dict = map.get(attr.name)
          if (dict.isDefined && dict.get) {
            alias.transform {
              case attrLocal: AttributeReference =>
                AttributeReference(attr.name,
                  IntegerType,
                  attr.nullable,
                  attr.metadata)(attr.exprId, attr.qualifier)
            }
          } else {
            alias
          }
        }
      case others => others
    }
  }

  private def hasFilterOnDictionaryColumn(filterColumns: AttributeSet,
      relation: CarbonDatasourceHadoopRelation): Boolean = {
    val map = relation.carbonRelation.metaData.dictionaryMap
    filterColumns.exists(c => map.get(c.name).getOrElse(false))
  }

  private def hasMoreDictionaryColumnsOnProjection(projectColumns: AttributeSet,
      relation: CarbonDatasourceHadoopRelation): Boolean = {
    val map = relation.carbonRelation.metaData.dictionaryMap
    var count = 0
    projectColumns.foreach{c =>
      if (map.get(c.name).getOrElse(false)) {
        count += 1
      }
    }
    count > CarbonCommonConstants.CARBON_ALLOW_DIRECT_FILL_DICT_COLS_LIMIT
  }

  private def getPartitioning(carbonTable: CarbonTable,
      output: Seq[Attribute]): Partitioning = {
    val info: BucketingInfo = carbonTable.getBucketingInfo(carbonTable.getTableName)
    if (info != null) {
      val cols = info.getListOfColumns.asScala
      val sortColumn = carbonTable.
        getDimensionByTableName(carbonTable.getTableName).get(0).getColName
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

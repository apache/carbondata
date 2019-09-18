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

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.statusmanager.{FileFormat => CarbonFileFormat, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, SessionParams, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath

object MixedFormatHandler {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  val supportedFormats: Seq[String] =
    Seq("carbon", "carbondata", "parquet", "orc", "json", "csv", "text")

  def validateFormat(format: String): Boolean = {
    supportedFormats.exists(_.equalsIgnoreCase(format))
  }

  def getSchema(sparkSession: SparkSession,
      options: Map[String, String],
      segPath: String): StructType = {
    val format = options.getOrElse("format", "carbondata")
    if ((format.equals("carbondata") || format.equals("carbon"))) {
      new SparkCarbonFileFormat().inferSchema(sparkSession, options, Seq.empty).get
    } else {
      val filePath = FileFactory.addSchemeIfNotExists(segPath.replace("\\", "/"))
      val path = new Path(filePath)
      val fs = path.getFileSystem(SparkSQLUtil.sessionState(sparkSession).newHadoopConf())
      val status = fs.listStatus(path, new PathFilter {
        override def accept(path: Path): Boolean = {
          !path.getName.equals("_SUCCESS") && !path.getName.endsWith(".crc")
        }
      })
      getFileFormat(new CarbonFileFormat(format)).inferSchema(sparkSession, options, status).get
    }
  }

  def extraRDD(l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      readCommittedScope: ReadCommittedScope,
      identier: AbsoluteTableIdentifier,
      supportBatch: Boolean = true): Option[(RDD[InternalRow], Boolean)] = {
    val loadMetadataDetails = readCommittedScope.getSegmentList
    val segsToAccess = getSegmentsToAccess(identier)
    val rdds = loadMetadataDetails.filterNot(l =>
      l.getFileFormat.equals(CarbonFileFormat.COLUMNAR_V3) ||
      l.getFileFormat.equals(CarbonFileFormat.ROW_V1) &&
      (!(l.getSegmentStatus.equals(SegmentStatus.SUCCESS) &&
         l.getSegmentStatus.equals(SegmentStatus.LOAD_PARTIAL_SUCCESS))))
      .filter(l => segsToAccess.isEmpty || segsToAccess.contains(l.getLoadName))
      .groupBy(_.getFileFormat)
      .map { case (format, detailses) =>
        val paths = detailses.flatMap { d =>
          SegmentFileStore.readSegmentFile(CarbonTablePath.getSegmentFilePath(readCommittedScope
            .getFilePath, d.getSegmentFile)).getLocationMap.asScala.flatMap { case (p, f) =>
            f.getFiles.asScala.map { ef =>
              p + CarbonCommonConstants.FILE_SEPARATOR + ef
            }.toSeq
          }.toSeq
        }.map(new Path(_))

        val fileFormat = getFileFormat(format, supportBatch)
        getRDDForExternalSegments(l, projects, filters, fileFormat, paths)
      }
    if (rdds.nonEmpty) {
      if (rdds.size == 1) {
        Some(rdds.head)
      } else {
        if (supportBatch && rdds.exists(!_._2)) {
          extraRDD(l, projects, filters, readCommittedScope, identier, false)
        } else {
          var rdd: RDD[InternalRow] = null
          rdds.foreach { r =>
            if (rdd == null) {
              rdd = r._1
            } else {
              rdd = rdd.union(r._1)
            }
          }
          Some(rdd, !rdds.exists(!_._2))
        }
      }
    } else {
      None
    }
  }

  def getFileFormat(fileFormat: CarbonFileFormat, supportBatch: Boolean = true): FileFormat = {
    if (fileFormat.equals(new CarbonFileFormat("parquet"))) {
      new ExtendedParquetFileFormat(supportBatch)
    } else if (fileFormat.equals(new CarbonFileFormat("orc"))) {
      new ExtendedOrcFileFormat(supportBatch)
    } else if (fileFormat.equals(new CarbonFileFormat("json"))) {
      new JsonFileFormat
    } else if (fileFormat.equals(new CarbonFileFormat("csv"))) {
      new CSVFileFormat
    } else if (fileFormat.equals(new CarbonFileFormat("text"))) {
      new TextFileFormat
    } else {
      throw new UnsupportedOperationException("Format not supported " + fileFormat)
    }
  }

  class ExtendedParquetFileFormat(supportBatch: Boolean) extends ParquetFileFormat {
    override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
      super.supportBatch(sparkSession, schema) && supportBatch
    }
  }

  class ExtendedOrcFileFormat(supportBatch: Boolean) extends OrcFileFormat {
    override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
      super.supportBatch(sparkSession, schema) && supportBatch
    }
  }


  def getRDDForExternalSegments(l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      fileFormat: FileFormat,
      paths: Seq[Path]): (RDD[InternalRow], Boolean) = {
    val sparkSession = l.relation.sqlContext.sparkSession
    val fs = paths.head.getFileSystem(SparkSQLUtil.sessionState(sparkSession).newHadoopConf())
    val s = paths.map { f =>
      fs.getFileStatus(f)
    }
    val fsRelation = l.catalogTable match {
      case Some(catalogTable) =>
        val fileIndex =
          new InMemoryFileIndex(sparkSession, paths, catalogTable.storage.properties, None)
        HadoopFsRelation(
          fileIndex,
          catalogTable.partitionSchema,
          catalogTable.schema,
          catalogTable.bucketSpec,
          fileFormat,
          catalogTable.storage.properties)(sparkSession)
      case _ =>
        HadoopFsRelation(
          new InMemoryFileIndex(sparkSession, Seq.empty, Map.empty, None),
          new StructType(),
          l.relation.schema,
          None,
          fileFormat,
          null)(sparkSession)
    }

    // Filters on this relation fall into four categories based on where we can use them to avoid
    // reading unneeded data:
    //  - partition keys only - used to prune directories to read
    //  - bucket keys only - optionally used to prune files to read
    //  - keys stored in the data only - optionally used to skip groups of data in files
    //  - filters that need to be evaluated again after the scan
    val filterSet = ExpressionSet(filters)

    // The attribute name of predicate could be different than the one in schema in case of
    // case insensitive, we should change them to match the one in schema, so we do not need to
    // worry about case sensitivity anymore.
    val normalizedFilters = filters.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(l.output.find(_.semanticEquals(a)).get.name)
      }
    }

    val partitionColumns =
      l.resolve(
        fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
    val partitionSet = AttributeSet(partitionColumns)
    val partitionKeyFilters =
      ExpressionSet(normalizedFilters
        .filter(_.references.subsetOf(partitionSet)))

    LOGGER.info(s"Pruning directories with: ${ partitionKeyFilters.mkString(",") }")

    val dataColumns =
      l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

    // Partition keys are not available in the statistics of the files.
    val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

    // Predicates with both partition keys and attributes need to be evaluated after the scan.
    val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
    LOGGER.info(s"Post-Scan Filters: ${ afterScanFilters.mkString(",") }")
    val filterAttributes = AttributeSet(afterScanFilters)
    val requiredExpressions = new util.LinkedHashSet[NamedExpression](
        (projects.map(p => dataColumns.find(_.exprId == p.exprId).get) ++
         filterAttributes.map(p => dataColumns.find(_.exprId == p.exprId).get)).asJava
    ).asScala.toSeq
    val readDataColumns =
      requiredExpressions.filterNot(partitionColumns.contains).asInstanceOf[Seq[Attribute]]
    val outputSchema = readDataColumns.toStructType
    LOGGER.info(s"Output Data Schema: ${ outputSchema.simpleString(5) }")

    val outputAttributes = readDataColumns ++ partitionColumns

    val scan =
      FileSourceScanExec(
        fsRelation,
        outputAttributes,
        outputSchema,
        partitionKeyFilters.toSeq,
        dataFilters,
        l.catalogTable.map(_.identifier))
    val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
    val withFilter = afterScanFilter.map(execution.FilterExec(_, scan)).getOrElse(scan)
    val withProjections = if (projects == withFilter.output) {
      withFilter
    } else {
      execution.ProjectExec(projects, withFilter)
    }
    (withProjections.inputRDDs().head, fileFormat.supportBatch(sparkSession, outputSchema))
  }

  def getSegmentsToAccess(identifier: AbsoluteTableIdentifier): Seq[String] = {
    val carbonSessionInfo: CarbonSessionInfo = {
      var info = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (info == null || info.getSessionParams == null) {
        info = new CarbonSessionInfo
        info.setSessionParams(new SessionParams())
      }
      info.getSessionParams.addProps(CarbonProperties.getInstance().getAddedProperty)
      info
    }
    val tableUniqueKey = identifier.getDatabaseName + "." + identifier.getTableName
    val inputSegmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS + tableUniqueKey
    val segmentsStr = carbonSessionInfo.getThreadParams
      .getProperty(inputSegmentsKey, carbonSessionInfo.getSessionParams
        .getProperty(inputSegmentsKey,
          CarbonProperties.getInstance().getProperty(inputSegmentsKey, "*")))
    if (!segmentsStr.equals("*")) {
      segmentsStr.split(",")
    } else {
      Seq.empty
    }
  }
}

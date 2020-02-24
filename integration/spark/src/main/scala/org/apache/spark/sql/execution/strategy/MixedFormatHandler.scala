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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{MixedFormatHandlerUtil, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}
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
import org.apache.carbondata.core.statusmanager.{FileFormat => FileFormatName, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, SessionParams, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath

object MixedFormatHandler {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  val supportedFormats: Seq[String] =
    Seq("carbon", "carbondata", "parquet", "orc", "json", "csv", "text")

  def validateFormat(format: String): Boolean = {
    supportedFormats.exists(_.equalsIgnoreCase(format))
  }

  /**
   * collect schema, list of last level directory and list of all data files under given path
   *
   * @param sparkSession spark session
   * @param options option for ADD SEGMENT
   * @param inputPath under which path to collect
   * @return schema of the data file, map of last level directory (partition folder) to its
   *         children file list (data files)
   */
  def collectInfo(
      sparkSession: SparkSession,
      options: Map[String, String],
      inputPath: String): (StructType, mutable.Map[String, Seq[FileStatus]]) = {
    val path = new Path(inputPath)
    val fs = path.getFileSystem(SparkSQLUtil.sessionState(sparkSession).newHadoopConf())
    val rootPath = fs.getFileStatus(path)
    val leafDirFileMap = collectAllLeafFileStatus(sparkSession, rootPath, fs)
    val format = options.getOrElse("format", "carbondata").toLowerCase
    val fileFormat = if (format.equalsIgnoreCase("carbondata") ||
                         format.equalsIgnoreCase("carbon")) {
      new SparkCarbonFileFormat()
    } else {
      getFileFormat(new FileFormatName(format))
    }
    if (leafDirFileMap.isEmpty) {
      throw new RuntimeException("no partition data is found")
    }
    val schema = fileFormat.inferSchema(sparkSession, options, leafDirFileMap.head._2).get
    (schema, leafDirFileMap)
  }

  /**
   * collect leaf directories and leaf files recursively in given path
   *
   * @param sparkSession spark session
   * @param path path to collect
   * @param fs hadoop file system
   * @return mapping of leaf directory to its children files
   */
  private def collectAllLeafFileStatus(
      sparkSession: SparkSession,
      path: FileStatus,
      fs: FileSystem): mutable.Map[String, Seq[FileStatus]] = {
    val directories: ArrayBuffer[FileStatus] = ArrayBuffer()
    val leafFiles: ArrayBuffer[FileStatus] = ArrayBuffer()
    val lastLevelFileMap = mutable.Map[String, Seq[FileStatus]]()

    // get all files under input path
    val fileStatus = fs.listStatus(path.getPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        !path.getName.equals("_SUCCESS") && !path.getName.endsWith(".crc")
      }
    })
    // collect directories and files
    fileStatus.foreach { file =>
      if (file.isDirectory) directories.append(file)
      else leafFiles.append(file)
    }
    if (leafFiles.nonEmpty) {
      // leaf file is found, so parent folder (input parameter) is the last level dir
      val updatedPath = FileFactory.getUpdatedFilePath(path.getPath.toString)
      lastLevelFileMap.put(updatedPath, leafFiles)
      lastLevelFileMap
    } else {
      // no leaf file is found, for each directory, collect recursively
      directories.foreach { dir =>
        val map = collectAllLeafFileStatus(sparkSession, dir, fs)
        lastLevelFileMap ++= map
      }
      lastLevelFileMap
    }
  }

  /**
   * Generates the RDD for non carbon segments. It uses the spark underlying fileformats and
   * generates the RDD in its native format without changing any of its flow to keep the original
   * performance and features.
   *
   * If multiple segments are with different formats like parquet , orc etc then it creates RDD for
   * each format segments and union them.
   */
  def extraRDD(
      l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      readCommittedScope: ReadCommittedScope,
      identier: AbsoluteTableIdentifier,
      supportBatch: Boolean = true): Option[(RDD[InternalRow], Boolean)] = {
    val loadMetadataDetails = readCommittedScope.getSegmentList
    val segsToAccess = getSegmentsToAccess(identier)
    val rdds = loadMetadataDetails.filter(metaDetail =>
      (metaDetail.getSegmentStatus.equals(SegmentStatus.SUCCESS) ||
       metaDetail.getSegmentStatus.equals(SegmentStatus.LOAD_PARTIAL_SUCCESS)))
      .filterNot(currLoad =>
        currLoad.getFileFormat.equals(FileFormatName.COLUMNAR_V3) ||
        currLoad.getFileFormat.equals(FileFormatName.ROW_V1))
      .filter(l => segsToAccess.isEmpty || segsToAccess.contains(l.getLoadName))
      .groupBy(_.getFileFormat)
      .map { case (format, detailses) =>
        // collect paths as input to scan RDD
        val paths = detailses. flatMap { d =>
          val segmentFile = SegmentFileStore.readSegmentFile(
            CarbonTablePath.getSegmentFilePath(readCommittedScope.getFilePath, d.getSegmentFile))

          // If it is a partition table, the path to create RDD should be the root path of the
          // partition folder (excluding the partition subfolder).
          // If it is not a partition folder, collect all data file paths
          if (segmentFile.getOptions.containsKey("partition")) {
            val segmentPath = segmentFile.getOptions.get("path")
            if (segmentPath == null) {
              throw new RuntimeException("invalid segment file, 'path' option not found")
            }
            Seq(new Path(segmentPath))
          } else {
            // If it is not a partition folder, collect all data file paths to create RDD
            segmentFile.getLocationMap.asScala.flatMap { case (p, f) =>
              f.getFiles.asScala.map { ef =>
                new Path(p + CarbonCommonConstants.FILE_SEPARATOR + ef)
              }.toSeq
            }.toSeq
          }
        }
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
          Some(rdd, rdds.forall(_._2))
        }
      }
    } else {
      None
    }
  }

  def getFileFormat(fileFormat: FileFormatName, supportBatch: Boolean = true): FileFormat = {
    if (fileFormat.equals(new FileFormatName("parquet"))) {
      new ExtendedParquetFileFormat(supportBatch)
    } else if (fileFormat.equals(new FileFormatName("orc"))) {
      new ExtendedOrcFileFormat(supportBatch)
    } else if (fileFormat.equals(new FileFormatName("json"))) {
      new JsonFileFormat
    } else if (fileFormat.equals(new FileFormatName("csv"))) {
      new CSVFileFormat
    } else if (fileFormat.equals(new FileFormatName("text"))) {
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

  /**
   * Generates the RDD using the spark fileformat.
   */
  private def getRDDForExternalSegments(l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      fileFormat: FileFormat,
      paths: Seq[Path]): (RDD[InternalRow], Boolean) = {
    val sparkSession = l.relation.sqlContext.sparkSession
    val fsRelation = l.catalogTable match {
      case Some(catalogTable) =>
        val fileIndex =
          new InMemoryFileIndex(sparkSession, paths, catalogTable.storage.properties, None)
        // exclude the partition in data schema
        val dataSchema = catalogTable.schema.filterNot { column =>
          catalogTable.partitionColumnNames.contains(column.name)}
        HadoopFsRelation(
          fileIndex,
          catalogTable.partitionSchema,
          new StructType(dataSchema.toArray),
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
      (projects.flatMap(p => findAttribute(dataColumns, p)) ++
       filterAttributes.map(p => dataColumns.find(_.exprId.equals(p.exprId)).get)).asJava
    ).asScala.toSeq
    val readDataColumns =
      requiredExpressions.filterNot(partitionColumns.contains).asInstanceOf[Seq[Attribute]]
    val outputSchema = readDataColumns.toStructType
    LOGGER.info(s"Output Data Schema: ${ outputSchema.simpleString(5) }")

    val outputAttributes = readDataColumns ++ partitionColumns

    val scan =
      MixedFormatHandlerUtil.getScanForSegments(
        fsRelation,
        outputAttributes,
        outputSchema,
        partitionKeyFilters.toSeq,
        dataFilters,
        l.catalogTable.map(_.identifier))
    val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
    val withFilter = afterScanFilter.map(FilterExec(_, scan)).getOrElse(scan)
    val withProjections = if (projects == withFilter.output) {
      withFilter
    } else {
      ProjectExec(projects, withFilter)
    }
    (withProjections.inputRDDs().head, fileFormat.supportBatch(sparkSession, outputSchema))
  }

  // This function is used to get the unique columns based on expression Id from
  // filters and the projections list
  def findAttribute(dataColumns: Seq[Attribute], p: Expression): Seq[Attribute] = {
    dataColumns.find {
      x =>
        val attr = findAttributeReference(p)
        attr.isDefined && x.exprId.equals(attr.get.exprId)
    } match {
      case Some(c) => Seq(c)
      case None => Seq()
    }
  }

  private def findAttributeReference(p: Expression): Option[NamedExpression] = {
    p match {
      case a: AttributeReference =>
        Some(a)
      case al =>
        if (al.children.nonEmpty) {
          al.children.map(findAttributeReference).head
        } else {
          None
        }
      case _ => None
    }
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

  /**
   * Returns true if any other non-carbon format segment exists
   */
  def otherFormatSegmentsExist(metadataPath: String): Boolean = {
    val allSegments = SegmentStatusManager.readLoadMetadata(metadataPath)
    allSegments.exists(a => a.getFileFormat != null && !a.isCarbonFormat)
  }
}

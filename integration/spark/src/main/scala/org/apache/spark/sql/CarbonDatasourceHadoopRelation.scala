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

package org.apache.spark.sql

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.ClassTag
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, JobID}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.sources.{Filter, HadoopFsRelation, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier
import org.apache.carbondata.core.carbon.path.CarbonTablePath
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.hadoop.util.{CarbonInputFormatUtil, SchemaReader}
import org.apache.carbondata.integration.spark.merger.TableMeta
import org.apache.carbondata.scan.expression.logical.AndExpression
import org.apache.carbondata.spark.{CarbonFilters, CarbonOption}
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl


private[sql] case class CarbonDatasourceHadoopRelation(
  sqlContext: SQLContext,
  paths: Array[String],
  parameters: Map[String, String],
  tableSchema: Option[StructType])
  extends HadoopFsRelation {

  lazy val schemaPath = new Path(CarbonTablePath.getSchemaFilePath(paths.head))
  if (!schemaPath.getFileSystem(new Configuration).exists(schemaPath)) {
    throw new IllegalArgumentException("invalid CarbonData file path: " + paths.head)
  }

  lazy val job = new Job(new JobConf())
  lazy val options = new CarbonOption(parameters)
  lazy val absIdentifier = AbsoluteTableIdentifier.fromTablePath(paths.head)
  lazy val relationRaw: CarbonRelation = {
    val carbonTable = SchemaReader.readCarbonTableFromStore(absIdentifier)
    if (carbonTable == null) {
      sys.error(s"CarbonData file path ${paths.head} is not valid")
    }
    CarbonRelation(
      carbonTable.getDatabaseName,
      carbonTable.getFactTableName,
      CarbonSparkUtil.createSparkMeta(carbonTable),
      new TableMeta(absIdentifier.getCarbonTableIdentifier, paths.head, carbonTable),
      None
    )(sqlContext)
  }

  override def dataSchema: StructType = tableSchema.getOrElse(relationRaw.schema)

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    // TODO
    throw new UnsupportedOperationException
  }

  override def buildScan(
    requiredColumns: Array[String],
    filters: Array[Filter],
    inputFiles: Array[FileStatus]): RDD[Row] = {
    val conf = new Configuration(job.getConfiguration)
    filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(dataSchema, filter)
    }.reduceOption(new AndExpression(_, _))
      .foreach(CarbonInputFormat.setFilterPredicates(conf, _))

    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)
    CarbonInputFormat.setColumnProjection(conf, projection)
    CarbonInputFormat.setCarbonReadSupport(classOf[SparkRowReadSupportImpl], conf)

    new CarbonHadoopFSRDD[Row](sqlContext.sparkContext,
      new SerializableConfiguration(conf),
      absIdentifier,
      classOf[CarbonInputFormat[Row]],
      classOf[Row]
    )
  }

}

class CarbonHadoopFSPartition(rddId: Int, val idx: Int,
  val carbonSplit: SerializableWritable[CarbonInputSplit])
  extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

class CarbonHadoopFSRDD[V: ClassTag](
  @transient sc: SparkContext,
  conf: SerializableConfiguration,
  identifier: AbsoluteTableIdentifier,
  inputFormatClass: Class[_ <: CarbonInputFormat[V]],
  valueClass: Class[V])
  extends RDD[V](sc, Nil) with SparkHadoopMapReduceUtil {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }
  @transient protected val jobId = new JobID(jobTrackerId, id)

  @DeveloperApi
  override def compute(split: Partition,
    context: TaskContext): Iterator[V] = {
    val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
    val hadoopAttemptContext = newTaskAttemptContext(conf.value, attemptId)
    val job: Job = new Job(hadoopAttemptContext.getConfiguration)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(identifier, job)
    hadoopAttemptContext.getConfiguration.set(FileInputFormat.INPUT_DIR, identifier.getTablePath)
    val reader =
      format.createRecordReader(split.asInstanceOf[CarbonHadoopFSPartition].carbonSplit.value,
        hadoopAttemptContext
      )
    reader.initialize(split.asInstanceOf[CarbonHadoopFSPartition].carbonSplit.value,
      hadoopAttemptContext
    )
    new Iterator[V] {
      private[this] var havePair = false
      private[this] var finished = false

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            reader.close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        reader.getCurrentValue
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val jobContext = newJobContext(conf.value, jobId)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(identifier, new Job(conf.value))
    jobContext.getConfiguration.set(FileInputFormat.INPUT_DIR, identifier.getTablePath)
    val splits = format.getSplits(jobContext).toArray
    val carbonInputSplits = splits
      .map(f => new SerializableWritable(f.asInstanceOf[CarbonInputSplit]))
    carbonInputSplits.zipWithIndex.map(f => new CarbonHadoopFSPartition(id, f._2, f._1))
  }
}

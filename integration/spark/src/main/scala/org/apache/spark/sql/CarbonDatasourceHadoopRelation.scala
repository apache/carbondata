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
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job, JobID}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.sql.hive.{CarbonMetastoreCatalog, DistributionUtil, TableMeta}
import org.apache.spark.sql.sources.{Filter, HadoopFsRelation, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit, CarbonProjection}
import org.carbondata.query.expression.logical.AndExpression
import org.carbondata.spark.{CarbonFilters, CarbonOption}
import org.carbondata.spark.readsupport.SparkRowReadSupportImpl
import org.carbondata.spark.util.CarbonScalaUtil.CarbonSparkUtil

private[sql] case class CarbonDatasourceHadoopRelation(sqlContext: SQLContext,
    paths: Array[String],
    parameters: Map[String, String])
  extends HadoopFsRelation {

  val (carbonRelation, jobConf) = {
    val options = new CarbonOption(parameters)
    val job: Job = new Job(new JobConf())
    FileInputFormat.setInputPaths(job, paths.head)
    val identifier = new CarbonTableIdentifier(options.dbName, options.tableName, options.tableId)
    CarbonInputFormat.setTableToAccess(job.getConfiguration, identifier)
    val table = CarbonInputFormat.getCarbonTable(job.getConfiguration)
    if(table == null) {
      sys.error(s"Store path ${paths.head} is not valid or " +
                s"table ${identifier.getTableUniqueName}  does not exist in path.")
    }
    val relation = CarbonRelation(table.getDatabaseName,
      table.getFactTableName,
      CarbonSparkUtil.createSparkMeta(table),
      TableMeta(identifier,
        paths.head,
        table,
        Partitioner(options.partitionClass,
          Array(""),
          options.partitionCount.toInt,
          DistributionUtil.getNodeList(sqlContext.sparkContext))),
      None)(sqlContext)

    (relation, job.getConfiguration)
  }

  def dataSchema: StructType = carbonRelation.schema

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    // TODO: implement it
    throw new UnsupportedOperationException
  }

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus]): RDD[Row] = {
    val conf = new Configuration(jobConf)
    filters.flatMap(f => CarbonFilters.createCarbonFilter(dataSchema, f))
      .reduceOption(new AndExpression(_, _))
      .foreach(CarbonInputFormat.setFilterPredicates(conf, _))
    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)
    CarbonInputFormat.setColumnProjection(projection, conf)
    CarbonInputFormat.setCarbonReadSupport(classOf[SparkRowReadSupportImpl], conf)

    new CarbonHadoopFSRDD[Row](sqlContext.sparkContext,
      new SerializableConfiguration(conf),
      classOf[CarbonInputFormat[Row]],
      classOf[Row])
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
    inputFormatClass: Class[_ <: CarbonInputFormat[V]],
    valueClass: Class[V])
  extends RDD[V](sc, Nil)
    with SparkHadoopMapReduceUtil
    with Logging {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  override protected def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    val jobContext = newJobContext(conf.value, jobId)
    val splits = inputFormat.getSplits(jobContext).toArray
    val carbonInputSplits = splits
      .map(f => new SerializableWritable(f.asInstanceOf[CarbonInputSplit]))
    carbonInputSplits.zipWithIndex.map(f => new CarbonHadoopFSPartition(id, f._2, f._1))
  }

  @DeveloperApi
  override def compute(split: Partition,
      context: TaskContext): Iterator[V] = {
    val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
    val hadoopAttemptContext = newTaskAttemptContext(conf.value, attemptId)
    val inputFormat = inputFormatClass.newInstance
    val reader =
      inputFormat.createRecordReader(split.asInstanceOf[CarbonHadoopFSPartition].carbonSplit.value,
        hadoopAttemptContext)
    reader
      .initialize(split.asInstanceOf[CarbonHadoopFSPartition].carbonSplit.value,
        hadoopAttemptContext)
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
}

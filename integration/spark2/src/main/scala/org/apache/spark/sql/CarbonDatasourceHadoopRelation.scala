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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonProjection}
import org.apache.carbondata.hadoop.util.SchemaReader
import org.apache.carbondata.integration.spark.merger.TableMeta
import org.apache.carbondata.scan.expression.Expression
import org.apache.carbondata.scan.expression.logical.AndExpression
import org.apache.carbondata.spark.CarbonFilters
import org.apache.carbondata.spark.rdd.CarbonScanRDD
import org.apache.carbondata.spark.readsupport.SparkRowReadSupportImpl
import org.apache.carbondata.spark.util.CarbonSparkUtil

private[sql] case class CarbonDatasourceHadoopRelation(
    sparkSession: SparkSession,
    paths: Array[String],
    parameters: Map[String, String],
    tableSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan {

  lazy val absIdentifier = AbsoluteTableIdentifier.fromTablePath(paths.head)
  lazy val carbonTable = SchemaReader.readCarbonTableFromStore(absIdentifier)
  lazy val carbonRelation: CarbonRelation = {
    CarbonRelation(
      carbonTable.getDatabaseName,
      carbonTable.getFactTableName,
      CarbonSparkUtil.createSparkMeta(carbonTable),
      new TableMeta(absIdentifier.getCarbonTableIdentifier, paths.head, carbonTable),
      None
    )
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(carbonRelation.schema)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val job = new Job(new JobConf())
    val conf = new Configuration(job.getConfiguration)
    val filterExpression: Option[Expression] = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(schema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)
    CarbonInputFormat.setColumnProjection(conf, projection)
    CarbonInputFormat.setCarbonReadSupport(classOf[SparkRowReadSupportImpl], conf)

    new CarbonScanRDD[Row](sqlContext.sparkContext, projection, filterExpression.orNull,
      absIdentifier, carbonTable)
  }

}

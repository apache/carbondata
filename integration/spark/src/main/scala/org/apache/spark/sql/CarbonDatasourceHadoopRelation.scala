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

import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

case class CarbonDatasourceHadoopRelation(
    sparkSession: SparkSession,
    paths: Array[String],
    parameters: Map[String, String],
    tableSchema: Option[StructType],
    limit: Int = -1)
  extends BaseRelation {

  val caseInsensitiveMap: Map[String, String] = parameters.map(f => (f._1.toLowerCase, f._2))
  lazy val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
    FileFactory.getUpdatedFilePath(paths.head),
    CarbonEnv.getDatabaseName(caseInsensitiveMap.get("dbname"))(sparkSession),
    caseInsensitiveMap("tablename"))
  CarbonThreadUtil.updateSessionInfoToCurrentThread(sparkSession)

  @transient lazy val carbonRelation: CarbonRelation =
    CarbonEnv.getInstance(sparkSession).carbonMetaStore.
    createCarbonRelation(parameters, identifier, sparkSession)


  @transient lazy val carbonTable: CarbonTable = carbonRelation.carbonTable

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = tableSchema.getOrElse(carbonRelation.schema)

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = new Array[Filter](0)

  override def toString: String = {
    "CarbonDatasourceHadoopRelation"
  }

  override def sizeInBytes: Long = carbonRelation.sizeInBytes
}

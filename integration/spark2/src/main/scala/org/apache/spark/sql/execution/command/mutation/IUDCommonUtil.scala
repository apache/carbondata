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

package org.apache.spark.sql.execution.command.mutation

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.HiveSessionCatalog

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Util for IUD common function
 */
object IUDCommonUtil {

  /**
   * iterates the plan  and check whether CarbonCommonConstants.CARBON_INPUT_SEGMENTS set for any
   * any table or not
   * @param sparkSession
   * @param logicalPlan
   */
  def checkIfSegmentListIsSet(sparkSession: SparkSession, logicalPlan: LogicalPlan): Unit = {
    val carbonProperties = CarbonProperties.getInstance()
    logicalPlan.foreach {
      case unresolvedRelation: UnresolvedRelation =>
        val dbAndTb =
          sparkSession.sessionState.catalog.asInstanceOf[HiveSessionCatalog].getCurrentDatabase +
          "." + unresolvedRelation.tableIdentifier.table
        val segmentProperties = carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
        if (!(segmentProperties.equals("") || segmentProperties.trim.equals("*"))) {
          throw new MalformedCarbonCommandException("carbon.input.segments." + dbAndTb +
                                                    "should not be set for table used in DELETE " +
                                                    "query. Please reset the property to carbon" +
                                                    ".input.segments." +
                                                    dbAndTb + "=*")
        }
      case logicalRelation: LogicalRelation if (logicalRelation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        val dbAndTb =
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .getDatabaseName + "." +
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .getTableName
        val sementProperty = carbonProperties
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
        if (!(sementProperty.equals("") || sementProperty.trim.equals("*"))) {
          throw new MalformedCarbonCommandException("carbon.input.segments." + dbAndTb +
                                                    "should not be set for table used in UPDATE " +
                                                    "query. Please reset the property to carbon" +
                                                    ".input.segments." +
                                                    dbAndTb + "=*")
        }
      case filter: Filter => filter.subqueries.toList
        .foreach(subquery => checkIfSegmentListIsSet(sparkSession, subquery))
      case _ =>
    }
  }
}

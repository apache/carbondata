/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.query.querystats.{QueryDetail, QueryStatsCollector}

/**
  * This plan is used by select queries to collect statistics for each query
  */
case class QueryStatsSparkPlan(child: SparkPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {

    child.execute()
  }

  override def executeCollect(): Array[Row] = {
    //queryId will be unique for each query, creting query detail holder
    val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
    val queryId: String = System.nanoTime() + ""
    val queryDetail: QueryDetail = new QueryDetail(queryId)
    queryStatsCollector.addQueryStats(queryId, queryDetail)
    sqlContext.setConf("queryId", queryId)

    val startTime = System.currentTimeMillis();
    //executing query
    val rows: Array[Row] = super.executeCollect()

    val endTime = System.currentTimeMillis()

    //log query data
    logQueryDetails(queryId, startTime, endTime, rows)

    //return the result
    rows
  }

  def logQueryDetails(queryId: String, startTime: Long, endTime: Long, rows: Array[Row]) {
    val LOGGER = LogServiceFactory.getLogService(QueryStatsSparkPlan.getClass().getName());
    try {
      val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
      val queryDetail: QueryDetail = queryStatsCollector.getQueryStats(queryId)
      val timeTaken = System.currentTimeMillis() - startTime;

      queryDetail.setQueryStartTime(startTime)
      queryDetail.setTotalExecutionTime(timeTaken)
      queryDetail.setRecordSize(rows.length)
      val partAcc = queryDetail.getPartitionsDetail
      if (null != partAcc && null != partAcc.value) {
        queryDetail.setNoOfNodesScanned(partAcc.value.getNumberOfNodesScanned)
        queryDetail.setNoOfRowsScanned(partAcc.value.getNoOfRowsScanned)
        //log query stats only if there is dimensions included in query
        if (null != queryDetail.getDimOrdinals && queryDetail.getDimOrdinals.length > 0) {
          queryStatsCollector.logQueryStats(queryDetail);
        }
      }

      //remove this query from cache
      queryStatsCollector.removeQueryStats(queryId)
    }
    catch {
      case e: Exception => LOGGER.audit("Error in logging query details" + e.getMessage)
    }


  }

  override protected def doPrepare(): Unit = {
    super.prepare()
  }

}
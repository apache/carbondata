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

package org.apache.spark.search

import org.apache.spark.SerializableWritable
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit
import org.apache.carbondata.store.worker.SearchRequestHandler

/**
 * Search service implementation
 */
class Searcher(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getName)

  override def onStart(): Unit = {
    LOG.info("Searcher Endpoint started")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req: SearchRequest =>
      val response = new SearchRequestHandler().handleSearch(req)
      context.reply(response)

    case req: ShutdownRequest =>
      val response = new SearchRequestHandler().handleShutdown(req)
      context.reply(response)

  }

  override def onStop(): Unit = {
    LOG.info("Searcher Endpoint stopped")
  }
}

// Search request sent from master to worker
case class SearchRequest(
    searchId: Int,
    split: SerializableWritable[CarbonMultiBlockSplit],
    tableInfo: TableInfo,
    projectColumns: Array[String],
    filterExpression: Expression,
    limit: Long)

// Search result sent from worker to master
case class SearchResult(
    queryId: Int,
    status: Int,
    message: String,
    rows: Array[Array[Object]])

// Shutdown request sent from master to worker
case class ShutdownRequest(
    reason: String)

// Shutdown response sent from worker to master
case class ShutdownResponse(
    status: Int,
    message: String)

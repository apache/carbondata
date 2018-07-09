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

import org.apache.spark.rpc.{Master, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 * Registry service implementation. It adds worker to master.
 */
class Registry(override val rpcEnv: RpcEnv, master: Master) extends RpcEndpoint {
  private val LOG = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  override def onStart(): Unit = {
    LOG.info("Registry Endpoint started")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case req@RegisterWorkerRequest(_, _, _) =>
      val response = master.addWorker(req)
      context.reply(response)
  }

  override def onStop(): Unit = {
    LOG.info("Registry Endpoint stopped")
  }

}

case class RegisterWorkerRequest(
    hostAddress: String,
    port: Int,
    cores: Int)

case class RegisterWorkerResponse(
    workerId: String)

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

package org.apache.carbondata.store.impl.worker;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.store.impl.rpc.StoreService;
import org.apache.carbondata.store.impl.rpc.model.BaseResponse;
import org.apache.carbondata.store.impl.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.impl.rpc.model.QueryResponse;
import org.apache.carbondata.store.impl.rpc.model.Scan;
import org.apache.carbondata.store.impl.rpc.model.ShutdownRequest;
import org.apache.carbondata.store.impl.rpc.model.ShutdownResponse;

import org.apache.hadoop.ipc.ProtocolSignature;

@InterfaceAudience.Internal
public class StoreServiceImpl implements StoreService {

  private Worker worker;
  RequestHandler handler;

  public StoreServiceImpl(Worker worker) {
    this.worker = worker;
    this.handler = new RequestHandler(worker.getConf(), worker.getHadoopConf());
  }

  @Override
  public BaseResponse loadData(LoadDataRequest request) {
    return handler.handleLoadData(request);
  }

  @Override
  public QueryResponse query(Scan scan) {
    return handler.handleScan(scan);
  }

  @Override
  public ShutdownResponse shutdown(ShutdownRequest request) {
    return handler.handleShutdown(request);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return versionID;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException {
    return null;
  }

  public Worker getWorker() {
    return worker;
  }

  public void setWorker(Worker worker) {
    this.worker = worker;
  }
}

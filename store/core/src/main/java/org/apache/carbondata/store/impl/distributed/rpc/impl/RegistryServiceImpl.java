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

package org.apache.carbondata.store.impl.distributed.rpc.impl;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.store.impl.distributed.Master;
import org.apache.carbondata.store.impl.distributed.rpc.RegistryService;
import org.apache.carbondata.store.impl.distributed.rpc.model.RegisterWorkerRequest;
import org.apache.carbondata.store.impl.distributed.rpc.model.RegisterWorkerResponse;

import org.apache.hadoop.ipc.ProtocolSignature;

@InterfaceAudience.Internal
public class RegistryServiceImpl implements RegistryService {

  private Master master;

  public RegistryServiceImpl(Master master) {
    this.master = master;
  }

  @Override
  public RegisterWorkerResponse registerWorker(RegisterWorkerRequest request) throws IOException {
    return master.addWorker(request);
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
}

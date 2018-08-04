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

package org.apache.carbondata.store.impl.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.carbondata.common.annotations.InterfaceAudience;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

@InterfaceAudience.Internal
public class ServiceFactory {

  public static RegistryService createRegistryService(String host, int port) throws IOException {
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(host), port);
    return RPC.getProxy(
        RegistryService.class, RegistryService.versionID, address, new Configuration());
  }

  public static PruneService createPruneService(String host, int port) throws IOException {
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(host), port);
    return RPC.getProxy(
        PruneService.class, PruneService.versionID, address, new Configuration());
  }

  public static DataService createDataService(String host, int port) throws IOException {
    InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(host), port);
    return RPC.getProxy(
        DataService.class, DataService.versionID, address, new Configuration());
  }

}

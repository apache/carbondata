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

package org.apache.carbondata.store.impl.distributed;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.store.impl.distributed.rpc.StoreService;

public class Schedulable {

  private String id;
  private String address;
  private int port;
  private int cores;
  public StoreService service;
  public AtomicInteger workload;

  public Schedulable(String id, String address, int port, int cores, StoreService service) {
    this.id = id;
    this.address = address;
    this.port = port;
    this.cores = cores;
    this.service = service;
    this.workload = new AtomicInteger();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  int getCores() {
    return cores;
  }

  @Override public String toString() {
    return "Schedulable{" + "id='" + id + '\'' + ", address='" + address + '\'' + ", port=" + port
        + '}';
  }
}

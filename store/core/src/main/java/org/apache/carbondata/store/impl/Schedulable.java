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

package org.apache.carbondata.store.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.annotations.InterfaceAudience;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.Internal
public class Schedulable implements Writable, Serializable {

  private String id;
  private String address;
  private int port;
  private int cores;
  public AtomicInteger workload;

  public Schedulable() {
  }

  public Schedulable(String id, String address, int port, int cores) {
    this.id = id;
    this.address = address;
    this.port = port;
    this.cores = cores;
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

  public int getCores() {
    return cores;
  }

  @Override public String toString() {
    return "Schedulable{" + "id='" + id + '\'' + ", address='" + address + '\'' + ", port=" + port
        + '}';
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeUTF(address);
    out.writeInt(port);
    out.writeInt(cores);
    // We are not writing workload since it is only useful for
    // Scheduler inside the Master. Client of the Master does
    // not need it
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readUTF();
    address = in.readUTF();
    port = in.readInt();
    cores = in.readInt();
  }
}

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

package org.apache.carbondata.store.rpc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;

import org.apache.hadoop.io.Writable;

@InterfaceAudience.Internal
public class RegisterWorkerRequest implements Serializable, Writable {
  private String hostAddress;
  private int port;
  private int cores;

  public RegisterWorkerRequest() {
  }

  public RegisterWorkerRequest(String hostAddress, int port, int cores) {
    this.hostAddress = hostAddress;
    this.port = port;
    this.cores = cores;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public int getPort() {
    return port;
  }

  public int getCores() {
    return cores;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(hostAddress);
    out.writeInt(port);
    out.writeInt(cores);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    hostAddress = in.readUTF();
    port = in.readInt();
    cores = in.readInt();
  }

  @Override public String toString() {
    return "RegisterWorkerRequest{" + "hostAddress='" + hostAddress + '\'' + ", port=" + port + '}';
  }
}

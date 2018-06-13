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
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

@InterfaceAudience.Internal
public class QueryResponse implements Serializable, Writable {
  private int queryId;
  private int status;
  private String message;
  private Object[][] rows;

  public QueryResponse() {
  }

  public QueryResponse(int queryId, int status, String message, Object[][] rows) {
    this.queryId = queryId;
    this.status = status;
    this.message = message;
    this.rows = rows;
  }

  public int getQueryId() {
    return queryId;
  }

  public int getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public Object[][] getRows() {
    return rows;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(queryId);
    out.writeInt(status);
    out.writeUTF(message);
    WritableUtils.writeCompressedByteArray(out, ObjectSerializationUtil.serialize(rows));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    queryId = in.readInt();
    status = in.readInt();
    message = in.readUTF();
    try {
      rows = (Object[][])ObjectSerializationUtil.deserialize(
          WritableUtils.readCompressedByteArray(in));
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}

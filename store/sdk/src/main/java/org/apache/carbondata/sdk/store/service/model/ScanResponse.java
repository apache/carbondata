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

package org.apache.carbondata.sdk.store.service.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

@InterfaceAudience.Internal
public class ScanResponse extends BaseResponse implements Serializable, Writable {
  private int queryId;
  private Object[][] rows;

  public ScanResponse() {
    super();
  }

  public ScanResponse(int queryId, int status, String message, Object[][] rows) {
    super(status, message);
    this.queryId = queryId;
    this.rows = rows;
  }

  public int getQueryId() {
    return queryId;
  }


  public Object[][] getRows() {
    return rows;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(queryId);
    WritableUtils.writeCompressedByteArray(out, ObjectSerializationUtil.serialize(rows));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    queryId = in.readInt();
    try {
      rows = (Object[][])ObjectSerializationUtil.deserialize(
          WritableUtils.readCompressedByteArray(in));
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}

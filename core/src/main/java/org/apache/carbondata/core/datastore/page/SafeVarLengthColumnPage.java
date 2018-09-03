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

package org.apache.carbondata.core.datastore.page;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;

public class SafeVarLengthColumnPage extends VarLengthColumnPageBase {

  // for string and decimal data
  private List<byte[]> byteArrayData;

  SafeVarLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    byteArrayData = new ArrayList<>();
  }

  @Override
  public void freeMemory() {
    byteArrayData = null;
    super.freeMemory();
  }

  @Override
  public void putBytesAtRow(int rowId, byte[] bytes) {
    byteArrayData.add(bytes);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    byteArrayData.add(bytes);
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    return byteArrayData.get(rowId);
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    for (byte[] data : byteArray) {
      byteArrayData.add(data);
    }
  }

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    for (byte[] byteArrayDatum : byteArrayData) {
      out.writeInt(byteArrayDatum.length);
      out.write(byteArrayDatum);
    }
    return stream.toByteArray();
  }

  @Override
  public byte[] getComplexChildrenLVFlattenedBytePage() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    for (byte[] byteArrayDatum : byteArrayData) {
      out.writeShort((short)byteArrayDatum.length);
      out.write(byteArrayDatum);
    }
    return stream.toByteArray();
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    for (byte[] byteArrayDatum : byteArrayData) {
      out.write(byteArrayDatum);
    }
    return stream.toByteArray();
  }

  @Override
  public byte[][] getByteArrayPage() {
    return byteArrayData.toArray(new byte[byteArrayData.size()][]);
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    System.arraycopy(byteArrayData.get(rowId), 0, dest, destOffset, length);
  }

}

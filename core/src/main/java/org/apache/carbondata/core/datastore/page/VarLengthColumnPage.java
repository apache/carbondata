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

import org.apache.carbondata.core.metadata.datatype.DataType;

// Represent a variable length columnar data in one page, e.g. for dictionary columns.
public class VarLengthColumnPage extends ColumnPage {

  // TODO: further optimizite it, to store length and data separately
  private byte[][] byteArrayData;

  public VarLengthColumnPage(int pageSize) {
    super(DataType.BYTE_ARRAY, pageSize);
    byteArrayData = new byte[pageSize][];
  }

  public void putByteArray(int rowId, byte[] value) {
    byteArrayData[rowId] = value;
    updateStatistics(value);
  }

  public byte[][] getByteArrayPage() {
    return byteArrayData;
  }

}

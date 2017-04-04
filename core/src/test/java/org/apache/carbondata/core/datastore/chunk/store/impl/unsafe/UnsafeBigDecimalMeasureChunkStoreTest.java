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
package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.util.DataTypeUtil;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class UnsafeBigDecimalMeasureChunkStoreTest {

  static UnsafeBigDecimalMeasureChunkStore unsafeBigDecimalMeasureChunkStore;

  @BeforeClass public static void setup() {
    unsafeBigDecimalMeasureChunkStore = new UnsafeBigDecimalMeasureChunkStore(2);
  }

  @Test public void putDataTest() throws IOException {
    byte[] val1 = DataTypeUtil.bigDecimalToByte(new BigDecimal(0.5));
    byte[] val2 = DataTypeUtil.bigDecimalToByte(new BigDecimal(0.7));
    byte[] size1 = { 0, 0, 0, 2 };
    byte[] size2 = { 0, 0, 0, 23 };
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(size1);
    outputStream.write(val1);
    outputStream.write(size2);
    outputStream.write(val2);
    byte[] data = outputStream.toByteArray();

    unsafeBigDecimalMeasureChunkStore.putData(data);
    boolean res = unsafeBigDecimalMeasureChunkStore.isMemoryOccupied;
    assert (res);
  }

  @Test public void testGetBigDecimal() {
    BigDecimal res = unsafeBigDecimalMeasureChunkStore.getBigDecimal(0);
    assert (res.equals(new BigDecimal(0.5)));
  }
}
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
package org.apache.carbondata.core.datastore.columnar;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnarKeyStoreDataHolderUnitTest {

  private static ColumnarKeyStoreDataHolder columnarKeyStoreDataHolder;
  private static ColumnarKeyStoreMetadata columnarKeyStoreMetadata;

  @BeforeClass public static void setup() {
    byte[] keyBlockData = new byte[] { 16, 8, 32, 40, 8, 8, 8 };
    int eachRowSize = 2;
    int[] reverseIndex = new int[] { 1, 5, 6, 3, 8 };
    columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(eachRowSize);
    columnarKeyStoreMetadata.setColumnReverseIndex(reverseIndex);
    columnarKeyStoreDataHolder =
        new ColumnarKeyStoreDataHolder(keyBlockData, columnarKeyStoreMetadata);
  }

  @Test public void testGetSurrogateKeyWithNullINGetColumnReverseIndex() {
    byte[] keyBlockData = new byte[] { 16, 8, 32, 40, 8, 8, 8 };
    int eachRowSize = 1;
    ColumnarKeyStoreMetadata columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(eachRowSize);
    ColumnarKeyStoreDataHolder columnarKeyStoreDataHolderNew =
        new ColumnarKeyStoreDataHolder(keyBlockData, columnarKeyStoreMetadata);
    int columnIndex = 5;
    int expected_result = 8;
    int result = columnarKeyStoreDataHolderNew.getSurrogateKey(columnIndex);
    assertEquals(expected_result, result);
  }

  @Test public void testGetSurrogateKeyWithNullINGetColumnReverseIndexAndRowSizeTwo() {
    byte[] keyBlockData = new byte[] { 16, 8, 32, 40, 8, 8, 8 };
    int eachRowSize = 2;
    ColumnarKeyStoreMetadata columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(eachRowSize);
    ColumnarKeyStoreDataHolder columnarKeyStoreDataHolderNew =
        new ColumnarKeyStoreDataHolder(keyBlockData, columnarKeyStoreMetadata);
    int columnIndex = 0;
    int expected_result = 4104;
    int result = columnarKeyStoreDataHolderNew.getSurrogateKey(columnIndex);
    assertEquals(expected_result, result);
  }

  @Test public void testGetSurrogateKeyWithNotNullINGetColumnReverseIndex() {
    int columnIndex = 0;
    int expected_result = 8232;
    int result = columnarKeyStoreDataHolder.getSurrogateKey(columnIndex);
    assertEquals(expected_result, result);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testExceptionInGetSurrogateKey() {
    int columnIndex = 10;
    int expected_result = 8232;
    int result = columnarKeyStoreDataHolder.getSurrogateKey(columnIndex);
    assertEquals(expected_result, result);
  }

  @Test public void testGetSurrogateKeyWithListOfByteWhileCreatingObject() {
    byte[] keyBlockData = new byte[] { 32, 64, 32, 40, 64, 8, 8 };
    List<byte[]> noDictionaryValBasedKeyBlockData = new java.util.ArrayList<>();
    noDictionaryValBasedKeyBlockData.add(keyBlockData);
    new ColumnarKeyStoreDataHolder(columnarKeyStoreMetadata);
    int columnIndex = 0;
    int expected_result = 8232;
    int result = columnarKeyStoreDataHolder.getSurrogateKey(columnIndex);
    assertEquals(expected_result, result);
  }

}

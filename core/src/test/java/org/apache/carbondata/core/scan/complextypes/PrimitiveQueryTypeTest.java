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

package org.apache.carbondata.core.scan.complextypes;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.cache.dictionary.ColumnDictionaryInfo;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionary;
import org.apache.carbondata.core.keygenerator.mdkey.Bits;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PrimitiveQueryTypeTest {
  private static PrimitiveQueryType primitiveQueryType, primitiveQueryTypeForInt,
      primitiveQueryTypeForLong, primitiveQueryTypeForDouble, primitiveQueryTypeForBoolean,
      primitiveQueryTypeForTimeStamp, primitiveQueryTypeForTimeStampForIsDictionaryFalse;
  private static Dictionary dictionary;
  private boolean isDirectDictionary = false;

  @BeforeClass public static void setUp() {
    String name = "test";
    String parentName = "testParent";
    int blockIndex = 1;
    int keySize = 1;
    boolean isDirectDictionary = true;
    primitiveQueryType =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.STRING, keySize, dictionary,
            isDirectDictionary);
    primitiveQueryTypeForInt =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.INT, keySize, dictionary,
            isDirectDictionary);
    primitiveQueryTypeForDouble =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.DOUBLE, keySize, dictionary,
            isDirectDictionary);
    primitiveQueryTypeForLong =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.LONG, keySize, dictionary,
            isDirectDictionary);
    primitiveQueryTypeForBoolean =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.BOOLEAN, keySize, dictionary,
            isDirectDictionary);
    primitiveQueryTypeForTimeStamp =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.TIMESTAMP, keySize,
            dictionary, isDirectDictionary);
    ColumnDictionaryInfo columnDictionaryInfo = new ColumnDictionaryInfo(DataTypes.STRING);
    ForwardDictionary forwardDictionary = new ForwardDictionary(columnDictionaryInfo);
    primitiveQueryTypeForTimeStampForIsDictionaryFalse =
        new PrimitiveQueryType(name, parentName, blockIndex, DataTypes.TIMESTAMP, keySize,
            forwardDictionary, false);

  }

  @Test public void testGetDataBasedOnDataTypeFromSurrogates() {
    ByteBuffer surrogateData = ByteBuffer.allocate(10);
    surrogateData.put(3, (byte) 1);
    new MockUp<Bits>() {
      @Mock public long[] getKeyArray(byte[] key, int offset) {
        return new long[] { 1313045L };
      }
    };
    Object expectedValue = 1313043000000L;

    Object actualValue =
        primitiveQueryTypeForTimeStamp.getDataBasedOnDataType(surrogateData);
    assertEquals(expectedValue, actualValue);
  }

}

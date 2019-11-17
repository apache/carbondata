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

package org.apache.carbondata.core.localdictionary;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;
import org.apache.carbondata.core.localdictionary.generator.ColumnLocalDictionaryGenerator;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;

import org.junit.Assert;
import org.junit.Test;

public class TestLocalDictionaryGenerator {

  @Test
  public void testColumnLocalDictionaryGeneratorWithValidDataWithinThreshold() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    try {
      for (int i = 1; i <= 1000; i++) {
        generator.generateDictionary(("" + i).getBytes());
      }
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertTrue(false);
    }

    int dictionaryValue = 2;
    for (int i = 1; i <= 1000; i++) {
      byte[] dictionaryKeyBasedOnValue = generator.getDictionaryKeyBasedOnValue(dictionaryValue);
      Assert
          .assertTrue(Arrays.equals(dictionaryKeyBasedOnValue, ("" + i).getBytes()));
      dictionaryValue++;
    }
  }

  @Test
  public void testColumnLocalDictionaryGeneratorWhenThresholdReached_ExceptionShouldBeThrown() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    try {
      for (int i = 1; i <= 10000; i++) {
        generator.generateDictionary(("" + i).getBytes());
      }
      Assert.assertTrue(false);
    } catch (DictionaryThresholdReachedException e) {
      Assert.assertTrue(true);
    }
    Assert.assertTrue(generator.isThresholdReached());
  }

  @Test
  public void testColumnLocalDictionaryGeneratorForNullValueIsPresentWithoutAddingAnyData() {
    LocalDictionaryGenerator generator = new ColumnLocalDictionaryGenerator(1000, 2);
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        2 + CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
    byteBuffer.putShort((short)CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
    byteBuffer.put(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);

    Assert.assertTrue(Arrays.equals(generator.getDictionaryKeyBasedOnValue(1),
        byteBuffer.array()));
  }
}

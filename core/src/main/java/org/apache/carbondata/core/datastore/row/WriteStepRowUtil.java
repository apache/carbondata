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

package org.apache.carbondata.core.datastore.row;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;

// Utility to create and retrieve data from CarbonRow in write step.
public class WriteStepRowUtil {

  // In write step, the element of CarbonRow is:
  // 0: Dictionary dimension columns, encoded as int for each column
  // 1: No dictionary and complex columns, they are all together encoded as one element (byte[][])
  // 2: Measure columns, encoded as Object for each column

  public static final int DICTIONARY_DIMENSION = 0;
  public static final int NO_DICTIONARY_AND_COMPLEX = 1;
  public static final int MEASURE = 2;

  public static CarbonRow fromColumnCategory(int[] dictDimensions, byte[][] noDictAndComplex,
      Object[] measures) {
    Object[] row = new Object[3];
    row[DICTIONARY_DIMENSION] = dictDimensions;
    row[NO_DICTIONARY_AND_COMPLEX] = noDictAndComplex;
    row[MEASURE] = measures;
    return new CarbonRow(row);
  }

  public static CarbonRow fromMergerRow(Object[] row, SegmentProperties segmentProperties) {
    Object[] converted = new Object[3];

    // dictionary dimension
    byte[] mdk = ((ByteArrayWrapper) row[0]).getDictionaryKey();
    long[] keys = segmentProperties.getDimensionKeyGenerator().getKeyArray(mdk);
    int[] dictDimensions = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      dictDimensions[i] = Long.valueOf(keys[i]).intValue();
    }
    converted[DICTIONARY_DIMENSION] = dictDimensions;

    // no dictionary and complex dimension
    converted[NO_DICTIONARY_AND_COMPLEX] = ((ByteArrayWrapper) row[0]).getNoDictionaryKeys();

    // measure
    int measureCount = row.length - 1;
    Object[] measures = new Object[measureCount];
    System.arraycopy(row, 1, measures, 0, measureCount);
    converted[MEASURE] = measures;

    return new CarbonRow(converted);
  }

  private static int[] getDictDimension(CarbonRow row) {
    return (int[]) row.getData()[DICTIONARY_DIMENSION];
  }

  public static byte[] getMdk(CarbonRow row, KeyGenerator keyGenerator) throws KeyGenException {
    return keyGenerator.generateKey(getDictDimension(row));
  }

  public static byte[][] getNoDictAndComplexDimension(CarbonRow row) {
    return (byte[][]) row.getData()[NO_DICTIONARY_AND_COMPLEX];
  }

  public static Object[] getMeasure(CarbonRow row) {
    return (Object[]) row.getData()[MEASURE];
  }

}

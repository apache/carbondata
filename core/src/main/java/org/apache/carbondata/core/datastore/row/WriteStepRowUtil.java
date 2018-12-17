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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.DataTypeUtil;

// Utility to create and retrieve data from CarbonRow in write step.
public class WriteStepRowUtil {

  // In write step, the element of CarbonRow is:
  // 0: Dictionary dimension columns, encoded as int for each column
  // 1: No dictionary and complex columns, they are all together encoded as one element (byte[][])
  // 2: Measure columns, encoded as Object for each column

  public static final int DICTIONARY_DIMENSION = 0;
  public static final int NO_DICTIONARY_AND_COMPLEX = 1;
  public static final int MEASURE = 2;

  public static CarbonRow fromColumnCategory(int[] dictDimensions, Object[] noDictAndComplex,
      Object[] measures) {
    Object[] row = new Object[3];
    row[DICTIONARY_DIMENSION] = dictDimensions;
    row[NO_DICTIONARY_AND_COMPLEX] = noDictAndComplex;
    row[MEASURE] = measures;
    return new CarbonRow(row);
  }

  public static CarbonRow fromMergerRow(Object[] row, SegmentProperties segmentProperties,
      CarbonColumn[] noDicAndComplexColumns) {
    Object[] converted = new Object[3];

    // dictionary dimension
    byte[] mdk = ((ByteArrayWrapper) row[0]).getDictionaryKey();
    long[] keys = segmentProperties.getDimensionKeyGenerator().getKeyArray(mdk);
    int[] dictDimensions = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      dictDimensions[i] = Long.valueOf(keys[i]).intValue();
    }
    converted[DICTIONARY_DIMENSION] = dictDimensions;

    Object[] noDictAndComplexKeys =
        new Object[segmentProperties.getNumberOfNoDictionaryDimension() + segmentProperties
            .getComplexDimensions().size()];

    byte[][] noDictionaryKeys = ((ByteArrayWrapper) row[0]).getNoDictionaryKeys();
    for (int i = 0; i < noDictionaryKeys.length; i++) {
      // in case of compaction rows are collected from result collector and are in byte[].
      // Convert the no dictionary columns to original data,
      // as load expects the no dictionary column with original data.
      if (DataTypeUtil.isPrimitiveColumn(noDicAndComplexColumns[i].getDataType())) {
        noDictAndComplexKeys[i] = DataTypeUtil
            .getDataBasedOnDataTypeForNoDictionaryColumn(noDictionaryKeys[i],
                noDicAndComplexColumns[i].getDataType());
        // for timestamp the above method will give the original data, so it should be
        // converted again to the format to be loaded (without micros)
        if (null != noDictAndComplexKeys[i]
            && noDicAndComplexColumns[i].getDataType() == DataTypes.TIMESTAMP) {
          noDictAndComplexKeys[i] = (long) noDictAndComplexKeys[i] / 1000L;
        }
      } else {
        noDictAndComplexKeys[i] = noDictionaryKeys[i];
      }
    }

    // For Complex Type Columns
    byte[][] complexKeys = ((ByteArrayWrapper) row[0]).getComplexTypesKeys();
    for (int i = segmentProperties.getNumberOfNoDictionaryDimension(), j = 0;
         i < segmentProperties.getNumberOfNoDictionaryDimension() + segmentProperties
             .getComplexDimensions().size(); i++) {
      noDictAndComplexKeys[i] = complexKeys[j++];
    }

    // no dictionary and complex dimension
    converted[NO_DICTIONARY_AND_COMPLEX] = noDictAndComplexKeys;

    // measure
    int measureCount = row.length - 1;
    Object[] measures = new Object[measureCount];
    System.arraycopy(row, 1, measures, 0, measureCount);
    converted[MEASURE] = measures;

    return new CarbonRow(converted);
  }

  public static int[] getDictDimension(CarbonRow row) {
    return (int[]) row.getData()[DICTIONARY_DIMENSION];
  }

  public static byte[] getMdk(CarbonRow row, KeyGenerator keyGenerator) throws KeyGenException {
    return keyGenerator.generateKey(getDictDimension(row));
  }

  public static Object[] getNoDictAndComplexDimension(CarbonRow row) {
    return (Object[]) row.getData()[NO_DICTIONARY_AND_COMPLEX];
  }

  public static Object[] getMeasure(CarbonRow row) {
    return (Object[]) row.getData()[MEASURE];
  }

}

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
package org.apache.carbondata.core.scan.filter;

import java.util.AbstractCollection;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.filter.executer.FilterBitSetUpdater;
import org.apache.carbondata.core.scan.filter.executer.MeasureColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.DataTypeUtil;

import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

/**
 * Utility class for executing the filter
 */
public class FilterExecutorUtil {
  /**
   * Below method will be used to execute measure filter based on data type
   * This is done to avoid conversion of primitive type to primitive object
   * as it may cause lots of gc when number of record is high and will impact performance
   *
   * @param page
   * @param bitSet
   * @param measureColumnExecuterFilterInfo
   * @param measureColumnResolvedFilterInfo
   * @param filterBitSetUpdater
   */
  public static void executeIncludeExcludeFilterForMeasure(ColumnPage page, BitSet bitSet,
      MeasureColumnExecuterFilterInfo measureColumnExecuterFilterInfo,
      MeasureColumnResolvedFilterInfo measureColumnResolvedFilterInfo,
      FilterBitSetUpdater filterBitSetUpdater) {
    final CarbonMeasure measure = measureColumnResolvedFilterInfo.getMeasure();
    final DataType dataType = FilterUtil.getMeasureDataType(measureColumnResolvedFilterInfo);
    int numberOfRows = page.getPageSize();
    BitSet nullBitSet = page.getNullBits();
    Object[] filterKeys = measureColumnExecuterFilterInfo.getFilterKeys();
    // to handle the null value
    for (int i = 0; i < filterKeys.length; i++) {
      if (filterKeys[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.flip(j);
        }
      }
    }
    AbstractCollection filterSet = measureColumnExecuterFilterInfo.getFilterSet();
    if (dataType == DataTypes.BYTE) {
      ByteOpenHashSet byteOpenHashSet = (ByteOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (byteOpenHashSet.contains((byte) page.getLong(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.BOOLEAN) {
      BooleanOpenHashSet booleanOpenHashSet = (BooleanOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (booleanOpenHashSet.contains(page.getBoolean(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.SHORT) {
      ShortOpenHashSet shortOpenHashSet = (ShortOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (shortOpenHashSet.contains((short) page.getLong(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.INT) {
      IntOpenHashSet intOpenHashSet = (IntOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (intOpenHashSet.contains((int) page.getLong(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.FLOAT) {
      FloatOpenHashSet floatOpenHashSet = (FloatOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (floatOpenHashSet.contains((float) page.getDouble(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.DOUBLE) {
      DoubleOpenHashSet doubleOpenHashSet = (DoubleOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (doubleOpenHashSet.contains(page.getDouble(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (dataType == DataTypes.LONG) {
      LongOpenHashSet longOpenHashSet = (LongOpenHashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          if (longOpenHashSet.contains(page.getLong(i))) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else if (DataTypes.isDecimal(dataType)) {
      Set bigDecimalHashSet = (HashSet) filterSet;
      for (int i = 0; i < numberOfRows; i++) {
        if (!nullBitSet.get(i)) {
          final Object measureObjectBasedOnDataType =
              DataTypeUtil.getMeasureObjectBasedOnDataType(page, i, dataType, measure);
          if (bigDecimalHashSet.contains(measureObjectBasedOnDataType)) {
            filterBitSetUpdater.updateBitset(bitSet, i);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid data type");
    }
  }
}

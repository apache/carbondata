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

package org.apache.carbondata.processing.loading.partition.impl;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.junit.Assert;
import org.junit.Test;

public class RawRowComparatorTest {
  private RawRowComparator new_comparator;

  @Test public void checkmap() {
    DataType noDicDataTypes[] =
        { DataTypes.INT, DataTypes.SHORT, DataTypes.BOOLEAN, DataTypes.BYTE, DataTypes.LONG };
    boolean isSortColumnNoDict[] = { true, true, true, true, true };
    int sortColumnIndices[] = { 1, 2, 3, 4, 5 };
    new_comparator = new RawRowComparator(sortColumnIndices, isSortColumnNoDict, noDicDataTypes);
    Field privateField = null;
    try {
      privateField = new_comparator.getClass().getDeclaredField("comparator_map");
    } catch (NoSuchFieldException | SecurityException e) {
      throw new RuntimeException(e);
    }
    privateField.setAccessible(true);
    Map<DataType, SerializableComparator> testmap = new HashMap<>();
    try {
      Map<DataType, SerializableComparator> refMap =
          (Map<DataType, SerializableComparator>) privateField.get(new_comparator);
      Assert.assertTrue(refMap.size() == noDicDataTypes.length);
    } catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }
 
  @Test public void compareint() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);

    Integer arr[] = { 1, 7, 5 };
    Integer arr1[] = { 2, 4, 6 };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res > 0);
  }

  @Test public void compareintreverse() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    
    Integer arr[] = { 2, 7, 5 };
    Integer arr1[] = { 1, 4, 6 };
    int res = new_comparator.compare(new CarbonRow(arr1), new CarbonRow(arr));
    Assert.assertTrue(res < 0);
  }

  @Test public void compareshort() {
    DataType noDicDataTypes[] = { DataTypes.SHORT };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);

    Short arr[] = { 1, 7, 5 };
    Short arr1[] = { 2, 4, 6 };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res > 0);
  }

  @Test public void comparelong() {
    DataType noDicDataTypes[] = { DataTypes.LONG };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    Long arr[] = { 1L, 7L, 5L };
    Long arr1[] = { 2L, 4L, 6L };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res > 0);
  }

  @Test public void comparefloat() {
    DataType noDicDataTypes[] = { DataTypes.FLOAT };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    Float arr[] = { 1F, 7F, 5F };
    Float arr1[] = { 2F, 4F, 6F };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res > 0);
  }

  @Test public void compareboolean() {
    DataType noDicDataTypes[] = { DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    Boolean arr[] = { false, false };
    Boolean arr1[] = { true, true };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res < 0);
  }

   @Test public void comparebyte() {
    DataType noDicDataTypes[] = { DataTypes.BYTE };
    boolean noDicSortColumnMapping[] = { true };
    int sortColumnIndices[] = { 1 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    Byte arr[] = { 1, 2, 3 };
    Byte arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res < 0);
  }

  @Test public void comparemixed() {
    DataType noDicDataTypes[] = { DataTypes.INT, DataTypes.SHORT, DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true, true, true };
    int sortColumnIndices[] = { 1, 2, 3 };
    new_comparator =
        new RawRowComparator(sortColumnIndices, noDicSortColumnMapping, noDicDataTypes);
    Object arr[] = { 1, 2, false };
    Object arr1[] = { 4, 5, true };
    int res = new_comparator.compare(new CarbonRow(arr), new CarbonRow(arr1));
    Assert.assertTrue(res < 0);
  }
}

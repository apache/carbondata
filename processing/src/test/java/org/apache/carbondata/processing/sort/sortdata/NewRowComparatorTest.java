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

package org.apache.carbondata.processing.sort.sortdata;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.junit.Assert;
import org.junit.Test;

public class NewRowComparatorTest {
  private NewRowComparator new_comparator;

  @Test public void compareint() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Integer arr[] = { 1, 7, 5 };
    Integer arr1[] = { 2, 4, 6 };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void compareintreverse() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Integer arr[] = { 1, 2, 3 };
    Integer arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(arr1, arr);
    Assert.assertTrue(res > 0);
  }

  @Test public void compareshort() {
    DataType noDicDataTypes[] = { DataTypes.SHORT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Short arr[] = { 1, 2, 3 };
    Short arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparelong() {
    DataType noDicDataTypes[] = { DataTypes.LONG };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Long arr[] = { 1L, 2L, 3L };
    Long arr1[] = { 4L, 5L, 6L };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparefloat() {
    DataType noDicDataTypes[] = { DataTypes.FLOAT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Float arr[] = { 1F, 2F, 3F };
    Float arr1[] = { 4F, 5F, 6F };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void compareboolean() {
    DataType noDicDataTypes[] = { DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Boolean arr[] = { false };
    Boolean arr1[] = { true };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparebyte() {
    DataType noDicDataTypes[] = { DataTypes.BYTE };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Byte arr[] = { 1, 2, 3 };
    Byte arr1[] = { 4, 5, 6 };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }
 
  @Test public void comparemixed() {
    DataType noDicDataTypes[] = { DataTypes.INT, DataTypes.FLOAT, DataTypes.BOOLEAN };
    boolean noDicSortColumnMapping[] = { true, true, true };
    new_comparator = new NewRowComparator(noDicSortColumnMapping, noDicDataTypes);
    Object arr[] = { 1, 2F, false };
    Object arr1[] = { 4, 5F, true };
    int res = new_comparator.compare(arr, arr1);
    Assert.assertTrue(res < 0);
  }
}

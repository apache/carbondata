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
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;

import org.junit.Assert;
import org.junit.Test;

public class IntermediateSortTempRowComparatorTest {
  private IntermediateSortTempRowComparator new_comparator;

  @Test public void compareint() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {1,2,3};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {4,5,6};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    int res = new_comparator.compare( a, a1);
    Assert.assertTrue(res > 0);
  }

  @Test public void compareintreverse() {
    DataType noDicDataTypes[] = { DataTypes.INT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {1,2,3};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {4,5,6};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparelong() {
    DataType noDicDataTypes[] = { DataTypes.LONG };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {1L,2L,3L};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {4L,5L,6L};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparefloat() {
    DataType noDicDataTypes[] = { DataTypes.FLOAT };
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {1F,2F,3F};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {4F,5F,6F};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res < 0);
  }

  @Test public void compareboolean() {
    DataType noDicDataTypes[] = { DataTypes.BOOLEAN};
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {true};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {false};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res> 0);
  }

  @Test public void comparebyte() {
    DataType noDicDataTypes[] = { DataTypes.BYTE};
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Byte[] noDictSortDims1 = {1,2,3};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Byte[] noDictSortDims = {3,4,5};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res < 0);
  }

  @Test public void comparemixed() {
    DataType noDicDataTypes[] = { DataTypes.BOOLEAN, DataTypes.INT, DataTypes.LONG};
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Object[] noDictSortDims1 = {true, 1, 2L};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Object[] noDictSortDims = {false, 3, 5L};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);
    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res > 0);
  }

  @Test public void compareshort() {
    DataType noDicDataTypes[] = { DataTypes.SHORT};
    boolean noDicSortColumnMapping[] = { true };
    new_comparator = new IntermediateSortTempRowComparator(noDicSortColumnMapping, noDicDataTypes);

    int[] dictSortDims1 = {1,2,3};
    Short[] noDictSortDims1 = {2};
    byte[] noSortDimsAndMeasures1 = {1,2,3};
    IntermediateSortTempRow a1 = new IntermediateSortTempRow(dictSortDims1, noDictSortDims1, noSortDimsAndMeasures1);

    int[] dictSortDims = {1,2,3};
    Short[] noDictSortDims = {1};
    byte[] noSortDimsAndMeasures = {1,2,3};
    IntermediateSortTempRow a = new IntermediateSortTempRow(dictSortDims, noDictSortDims, noSortDimsAndMeasures);

    int res = new_comparator.compare( a1, a);
    Assert.assertTrue(res > 0);
  }
} 

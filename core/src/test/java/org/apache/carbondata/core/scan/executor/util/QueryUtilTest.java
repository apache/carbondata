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
package org.apache.carbondata.core.scan.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesTestUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.scan.model.ProjectionDimension;

import junit.framework.TestCase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class QueryUtilTest extends TestCase {

  private SegmentProperties segmentProperties;

  @BeforeClass public void setUp() {
    segmentProperties = SegmentPropertiesTestUtil.getSegmentProperties();
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRange() {

    ProjectionDimension dimension =
        new ProjectionDimension(segmentProperties.getDimensions().get(0));
    int[] maskedByteRange = QueryUtil
        .getMaskedByteRange(Arrays.asList(dimension), segmentProperties.getDimensionKeyGenerator());
    int[] expectedMaskedByteRange = { 0 };
    for (int i = 0; i < maskedByteRange.length; i++) {
      assertEquals(expectedMaskedByteRange[i], maskedByteRange[i]);
    }
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRangeOnlyForDictionaryKey() {
    List<ProjectionDimension> dimensions = new ArrayList<ProjectionDimension>();
    for (int i = 0; i < 2; i++) {
      ProjectionDimension dimension =
          new ProjectionDimension(segmentProperties.getDimensions().get(i));
      dimensions.add(dimension);
    }
    int[] maskedByteRange =
        QueryUtil.getMaskedByteRange(dimensions, segmentProperties.getDimensionKeyGenerator());
    int[] expectedMaskedByteRange = { 0 };
    for (int i = 0; i < maskedByteRange.length; i++) {
      assertEquals(expectedMaskedByteRange[i], maskedByteRange[i]);
    }
  }

  @Test public void testGetMaskedByteRangeBasedOrdinalGivingProperMaskedByte() {
    List<Integer> dimensionOrdinal = new ArrayList<Integer>();
    dimensionOrdinal.add(0);
    int[] maskedByteRange = QueryUtil.getMaskedByteRangeBasedOrdinal(dimensionOrdinal,
        segmentProperties.getDimensionKeyGenerator());
    int[] expectedMaskedByteRange = { 0 };
    for (int i = 0; i < maskedByteRange.length; i++) {
      assertEquals(expectedMaskedByteRange[i], maskedByteRange[i]);
    }
  }

  @Test public void testGetMaxKeyBasedOnDimensions() {
    List<ProjectionDimension> dimensions = new ArrayList<ProjectionDimension>();
    for (int i = 0; i < 2; i++) {
      ProjectionDimension dimension =
          new ProjectionDimension(segmentProperties.getDimensions().get(i));
      dimensions.add(dimension);
    }
    byte[] maxKeyBasedOnDimensions = null;
    try {
      maxKeyBasedOnDimensions = QueryUtil
          .getMaxKeyBasedOnDimensions(dimensions, segmentProperties.getDimensionKeyGenerator());
    } catch (KeyGenException e) {
      assertTrue(false);
    }
    byte[] expectedMaxKeyBasedOnDimensions = { -1, 0, 0, 0, 0, 0 };
    for (int i = 0; i < expectedMaxKeyBasedOnDimensions.length; i++) {
      if (expectedMaxKeyBasedOnDimensions[i] != maxKeyBasedOnDimensions[i]) {
        assertTrue(false);
      }
    }
    long[] expectedKeyArray = { 255, 0, 0, 0, 0, 0 };
    long[] keyArray =
        segmentProperties.getDimensionKeyGenerator().getKeyArray(maxKeyBasedOnDimensions);
    for (int i = 0; i < keyArray.length; i++) {
      if (expectedKeyArray[i] != keyArray[i]) {
        assertTrue(false);
      }
    }
  }

  @Test public void testGetMaksedByte() {
    ProjectionDimension dimension =
        new ProjectionDimension(segmentProperties.getDimensions().get(0));
    int[] maskedByteRange = QueryUtil
        .getMaskedByteRange(Arrays.asList(dimension), segmentProperties.getDimensionKeyGenerator());
    int[] maskedByte = QueryUtil
        .getMaskedByte(segmentProperties.getDimensionKeyGenerator().getDimCount(), maskedByteRange);
    int[] expectedMaskedByte = { 0, -1, -1, -1, -1, -1 };

    for (int i = 0; i < expectedMaskedByte.length; i++) {
      if (expectedMaskedByte[i] != maskedByte[i]) {
        assertTrue(false);
      }
    }
  }

  @Test public void testSearchInArrayWithSearchInputNotPresentInArray() {
    int[] dummyArray = { 1, 2, 3, 4, 5 };
    int searchInput = 6;
    boolean result = QueryUtil.searchInArray(dummyArray, searchInput);
    Assert.assertTrue(!result);
  }

  @Test public void testSearchInArrayWithSearchInputPresentInArray() {
    int[] dummyArray = { 1, 2, 3, 4, 5 };
    int searchInput = 1;
    boolean result = QueryUtil.searchInArray(dummyArray, searchInput);
    Assert.assertTrue(result);
  }

  @Test public void testGetColumnGroupIdWhenOrdinalValueNotPresentInArrayIndex() {
    int ordinal = 0;
    new MockUp<SegmentProperties>() {
      @Mock public int[][] getColumnGroups() {
        return new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 5 } };
      }
    };
    int actualValue = QueryUtil.getColumnGroupId(segmentProperties, ordinal);
    int expectedValue = 4; //expectedValue will always be arrayLength - 1
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetColumnGroupIdWhenOrdinalValuePresentInArrayIndex() {
    int ordinal = 1;
    new MockUp<SegmentProperties>() {
      @Mock public int[][] getColumnGroups() {
        return new int[][] { { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 5 } };
      }
    };
    int actualValue = QueryUtil.getColumnGroupId(segmentProperties, ordinal);
    int expectedValue = 0;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetColumnGroupIdWhenColumnGroupsIndexValueLengthLessThanOne() {
    int ordinal = 1;
    new MockUp<SegmentProperties>() {
      @Mock public int[][] getColumnGroups() {
        return new int[][] { { 1 } };
      }
    };
    int actualValue = QueryUtil.getColumnGroupId(segmentProperties, ordinal);
    int expectedValue = -1;
    assertEquals(expectedValue, actualValue);
  }

  @Test public void testGetMaskedKey() {
    byte[] data = { 1, 2, 3, 4, 5, 5 };
    byte[] maxKey = { 15, 20, 25, 30, 35, 35 };
    int[] maskByteRanges = { 1, 2, 3, 4, 5 };
    int byteCount = 5;
    byte[] actualValue = QueryUtil.getMaskedKey(data, maxKey, maskByteRanges, byteCount);
    byte[] expectedValue = { 0, 1, 4, 1, 1 };
    assertArrayEquals(expectedValue, actualValue);
  }

  @Test public void testGetMaxKeyBasedOnOrinal() throws Exception {
    List<Integer> dummyList = new ArrayList<>();
    dummyList.add(0, 1);
    dummyList.add(1, 2);
    byte[] actualValue =
        QueryUtil.getMaxKeyBasedOnOrinal(dummyList, segmentProperties.getDimensionKeyGenerator());
    byte[] expectedValue = { 0, -1, -1, 0, 0, 0 };
    assertArrayEquals(expectedValue, actualValue);
  }

  @Test public void testGetSortDimensionIndexes() {
    List<ProjectionDimension> sortedDimensions = new ArrayList<ProjectionDimension>();
    for (int i = 0; i < 2; i++) {
      ProjectionDimension dimension =
          new ProjectionDimension(segmentProperties.getDimensions().get(i));
      sortedDimensions.add(dimension);
    }
    List<ProjectionDimension> queryDimensions = new ArrayList<ProjectionDimension>();
    for (int i = 0; i < 2; i++) {
      ProjectionDimension dimension =
          new ProjectionDimension(segmentProperties.getDimensions().get(i));
      queryDimensions.add(dimension);
    }
    byte[] actualValue = QueryUtil.getSortDimensionIndexes(sortedDimensions, queryDimensions);
    byte[] expectedValue = { 0, 0 };
    assertArrayEquals(expectedValue, actualValue);
  }

  @AfterClass public void tearDown() {
    segmentProperties = null;
  }
}

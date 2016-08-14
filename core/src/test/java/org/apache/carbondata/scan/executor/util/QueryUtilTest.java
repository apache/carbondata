/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.block.SegmentPropertiesTestUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.scan.model.QueryDimension;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryUtilTest extends TestCase {

  private SegmentProperties segmentProperties;

  @BeforeClass public void setUp() {
    segmentProperties = SegmentPropertiesTestUtil.getSegmentProperties();
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRange() {
	  
	QueryDimension dimension = new QueryDimension(segmentProperties.getDimensions().get(0).getColName());
	dimension.setDimension(segmentProperties.getDimensions().get(0));  
    int[] maskedByteRange = QueryUtil
        .getMaskedByteRange(Arrays.asList(dimension),
            segmentProperties.getDimensionKeyGenerator());
    int[] expectedMaskedByteRange = { 0 };
    for (int i = 0; i < maskedByteRange.length; i++) {
      assertEquals(expectedMaskedByteRange[i], maskedByteRange[i]);
    }
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRangeOnlyForDictionaryKey() {
    List<QueryDimension> dimensions = new ArrayList<QueryDimension>();
    for (int i = 0; i < 2; i++) {
      QueryDimension dimension = new QueryDimension(segmentProperties.getDimensions().get(i).getColName());
      dimension.setDimension(segmentProperties.getDimensions().get(i));
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
	  List<QueryDimension> dimensions = new ArrayList<QueryDimension>();
	    for (int i = 0; i < 2; i++) {
	      QueryDimension dimension = new QueryDimension(segmentProperties.getDimensions().get(i).getColName());
	      dimension.setDimension(segmentProperties.getDimensions().get(i));
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
	  QueryDimension dimension = new QueryDimension(segmentProperties.getDimensions().get(0).getColName());
		dimension.setDimension(segmentProperties.getDimensions().get(0)); 
		dimension.setDimension(segmentProperties.getDimensions().get(0));
    int[] maskedByteRange = QueryUtil
        .getMaskedByteRange(Arrays.asList(dimension),
            segmentProperties.getDimensionKeyGenerator());
    int[] maskedByte = QueryUtil
        .getMaskedByte(segmentProperties.getDimensionKeyGenerator().getDimCount(), maskedByteRange);
    int[] expectedMaskedByte = { 0, -1, -1, -1, -1, -1 };

    for (int i = 0; i < expectedMaskedByte.length; i++) {
      if (expectedMaskedByte[i] != maskedByte[i]) {
        assertTrue(false);
      }
    }
  }

  @AfterClass public void tearDown() {
    segmentProperties = null;
  }
}

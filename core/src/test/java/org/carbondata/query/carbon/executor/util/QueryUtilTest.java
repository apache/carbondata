package org.carbondata.query.carbon.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.block.SegmentPropertiesTestUtil;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.keygenerator.KeyGenException;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryUtilTest extends TestCase {

  private SegmentProperties segmentProperties;

  public static void main(String[] args) {

  }

  @BeforeClass public void setUp() {
    segmentProperties = SegmentPropertiesTestUtil.getSegmentProperties();
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRange() {
    int[] maskedByteRange = QueryUtil
        .getMaskedByteRange(Arrays.asList(segmentProperties.getDimensions().get(0)),
            segmentProperties.getDimensionKeyGenerator());
    int[] expectedMaskedByteRange = { 0 };
    for (int i = 0; i < maskedByteRange.length; i++) {
      assertEquals(expectedMaskedByteRange[i], maskedByteRange[i]);
    }
  }

  @Test public void testGetMaskedByteRangeGivingProperMaksedByteRangeOnlyForDictionaryKey() {
    List<CarbonDimension> dimensions = new ArrayList<CarbonDimension>();
    for (int i = 0; i < 2; i++) {
      dimensions.add(segmentProperties.getDimensions().get(i));
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
    List<CarbonDimension> dimensions = new ArrayList<CarbonDimension>();
    for (int i = 0; i < 2; i++) {
      dimensions.add(segmentProperties.getDimensions().get(i));
    }
    byte[] maxKeyBasedOnDimensions = null;
    try {
      maxKeyBasedOnDimensions = QueryUtil
          .getMaxKeyBasedOnDimensions(dimensions, segmentProperties.getDimensionKeyGenerator());
    } catch (KeyGenException e) {
      assertTrue(false);
    }
    System.out.println(Arrays.toString(maxKeyBasedOnDimensions));
    System.out.println(Arrays.toString(
        segmentProperties.getDimensionKeyGenerator().getKeyArray(maxKeyBasedOnDimensions)));
  }

  @AfterClass public void tearDown() {

  }
}

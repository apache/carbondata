package org.carbondata.core.util;

import junit.framework.TestCase;
import org.junit.Test;

public class CarbonUtilTest extends TestCase {

  @Test public void testGetBitLengthForDimensionGiveProperValue() {
    int[] cardinality = { 10, 1, 10000, 1, 2, 3 };
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(cardinality, new int[] { 1, 1, 3, 1 });
    int[] expectedOutPut = { 8, 8, 14, 2, 8, 8 };
    for (int i = 0; i < dimensionBitLength.length; i++) {
      assertEquals(expectedOutPut[i], dimensionBitLength[i]);
    }
  }
}

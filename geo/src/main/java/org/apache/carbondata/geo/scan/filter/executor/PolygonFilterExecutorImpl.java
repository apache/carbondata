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

package org.apache.carbondata.geo.scan.filter.executor;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.executer.RowLevelFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.geo.scan.expression.PolygonExpression;

/**
 * Polygon filter executor. Prunes Blocks and Blocklets based on the selected ranges of polygon.
 */
public class PolygonFilterExecutorImpl extends RowLevelFilterExecutorImpl {
  public PolygonFilterExecutorImpl(List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        complexDimensionInfoMap, -1);
  }

  private int getNearestRangeIndex(List<Long[]> ranges, long searchForNumber) {
    Long[] range;
    int low = 0, mid = 0, high = ranges.size() - 1;
    while (low <= high) {
      mid = low + ((high - low) / 2);
      range = ranges.get(mid);
      if (searchForNumber >= range[0]) {
        if (searchForNumber <= range[1]) {
          // Return the range index if the number is between min and max values of the range
          return mid;
        } else {
          // Number is bigger than this range's min and max. Search on the right side of the range
          low = mid + 1;
        }
      } else {
        // Number is smaller than this range's min and max. Search on the left side of the range
        high = mid - 1;
      }
    }
    return mid;
  }

  /**
   * Checks if the current block or blocklet needs to be scanned
   * @param maxValue Max value in the current block or blocklet
   * @param minValue Min value in te current block or blocklet
   * @return True or False  True if current block or blocket needs to be scanned. Otherwise False.
   */
  private boolean isScanRequired(byte[] maxValue, byte[] minValue) {
    PolygonExpression polygon = (PolygonExpression) exp;
    List<Long[]> ranges = polygon.getRanges();
    if (ranges.isEmpty()) {
      // If the ranges is empty, no need to scan block or blocklet
      return false;
    }
    Long min =
        (Long) DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(minValue, DataTypes.LONG);
    Long max =
        (Long) DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(maxValue, DataTypes.LONG);

    // Find the nearest possible range index for both the min and max values. If a value do not
    // exist in the any of the range, get the preceding range index where it fits best
    int startIndex = getNearestRangeIndex(ranges, min);
    int endIndex = getNearestRangeIndex(ranges, max);
    if (endIndex > startIndex) {
      // Multiple ranges fall between min and max. Need to scan this block or blocklet
      return true;
    }
    // Got same index for both min and max values.
    Long[] oneRange = ranges.subList(startIndex, endIndex + 1).get(0);
    if ((min >= oneRange[0] && min <= oneRange[1]) || (max >= oneRange[0] && max <= oneRange[1]) ||
        (oneRange[0] >= min && oneRange[0] <= max) || (oneRange[1] >= min && oneRange[1] <= max)) {
      // Either min or max is within the range
      // either min or max of the range is within the min and max values
      return true;
    }
    // No range between min and max values. Scan can be avoided for this block or blocklet
    return false;
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    int dimIndex = dimensionChunkIndex[0];
    BitSet bitSet = new BitSet(1);
    if (isMinMaxSet[dimIndex] && isScanRequired(blockMaxValue[dimIndex], blockMinValue[dimIndex])) {
      bitSet.set(0);
    }
    return bitSet;
  }
}

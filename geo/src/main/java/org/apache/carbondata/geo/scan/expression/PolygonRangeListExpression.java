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

package org.apache.carbondata.geo.scan.expression;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.util.CustomIndex;
import org.apache.carbondata.geo.GeoConstants;
import org.apache.carbondata.geo.GeoHashUtils;
import org.apache.carbondata.geo.GeoOperationType;

/**
 * InPolygonRangeList expression processor. It inputs the InPolygonRangeList string to
 * the Geo implementation's query method, inputs lists of range of IDs and is to be calculated
 * the and/or/diff range list to filter. And then, build InExpression with list of all the IDs
 * present in those list of ranges.
 */
@InterfaceAudience.Internal
public class PolygonRangeListExpression extends PolygonExpression {

  private String opType;

  /**
   * If range start's with RANGELIST_REG_EXPRESSION or not
   */
  private boolean computeRange = true;

  private List<String> polygonRangeLists = new ArrayList<>();

  public PolygonRangeListExpression(String polygonRangeList, String opType, String columnName,
      CustomIndex indexInstance) {
    super(polygonRangeList, columnName, indexInstance);
    this.opType = opType;
  }

  public PolygonRangeListExpression(String polygonRangeList, String opType, String columnName,
      CustomIndex indexInstance, boolean computeRange, List<String> polygonRangeLists) {
    super(polygonRangeList, columnName, indexInstance);
    this.opType = opType;
    this.computeRange = computeRange;
    this.polygonRangeLists = polygonRangeLists;
  }

  @Override
  public void processExpression() {
    // 1. parse the range list string
    List<String> rangeLists = new ArrayList<>();
    if (computeRange) {
      Pattern pattern =
          Pattern.compile(GeoConstants.RANGELIST_REG_EXPRESSION, Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(polygon);
      while (matcher.find()) {
        String matchedStr = matcher.group();
        rangeLists.add(matchedStr);
      }
    } else {
      rangeLists.addAll(polygonRangeLists);
    }
    // 2. process the range lists
    if (rangeLists.size() > 0) {
      GeoOperationType operationType = GeoOperationType.getEnum(opType);
      if (operationType == null) {
        throw new RuntimeException("Unsupported operation type " + opType);
      }
      List<Long[]> processedRangeList = getRangeListFromString(rangeLists.get(0));
      for (int i = 1; i < rangeLists.size(); i++) {
        List<Long[]> tempRangeList = getRangeListFromString(rangeLists.get(i));
        processedRangeList = GeoHashUtils.processRangeList(
            processedRangeList, tempRangeList, operationType);
      }
      ranges = processedRangeList;
    }
  }

  public static void sortRange(List<Long[]> rangeList) {
    rangeList.sort(new Comparator<Long[]>() {
      @Override
      public int compare(Long[] x, Long[] y) {
        return Long.compare(x[0], y[0]);
      }
    });
  }

  public static void combineRange(List<Long[]> rangeList) {
    for (int i = 0, j = i + 1; i < rangeList.size() - 1; i++, j++) {
      long previousEnd = rangeList.get(i)[1];
      long nextStart = rangeList.get(j)[0];
      long nextEnd = rangeList.get(j)[1];
      if (previousEnd + 1 >= nextStart) {
        rangeList.get(j)[0] = rangeList.get(i)[0];
        rangeList.get(j)[1] = previousEnd >= nextEnd ? previousEnd : nextEnd;
        rangeList.get(i)[0] = null;
        rangeList.get(i)[1] = null;
      }
    }
    rangeList.removeIf(item -> item[0] == null && item[1] == null);
  }

  public static List<Long[]> getRangeListFromString(String rangeListString) {
    String[] rangeStringList = rangeListString.trim().split(GeoConstants.DEFAULT_DELIMITER);
    List<Long[]> rangeList = new ArrayList<>();
    for (String rangeString : rangeStringList) {
      String[] range = GeoHashUtils.splitStringToPoint(rangeString);
      if (range.length != 2) {
        throw new RuntimeException("each range is a pair need 2 data");
      }
      Long[] rangeMinMax;
      try {
        rangeMinMax = new Long[]{Long.valueOf(range[0]), Long.valueOf(range[1])};
      } catch (NumberFormatException e) {
        throw new RuntimeException("can not covert the range from String to Long", e);
      }
      if (rangeMinMax[0] > rangeMinMax[1]) {
        throw new RuntimeException(
            "first value need to be smaller than second value of each range");
      } else {
        rangeList.add(rangeMinMax);
      }
    }
    sortRange(rangeList);
    combineRange(rangeList);
    return rangeList;
  }

  @Override
  public String getStatement() {
    return "IN_POLYGON_RANGE_LIST('" + polygon + "', '" + opType + "')";
  }
}

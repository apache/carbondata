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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.util.CustomIndex;
import org.apache.carbondata.geo.GeoConstants;
import org.apache.carbondata.geo.GeoHashUtils;
import org.apache.carbondata.geo.GeoOperationType;

import org.apache.commons.lang.StringUtils;

/**
 * InPolygonList expression processor. It inputs the InPolygonList string to the Geo
 * implementation's query method, gets a list of range of IDs from each polygon and
 * calculates the and/or/diff range list to filter as an output. And then, build
 * InExpression with list of all the IDs present in those list of ranges.
 */
@InterfaceAudience.Internal
public class PolygonListExpression extends PolygonExpression {

  private String opType;

  public PolygonListExpression(String polygonListString, String opType, String columnName,
                               CustomIndex indexInstance) {
    super(polygonListString, columnName, indexInstance);
    this.opType = opType;
  }

  @Override
  public void processExpression() {
    try {
      // 1. parse the polygon list string
      List<String> polygons = new ArrayList<>();
      Pattern pattern =
          Pattern.compile(GeoConstants.POLYGON_REG_EXPRESSION, Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(polygon);
      while (matcher.find()) {
        String matchedStr = matcher.group();
        if (!(matchedStr == null || StringUtils.isEmpty(matchedStr))) {
          polygons.add(matchedStr);
        }
      }
      if (polygons.size() < 2) {
        throw new RuntimeException("polygon list need at least 2 polygons, really has " +
            polygons.size());
      }
      // 2. get the range list of each polygon
      List<Long[]> processedRangeList = instance.query(polygons.get(0));
      GeoHashUtils.validateRangeList(processedRangeList);
      GeoOperationType operationType = GeoOperationType.getEnum(opType);
      if (operationType == null) {
        throw new RuntimeException("Unsupported operation type " + opType);
      }
      for (int i = 1; i < polygons.size(); i++) {
        List<Long[]> tempRangeList = instance.query(polygons.get(i));
        GeoHashUtils.validateRangeList(tempRangeList);
        processedRangeList = GeoHashUtils.processRangeList(
            processedRangeList, tempRangeList, operationType);
      }
      ranges = processedRangeList;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getStatement() {
    return "IN_POLYGON_LIST('" + polygon + "', '" + opType + "')";
  }
}

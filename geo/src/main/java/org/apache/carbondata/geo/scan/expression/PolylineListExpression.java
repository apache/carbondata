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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.buffer.BufferParameters;

/**
 * InPolylineList expression processor. It inputs the InPolylineList string to the Geo
 * implementation's query method, gets a list of range of IDs from each polygon and
 * calculates the and/or/diff range list to filter as an output. And then, build
 * InExpression with list of all the IDs present in those list of ranges.
 */
@InterfaceAudience.Internal
public class PolylineListExpression extends PolygonExpression {

  private Float bufferInMeter;

  public PolylineListExpression(String polylineString, Float bufferInMeter, String columnName,
      CustomIndex indexInstance) {
    super(polylineString, columnName, indexInstance);
    this.bufferInMeter = bufferInMeter;
  }

  @Override
  public void processExpression() {
    try {
      // transform the distance unit meter to degree
      double buffer = bufferInMeter / GeoConstants.CONVERSION_FACTOR_OF_METER_TO_DEGREE;

      // 1. parse the polyline list string and get polygon from each polyline
      List<Geometry> polygonList = new ArrayList<>();
      WKTReader wktReader = new WKTReader();
      Pattern pattern =
          Pattern.compile(GeoConstants.POLYLINE_REG_EXPRESSION, Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(polygon);
      while (matcher.find()) {
        String matchedStr = matcher.group();
        LineString polylineCreatedFromStr = (LineString) wktReader.read(matchedStr);
        Polygon polygonFromPolylineBuffer = (Polygon) polylineCreatedFromStr.buffer(
            buffer, 0, BufferParameters.CAP_SQUARE);
        polygonList.add(polygonFromPolylineBuffer);
      }
      // 2. get the range list of each polygon
      if (polygonList.size() > 0) {
        List<double[]> pointList = getPointListFromGeometry(polygonList.get(0));
        List<Long[]> processedRangeList = instance.query(pointList);
        GeoHashUtils.validateRangeList(processedRangeList);
        for (int i = 1; i < polygonList.size(); i++) {
          List<double[]> tempPointList = getPointListFromGeometry(polygonList.get(i));
          List<Long[]> tempRangeList = instance.query(tempPointList);
          GeoHashUtils.validateRangeList(tempRangeList);
          processedRangeList = GeoHashUtils.processRangeList(
            processedRangeList, tempRangeList, GeoOperationType.OR);
        }
        ranges = processedRangeList;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<double[]> getPointListFromGeometry(Geometry geometry) {
    List<double[]> pointList = new ArrayList<>();
    Coordinate[] coords = geometry.getCoordinates();
    for (Coordinate coord : coords) {
      pointList.add(new double[] {coord.x, coord.y});
    }
    return pointList;
  }

  @Override
  public String getStatement() {
    return "IN_POLYLINE_LIST('" + polygon + "', '" + bufferInMeter + "')";
  }
}

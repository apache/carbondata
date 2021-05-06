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

package org.apache.carbondata.geo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.geo.scan.expression.PolygonRangeListExpression;

import static org.apache.carbondata.geo.GeoConstants.POSITIVE_INTEGER_REGEX;

import org.apache.commons.lang3.StringUtils;

public class GeoHashUtils {

  /**
   * Get the degree of each grid in the east-west direction.
   *
   * @param originLatitude the origin point latitude
   * @param gridSize the grid size
   * @return Delta X is the degree of each grid in the east-west direction
   */
  public static double getDeltaX(double originLatitude, int gridSize) {
    double mCos = Math.cos(originLatitude * Math.PI / GeoConstants.CONVERT_FACTOR);
    return (GeoConstants.CONVERT_FACTOR * gridSize) / (Math.PI * GeoConstants.EARTH_RADIUS * mCos);
  }

  /**
   * Get the degree of each grid in the north-south direction.
   *
   * @param gridSize the grid size
   * @return Delta Y is the degree of each grid in the north-south direction
   */
  public static double getDeltaY(int gridSize) {
    return (GeoConstants.CONVERT_FACTOR * gridSize) / (Math.PI * GeoConstants.EARTH_RADIUS);
  }

  /**
   * Calculate the number of knives cut
   *
   * @param gridSize the grid size
   * @param originLatitude the origin point latitude
   * @return The number of knives cut
   */
  public static int getCutCount(int gridSize, double originLatitude) {
    double deltaX = getDeltaX(originLatitude, gridSize);
    int countX = Double.valueOf(
        Math.ceil(Math.log(2 * GeoConstants.CONVERT_FACTOR / deltaX) / Math.log(2))).intValue();
    double deltaY = getDeltaY(gridSize);
    int countY = Double.valueOf(
        Math.ceil(Math.log(GeoConstants.CONVERT_FACTOR / deltaY) / Math.log(2))).intValue();
    return Math.max(countX, countY);
  }

  /**
   * Convert input longitude and latitude to GeoID
   *
   * @param longitude Longitude, the actual longitude and latitude are processed by * coefficient,
   *                  and the floating-point calculation is converted to integer calculation
   * @param latitude Latitude, the actual longitude and latitude are processed by * coefficient,
   *                  and the floating-point calculation is converted to integer calculation.
   * @param oriLatitude the origin point latitude
   * @param gridSize the grid size
   * @return GeoID
   */
  public static long lonLat2GeoID(long longitude, long latitude, double oriLatitude, int gridSize) {
    long longtitudeByRatio = longitude * GeoConstants.CONVERSION_FACTOR_FOR_ACCURACY;
    long latitudeByRatio = latitude * GeoConstants.CONVERSION_FACTOR_FOR_ACCURACY;
    int[] ij = lonLat2ColRow(longtitudeByRatio, latitudeByRatio, oriLatitude, gridSize);
    return colRow2GeoID(ij[0], ij[1]);
  }

  public static void validateUDFInputValue(Object input, String inputName, String datatype)
      throws MalformedCarbonCommandException {
    if (inputName.equalsIgnoreCase(GeoConstants.GRID_SIZE) && (input == null
        || !Pattern.compile(POSITIVE_INTEGER_REGEX).matcher(input.toString()).find())) {
      throw new MalformedCarbonCommandException("Expect grid size to be a positive integer");
    } else if (input == null || input.toString().equals("null")) {
      throw new MalformedCarbonCommandException(
          "Expect " + inputName + " to be of " + datatype + " type");
    }
  }

  public static void validateGeoProperty(String propertyValue, String propertyName)
      throws MalformedCarbonCommandException {
    if (StringUtils.isEmpty(propertyValue) ||
        !Pattern.compile(POSITIVE_INTEGER_REGEX).matcher(propertyValue).find()) {
      throw new MalformedCarbonCommandException(
          String.format("%s property is invalid. %s property must be specified, "
                  + "and the value must be positive integer.",
              CarbonCommonConstants.SPATIAL_INDEX, propertyName));
    }
  }

  /**
   * Calculate geo id through grid index coordinates, the row and column of grid coordinates
   * can be transformed by latitude and longitude
   *
   * @param longitude Longitude, the actual longitude and latitude are processed by * coefficient,
   * and the floating-point calculation is converted to integer calculation
   * @param latitude Latitude, the actual longitude and latitude are processed by * coefficient,
   * and the floating-point calculation is converted to integer calculation
   * @param oriLatitude the latitude of origin point,which is used to calculate the deltaX and cut
   * level.
   * @param gridSize the size of minimal grid after cut
   * @return Grid ID value [row, column], column starts from 1
   */
  public static int[] lonLat2ColRow(long longitude, long latitude, double oriLatitude,
      int gridSize) {
    int cutLevel = getCutCount(gridSize, oriLatitude);
    int column = (int) Math.floor(longitude / getDeltaX(oriLatitude, gridSize) /
        GeoConstants.CONVERSION_RATIO) + (1 << (cutLevel - 1));
    int row = (int) Math.floor(latitude / getDeltaY(gridSize) /
        GeoConstants.CONVERSION_RATIO) + (1 << (cutLevel - 1));
    return new int[] {row, column};
  }

  /**
   * Calculate the corresponding GeoId value from the grid coordinates
   *
   * @param row Gridded row index
   * @param column Gridded column index
   * @return hash id
   */
  public static long colRow2GeoID(int row, int column) {
    long geoID = 0L;
    int bit = 0;
    long sourceRow = (long) row;
    long sourceColumn = (long)column;
    while (sourceRow > 0 || sourceColumn > 0) {
      geoID = geoID | ((sourceRow & 1) << (2 * bit + 1)) | ((sourceColumn & 1) << 2 * bit);
      sourceRow >>= 1;
      sourceColumn >>= 1;
      bit++;
    }
    return geoID;
  }

  /**
   * Convert input GeoID to longitude and latitude
   *
   * @param geoId GeoID
   * @param oriLatitude the origin point latitude
   * @param gridSize the grid size
   * @return Longitude and latitude of grid center point
   */
  public static double[] geoID2LatLng(long geoId, double oriLatitude, int gridSize) {
    int[] rowCol = geoID2ColRow(geoId);
    int column = rowCol[1];
    int row = rowCol[0];
    int cutLevel = getCutCount(gridSize, oriLatitude);
    double deltaX = getDeltaX(oriLatitude, gridSize);
    double deltaY = getDeltaY(gridSize);
    double longitude = (column - (1 << (cutLevel - 1)) + 0.5) * deltaX;
    double latitude = (row - (1 << (cutLevel - 1)) + 0.5) * deltaY;
    longitude = new BigDecimal(longitude).setScale(GeoConstants.SCALE_OF_LONGITUDE_AND_LATITUDE,
        BigDecimal.ROUND_HALF_UP).doubleValue();
    latitude = new BigDecimal(latitude).setScale(GeoConstants.SCALE_OF_LONGITUDE_AND_LATITUDE,
        BigDecimal.ROUND_HALF_UP).doubleValue();
    return new double[]{latitude, longitude};
  }

  /**
   * Convert input GeoID to grid column and row
   *
   * @param geoId GeoID
   * @return grid column index and row index
   */
  public static int[] geoID2ColRow(long geoId) {
    int row = 0;
    int column = 0;
    int bit = 0;
    long source = geoId;
    while (source > 0) {
      column |= (source & 1) << bit;
      source >>= 1;
      row |= (source & 1) << bit;
      source >>= 1;
      bit++;
    }
    return new int[] {row, column};
  }

  /**
   * Convert input string polygon to GeoID range list
   *
   * @param polygon input polygon string
   * @param oriLatitude the origin point latitude
   * @param gridSize the grid size
   * @return GeoID range list of the polygon
   */
  public static List<Long[]> getRangeList(String polygon, double oriLatitude, int gridSize) {
    List<double[]> queryPointList = getPointListFromPolygon(polygon);
    int cutLevel = getCutCount(gridSize, oriLatitude);
    double deltaY = getDeltaY(gridSize);
    double maxLatitudeOfInitialArea = deltaY * Math.pow(2, cutLevel - 1);
    double mCos = Math.cos(oriLatitude * Math.PI / GeoConstants.CONVERT_FACTOR);
    double maxLongitudeOfInitialArea = maxLatitudeOfInitialArea / mCos;
    double minLatitudeOfInitialArea = -maxLatitudeOfInitialArea;
    double minLongitudeOfInitialArea = -maxLongitudeOfInitialArea;
    QuadTreeCls qTreee = new QuadTreeCls(minLongitudeOfInitialArea, minLatitudeOfInitialArea,
        maxLongitudeOfInitialArea, maxLatitudeOfInitialArea, cutLevel,
        GeoConstants.SCALE_OF_LONGITUDE_AND_LATITUDE);
    qTreee.insert(queryPointList);
    return qTreee.getNodesData();
  }

  /**
   * Convert input GeoID to upper layer GeoID of pyramid
   *
   * @param geoId GeoID
   * @return the upper layer GeoID
   */
  public static long convertToUpperLayerGeoId(long geoId) {
    return geoId >> 2;
  }

  /**
   * Parse input polygon string to point list
   *
   * @param polygon input polygon string, example: POLYGON (35 10, 45 45, 15 40, 10 20, 35 10)
   * @return the point list
   */
  public static List<double[]> getPointListFromPolygon(String polygon) {
    String[] pointStringList = polygon.trim().split(GeoConstants.DEFAULT_DELIMITER);
    if (4 > pointStringList.length) {
      throw new RuntimeException(
          "polygon need at least 3 points, really has " + pointStringList.length);
    }
    List<double[]> queryList = new ArrayList<>();
    for (String pointString : pointStringList) {
      String[] point = splitStringToPoint(pointString);
      if (2 != point.length) {
        throw new RuntimeException("longitude and latitude is a pair need 2 data");
      }
      try {
        queryList.add(new double[] {Double.valueOf(point[0]), Double.valueOf(point[1])});
      } catch (NumberFormatException e) {
        throw new RuntimeException("can not covert the string data to double", e);
      }
    }
    if (!checkPointsSame(pointStringList[0], pointStringList[pointStringList.length - 1])) {
      throw new RuntimeException("the first point and last point in polygon should be same");
    } else {
      return queryList;
    }
  }

  private static boolean checkPointsSame(String point1, String point2) {
    String[] points1 = splitStringToPoint(point1);
    String[] points2 = splitStringToPoint(point2);
    return points1[0].equals(points2[0]) && points1[1].equals(points2[1]);
  }

  public static String[] splitStringToPoint(String str) {
    return str.trim().split("\\s+");
  }

  public static String getRangeListAsString(List<Long[]> rangeList) {
    StringBuilder rangeString = null;
    for (Long[] range : rangeList) {
      if (rangeString != null) {
        rangeString.append(",");
      }
      if (rangeString == null) {
        rangeString = new StringBuilder(StringUtils.join(range, " "));
      } else {
        rangeString.append(StringUtils.join(range, " "));
      }
    }
    return rangeString.toString();
  }

  public static void validateRangeList(List<Long[]> ranges) {
    for (Long[] range : ranges) {
      if (range.length != 2) {
        throw new RuntimeException("Query processor must return list of ranges with each range "
            + "containing minimum and maximum values");
      }
    }
  }

  /**
   * Get two polygon's union and intersection
   *
   * @param rangeListA geoId range list of polygonA
   * @param rangeListB geoId range list of polygonB
   * @return geoId range list of processed set
   */
  public static List<Long[]> processRangeList(List<Long[]> rangeListA, List<Long[]> rangeListB,
      GeoOperationType operationType) {
    List<Long[]> processedRangeList;
    switch (operationType) {
      case OR:
        processedRangeList = getPolygonUnion(rangeListA, rangeListB);
        break;
      case AND:
        processedRangeList = getPolygonIntersection(rangeListA, rangeListB);
        break;
      default:
        throw new RuntimeException("Unsupported operation type " + operationType.toString());
    }
    return processedRangeList;
  }

  /**
   * Get two polygon's union
   *
   * @param rangeListA geoId range list of polygonA
   * @param rangeListB geoId range list of polygonB
   * @return geoId range list after union
   */
  private static List<Long[]> getPolygonUnion(List<Long[]> rangeListA, List<Long[]> rangeListB) {
    if (Objects.isNull(rangeListA)) {
      return rangeListB;
    }
    if (Objects.isNull(rangeListB)) {
      return rangeListA;
    }
    int sizeFirst = rangeListA.size();
    int sizeSecond = rangeListB.size();
    if (sizeFirst > sizeSecond) {
      rangeListA.addAll(sizeFirst, rangeListB);
      return mergeList(rangeListA);
    } else {
      rangeListB.addAll(sizeSecond, rangeListA);
      return mergeList(rangeListB);
    }
  }

  private static List<Long[]> mergeList(List<Long[]> list) {
    if (list.size() == 0) {
      return list;
    }
    Collections.sort(list, new Comparator<Long[]>() {
      @Override
      public int compare(Long[] arr1, Long[] arr2) {
        return Long.compare(arr1[0], arr2[0]);
      }
    });
    Long[] min;
    Long[] max;
    for (int i = 0; i < list.size(); i++) {
      min = list.get(i);
      for (int j = i + 1; j < list.size(); j++) {
        max = list.get(j);
        if (min[1] + 1 >= max[0]) {
          min[1] = Math.max(max[1], min[1]);
          list.remove(j);
          j--;
        } else {
          break;
        }
      }
    }
    return list;
  }

  /**
   * Get two polygon's intersection
   *
   * @param rangeListA geoId range list of polygonA
   * @param rangeListB geoId range list of polygonB
   * @return geoId range list after intersection
   */
  private static List<Long[]> getPolygonIntersection(List<Long[]> rangeListA,
      List<Long[]> rangeListB) {
    List<Long[]> intersectionList = new ArrayList<>();
    if (Objects.isNull(rangeListA) || Objects.isNull(rangeListB)) {
      return Collections.emptyList();
    }
    int endIndex1 = rangeListA.size();
    int endIndex2 = rangeListB.size();
    int startIndex1 = 0;
    int startIndex2 = 0;

    long start = 0;
    long end = 0;
    while (startIndex1 < endIndex1 && startIndex2 < endIndex2) {
      start = Math.max(rangeListA.get(startIndex1)[0], rangeListB.get(startIndex2)[0]);
      if (rangeListA.get(startIndex1)[1] < rangeListB.get(startIndex2)[1]) {
        end = rangeListA.get(startIndex1)[1];
        startIndex1++;
      } else {
        end = rangeListB.get(startIndex2)[1];
        startIndex2++;
      }
      if (start <= end) {
        intersectionList.add(new Long[]{start, end});
      }
    }
    return intersectionList;
  }

  /**
   * Evaluate whether the search value is in the GeoId ranges.
   * If the search value is greater than or equal to the minimum of one range, i.e.ranges.get(i)[0]
   * and less than or equal to the maximum of this range, i.e.ranges.get(i)[1], return true,
   * otherwise false.
   *
   * Used in method evaluate(RowIntf value) of PolygonExpression, PolygonListExpression,
   * PolylineListExpression, PolygonRangeListExpression.
   *
   * @param ranges GeoId ranges of expression, including PolygonExpression, PolygonListExpression,
   *               PolylineListExpression and PolygonRangeListExpression
   * @param searchForNumber the search value, which is GeoId of each row of geo table
   */
  public static boolean rangeBinarySearch(List<Long[]> ranges, long searchForNumber) {
    Long[] range;
    int low = 0, mid, high = ranges.size() - 1;
    while (low <= high) {
      mid = low + ((high - low) / 2);
      range = ranges.get(mid);
      if (searchForNumber >= range[0]) {
        if (searchForNumber <= range[1]) {
          // Return true if the number is between min and max values of the range
          return true;
        } else {
          // Number is bigger than this range's min and max. Search on the right side of the range
          low = mid + 1;
        }
      } else {
        // Number is smaller than this range's min and max. Search on the left side of the range
        high = mid - 1;
      }
    }
    return false;
  }

  /**
   * Evaluate whether the search value(geoId) is present in the GeoId polygon ranges.
   */
  public static boolean performRangeSearch(String polygonRanges, String geoId) {
    if (null == polygonRanges || polygonRanges.equalsIgnoreCase("null")) {
      return false;
    }
    List<Long[]> ranges = PolygonRangeListExpression.getRangeListFromString(polygonRanges);
    // check if the geoId is present within the ranges
    return GeoHashUtils.rangeBinarySearch(ranges, Long.parseLong(geoId));
  }

  /**
   * Apply pattern for a input polygon row
   * @param regex to be applied to a pattern
   * @param polygonOrRanges could be polygon or GeoId RangeList
   * @return polygon or GeoId range
   */
  public static String getRange(String regex, String polygonOrRanges) {
    // parser and get the range list
    Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(polygonOrRanges);
    String range = polygonOrRanges;
    while (matcher.find()) {
      range = matcher.group();
    }
    return range;
  }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CustomIndex;

import org.apache.commons.lang3.StringUtils;

import org.apache.log4j.Logger;

/**
 * GeoHash custom implementation.
 * This class extends {@link CustomIndex}. It provides methods to
 * 1. Extracts the sub-properties of geohash type index handler such as type, source columns,
 * grid size, origin, min and max longitude and latitude of data. Validates and stores them in
 * instance.
 * 2. Generates column value from the longitude and latitude column values.
 * 3. Query processor to handle the custom UDF filter queries based on longitude and latitude
 * columns.
 */
public class GeoHashImpl extends CustomIndex<List<Long[]>> {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(GeoHashImpl.class.getName());

  // conversion factor of angle to radian
  private static final double CONVERT_FACTOR = 180.0;
  // Earth radius
  private static final double EARTH_RADIUS = 6371004.0;
  // Latitude of coordinate origin
  private double oriLatitude;
  // User defined maximum longitude of map
  private double userDefineMaxLongitude;
  // User defined maximum latitude of map
  private double userDefineMaxLatitude;
  // User defined map minimum longitude
  private double userDefineMinLongitude;
  // User defined map minimum latitude
  private double userDefineMinLatitude;
  // The maximum longitude of the completed map after calculation
  private double CalculateMaxLongitude;
  // The maximum latitude of the completed map after calculation
  private double CalculateMaxLatitude;
  // Grid length is in meters
  private int gridSize;
  // cos value of latitude of origin of coordinate
  private double mCos;
  // The degree of Y axis corresponding to each grid size length
  private double deltaY;
  // Each grid size length should be the degree of X axis
  private double deltaX;
  // Degree * coefficient of Y axis corresponding to each grid size length
  private double deltaYByRatio;
  // Each grid size length should be X-axis Degree * coefficient
  private double deltaXByRatio;
  // The number of knives cut for the whole area (one horizontally and one vertically)
  // is the depth of quad tree
  private int cutLevel;
  // used to convert the latitude and longitude of double type to int type for calculation
  private int conversionRatio;
  // * Constant of coefficient
  private double lon0ByRation;
  // * Constant of coefficient
  private double lat0ByRation;


  /**
   * Initialize the geohash index handler instance.
   * the properties is like that:
   * TBLPROPERTIES ('INDEX_HANDLER'='mygeohash',
   * 'INDEX_HANDLER.mygeohash.type'='geohash',
   * 'INDEX_HANDLER.mygeohash.sourcecolumns'='longitude, latitude',
   * 'INDEX_HANDLER.mygeohash.gridSize'=''
   * 'INDEX_HANDLER.mygeohash.minLongitude'=''
   * 'INDEX_HANDLER.mygeohash.maxLongitude'=''
   * 'INDEX_HANDLER.mygeohash.minLatitude'=''
   * 'INDEX_HANDLER.mygeohash.maxLatitude'=''
   * 'INDEX_HANDLER.mygeohash.orilatitude''')
   * @param handlerName the class name of generating algorithm
   * @param properties input properties,please check the describe
   * @throws Exception
   */
  @Override
  public void init(String handlerName, Map<String, String> properties) throws Exception {
    String options = properties.get(CarbonCommonConstants.INDEX_HANDLER);
    if (StringUtils.isEmpty(options)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid.", CarbonCommonConstants.INDEX_HANDLER));
    }
    options = options.toLowerCase();
    if (!options.contains(handlerName.toLowerCase())) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s is not present.",
                      CarbonCommonConstants.INDEX_HANDLER, handlerName));
    }
    String commonKey = CarbonCommonConstants.INDEX_HANDLER + CarbonCommonConstants.POINT +
            handlerName + CarbonCommonConstants.POINT;
    String TYPE = commonKey + "type";
    String type = properties.get(TYPE);
    if (!CarbonCommonConstants.GEOHASH.equalsIgnoreCase(type)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be %s for this class.",
                      CarbonCommonConstants.INDEX_HANDLER, TYPE, CarbonCommonConstants.GEOHASH));
    }
    String SOURCE_COLUMNS = commonKey + "sourcecolumns";
    String sourceColumnsOption = properties.get(SOURCE_COLUMNS);
    if (StringUtils.isEmpty(sourceColumnsOption)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. Must specify %s property.",
                      CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
    }
    if (sourceColumnsOption.split(",").length != 2) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must have 2 columns.",
                      CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
    }
    String SOURCE_COLUMN_TYPES = commonKey + "sourcecolumntypes";
    String sourceDataTypes = properties.get(SOURCE_COLUMN_TYPES);
    String[] srcTypes = sourceDataTypes.split(",");
    for (String srcdataType : srcTypes) {
      if (!"bigint".equalsIgnoreCase(srcdataType)) {
        throw new MalformedCarbonCommandException(
                String.format("%s property is invalid. %s datatypes must be long.",
                        CarbonCommonConstants.INDEX_HANDLER, SOURCE_COLUMNS));
      }
    }
    // Set the generated column data type as long
    String TARGET_DATA_TYPE = commonKey + "datatype";
    properties.put(TARGET_DATA_TYPE, "long");
    String ORIGIN_LATITUDE = commonKey + "originlatitude";
    String originLatitude = properties.get(ORIGIN_LATITUDE);
    if (StringUtils.isEmpty(originLatitude)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. Must specify %s property.",
                      CarbonCommonConstants.INDEX_HANDLER, ORIGIN_LATITUDE));
    }
    String MIN_LONGITUDE = commonKey + "minlongitude";
    String MAX_LONGITUDE = commonKey + "maxlongitude";
    String MIN_LATITUDE = commonKey + "minlatitude";
    String MAX_LATITUDE = commonKey + "maxlatitude";
    String minLongitude = properties.get(MIN_LONGITUDE);
    String maxLongitude = properties.get(MAX_LONGITUDE);
    String minLatitude = properties.get(MIN_LATITUDE);
    String maxLatitude = properties.get(MAX_LATITUDE);
    if (StringUtils.isEmpty(minLongitude)) {
      throw new MalformedCarbonCommandException(
          String.format("%s property is invalid. Must specify %s property.",
              CarbonCommonConstants.INDEX_HANDLER, MIN_LONGITUDE));
    }
    if (StringUtils.isEmpty(minLatitude)) {
      throw new MalformedCarbonCommandException(
          String.format("%s property is invalid. Must specify %s property.",
              CarbonCommonConstants.INDEX_HANDLER, MIN_LATITUDE));
    }
    if (StringUtils.isEmpty(maxLongitude)) {
      throw new MalformedCarbonCommandException(
          String.format("%s property is invalid. Must specify %s property.",
              CarbonCommonConstants.INDEX_HANDLER, MAX_LONGITUDE));
    }
    if (StringUtils.isEmpty(maxLatitude)) {
      throw new MalformedCarbonCommandException(
          String.format("%s property is invalid. Must specify %s property.",
              CarbonCommonConstants.INDEX_HANDLER, MAX_LATITUDE));
    }
    String GRID_SIZE = commonKey + "gridsize";
    String gridSize = properties.get(GRID_SIZE);
    if (StringUtils.isEmpty(gridSize)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be specified.",
                      CarbonCommonConstants.INDEX_HANDLER, GRID_SIZE));
    }
    String CONVERSION_RATIO = commonKey + "conversionratio";
    String conversionRatio = properties.get(CONVERSION_RATIO);
    if (StringUtils.isEmpty(conversionRatio)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be specified.",
                      CarbonCommonConstants.INDEX_HANDLER, CONVERSION_RATIO));
    }

    // Fill the values to the instance fields
    this.oriLatitude = Double.valueOf(originLatitude);
    this.userDefineMaxLongitude = Double.valueOf(maxLongitude);
    this.userDefineMaxLatitude = Double.valueOf(maxLatitude);
    this.userDefineMinLongitude = Double.valueOf(minLongitude);
    this.userDefineMinLatitude = Double.valueOf(minLatitude);
    this.gridSize = Integer.parseInt(gridSize);
    this.conversionRatio = Integer.parseInt(conversionRatio);
    calculateData();
  }

  /**
   * Generates the GeoHash ID column value from the given source columns.
   * @param sources Longitude and Latitude
   * @return Returns the generated hash id
   * @throws Exception
   */
  @Override
  public String generate(List<?> sources) throws Exception {
    if (sources.size() != 2) {
      throw new RuntimeException("Source columns list must be of size 2.");
    }
    if (sources.get(0) == null || sources.get(1) == null) {
      // Bad record. Just return null
      return null;
    }
    if (!(sources.get(0) instanceof Long) || !(sources.get(1) instanceof Long)) {
      throw new RuntimeException("Source columns must be of Long type.");
    }
    Long longitude = (Long) sources.get(0);
    Long latitude  = (Long) sources.get(1);
    // generate the hash code
    int[] gridPoint = calculateID(longitude, latitude);
    Long hashId = createHashID(gridPoint[0], gridPoint[1]);
    return String.valueOf(hashId);
  }

  /**
   * Query processor for GeoHash.
   * example: POLYGON (35 10, 45 45, 15 40, 10 20, 35 10)
   * so there will be a sample check
   * @param polygon a group of pints, close out to form an area
   * @return Returns list of ranges of GeoHash IDs
   * @throws Exception
   */
  @Override
  public List<Long[]> query(String polygon) throws Exception {
    if (!validate(polygon)) {
      return null;
    } else {
      String[] pointList = polygon.trim().split(",");
      List<double[]> queryList = new ArrayList<>();
      for (String str: pointList) {
        String[] points = splitString(str);
        if (2 != points.length) {
          throw new RuntimeException("longitude and latitude is a pair need 2 data");
        } else {
          try {
            queryList.add(new double[] {Double.valueOf(points[0]), Double.valueOf(points[1])});
          } catch (NumberFormatException e) {
            throw new RuntimeException("can not covert the string data to double", e);
          }
        }
      }
      if (!checkPointsSame(pointList[0], pointList[pointList.length - 1])) {
        throw new RuntimeException("the first point and last point in polygon should be same");
      } else {
        List<Long[]> rangeList = getPolygonRangeList(queryList);
        return rangeList;
      }
    }
  }

  private String[] splitString(String str) {
    return str.trim().split("\\s+");
  }

  private boolean checkPointsSame(String point1, String point2) throws Exception {
    String[] points1 = splitString(point1);
    String[] points2 = splitString(point2);
    return points1[0].equals(points2[0]) && points1[1].equals(points2[1]);
  }

  private boolean validate(String polygon) throws Exception {
    String[] pointList = polygon.trim().split(",");
    if (4 > pointList.length) {
      throw new RuntimeException("polygon need at least 3 points, really has " + pointList.length);
    }
    return true;
  }

  /**
   * use query polygon condition to get the hash id list, the list is merged and sorted.
   * @param queryList polygon points close out to form an area
   * @return hash id list
   * @throws Exception
   */
  private  List<Long[]> getPolygonRangeList(List<double[]> queryList) throws Exception {
    QuadTreeCls qTreee = new QuadTreeCls(userDefineMinLongitude, userDefineMinLatitude,
        CalculateMaxLongitude, CalculateMaxLatitude, cutLevel);
    qTreee.insert(queryList);
    return qTreee.getNodesData();
  }

  /**
   *  After necessary attributes, perform necessary calculation
   * @throws Exception
   */
  private void calculateData() throws Exception {
    // Angular to radian, radians = (Math.PI / 180) * degrees
    // Cosine value of latitude angle of origin
    this.mCos = Math.cos(this.oriLatitude / this.conversionRatio * Math.PI / CONVERT_FACTOR);
    // get δx=L∗360/(2πR∗cos(lat))
    this.deltaX = (this.gridSize * 360) / (2 * Math.PI * EARTH_RADIUS * this.mCos);
    this.deltaXByRatio = this.deltaX * this.conversionRatio;
    // get δy=L∗360/2πR
    this.deltaY = (this.gridSize * 360) / (2 * Math.PI * EARTH_RADIUS);
    this.deltaYByRatio = this.deltaY * this.conversionRatio;
    LOGGER.info("after spatial calculate delta X is: " + String.format("%f", this.deltaX)
                    + "the delta Y is: " + String.format("%f", this.deltaY));
    LOGGER.info("after spatial calculate X ByRatio is: " + String.format("%f", this.deltaXByRatio)
                    + "the Y ByRatio is: " + String.format("%f", this.deltaYByRatio));
    // Calculate the complement area and grid i,j for grid number
    // Xmax = x0+(2^n∗δx) Ymax = y0+(2^n∗δx) Where n is the number of cut
    // Where x0, Y0 are the minimum x, y coordinates of a given region，
    // Xmax >= maxLongitude Ymax >= maxLatitude
    // In the calculation process, first substitute maxlongitude and maxlatitude to calculate n.
    // if n is not an integer, then take the next integer of N, and then substitute to find
    // xmax and ymax。
    this.calculateArea();
  }

  /**
   * Calculate the complement area, including the range of the complement area, t
   * he number of knives cut and the depth of the quad tree
   */
  private void calculateArea() {
    // step 1 calculate xn, yn by using maxLongitude, maxLatitude, minLongitude, minLatitude
    // substitution formula
    // Here, the user's given area is mostly rectangle, which needs to be extended to
    // square processing to find the maximum value of XN and yn
    // n=log_2 （Xmax−X0)/δx， log_2 （Ymax−Y0)/δy
    userDefineMinLongitude = userDefineMinLongitude - deltaX / 2;
    userDefineMaxLongitude = userDefineMaxLongitude + deltaX / 2;
    userDefineMinLatitude = userDefineMinLatitude - deltaY / 2;
    userDefineMaxLatitude = userDefineMaxLatitude + deltaY / 2;

    this.lon0ByRation = userDefineMinLongitude * this.conversionRatio;
    this.lat0ByRation = userDefineMinLatitude * this.conversionRatio;

    double Xn = Math.log((userDefineMaxLongitude - userDefineMinLongitude) / deltaX)
                    / Math.log(2);
    double Yn = Math.log((userDefineMaxLatitude - userDefineMinLatitude) / deltaY)
                    / Math.log(2);
    double doubleMax = Math.max(Xn, Yn);
    this.cutLevel = doubleMax % 1 == 0 ? (int) doubleMax : (int) (doubleMax + 1);
    // step 2 recalculate the region according to the number of segmentation
    this.CalculateMaxLongitude = userDefineMinLongitude + Math.pow(2, this.cutLevel)
                                                              * deltaX;
    this.CalculateMaxLatitude = userDefineMinLatitude + Math.pow(2, this.cutLevel)
                                                            * deltaY;
    LOGGER.info("After spatial calculate the cut level is: " + String.format("%d", this.cutLevel));
    LOGGER.info("the min longitude is: " + String.format("%f", this.userDefineMinLongitude) +
                    " the max longitude is: " + String.format("%f", this.CalculateMaxLongitude));
    LOGGER.info("the min latitude is: " + String.format("%f", this.userDefineMinLatitude) +
                    " the max latitude is: " + String.format("%f", this.CalculateMaxLatitude));
  }

  /**
   * Through grid index coordinates and calculation of hashid, grid latitude and longitude
   * coordinates can be transformed by latitude and longitude
   * @param longitude Longitude, the actual longitude and latitude are processed by * coefficient,
   *                  and the floating-point calculation is converted to integer calculation
   * @param latitude Latitude, the actual longitude and latitude are processed by * coefficient,
   *                 and the floating-point calculation is converted to integer calculation.
   * @return Grid ID value [row, column] column starts from 1
   */
  private int[] calculateID(long longitude, long latitude) throws Exception {
    try {
      int row = (int) ((longitude - this.lon0ByRation) / this.deltaXByRatio);
      int column = (int) ((latitude - this.lat0ByRation) / this.deltaYByRatio);
      return new int[]{row, column};
    } catch (ArithmeticException  e) {
      throw new RuntimeException("can not divide by zero.");
    }
  }

  /**
   * Calculate the corresponding hashid value from the grid coordinates
   * @param row Gridded row index
   * @param column Gridded column index
   * @return hash id
   */
  private long createHashID(long row, long column) {
    long index = 0L;
    for (int i = 0; i < cutLevel + 1; i++) {
      long x = (row >> i) & 1;    // take position I
      long y = (column >> i) & 1;
      index = index | (x << (2 * i + 1)) | (y << 2 * i);
    }
    return index;
  }
}

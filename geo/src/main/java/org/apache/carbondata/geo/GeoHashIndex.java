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

import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CustomIndex;

import org.apache.commons.lang3.StringUtils;

import org.apache.log4j.Logger;

/**
 * GeoHash Type Spatial Index Custom Implementation.
 * This class extends {@link CustomIndex}. It provides methods to
 * 1. Extracts the sub-properties of geohash type spatial index such as type, source columns,
 * grid size, origin, min and max longitude and latitude of data. Validates and stores them in
 * instance.
 * 2. Generates column value from the longitude and latitude column values.
 * 3. Query processor to handle the custom UDF filter queries based on longitude and latitude
 * columns.
 */
public class GeoHashIndex extends CustomIndex<List<Long[]>> {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(GeoHashIndex.class.getName());

  // Latitude of coordinate origin
  private double oriLatitude;
  // The minimum longitude of the completed map after calculation
  private double calculateMinLongitude;
  // The minimum latitude of the completed map after calculation
  private double calculateMinLatitude;
  // The maximum longitude of the completed map after calculation
  private double calculateMaxLongitude;
  // The maximum latitude of the completed map after calculation
  private double calculateMaxLatitude;
  // Grid length is in meters
  private int gridSize;
  // cos value of latitude of origin of coordinate
  private double mCos;
  // The degree of Y axis corresponding to each grid size length
  private double deltaY;
  // Each grid size length should be the degree of X axis
  private double deltaX;
  // The number of knives cut for the whole area (one horizontally and one vertically)
  // is the depth of quad tree
  private int cutLevel;
  // used to convert the latitude and longitude of double type to int type for calculation
  private int conversionRatio;


  /**
   * Initialize the geohash spatial index instance.
   * the properties is like that:
   * TBLPROPERTIES ('SPATIAL_INDEX'='mygeohash',
   * 'SPATIAL_INDEX.mygeohash.type'='geohash',
   * 'SPATIAL_INDEX.mygeohash.sourcecolumns'='longitude, latitude',
   * 'SPATIAL_INDEX.mygeohash.gridSize'=''
   * 'SPATIAL_INDEX.mygeohash.orilatitude''')
   * @param indexName index name. Implicitly a column is created with index name.
   * @param properties input properties,please check the describe
   * @throws Exception
   */
  @Override
  public void init(String indexName, Map<String, String> properties) throws Exception {
    String options = properties.get(CarbonCommonConstants.SPATIAL_INDEX);
    if (StringUtils.isEmpty(options)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid.", CarbonCommonConstants.SPATIAL_INDEX));
    }
    options = options.toLowerCase();
    if (!options.contains(indexName.toLowerCase())) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s is not present.",
                      CarbonCommonConstants.SPATIAL_INDEX, indexName));
    }
    String commonKey = CarbonCommonConstants.SPATIAL_INDEX + CarbonCommonConstants.POINT + indexName
        + CarbonCommonConstants.POINT;
    String TYPE = commonKey + "type";
    String type = properties.get(TYPE);
    if (!GeoConstants.GEOHASH.equalsIgnoreCase(type)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must be %s for this class.",
                      CarbonCommonConstants.SPATIAL_INDEX, TYPE, GeoConstants.GEOHASH));
    }
    String SOURCE_COLUMNS = commonKey + "sourcecolumns";
    String sourceColumnsOption = properties.get(SOURCE_COLUMNS);
    if (StringUtils.isEmpty(sourceColumnsOption)) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. Must specify %s property.",
                      CarbonCommonConstants.SPATIAL_INDEX, SOURCE_COLUMNS));
    }
    if (sourceColumnsOption.split(",").length != 2) {
      throw new MalformedCarbonCommandException(
              String.format("%s property is invalid. %s property must have 2 columns.",
                      CarbonCommonConstants.SPATIAL_INDEX, SOURCE_COLUMNS));
    }
    String SOURCE_COLUMN_TYPES = commonKey + "sourcecolumntypes";
    String sourceDataTypes = properties.get(SOURCE_COLUMN_TYPES);
    String[] srcTypes = sourceDataTypes.split(",");
    for (String srcdataType : srcTypes) {
      if (!"bigint".equalsIgnoreCase(srcdataType)) {
        throw new MalformedCarbonCommandException(
                String.format("%s property is invalid. %s datatypes must be long.",
                        CarbonCommonConstants.SPATIAL_INDEX, SOURCE_COLUMNS));
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
                      CarbonCommonConstants.SPATIAL_INDEX, ORIGIN_LATITUDE));
    }
    String GRID_SIZE = commonKey + "gridsize";
    String gridSize = properties.get(GRID_SIZE);
    GeoHashUtils.validateGeoProperty(gridSize, GRID_SIZE);
    String CONVERSION_RATIO = commonKey + "conversionratio";
    String conversionRatio = properties.get(CONVERSION_RATIO);
    GeoHashUtils.validateGeoProperty(conversionRatio, CONVERSION_RATIO);

    // Fill the values to the instance fields
    this.oriLatitude = Double.valueOf(originLatitude);
    this.gridSize = Integer.parseInt(gridSize);
    this.conversionRatio = Integer.parseInt(conversionRatio);
    calculateInitialArea();
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
    long longtitudeByRatio = longitude * (GeoConstants.CONVERSION_RATIO / this.conversionRatio);
    long latitudeByRatio = latitude * (GeoConstants.CONVERSION_RATIO / this.conversionRatio);
    Long hashId = lonLat2GeoID(longtitudeByRatio, latitudeByRatio, this.oriLatitude, this.gridSize);
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
    List<double[]> queryList = GeoHashUtils.getPointListFromPolygon(polygon);
    return getPolygonRangeList(queryList);
  }

  /**
   * Query processor for GeoHash.
   * example: [[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]]
   * @param queryPointList point list of a polygon, close out to form an area
   * @return Returns list of ranges of GeoHash IDs
   * @throws Exception
   */
  @Override
  public List<Long[]> query(List<double[]> queryPointList) throws Exception {
    return getPolygonRangeList(queryPointList);
  }

  /**
   * use query polygon condition to get the hash id list, the list is merged and sorted.
   * @param queryList polygon points close out to form an area
   * @return hash id list
   * @throws Exception
   */
  private  List<Long[]> getPolygonRangeList(List<double[]> queryList) throws Exception {
    QuadTreeCls qTreee = new QuadTreeCls(calculateMinLongitude, calculateMinLatitude,
        calculateMaxLongitude, calculateMaxLatitude, cutLevel, (int)Math.log10(conversionRatio));
    qTreee.insert(queryList);
    return qTreee.getNodesData();
  }

  /**
   * Calculate the initial partition area and some variables, that are used to generate GeoId,
   * including the minLatitude, maxLatitude, minLongitude and maxLongitude of the partition area,
   * the number of knives cut, which is also the depth of the quad tree.
   */
  private void calculateInitialArea() {
    this.cutLevel = GeoHashUtils.getCutCount(gridSize, oriLatitude);
    this.deltaX = GeoHashUtils.getDeltaX(oriLatitude, gridSize);
    this.deltaY = GeoHashUtils.getDeltaY(gridSize);
    double maxLatitudeOfInitialArea = this.deltaY * Math.pow(2, this.cutLevel - 1);
    this.mCos = Math.cos(oriLatitude * Math.PI / GeoConstants.CONVERT_FACTOR);
    double maxLongitudeOfInitialArea = maxLatitudeOfInitialArea / mCos;
    this.calculateMinLatitude = -maxLatitudeOfInitialArea;
    this.calculateMaxLatitude = maxLatitudeOfInitialArea;
    this.calculateMinLongitude = -maxLongitudeOfInitialArea;
    this.calculateMaxLongitude = maxLongitudeOfInitialArea;
    LOGGER.info("after spatial calculate delta X is: " + String.format("%f", this.deltaX) +
        "the delta Y is: " + String.format("%f", this.deltaY));
    LOGGER.info("After spatial calculate the cut level is: " + String.format("%d", this.cutLevel));
    LOGGER.info("the min longitude is: " + String.format("%f", this.calculateMinLongitude) +
        " the max longitude is: " + String.format("%f", this.calculateMaxLongitude));
    LOGGER.info("the min latitude is: " + String.format("%f", this.calculateMinLatitude) +
        " the max latitude is: " + String.format("%f", this.calculateMaxLatitude));
  }

  /**
   * Transform longitude and latitude to geo id
   *
   * @param longitude the point longitude
   * @param latitude the point latitude
   * @param oriLatitude the origin point latitude
   * @param gridSize the grid size
   * @return GeoID
   */
  private long lonLat2GeoID(long longitude, long latitude, double oriLatitude, int gridSize) {
    int[] ij = GeoHashUtils.lonLat2ColRow(longitude, latitude, oriLatitude, gridSize);
    return GeoHashUtils.colRow2GeoID(ij[0], ij[1]);
  }
}

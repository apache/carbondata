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

/**
 * Geo Constants
 */
public class GeoConstants {
  private GeoConstants() {
  }

  // GeoHash type Spatial Index
  public static final String GEOHASH = "geohash";

  // Regular expression to validate whether the input value is positive integer
  public static final String POSITIVE_INTEGER_REGEX = "^[+]?\\d*[1-9]\\d*$";

  // Regular expression to parse input polygons for IN_POLYGON_LIST
  public static final String POLYGON_REG_EXPRESSION = "(?<=POLYGON[ ]{0,1}\\(\\()(.*?)(?=(\\)\\)))";

  // Regular expression to parse input polylines for IN_POLYLINE_LIST
  public static final String POLYLINE_REG_EXPRESSION = "LINESTRING *\\(.*?\\)";

  // Regular expression to parse input rangelists for IN_POLYGON_RANGE_LIST
  public static final String RANGELIST_REG_EXPRESSION = "(?<=RANGELIST[ ]{0,1}\\()(.*?)(?=\\))";

  public static final String GRID_SIZE = "gridSize";

  public static final String RANGE_LIST = "rangelist";

  // delimiter of input points or ranges
  public static final String DEFAULT_DELIMITER = ",";

  // conversion factor of angle to radian
  public static final double CONVERT_FACTOR = 180.0;

  // Earth radius
  public static final double EARTH_RADIUS = 6371004.0;

  // used in Geo Hash calculation formula for improving calculation accuracy
  public static final int CONVERSION_RATIO = 100000000;

  // used for multiplying input longitude and latitude which are processed by * 10E6
  public static final int CONVERSION_FACTOR_FOR_ACCURACY = 100;

  // used in transforming UDF geoID2LngLat, set scale of BigDecimal
  public static final int SCALE_OF_LONGITUDE_AND_LATITUDE = 6;

  // Length in meters of 1 degree of latitude
  public static final double CONVERSION_FACTOR_OF_METER_TO_DEGREE = 111320;
}

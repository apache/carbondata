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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CustomIndex;

import org.apache.commons.lang3.StringUtils;

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
  /**
   * Initialize the geohash index handler instance.
   * @param handlerName
   * @param properties
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

    // TODO: Fill the values to the instance fields
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
    //TODO: generate geohashId
    return String.valueOf(0);
  }

  /**
   * Query processor for GeoHash.
   * @param polygon
   * @return Returns list of ranges of GeoHash IDs
   * @throws Exception
   */
  @Override
  public List<Long[]> query(String polygon) throws Exception {
    List<Long[]> rangeList = new ArrayList<Long[]>();
    // TODO: process the polygon coordinates and generate the list of ranges of GeoHash IDs
    return rangeList;
  }
}

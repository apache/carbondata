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

package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.exception.InvalidConfigurationException;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_ENABLE_INDEX_SERVER;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_MAJOR_COMPACTION_SIZE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_QUERY_STAGE_INPUT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.DISABLE_SQL_REWRITE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_OFFHEAP_SORT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_UNSAFE_SORT;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_VECTOR_READER;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.NUM_CORES_COMPACTING;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.NUM_CORES_LOADING;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_TIMESTAMPFORMAT;
import static org.apache.carbondata.core.constants.CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB;

import org.apache.log4j.Logger;

/**
 * This class maintains carbon session params
 */
public class SessionParams implements Serializable, Cloneable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CacheProvider.class.getName());
  private static final long serialVersionUID = -7801994600594915264L;

  private Map<String, String> sProps;
  private ConcurrentHashMap<String, String> addedProps;
  // below field to be used when we want the objects to be serialized
  private Map<String, Object> extraInfo;
  public SessionParams() {
    sProps = new HashMap<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2844
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2865
    addedProps = new ConcurrentHashMap<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2076
    extraInfo = new HashMap<>();
  }

  public void setExtraInfo(String key, Object value) {
    this.extraInfo.put(key, value);
  }

  public Object getExtraInfo(String key) {
    return this.extraInfo.get(key);
  }

  /**
   * This method will be used to get the properties value
   *
   * @param key
   * @return properties value
   */
  public String getProperty(String key) {
    return sProps.get(key);
  }

  public String getProperty(String key, String defaultValue) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1520
    if (!sProps.containsKey(key)) {
      return defaultValue;
    }
    return sProps.get(key);
  }

  /**
   * This method will be used to add a new property
   *
   * @param key
   * @return properties value
   */
  public SessionParams addProperty(String key, String value) throws InvalidConfigurationException {
    boolean isValidConf = validateKeyValue(key, value);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1701
    if (isValidConf) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1964
      if (key.equals(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION)) {
        value = value.toUpperCase();
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3064
      LOGGER.info(
          "The key " + key + " with value " + value + " added in the session param");
      sProps.put(key, value);
    }
    return this;
  }

  public Map<String, String> getAll() {
    return sProps;
  }

  public SessionParams addProps(Map<String, String> addedProps) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1361
    this.addedProps.putAll(addedProps);
    return this;
  }

  public Map<String, String> getAddedProps() {
    return addedProps;
  }

  /**
   * validate the key value to be set using set command
   * @param key
   * @param value
   * @return
   * @throws InvalidConfigurationException
   */
  private boolean validateKeyValue(String key, String value) throws InvalidConfigurationException {
    boolean isValid = false;
    switch (key) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
      case ENABLE_SI_LOOKUP_PARTIALSTRING:
      case ENABLE_UNSAFE_SORT:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2507
      case ENABLE_OFFHEAP_SORT:
      case CARBON_CUSTOM_BLOCK_DISTRIBUTION:
      case CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE:
      case CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2900
      case ENABLE_VECTOR_READER:
      case ENABLE_UNSAFE_IN_QUERY_EXECUTION:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2919
      case ENABLE_AUTO_LOAD_MERGE:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3038
      case CARBON_PUSH_ROW_FILTERS_FOR_VECTOR:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
      case CARBON_ENABLE_INDEX_SERVER:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3710
      case CARBON_QUERY_STAGE_INPUT:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      case DISABLE_SQL_REWRITE:
        isValid = CarbonUtil.validateBoolean(value);
        if (!isValid) {
          throw new InvalidConfigurationException("Invalid value " + value + " for key " + key);
        }
        break;
      case CARBON_OPTIONS_BAD_RECORDS_ACTION:
        try {
          LoggerAction.valueOf(value.toUpperCase());
          isValid = true;
        } catch (IllegalArgumentException iae) {
          throw new InvalidConfigurationException(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2194
              "The key " + key + " can have only either FORCE or IGNORE or REDIRECT or FAIL.");
        }
        break;
      case CARBON_OPTIONS_SORT_SCOPE:
        isValid = CarbonUtil.isValidSortOption(value);
        if (!isValid) {
          throw new InvalidConfigurationException("The sort scope " + key
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3607
              + " can have only either NO_SORT, LOCAL_SORT or GLOBAL_SORT.");
        }
        break;
      case CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2900
      case NUM_CORES_LOADING:
      case NUM_CORES_COMPACTING:
      case BLOCKLET_SIZE_IN_MB:
      case CARBON_MAJOR_COMPACTION_SIZE:
        isValid = CarbonUtil.validateValidIntType(value);
        if (!isValid) {
          throw new InvalidConfigurationException(
              "The configured value for key " + key + " must be valid integer.");
        }
        break;
      case CARBON_OPTIONS_BAD_RECORD_PATH:
        isValid = CarbonUtil.isValidBadStorePath(value);
        if (!isValid) {
          throw new InvalidConfigurationException("Invalid bad records location.");
        }
        break;
      // no validation needed while set for CARBON_OPTIONS_DATEFORMAT
      case CARBON_OPTIONS_DATEFORMAT:
        isValid = true;
        break;
      // no validation needed while set for CARBON_OPTIONS_TIMESTAMPFORMAT
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1936
      case CARBON_OPTIONS_TIMESTAMPFORMAT:
        isValid = true;
        break;
      // no validation needed while set for CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT
      case CARBON_OPTIONS_SERIALIZATION_NULL_FORMAT:
        isValid = true;
        break;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2900
      case COMPACTION_SEGMENT_LEVEL_THRESHOLD:
        int[] values = CarbonProperties.getInstance().getIntArray(value);
        if (values.length != 2) {
          throw new InvalidConfigurationException(
              "Invalid COMPACTION_SEGMENT_LEVEL_THRESHOLD: " + value);
        }
        isValid = true;
        break;
      default:
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3440
        if (key.startsWith(CARBON_ENABLE_INDEX_SERVER) && key.split("\\.").length == 6) {
          isValid = true;
        } else if (key.startsWith(CarbonCommonConstants.CARBON_INPUT_SEGMENTS)) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1398
          isValid = CarbonUtil.validateRangeOfSegmentList(value);
          if (!isValid) {
            throw new InvalidConfigurationException("Invalid CARBON_INPUT_SEGMENT_IDs");
          }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3765
        } else if (key.startsWith(CarbonCommonConstants.CARBON_INDEX_VISIBLE)) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3496
          isValid = true;
        } else if (key.startsWith(CarbonCommonConstants.CARBON_LOAD_INDEXES_PARALLEL)) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2310
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2362
          isValid = CarbonUtil.validateBoolean(value);
          if (!isValid) {
            throw new InvalidConfigurationException("Invalid value " + value + " for key " + key);
          }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3201
        } else if (key.startsWith(CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE)) {
          isValid = CarbonUtil.isValidSortOption(value);
          if (!isValid) {
            throw new InvalidConfigurationException("The sort scope " + key
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3607
                + " can have only either NO_SORT, LOCAL_SORT or GLOBAL_SORT.");
          }
        } else {
          throw new InvalidConfigurationException(
              "The key " + key + " not supported for dynamic configuration.");
        }
    }
    return isValid;
  }

  public void removeProperty(String property) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1520
    sProps.remove(property);
  }

  public void removeExtraInfo(String key) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2076
    extraInfo.remove(key);
  }

  /**
   * clear the set properties
   */
  public void clear() {
    sProps.clear();
  }

  public SessionParams clone() throws CloneNotSupportedException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2294
    super.clone();
    SessionParams newObj = new SessionParams();
    newObj.addedProps.putAll(this.addedProps);
    newObj.sProps.putAll(this.sProps);
    newObj.extraInfo.putAll(this.extraInfo);
    return newObj;
  }
}

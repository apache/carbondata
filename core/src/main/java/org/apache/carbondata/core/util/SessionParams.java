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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.*;

import org.apache.commons.lang.StringUtils;


/**
 * This class maintains carbon session params
 */
public class SessionParams implements Serializable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CacheProvider.class.getName());

  private Map<String, String> sProps;

  public SessionParams() {
    sProps = new HashMap<>();
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

  /**
   * This method will be used to add a new property
   *
   * @param key
   * @return properties value
   */
  public SessionParams addProperty(String key, String value) throws InvalidConfigurationException {
    boolean isValidConf = validateKeyValue(key, value);
    if (isValidConf) {
      LOGGER.audit("The key " + key + " with value " + value + " added in the session param");
      sProps.put(key, value);
    }
    return this;
  }

  private boolean validateKeyValue(String key, String value) throws InvalidConfigurationException {
    boolean isValid;
    switch (key) {
      // case ENABLE_DATA_LOADING_STATISTICS:
      // case CARBON_MERGE_SORT_PREFETCH:
      case ENABLE_UNSAFE_SORT:
      case ENABLE_OFFHEAP_SORT:
        // case ENABLE_INMEMORY_MERGE_SORT:
      case ENABLE_UNSAFE_IN_QUERY_EXECUTION:
        // case USE_OFFHEAP_IN_QUERY_PROCSSING:
        //  case USE_PREFETCH_WHILE_LOADING:
      case CARBON_CUSTOM_BLOCK_DISTRIBUTION:
        isValid = validateBoolean(value);
        if (!isValid) {
          handleFailure(key, value);
        }
        break;
      default:
        isValid = false;
        throw new InvalidConfigurationException(
            "The key " + key + " not supported for dynamic configuration.");
    }
    return isValid;
  }

  /**
   * the method does the range validation if the min or max value is passed.
   *
   * @param value
   * @param minValue
   * @param maxValue
   * @return
   */
  private boolean validateIntRange(String value, String minValue, String maxValue) {
    boolean isValid = StringUtils.isNotBlank(value);
    if (isValid) {
      int confValue = -1;
      try {
        confValue = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        isValid = false;
      }
      if (isValid && StringUtils.isNotBlank(minValue) && confValue < Integer.parseInt(minValue)) {
        isValid = false;
      }
      if (isValid && StringUtils.isNotBlank(minValue) && confValue > Integer.parseInt(maxValue)) {
        isValid = false;
      }
    }
    return isValid;
  }

  private boolean validateBoolean(String value) {
    if (null == value) {
      return false;
    } else if (!("false".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value))) {
      return false;
    }
    return true;
  }

  private void handleFailure(String key, String value) throws InvalidConfigurationException {
    throw new InvalidConfigurationException("Invalid value " + value + " for key " + key);
  }

  private boolean validateCore(String value) {
    boolean isValid = false;
    int numCores = Integer.parseInt(value);
    if (!(numCores < NUM_CORES_MIN_VAL || numCores > NUM_CORES_MAX_VAL)) {
      isValid = true;
    }
    return isValid;
  }

  public void clear() {
    sProps.clear();
  }

}

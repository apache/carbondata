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

import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.exception.InvalidConfigurationException;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ENABLE_UNSAFE_SORT;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_ACTION;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_BATCH_SORT_SIZE_INMB;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_DATEFORMAT;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_SINGLE_PASS;
import static org.apache.carbondata.core.constants.CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE;

/**
 * This class maintains carbon session params
 */
public class SessionParams implements Serializable {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CacheProvider.class.getName());

  private Map<String, String> sProps;
  private Map<String, String> addedProps;

  public SessionParams() {
    sProps = new HashMap<>();
    addedProps = new HashMap<>();
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

  public SessionParams addProps(Map<String, String> addedProps) {
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
      case ENABLE_UNSAFE_SORT:
      case CARBON_CUSTOM_BLOCK_DISTRIBUTION:
      case CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE:
      case CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD:
      case CARBON_OPTIONS_SINGLE_PASS:
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
              "The key " + key + " can have only either FORCE or IGNORE or REDIRECT.");
        }
        break;
      case CARBON_OPTIONS_SORT_SCOPE:
        isValid = CarbonUtil.isValidSortOption(value);
        if (!isValid) {
          throw new InvalidConfigurationException("The sort scope " + key
              + " can have only either BATCH_SORT or LOCAL_SORT or NO_SORT.");
        }
        break;
      case CARBON_OPTIONS_BATCH_SORT_SIZE_INMB:
      case CARBON_OPTIONS_GLOBAL_SORT_PARTITIONS:
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
      default:
        throw new InvalidConfigurationException(
            "The key " + key + " not supported for dynamic configuration.");
    }
    return isValid;
  }

  /**
   * clear the set properties
   */
  public void clear() {
    sProps.clear();
  }

}

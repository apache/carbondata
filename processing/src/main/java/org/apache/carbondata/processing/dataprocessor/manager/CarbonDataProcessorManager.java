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

package org.apache.carbondata.processing.dataprocessor.manager;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public final class CarbonDataProcessorManager {
  /**
   * instance
   */
  private static final CarbonDataProcessorManager INSTANCE = new CarbonDataProcessorManager();

  /**
   * managerHandlerMap
   */
  private Map<String, Object> managerHandlerMap;

  /**
   * private constructor
   */
  private CarbonDataProcessorManager() {
    managerHandlerMap = new HashMap<String, Object>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * Get instance method will be used to get the class instance
   *
   * @return
   */
  public static CarbonDataProcessorManager getInstance() {
    return INSTANCE;
  }

  /**
   * Below method will be used to get the lock object for all the data processing request.
   * form the local map, if empty than it will update the map and return the lock object
   *
   * @param key
   * @return
   */
  public synchronized Object getDataProcessingLockObject(String key) {
    Object object = managerHandlerMap.get(key);
    if (null == object) {
      object = new Object();
      managerHandlerMap.put(key, object);
    }
    return object;
  }
}

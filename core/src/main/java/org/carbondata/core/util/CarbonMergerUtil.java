/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.util;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * Util class for merge activities of 2 loads.
 */
public class CarbonMergerUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonMergerUtil.class.getName());

  public static int[] getCardinalityFromLevelMetadata(String path, String tableName) {
    int[] localCardinality = null;
    try {
      localCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(
          path + '/' + CarbonCommonConstants.LEVEL_METADATA_FILE + tableName + ".metadata");
    } catch (CarbonUtilException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }

    return localCardinality;
  }

}

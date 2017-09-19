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

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.apache.hadoop.conf.Configuration;

/**
 * Util class for merge activities of 2 loads.
 */
public class CarbonMergerUtil {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonMergerUtil.class.getName());

  public static int[] getCardinalityFromLevelMetadata(Configuration configuration,  String path,
      String tableName) {
    int[] localCardinality = null;
    try {
      localCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(configuration,
          path + '/' + CarbonCommonConstants.LEVEL_METADATA_FILE + tableName + ".metadata");
    } catch (IOException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }

    return localCardinality;
  }

  /**
   * read from the first non-empty level metadata
   * @param paths paths
   * @param tableName table name
   * @return cardinality
   */
  public static int[] getCardinalityFromLevelMetadata(Configuration configuration, String[] paths,
      String tableName) {
    int[] localCardinality = null;
    for (String path : paths) {
      localCardinality = getCardinalityFromLevelMetadata(configuration, path, tableName);
      if (null != localCardinality) {
        break;
      }
    }
    return localCardinality;
  }
}

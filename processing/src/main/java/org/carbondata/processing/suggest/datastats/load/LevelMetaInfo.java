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

package org.carbondata.processing.suggest.datastats.load;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * This class will have information about level metadata
 *
 * @author A00902717
 */
public class LevelMetaInfo {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LevelMetaInfo.class.getName());

  private int[] dimCardinality;

  public LevelMetaInfo(CarbonFile file, String tableName) {
    initialise(file, tableName);
  }

  private void initialise(CarbonFile file, final String tableName) {

    if (file.isDirectory()) {
      CarbonFile[] files = file.listFiles(new CarbonFileFilter() {
        public boolean accept(CarbonFile pathname) {
          return (!pathname.isDirectory()) && pathname.getName()
              .startsWith(CarbonCommonConstants.LEVEL_METADATA_FILE) && pathname.getName()
              .endsWith(tableName + ".metadata");
        }

      });
      try {
        dimCardinality = CarbonUtil.getCardinalityFromLevelMetadataFile(files[0].getAbsolutePath());
      } catch (CarbonUtilException e) {
        LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
      }
    }

  }

  public int[] getDimCardinality() {
    return dimCardinality;
  }

}

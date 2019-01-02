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

package org.apache.carbondata.processing.loading;

import org.apache.carbondata.common.constants.LoggerAction;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;

/**
 * This class provides the BadRecordsLogger instance
 */
public class BadRecordsLoggerProvider {
  /**
   * method returns the BadRecordsLogger instance
   * @param configuration
   * @return
   */
  public static BadRecordsLogger createBadRecordLogger(CarbonDataLoadConfiguration configuration) {
    boolean badRecordsLogRedirect = false;
    boolean badRecordConvertNullDisable = false;
    boolean isDataLoadFail = false;
    boolean badRecordsLoggerEnable = Boolean.parseBoolean(
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ENABLE)
            .toString());
    Object bad_records_action =
        configuration.getDataLoadProperty(DataLoadProcessorConstants.BAD_RECORDS_LOGGER_ACTION)
            .toString();
    if (null != bad_records_action) {
      LoggerAction loggerAction = null;
      try {
        loggerAction = LoggerAction.valueOf(bad_records_action.toString().toUpperCase());
      } catch (IllegalArgumentException e) {
        loggerAction = LoggerAction.FORCE;
      }
      switch (loggerAction) {
        case FORCE:
          badRecordConvertNullDisable = false;
          break;
        case REDIRECT:
          badRecordsLogRedirect = true;
          badRecordConvertNullDisable = true;
          break;
        case IGNORE:
          badRecordsLogRedirect = false;
          badRecordConvertNullDisable = true;
          break;
        case FAIL:
          isDataLoadFail = true;
          break;
      }
    }
    CarbonTableIdentifier identifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();

    return new BadRecordsLogger(identifier.getBadRecordLoggerKey(),
        identifier.getTableName() + '_' + System.currentTimeMillis(),
        getBadLogStoreLocation(configuration), badRecordsLogRedirect,
        badRecordsLoggerEnable, badRecordConvertNullDisable, isDataLoadFail);
  }

  public static String getBadLogStoreLocation(CarbonDataLoadConfiguration configuration) {
    String badLogStoreLocation = (String) configuration
        .getDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH);
    return CarbonTablePath.generateBadRecordsPath(badLogStoreLocation, configuration.getSegmentId(),
        configuration.getTaskNo(), configuration.isCarbonTransactionalTable());
  }
}


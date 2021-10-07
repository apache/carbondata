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

package org.apache.carbondata.processing.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Common methods used for the bad record handling
 */
public class CarbonBadRecordUtil {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonDataProcessorUtil.class.getName());

  /**
   * The method used to rename badrecord files from inprogress to normal
   *
   * @param configuration
   */
  public static void renameBadRecord(CarbonDataLoadConfiguration configuration) {
    // rename the bad record in progress to normal
    String storeLocation = "";
    if (configuration.isCarbonTransactionalTable()) {
      storeLocation =
          configuration.getSegmentId() + CarbonCommonConstants.FILE_SEPARATOR + configuration
              .getTaskNo();
    } else {
      storeLocation =
          "SdkWriterBadRecords" + CarbonCommonConstants.FILE_SEPARATOR + configuration.getTaskNo();
    }
    renameBadRecordsFromInProgressToNormal(configuration, storeLocation);
  }

  /**
   * @param configuration
   * @param storeLocation
   */
  private static void renameBadRecordsFromInProgressToNormal(
      CarbonDataLoadConfiguration configuration, String storeLocation) {
    // get the base store location
    String badLogStoreLocation = (String) configuration
        .getDataLoadProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH);
    if (null == badLogStoreLocation) {
      badLogStoreLocation =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    }
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    try {
      if (!FileFactory.isFileExist(badLogStoreLocation)) {
        return;
      }
    } catch (IOException e1) {
      LOGGER.info("bad record folder does not exist");
    }
    CarbonFile carbonFile = FileFactory.getCarbonFile(badLogStoreLocation);

    CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile pathname) {
        if (pathname.getName().indexOf(CarbonCommonConstants.FILE_INPROGRESS_STATUS) > -1) {
          return true;
        }
        return false;
      }
    });

    String badRecordsInProgressFileName = null;
    String changedFileName = null;
    for (CarbonFile badFiles : listFiles) {
      badRecordsInProgressFileName = badFiles.getName();

      changedFileName = badLogStoreLocation + File.separator + badRecordsInProgressFileName
          .substring(0, badRecordsInProgressFileName.lastIndexOf('.'));

      badFiles.renameTo(changedFileName);

      if (badFiles.exists()) {
        if (!badFiles.delete()) {
          LOGGER.error("Unable to delete File : " + badFiles.getName());
        }
      }
    }
  }

  /**
   * The method removes the entry if exist and returns <code>true</code> if bad records exist
   * else <code>false</code>
   *
   * @param loadModel
   * @return
   */
  public static boolean hasBadRecord(CarbonLoadModel loadModel) {
    String key = loadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier()
        .getBadRecordLoggerKey();
    return (null != BadRecordsLogger.hasBadRecord(key));
  }

  public static String getBadRecordsPath(Map<String, String> loadOptions, CarbonTable table) {
    String badRecordsFromLoad = loadOptions.get("bad_record_path");
    String badRecordsFromCreate =
        table.getTableInfo().getFactTable().getTableProperties().get("bad_record_path");
    String badRecordsPath;
    if (StringUtils.isNotEmpty(badRecordsFromLoad)) {
      badRecordsPath =
          badRecordsFromLoad + CarbonCommonConstants.FILE_SEPARATOR + table.getDatabaseName()
              + CarbonCommonConstants.FILE_SEPARATOR + table.getTableName();
    } else if (StringUtils.isNotEmpty(badRecordsFromCreate)) {
      badRecordsPath = badRecordsFromCreate;
    } else {
      String badRecordsFromProp = CarbonProperties.getInstance()
          .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORD_PATH,
              CarbonProperties.getInstance()
                  .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
                      CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL));
      if (!badRecordsFromProp.isEmpty()) {
        badRecordsFromProp =
            badRecordsFromProp + CarbonCommonConstants.FILE_SEPARATOR + table.getDatabaseName()
                + CarbonCommonConstants.FILE_SEPARATOR + table.getTableName();
      }
      badRecordsPath = badRecordsFromProp;
    }
    return badRecordsPath;
  }

  public static void updateEmptyValue(DataOutputStream dataOutputStream, boolean isEmptyBadRecord,
      BadRecordLogHolder logHolder, String parentName, DataType dataType) throws IOException {
    CarbonUtil.updateWithEmptyValueBasedOnDatatype(dataOutputStream, dataType);
    if (isEmptyBadRecord) {
      CarbonBadRecordUtil.setErrorMessage(logHolder, parentName, dataType.getName());
    }
  }

  public static void setErrorMessage(BadRecordLogHolder logHolder, String columnName,
      String datatypeName) {
    String message = logHolder.getColumnMessageMap().get(columnName);
    if (null == message) {
      message = "The value with column name " + columnName + " and column data type " + datatypeName
          + " is not a valid " + datatypeName + " type.";
      logHolder.getColumnMessageMap().put(columnName, message);
    }
    logHolder.setReason(message);
  }
}

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

package org.carbondata.processing.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.filesystem.HDFSCarbonFile;
import org.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.StepMeta;

public final class CarbonDataProcessorUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataProcessorUtil.class.getName());

  private CarbonDataProcessorUtil() {

  }

  /**
   * Below method will be used to get the buffer size
   *
   * @param numberOfFiles
   * @return buffer size
   */
  public static int getFileBufferSize(int numberOfFiles, CarbonProperties instance,
      int deafultvalue) {
    int configuredBufferSize = 0;
    try {
      configuredBufferSize =
          Integer.parseInt(instance.getProperty(CarbonCommonConstants.SORT_FILE_BUFFER_SIZE));
    } catch (NumberFormatException e) {
      configuredBufferSize = deafultvalue;
    }
    int fileBufferSize = (configuredBufferSize *
        CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) / numberOfFiles;
    if (fileBufferSize < CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) {
      fileBufferSize = CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    }
    return fileBufferSize;
  }

  /**
   * Utility method to get level cardinality string
   *
   * @param dimCardinalities
   * @param aggDims
   * @return level cardinality string
   */
  public static String getLevelCardinalitiesString(Map<String, String> dimCardinalities,
      String[] aggDims) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < aggDims.length; i++) {
      String string = dimCardinalities.get(aggDims[i]);
      if (string != null) {
        sb.append(string);
        sb.append(CarbonCommonConstants.COMA_SPC_CHARACTER);
      }
    }
    String resultStr = sb.toString();
    if (resultStr.endsWith(CarbonCommonConstants.COMA_SPC_CHARACTER)) {
      resultStr = resultStr
          .substring(0, resultStr.length() - CarbonCommonConstants.COMA_SPC_CHARACTER.length());
    }
    return resultStr;
  }

  /**
   * @param storeLocation
   */
  public static void renameBadRecordsFromInProgressToNormal(String storeLocation) {
    // get the base store location
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    FileType fileType = FileFactory.getFileType(badLogStoreLocation);
    try {
      if (!FileFactory.isFileExist(badLogStoreLocation, fileType)) {
        return;
      }
    } catch (IOException e1) {
      LOGGER.info("bad record folder does not exist");
    }
    CarbonFile carbonFile = null;
    if (fileType.equals(FileFactory.FileType.HDFS)) {
      carbonFile = new HDFSCarbonFile(badLogStoreLocation);
    } else {
      carbonFile = new LocalCarbonFile(badLogStoreLocation);
    }

    CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile pathname) {
        if (pathname.getName().indexOf(CarbonCommonConstants.FILE_INPROGRESS_STATUS) > -1) {
          return true;
        }
        return false;
      }
    });

    String badRecordsInProgressFileName = null;
    String changedFileName = null;
    // CHECKSTYLE:OFF
    for (CarbonFile badFiles : listFiles) {
      // CHECKSTYLE:ON
      badRecordsInProgressFileName = badFiles.getName();

      changedFileName = badLogStoreLocation + File.separator + badRecordsInProgressFileName
          .substring(0, badRecordsInProgressFileName.lastIndexOf('.'));

      badFiles.renameTo(changedFileName);

      if (badFiles.exists()) {
        if (!badFiles.delete()) {
          LOGGER.error("Unable to delete File : " + badFiles.getName());
        }
      }
    }// CHECKSTYLE:ON
  }

  public static void checkResult(List<CheckResultInterface> remarks, StepMeta stepMeta,
      String[] input) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.",
          stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!",
          stepMeta);
      remarks.add(cr);
    }
  }

  public static void check(Class<?> pkg, List<CheckResultInterface> remarks, StepMeta stepMeta,
      RowMetaInterface prev, String[] input) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
          BaseMessages.getString(pkg, "CarbonStep.Check.StepIsReceivingInfoFromOtherSteps"),
          stepMeta);
      remarks.add(cr);
    } else {
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(pkg, "CarbonStep.Check.NoInputReceivedFromOtherSteps"), stepMeta);
      remarks.add(cr);
    }

    // also check that each expected key fields are acually coming
    if (prev != null && prev.size() > 0) {
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
          BaseMessages.getString(pkg, "CarbonStep.Check.AllFieldsFoundInInput"), stepMeta);
      remarks.add(cr);
    } else {
      String errorMessage =
          BaseMessages.getString(pkg, "CarbonStep.Check.CouldNotReadFromPreviousSteps") + Const.CR;
      cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, errorMessage, stepMeta);
      remarks.add(cr);
    }
  }

  /**
   * This method will be used to delete sort temp location is it is exites
   *
   * @throws CarbonSortKeyAndGroupByException
   */
  public static void deleteSortLocationIfExists(String tempFileLocation)
      throws CarbonSortKeyAndGroupByException {
    // create new temp file location where this class
    //will write all the temp files
    File file = new File(tempFileLocation);

    if (file.exists()) {
      try {
        CarbonUtil.deleteFoldersAndFiles(file);
      } catch (CarbonUtilException e) {
        LOGGER.error(e);
      }
    }
  }

  /**
   * return the modification TimeStamp Separated by HASH_SPC_CHARACTER
   */
  public static String getLoadNameFromLoadMetaDataDetails(
      List<LoadMetadataDetails> loadMetadataDetails) {
    StringBuilder builder = new StringBuilder();
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      builder.append(CarbonCommonConstants.LOAD_FOLDER).append(loadMetadataDetail.getLoadName())
          .append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    String loadNames =
        builder.substring(0, builder.lastIndexOf(CarbonCommonConstants.HASH_SPC_CHARACTER))
            .toString();
    return loadNames;
  }

  /**
   * return the modOrDelTimesStamp TimeStamp Separated by HASH_SPC_CHARACTER
   */
  public static String getModificationOrDeletionTimesFromLoadMetadataDetails(
      List<LoadMetadataDetails> loadMetadataDetails) {
    StringBuilder builder = new StringBuilder();
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      builder.append(loadMetadataDetail.getModificationOrdeletionTimesStamp())
          .append(CarbonCommonConstants.HASH_SPC_CHARACTER);
    }
    String modOrDelTimesStamp =
        builder.substring(0, builder.indexOf(CarbonCommonConstants.HASH_SPC_CHARACTER)).toString();
    return modOrDelTimesStamp;
  }

  /**
   * This method will form the local data folder store location
   *
   * @param databaseName
   * @param tableName
   * @param taskId
   * @param partitionId
   * @param segmentId
   * @return
   */
  public static String getLocalDataFolderLocation(String databaseName, String tableName,
      String taskId, String partitionId, String segmentId) {
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + taskId;
    String baseStorePath = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(baseStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId + "");
    String localDataLoadFolderLocation = carbonDataDirectoryPath + File.separator + taskId;
    return localDataLoadFolderLocation;
  }

  /**
   * The method returns the bad record store location
   *
   * @param storeLocation
   * @return
   */
  public static String getBadLogStoreLocation(String storeLocation) {
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    return badLogStoreLocation;
  }

  /**
   * method returns the bad log file name
   *
   * @param csvFilepath
   * @return
   */
  public static String getBagLogFileName(String csvFilepath) {
    csvFilepath = new File(csvFilepath).getName();
    if (csvFilepath.indexOf(".") > -1) {
      csvFilepath = csvFilepath.substring(0, csvFilepath.indexOf("."));
    }

    return csvFilepath + '_' + System.currentTimeMillis() + ".log";

  }
}

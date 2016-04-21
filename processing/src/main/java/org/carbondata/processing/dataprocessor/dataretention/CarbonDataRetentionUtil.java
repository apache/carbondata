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

package org.carbondata.processing.dataprocessor.dataretention;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonFileFolderComparator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

import org.apache.commons.codec.binary.Base64;

public final class CarbonDataRetentionUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataRetentionUtil.class.getName());

  private CarbonDataRetentionUtil() {

  }

  /**
   * This API will scan the level file and return the surrogate key for the
   * valid member
   *
   * @param levelName
   * @throws ParseException
   */
  public static Map<Integer, Integer> getSurrogateKeyForRetentionMember(CarbonFile memberFile,
      String levelName, String columnValue, String format,
      Map<Integer, Integer> mapOfSurrKeyAndAvailStatus) {
    DataInputStream inputStream = null;
    Date storeDateMember = null;
    Date columnValDateMember = null;
    DataInputStream inputStreamForMaxVal = null;
    try {
      columnValDateMember = convertToDateObjectFromStringVal(columnValue, format, true);
    } catch (ParseException e) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
          "Not able to get surrogate key for value : " + columnValue);
      return mapOfSurrKeyAndAvailStatus;
    }
    try {
      inputStream = FileFactory
          .getDataInputStream(memberFile.getPath(), FileFactory.getFileType(memberFile.getPath()));

      long currPosIndex = 0;
      long size = memberFile.getSize() - 4;
      int minVal = inputStream.readInt();
      int surrogateKeyIndex = minVal;
      currPosIndex += 4;
      //
      int current = 0;
      boolean enableEncoding = Boolean.valueOf(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.ENABLE_BASE64_ENCODING,
              CarbonCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
      String memberName = null;
      while (currPosIndex < size) {
        int len = inputStream.readInt();
        currPosIndex += 4;
        byte[] rowBytes = new byte[len];
        inputStream.readFully(rowBytes);
        currPosIndex += len;
        if (enableEncoding) {
          memberName = new String(Base64.decodeBase64(rowBytes), Charset.defaultCharset());
        } else {
          memberName = new String(rowBytes, Charset.defaultCharset());
        }
        int surrogateVal = surrogateKeyIndex + current;
        current++;
        try {
          storeDateMember = convertToDateObjectFromStringVal(memberName, format, false);

        } catch (Exception e) {
          LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
              "Not able to get surrogate key for value : " + memberName);
          continue;
        }
        // means date1 is before date2
        if (null != columnValDateMember && null != storeDateMember) {
          if (storeDateMember.compareTo(columnValDateMember) < 0) {
            mapOfSurrKeyAndAvailStatus.put(surrogateVal, surrogateVal);
          }
        }

      }

    } catch (IOException e) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
          "Not able to read level file for Populating Cache : " + memberFile.getName());

    } finally {
      CarbonUtil.closeStreams(inputStream);
      CarbonUtil.closeStreams(inputStreamForMaxVal);

    }

    return mapOfSurrKeyAndAvailStatus;
  }

  @SuppressWarnings("deprecation")
  private static Date convertToDateObjectFromStringVal(String value, String dateFormatVal,
      boolean isUserInPut) throws ParseException {

    if (!value.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {

      Date dateToConvert = null;
      String dateFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
      // Format the date to Strings

      if (isUserInPut && (null != dateFormatVal || !"".equals(dateFormatVal))) {
        SimpleDateFormat defaultDateFormat = new SimpleDateFormat(dateFormatVal);
        dateToConvert = defaultDateFormat.parse(value);
        String dmy = simpleDateFormat.format(dateToConvert);
        return simpleDateFormat.parse(dmy);
      }

      return simpleDateFormat.parse(value);
    }
    return null;
  }

  /**
   * @param baseStorePath
   * @param fileNameSearchPattern
   * @return
   */
  public static CarbonFile[] getFilesArray(String baseStorePath,
      final String fileNameSearchPattern) {
    FileType fileType = FileFactory.getFileType(baseStorePath);
    CarbonFile storeFolder = FileFactory.getCarbonFile(baseStorePath, fileType);

    CarbonFile[] listFiles = storeFolder.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile pathname) {
        if (pathname.getName().indexOf(fileNameSearchPattern) > -1 && !pathname.getName()
            .endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
          return true;
        }
        return false;
      }
    });

    return listFiles;
  }

  public static CarbonFile[] getAllLoadFolderSlices(String schemaName, String cubeName,
      String tableName, String hdsfStoreLocation, int currentRestructNumber) {
    String hdfsLevelRSPath = CarbonUtil
        .getRSPath(schemaName, cubeName, tableName, hdsfStoreLocation, currentRestructNumber);

    CarbonFile file =
        FileFactory.getCarbonFile(hdfsLevelRSPath + '/', FileFactory.getFileType(hdfsLevelRSPath));

    CarbonFile[] listFiles = listFiles(file);
    if (null != listFiles) {
      Arrays.sort(listFiles, new CarbonFileFolderComparator());
    }
    return listFiles;
  }

  /**
   * @param file
   * @return
   */
  private static CarbonFile[] listFiles(CarbonFile file) {
    CarbonFile[] listFiles = file.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile pathname) {
        return pathname.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER) && !pathname
            .getName().endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS);
      }
    });
    return listFiles;
  }

  /**
   * @param loadFiles
   * @param loadMetadataDetails
   * @return
   */
  public static CarbonFile[] excludeUnwantedLoads(CarbonFile[] loadFiles,
      List<LoadMetadataDetails> loadMetadataDetails) {
    List<CarbonFile> validLoads = new ArrayList<CarbonFile>();

    List<String> validLoadsForRetention = getValidLoadsForRetention(loadMetadataDetails);

    for (CarbonFile loadFolder : loadFiles) {
      String loadName = loadFolder.getName().substring(
          loadFolder.getName().indexOf(CarbonCommonConstants.LOAD_FOLDER)
              + CarbonCommonConstants.LOAD_FOLDER.length(), loadFolder.getName().length());

      if (validLoadsForRetention.contains(loadName)) {
        validLoads.add(loadFolder);
      }

    }

    return validLoads.toArray(new CarbonFile[validLoads.size()]);
  }

  /**
   * @param loadMetadataDetails
   * @return
   */
  private static List<String> getValidLoadsForRetention(
      List<LoadMetadataDetails> loadMetadataDetails) {
    List<String> validLoadNameForRetention =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (LoadMetadataDetails loadDetail : loadMetadataDetails) {
      //load should not be deleted and load should not be merged.
      if (!loadDetail.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
        if (null == loadDetail.getMergedLoadName()) {
          validLoadNameForRetention.add(loadDetail.getLoadName());
        } else {
          validLoadNameForRetention.add(loadDetail.getMergedLoadName());
        }

      }
    }

    return validLoadNameForRetention;
  }
}

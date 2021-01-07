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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Maintains the trash folder in carbondata. This class has methods to copy data to the trash and
 * remove data from the trash.
 */
public final class TrashUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(TrashUtil.class.getName());

  /**
   * Base method to copy the data to the trash folder.
   *
   * @param sourcePath      the path from which to copy the file
   * @param destinationPath the path where the file will be copied
   */
  private static void copyToTrashFolder(String sourcePath, String destinationPath)
    throws IOException {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    try {
      dataOutputStream = FileFactory.getDataOutputStream(destinationPath);
      dataInputStream = FileFactory.getDataInputStream(sourcePath);
      IOUtils.copyBytes(dataInputStream, dataOutputStream, CarbonCommonConstants.BYTEBUFFER_SIZE);
    } catch (IOException exception) {
      LOGGER.error("Unable to copy " + sourcePath + " to the trash folder", exception);
      throw exception;
    } finally {
      CarbonUtil.closeStreams(dataInputStream, dataOutputStream);
    }
  }

  /**
   * The below method copies the complete a file to the trash folder.
   *
   * @param filePathToCopy           the files which are to be moved to the trash folder
   * @param trashFolderWithTimestamp timestamp, partition folder(if any) and segment number
   */
  public static void copyFileToTrashFolder(String filePathToCopy,
      String trashFolderWithTimestamp) throws IOException {
    CarbonFile carbonFileToCopy = FileFactory.getCarbonFile(filePathToCopy);
    String destinationPath = trashFolderWithTimestamp + CarbonCommonConstants
        .FILE_SEPARATOR + carbonFileToCopy.getName();
    try {
      if (!FileFactory.isFileExist(destinationPath)) {
        copyToTrashFolder(filePathToCopy, destinationPath);
      }
    } catch (IOException e) {
      // in case there is any issue while copying the file to the trash folder, we need to delete
      // the complete segment folder from the trash folder. The trashFolderWithTimestamp contains
      // the segment folder too. Delete the folder as it is.
      FileFactory.deleteFile(trashFolderWithTimestamp);
      LOGGER.error("Error while checking trash folder: " + destinationPath + " or copying" +
          " file: " + filePathToCopy + " to the trash folder at path", e);
      throw e;
    }
  }

  /**
   * The below method copies the complete segment folder to the trash folder. Here, the data files
   * in segment are listed and copied one by one to the trash folder.
   *
   * @param segmentPath              the folder which are to be moved to the trash folder
   * @param trashFolderWithTimestamp trashfolderpath with complete timestamp and segment number
   */
  public static void copySegmentToTrash(CarbonFile segmentPath,
      String trashFolderWithTimestamp) throws IOException {
    try {
      if (segmentPath.isFileExist()) {
        if (!FileFactory.isFileExist(trashFolderWithTimestamp)) {
          FileFactory.mkdirs(trashFolderWithTimestamp);
        }
        CarbonFile[] dataFiles = segmentPath.listFiles();
        for (CarbonFile carbonFile : dataFiles) {
          copyFileToTrashFolder(carbonFile.getAbsolutePath(), trashFolderWithTimestamp);
        }
        LOGGER.info("Segment: " + segmentPath.getAbsolutePath() + " has been copied to" +
            " the trash folder successfully. Total files copied: " + dataFiles.length);
      } else {
        LOGGER.info("Segment: " + segmentPath.getName() + " does not exist");
      }
    } catch (IOException e) {
      LOGGER.error("Error while copying the segment: " + segmentPath.getName() + " to the trash" +
          " Folder: " + trashFolderWithTimestamp, e);
      throw e;
    }
  }

  /**
   * The below method copies multiple files belonging to 1 segment to the trash folder.
   *
   * @param filesToCopy              absolute paths of the files to copy to the trash folder
   * @param trashFolderWithTimestamp trashfolderpath with complete timestamp and segment number
   * @param segmentNumber            segment number of the files which are being copied to trash
   */
  public static void copyFilesToTrash(List<String> filesToCopy,
      String trashFolderWithTimestamp, String segmentNumber) throws IOException {
    try {
      if (!FileFactory.isFileExist(trashFolderWithTimestamp)) {
        FileFactory.mkdirs(trashFolderWithTimestamp);
      }
      for (String fileToCopy : filesToCopy) {
        // check if file exists before copying
        if (FileFactory.isFileExist(fileToCopy)) {
          copyFileToTrashFolder(fileToCopy, trashFolderWithTimestamp);
        }
      }
      LOGGER.info("Segment: " + segmentNumber + " has been copied to" +
          " the trash folder successfully");
    } catch (IOException e) {
      LOGGER.error("Error while copying files of segment: " + segmentNumber + " to the trash" +
          " folder", e);
      throw e;
    }
  }

  /**
   * The below method deletes timestamp subdirectories in the trash folder which have expired as
   * per the user defined retention time. It return an array where the first element has the size
   * freed from the trash folder and the second element has the remaining size in the trash folder
   */
  public static long[] deleteExpiredDataFromTrash(String tablePath, Boolean isDryRun,
      Boolean showStats) {
    CarbonFile trashFolder = FileFactory.getCarbonFile(CarbonTablePath
        .getTrashFolderPath(tablePath));
    long sizeFreed = 0;
    long trashFolderSize = 0;
    // Deleting the timestamp based subdirectories in the trashfolder by the given timestamp.
    try {
      if (trashFolder.isFileExist()) {
        if (isDryRun || showStats) {
          trashFolderSize = FileFactory.getDirectorySize(trashFolder.getAbsolutePath());
        }
        CarbonFile[] timestampFolderList = trashFolder.listFiles();
        List<CarbonFile> filesToDelete = new ArrayList<>();
        for (CarbonFile timestampFolder : timestampFolderList) {
          // If the timeStamp at which the timeStamp subdirectory has expired as per the user
          // defined value, delete the complete timeStamp subdirectory
          if (timestampFolder.isDirectory() && isTrashRetentionTimeoutExceeded(Long
              .parseLong(timestampFolder.getName()))) {
            // only calculate size in case of dry run or in case clean files is with show stats
            if (isDryRun || showStats) {
              sizeFreed += FileFactory.getDirectorySize(timestampFolder.getAbsolutePath());
            }
            filesToDelete.add(timestampFolder);
          }
        }
        if (!isDryRun) {
          for (CarbonFile carbonFile : filesToDelete) {
            LOGGER.info("Timestamp subfolder from the Trash folder deleted: " + carbonFile
                .getAbsolutePath());
            FileFactory.deleteAllCarbonFilesOfDir(carbonFile);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error during deleting expired timestamp folder from the trash folder", e);
    }
    return new long[] {sizeFreed, trashFolderSize - sizeFreed};
  }

  /**
   * The below method deletes all the files and folders in the trash folder of a carbon table.
   * Returns an array in which the first element contains the size freed in case of clean files
   * operation or size that can be freed in case of dry run and the second element contains the
   * remaining size.
   */
  public static long[] emptyTrash(String tablePath, Boolean isDryRun, Boolean showStats) {
    CarbonFile trashFolder = FileFactory.getCarbonFile(CarbonTablePath
        .getTrashFolderPath(tablePath));
    // if the trash folder exists delete the contents of the trash folder
    long sizeFreed = 0;
    long[] sizeStatistics = new long[]{0, 0};
    try {
      if (trashFolder.isFileExist()) {
        CarbonFile[] carbonFileList = trashFolder.listFiles();
        List<CarbonFile> filesToDelete = new ArrayList<>();
        for (CarbonFile carbonFile : carbonFileList) {
          //Only calculate size when it is dry run operation or when show statistics is
          // true with actual operation
          if (isDryRun || showStats) {
            sizeFreed += FileFactory.getDirectorySize(carbonFile.getAbsolutePath());
          }
          filesToDelete.add(carbonFile);
        }
        sizeStatistics[0] = sizeFreed;
        if (!isDryRun) {
          for (CarbonFile carbonFile : filesToDelete) {
            FileFactory.deleteAllCarbonFilesOfDir(carbonFile);
          }
          LOGGER.info("Trash Folder has been emptied for table: " + tablePath);
          if (showStats) {
            sizeStatistics[1] = FileFactory.getDirectorySize(trashFolder.getAbsolutePath());
          }
        } else {
          sizeStatistics[1] = FileFactory.getDirectorySize(trashFolder.getAbsolutePath()) -
              sizeFreed;
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error while emptying the trash folder", e);
    }
    return sizeStatistics;
  }

  /**
   * check If the fileTimestamp is expired based on `carbon.trash.retention.days`
   */
  private static boolean isTrashRetentionTimeoutExceeded(long fileTimestamp) {
    int retentionDays = CarbonProperties.getInstance().getTrashFolderRetentionTime();
    long retentionMilliSeconds = TimeUnit.DAYS.toMillis(1) * retentionDays;
    return CarbonUpdateUtil.readCurrentTime() - fileTimestamp > retentionMilliSeconds;
  }

  /**
   * whether trash data outside of .Trash folder is time out
   */
  public static boolean isDataOutsideTrashIsExpired(long fileTimestamp) {
    return isTrashRetentionTimeoutExceeded(fileTimestamp) &&
        CarbonUpdateUtil.isMaxQueryTimeoutExceeded(fileTimestamp);
  }

  /**
   * This will give the complete path of the trash folder with the timestamp and the segment number
   *
   * @param tablePath          absolute table path
   * @param timeStampSubFolder the timestamp for the clean files operation
   * @param segmentNumber      the segment number for which files are moved to the trash folder
   */
  public static String getCompleteTrashFolderPath(String tablePath, long timeStampSubFolder,
      String segmentNumber) {
    return CarbonTablePath.getTrashFolderPath(tablePath) + CarbonCommonConstants.FILE_SEPARATOR +
      timeStampSubFolder + CarbonCommonConstants.FILE_SEPARATOR + CarbonTablePath
      .SEGMENT_PREFIX + segmentNumber;
  }
}

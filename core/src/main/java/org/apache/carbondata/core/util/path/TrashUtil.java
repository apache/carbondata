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

package org.apache.carbondata.core.util.path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.CarbonFileException;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Mantains the trash folder in carbondata. This class has methods to copy data to the trash and
 * remove data from the trash.
 */
public final class TrashUtil {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(CarbonUtil.class.getName());

  /**
   * The below method copies the complete a file to the trash folder. Provide necessary
   * timestamp and the segment number in the suffixToAdd  variable, so that the proper folder is
   * created in the trash folder.
   *
   * @param carbonTablePath table path of the carbon table
   * @param pathOfFileToCopy the files which are to be moved to the trash folder
   * @param suffixToAdd timestamp, partition folder(if any) and segment number
   * @return
   */
  public static void copyDataToTrashFolderByFile(String carbonTablePath, String pathOfFileToCopy,
      String suffixToAdd) {
    String trashFolderPath = CarbonTablePath.getTrashFolderPath(carbonTablePath) +
        CarbonCommonConstants.FILE_SEPARATOR + suffixToAdd;
    CarbonFile carbonFile  = FileFactory.getCarbonFile(pathOfFileToCopy);
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    try {
      if (carbonFile.exists()) {
        // FileUtils.copyFileToDirectory(new File(pathOfFileToCopy), new File(trashFolderPath));
        if (!new File(trashFolderPath).exists()) {
          FileFactory.createDirectoryAndSetPermission(trashFolderPath,
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
        }
        dataOutputStream = FileFactory
          .getDataOutputStream(trashFolderPath + CarbonCommonConstants.FILE_SEPARATOR +
            carbonFile.getName());
        dataInputStream = FileFactory
          .getDataInputStream(pathOfFileToCopy);
        IOUtils.copyBytes(dataInputStream, dataOutputStream, CarbonCommonConstants.BYTEBUFFER_SIZE);
        LOGGER.info("File: " + pathOfFileToCopy + " successfully copied to the trash folder: "
            + trashFolderPath);
      }
    } catch (IOException exception) {
      LOGGER.error("Unable to copy " + pathOfFileToCopy + " to the trash folder", exception);
    } finally {
      try {
        CarbonUtil.closeStream(dataInputStream);
        CarbonUtil.closeStream(dataOutputStream);
      } catch (IOException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
    }
  }

  /**
   * The below method copies the complete segment folder to the trash folder. Provide necessary
   * timestamp and the segment number in the suffixToAdd  variable, so that the proper folder is
   * created in the trash folder.
   *
   * @param carbonTablePath table path of the carbon table
   * @param path the folder which are to be moved to the trash folder
   * @param suffixToAdd timestamp, partition folder(if any) and segment number
   * @return
   */
  public static void copyDataToTrashBySegment(CarbonFile path, String carbonTablePath,
      String suffixToAdd) {
    String trashFolderPath = CarbonTablePath.getTrashFolderPath(carbonTablePath) +
        CarbonCommonConstants.FILE_SEPARATOR + suffixToAdd;
    try {
      FileUtils.copyDirectory(new File(path.getAbsolutePath()), new File(trashFolderPath));
      LOGGER.info("Segment: " + path.getAbsolutePath() + " has been copied to the trash folder" +
          " successfully");
    } catch (IOException e) {
      LOGGER.error("Unable to create the trash folder and copy data to it", e);
    }
  }

  /**
   * The below method deletes timestamp subdirectories in the trash folder which have expired as
   * per the user defined expiration time
   */
  public static void deleteAllDataFromTrashFolderByTimeStamp(String carbonTablePath, long timeStamp)
          throws IOException {
    String pathOfTrashFolder = CarbonTablePath.getTrashFolderPath(carbonTablePath);
    // Deleting the timestamp based subdirectories in the trashfolder by the given timestamp.
    if (FileFactory.isFileExist(pathOfTrashFolder)) {
      try {
        List<CarbonFile> carbonFileList = FileFactory.getFolderList(pathOfTrashFolder);
        long currentTime = new Timestamp(System.currentTimeMillis()).getTime();
        for (CarbonFile carbonFile : carbonFileList) {
          long givenTime = Long.parseLong(carbonFile.getName());
          // If the timeStamp at which the timeStamp subdirectory has expired as per the user
          // defined value, delete the complete timeStamp subdirectory
          if (givenTime + timeStamp < currentTime) {
            deleteDataFromTrashFolder(carbonFile);
          } else {
            LOGGER.info("Timestamp folder not expired yet: " + carbonFile.getAbsolutePath());
          }
        }
      } catch (IOException e) {
        LOGGER.error("Error during deleting from trash folder", e);
      }
    }
  }

  /**
   * The below method deletes all the files and folders in the trash folder of a carbon table.
   */
  public static void deleteAllDataFromTrashFolder(String carbonTablePath)
          throws IOException {
    String pathOfTrashFolder = CarbonTablePath.getTrashFolderPath(carbonTablePath);
    // if the trash folder exists delete the contents of the trash folder
    if (FileFactory.isFileExist(pathOfTrashFolder)) {
      try {
        List<CarbonFile> carbonFileList = FileFactory.getFolderList(pathOfTrashFolder);
        for (CarbonFile carbonFile : carbonFileList) {
          deleteDataFromTrashFolder(carbonFile);
        }
      } catch (IOException e) {
        LOGGER.error("Error during deleting from trash folder", e);
      }
    }
  }

  /**
   * The below method deletes carbonFiles from the trash folder, if it is a directory, it
   * will delete recursively
   */
  private static void deleteDataFromTrashFolder(CarbonFile carbonFile) {
    try {
      FileFactory.deleteAllCarbonFilesOfDir(carbonFile);
      LOGGER.info("delete file from trash+ " + carbonFile.getPath());
    } catch (CarbonFileException e) {
      LOGGER.error("Error during deleting from trash folder", e);
    }
  }

  /**
   * The below method will list all files in the trash folder
   */
  public static List<String> listSegmentsInTrashFolder(String carbonTablePath)
          throws IOException {
    String pathOfTrashFolder = CarbonTablePath.getTrashFolderPath(carbonTablePath);
    List<String> segmentsList = new ArrayList<String>();
    if (FileFactory.isFileExist(pathOfTrashFolder)) {
      try {
        List<CarbonFile> timeStampList = FileFactory.getFolderList(pathOfTrashFolder);
        for (CarbonFile carbonFile : timeStampList) {
          List<CarbonFile> segmentList = FileFactory.getFolderList(carbonFile);
          for (CarbonFile list : segmentList) {
            segmentsList.add(list.getAbsolutePath().substring(pathOfTrashFolder.length()));
          }
        }
      } catch (IOException e) {
        LOGGER.error("Error during deleting from trash folder", e);
      }
    }
    return segmentsList;
  }
}

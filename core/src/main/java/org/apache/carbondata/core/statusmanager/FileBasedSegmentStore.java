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
package org.apache.carbondata.core.statusmanager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

import static org.apache.carbondata.core.util.CarbonUtil.closeStreams;

public class FileBasedSegmentStore implements SegmentStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  @Override public List<SegmentDetailVO> getSegments(AbsoluteTableIdentifier identifier,
      List<Expression> filters) {
    try {
      LoadMetadataDetails[] details = readTableStatusFile(identifier);
      List<SegmentDetailVO> detailVOS = new ArrayList<>();
      for (LoadMetadataDetails detail : details) {
        detailVOS.add(SegmentManagerHelper.convertToSegmentDetailVO(detail));
      }
      return detailVOS;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public String generateSegmentIdAndInsert(AbsoluteTableIdentifier identifier,
      SegmentDetailVO segment) throws IOException {
    ICarbonLock carbonLock = getTableStatusLock(identifier);
    int retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
    int maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info(
            "Acquired lock for table" + identifier.uniqueName() + " for table status updation");
        LoadMetadataDetails[] details = readTableStatusFile(identifier);
        if (segment.getSegmentId() == null) {
          int newSegmentId = createNewSegmentId(details);
          segment.setSegmentId(String.valueOf(newSegmentId));
        }
        List<LoadMetadataDetails> listOfLoadFolderDetails = new ArrayList<>();
        Collections.addAll(listOfLoadFolderDetails, details);
        LoadMetadataDetails detail = SegmentManagerHelper.createLoadMetadataDetails(segment);
        listOfLoadFolderDetails.add(detail);
        writeLoadDetailsIntoFile(identifier, listOfLoadFolderDetails
            .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + identifier.uniqueName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + identifier.uniqueName()
            + " during table status updation");
      }
    }
    return null;
  }

  @Override public boolean updateSegments(AbsoluteTableIdentifier identifier,
      List<SegmentDetailVO> detailVOS) {
    try {
      LoadMetadataDetails[] details = readTableStatusFile(identifier);
      for (LoadMetadataDetails detail : details) {
        for (SegmentDetailVO detailVO : detailVOS) {
          if (detailVO.getSegmentId().equals(detail.getLoadName())) {
            SegmentManagerHelper.updateLoadMetadataDetails(detailVO, detail);
          }
        }
      }
      writeLoadDetailsIntoFile(identifier, details);
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void deleteSegments(AbsoluteTableIdentifier identifier) {

  }


  /**
   * This method will get the max segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private int getMaxSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = -1;
    for (int i = 0; i < loadMetadataDetails.length; i++) {
      try {
        int loadCount = Integer.parseInt(loadMetadataDetails[i].getLoadName());
        if (newSegmentId < loadCount) {
          newSegmentId = loadCount;
        }
      } catch (NumberFormatException ne) {
        // this case is for compacted folders. For compacted folders Id will be like 0.1, 2.1
        // consider a case when 12 loads are completed and after that major compaction is triggered.
        // In this case new compacted folder will be created with name 12.1 and after query time
        // out all the compacted folders will be deleted and entry will also be removed from the
        // table status file. In that case also if a new load comes the new segment Id assigned
        // should be 13 and not 0
        String loadName = loadMetadataDetails[i].getLoadName();
        if (loadName.contains(".")) {
          int loadCount = Integer.parseInt(loadName.split("\\.")[0]);
          if (newSegmentId < loadCount) {
            newSegmentId = loadCount;
          }
        }
      }
    }
    return newSegmentId;
  }

  /**
   * This method will create new segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private int createNewSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = getMaxSegmentId(loadMetadataDetails);
    newSegmentId++;
    return newSegmentId;
  }

  private ICarbonLock getTableStatusLock(AbsoluteTableIdentifier identifier) {
    return CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
  }

  private LoadMetadataDetails[] readTableStatusFile(AbsoluteTableIdentifier identifier)
      throws IOException {
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(tableStatusPath, FileFactory.getFileType(tableStatusPath));

    try {
      if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to read metadata of load");
      throw e;
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    // if listOfLoadFolderDetailsArray is null, return empty array
    if (null == listOfLoadFolderDetailsArray) {
      return new LoadMetadataDetails[0];
    }

    return listOfLoadFolderDetailsArray;
  }

  /**
   * writes load details into a given file at @param dataLoadLocation
   *
   * @param identifier
   * @param listOfLoadFolderDetailsArray
   * @throws IOException
   */
  public static void writeLoadDetailsIntoFile(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    AtomicFileOperations fileWrite =
        new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the metadata file.

    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetailsArray);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOGGER.error("Error message: " + ioe.getLocalizedMessage());
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

}

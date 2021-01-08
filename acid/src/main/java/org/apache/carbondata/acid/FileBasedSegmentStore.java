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

package org.apache.carbondata.acid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.acid.transaction.TransactionDetail;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import static org.apache.carbondata.core.util.CarbonUtil.closeStreams;

/**
 * Stores all the information related to segments as files.
 * Table status file, segment file, update status file
 * TODO: later add merge index file also by this interface to support all the formats.
 */
public class FileBasedSegmentStore implements SegmentStore {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  private TransactionDetail baseTransaction;

  private TransactionDetail currentTransaction;

  public FileBasedSegmentStore(TransactionDetail baseTransaction,
      TransactionDetail currentTransaction) {
    this.baseTransaction = baseTransaction;
    this.currentTransaction = currentTransaction;
  }

  @Override public void writeSegment(AbsoluteTableIdentifier identifier, SegmentDetailVO segment) {
    LoadMetadataDetails[] details;
    try {
      details = readTableStatusFile(identifier, baseTransaction);
      if (segment.getSegmentId() == null) {
        int newSegmentId = createNewSegmentId(details);
        segment.setSegmentId(String.valueOf(newSegmentId));
      }
      List<LoadMetadataDetails> listOfLoadFolderDetails = new ArrayList<>();
      Collections.addAll(listOfLoadFolderDetails, details);
      LoadMetadataDetails detail = SegmentManagerHelper.createLoadMetadataDetails(segment);
      listOfLoadFolderDetails.add(detail);
      writeLoadDetailsIntoFile(identifier,
          listOfLoadFolderDetails.toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public List<SegmentDetailVO> readSegments(AbsoluteTableIdentifier identifier) {
    try {
      LoadMetadataDetails[] details = readTableStatusFile(identifier, currentTransaction);
      List<SegmentDetailVO> detailVOS = new ArrayList<>();
      for (LoadMetadataDetails detail : details) {
        detailVOS.add(SegmentManagerHelper.convertToSegmentDetailVO(detail));
      }
      return detailVOS;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateSegments(AbsoluteTableIdentifier identifier, List<SegmentDetailVO> detailVOs) {
    try {
      LoadMetadataDetails[] details = readTableStatusFile(identifier, currentTransaction);
      for (LoadMetadataDetails detail : details) {
        for (SegmentDetailVO detailVO : detailVOs) {
          if (detailVO.getSegmentId().equals(detail.getLoadName())) {
            SegmentManagerHelper.updateLoadMetadataDetails(detailVO, detail);
          }
        }
      }
      writeLoadDetailsIntoFile(identifier, details);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deleteSegments(AbsoluteTableIdentifier identifier, List<String> segmentIDs) {

  }

  /**
   * This method will get the max segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private int getMaxSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = -1;
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      try {
        int loadCount = Integer.parseInt(loadMetadataDetail.getLoadName());
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
        String loadName = loadMetadataDetail.getLoadName();
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
    // TODO: doesn't work in concurrent scenario due to lack of table status lock
    int newSegmentId = getMaxSegmentId(loadMetadataDetails);
    newSegmentId++;
    return newSegmentId;
  }

  private LoadMetadataDetails[] readTableStatusFile(AbsoluteTableIdentifier identifier,
      TransactionDetail transactionDetail) throws IOException {
    if (transactionDetail == null) {
      // for the first time transaction the base transaction will be null, return empty.
      // for upgrade scenario, user need to manually call register transaction
      return new LoadMetadataDetails[0];
    }
    String tableStatusPath = CarbonTablePath
        .getTableStatusFilePath(identifier.getTablePath(), transactionDetail.getTransactionId());
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableStatusPath);
    try {
      if (!FileFactory.isFileExist(tableStatusPath)) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      LOGGER.error("Failed to read metadata of load");
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

  // TODO: check all the public method headers and correct it.

  /**
   * writes load details into a given file at @param dataLoadLocation
   *
   * @param identifier
   * @param listOfLoadFolderDetailsArray
   * @throws IOException
   */
  public void writeLoadDetailsIntoFile(AbsoluteTableIdentifier identifier,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    String dataLoadLocation = CarbonTablePath
        .getTableStatusFilePath(identifier.getTablePath(), currentTransaction.getTransactionId());
    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(dataLoadLocation);
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

  public void updateCurrentTableStatusWithNewBase(AbsoluteTableIdentifier identifier,
      TransactionDetail currentTransaction, TransactionDetail latestCommittedTransaction)
      throws IOException {
    // read the table status of latestCommittedTransaction
    LoadMetadataDetails[] latestCommittedDetails =
        readTableStatusFile(identifier, latestCommittedTransaction);
    // read the table status of currentTransaction
    LoadMetadataDetails[] currentTransactionDetails =
        readTableStatusFile(identifier, currentTransaction);
    // filter the load details of currentTransaction.getInvolvedSegments()
    Set<String> involvedSegments = currentTransaction.getInvolvedSegments();
    // Add current load details on top of load details of latestCommittedTransaction
    LoadMetadataDetails[] result =
        new LoadMetadataDetails[involvedSegments.size() + latestCommittedDetails.length];
    int index = 0;
    for (LoadMetadataDetails detail : currentTransactionDetails) {
      if (involvedSegments.contains(detail.getLoadName())) {
        result[index++] = detail;
      }
      if (index == involvedSegments.size()) break;
    }
    if (index != involvedSegments.size()) {
      throw new RuntimeException(
          "problem while merging the table status during conflict resolution");
    }
    System.arraycopy(latestCommittedDetails, 0, result, index, latestCommittedDetails.length);
    // overwrite the table status of currentTransaction with new load details
    writeLoadDetailsIntoFile(identifier, result);
  }
}
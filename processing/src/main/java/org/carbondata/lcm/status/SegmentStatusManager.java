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
package org.carbondata.lcm.status;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonCoreLogEvent;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

  private static final Log LOG = LogFactory.getLog(SegmentStatusManager.class);

  AbsoluteTableIdentifier absoluteTableIdentifier;

  public SegmentStatusManager(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  public class ValidSegmentsInfo {
    public List<String> listOfValidSegments;
    public List<String> listOfValidUpdatedSegments;

    public ValidSegmentsInfo(List<String> listOfValidSegments,
        List<String> listOfValidUpdatedSegments) {
      this.listOfValidSegments = listOfValidSegments;
      this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
    }
  }

  /**
   * get valid segment for given table
   * @return
   * @throws IOException
   */
  public ValidSegmentsInfo getValidSegments() throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<String> listOfValidSegments = new ArrayList<String>(10);
    List<String> listOfValidUpdatedSegments = new ArrayList<String>(10);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    String dataPath = carbonTablePath.getTableStatusFilePath();

    DataInputStream dataInputStream = null;
    Gson gsonObjectToRead = new Gson();
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
    LoadMetadataDetails[] loadFolderDetailsArray;
    try {
      if (FileFactory.isFileExist(dataPath, FileFactory.getFileType(dataPath))) {

        dataInputStream = fileOperation.openForRead();

        BufferedReader buffReader =
            new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

        loadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
        //just directly iterate Array
        List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

        for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
          if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.MARKED_FOR_UPDATE
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
            // check for merged loads.
            if (null != loadMetadataDetails.getMergedLoadName()) {

              if (!listOfValidSegments.contains(loadMetadataDetails.getMergedLoadName())) {
                listOfValidSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              // if merged load is updated then put it in updated list
              if (CarbonCommonConstants.MARKED_FOR_UPDATE
                  .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                listOfValidUpdatedSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              continue;
            }

            if (CarbonCommonConstants.MARKED_FOR_UPDATE
                .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {

              listOfValidUpdatedSegments.add(loadMetadataDetails.getLoadName());
            }
            listOfValidSegments.add(loadMetadataDetails.getLoadName());

          }
        }
      } else {
        loadFolderDetailsArray = new LoadMetadataDetails[0];
      }
    } catch (IOException e) {
      LOG.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
      throw e;
    } finally {
      try {

        if (null != dataInputStream) {
          dataInputStream.close();
        }
      } catch (Exception e) {
        LOG.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
        throw e;
      }

    }
    return new ValidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments);
  }

}

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

package org.apache.carbondata.processing.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.load.LoadMetadataDetails;

/**
 * This class contains all table status file utilities
 */
public final class CarbonTableStatusUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonTableStatusUtil.class.getName());

  private CarbonTableStatusUtil() {

  }

  /**
   * updates table status details using latest metadata
   *
   * @param oldMetadata
   * @param newMetadata
   * @return
   */

  public static List<LoadMetadataDetails> updateLatestTableStatusDetails(
      LoadMetadataDetails[] oldMetadata, LoadMetadataDetails[] newMetadata) {

    List<LoadMetadataDetails> newListMetadata =
        new ArrayList<LoadMetadataDetails>(Arrays.asList(newMetadata));
    for (LoadMetadataDetails oldSegment : oldMetadata) {
      if (CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oldSegment.getLoadStatus())) {
        updateSegmentMetadataDetails(newListMetadata.get(newListMetadata.indexOf(oldSegment)));
      }
    }
    return newListMetadata;
  }

  /**
   * returns current time
   *
   * @return
   */
  private static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  /**
   * updates segment status and modificaton time details
   *
   * @param loadMetadata
   */
  public static void updateSegmentMetadataDetails(LoadMetadataDetails loadMetadata) {
    // update status only if the segment is not marked for delete
    if (!CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(loadMetadata.getLoadStatus())) {
      loadMetadata.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
      loadMetadata.setModificationOrdeletionTimesStamp(readCurrentTime());
    }
  }

}
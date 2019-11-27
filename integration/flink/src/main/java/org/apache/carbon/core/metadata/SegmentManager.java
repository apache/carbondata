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
package org.apache.carbon.core.metadata;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.log4j.Logger;

public final class SegmentManager {

  private static final Logger LOGGER
          = LogServiceFactory.getLogService(SegmentManager.class.getName());

  /**
   * Generate a segment file of carbon table, and write it to the specified location.
   *
   * @param table Carbon table
   * @param options Options, the specified target location set in this options.
   * @return CarbonFile
   */
  public static CarbonFile createSegmentFile(
          final CarbonTable table,
          final Map<String, String> options
  ) throws IOException {
    if (options == null) {
      throw new IllegalArgumentException("Argument [options] is null.");
    }
    final String segmentPath = options.get("path");
    if (segmentPath == null) {
      throw new IllegalArgumentException("Option[path] is not set.");
    }
    if (!table.getTableInfo().isTransactionalTable()) {
      throw new IllegalArgumentException("Unsupported operation on non-transactional table.");
    }
    final LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
    final String loadMetadataDetailsFileName = UUID.randomUUID().toString().replace("-", "");
    CarbonLoaderUtil.populateNewLoadMetaEntry(
        loadMetadataDetails,
        SegmentStatus.INSERT_IN_PROGRESS,
        CarbonUpdateUtil.readCurrentTime(),
        false
    );
    String format = options.get("format");
    if (format == null) {
      format = "carbondata";
    }
    if (!(format.equals("carbondata") || format.equals("carbon"))) {
      loadMetadataDetails.setFileFormat(new FileFormat(format));
    }
    final String segmentIdentifier = "";
    final Segment segment = new Segment(
        segmentIdentifier,
        SegmentFileStore.genSegmentFileName(
            segmentIdentifier,
            loadMetadataDetailsFileName
        ) + CarbonTablePath.SEGMENT_EXT,
        segmentPath,
        options
    );
    if (SegmentFileStore.writeSegmentFile(table, segment)) {
      return createLoadMetadataDetails(
          table,
          loadMetadataDetails,
          loadMetadataDetailsFileName,
          segment.getSegmentFileName(),
          segmentPath
      );
    } else {
      // TODO
      throw new IOException("Adding segment with path failed.");
    }
  }

  /**
   * Delete the specified segment file without and exception.
   *
   * @param segmentFile The specified segment file.
   */
  public static void deleteSegmentFileQuietly(final CarbonFile segmentFile) {
    try {
      CarbonUtil.deleteFoldersAndFiles(segmentFile);
    } catch (Throwable exception) {
      LOGGER.error("Fail to delete segment data path [" + segmentFile + "].", exception);
    }
  }

  private static CarbonFile createLoadMetadataDetails(
      final CarbonTable table,
      final LoadMetadataDetails loadMetadataDetails,
      final String loadMetadataDetailsFileName,
      final String segmentName,
      final String segmentPath
  ) throws IOException {
    final Map<String, Long> dataSizeAndIndexSize = CarbonUtil.getDataSizeAndIndexSize(segmentPath);
    // Set empty load name, it will be replaced at compact.
    loadMetadataDetails.setLoadName("");
    loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
    loadMetadataDetails.setDataSize(
            dataSizeAndIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString());
    loadMetadataDetails.setIndexSize(
            dataSizeAndIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString());
    loadMetadataDetails.setPath(null);
    loadMetadataDetails.setSegmentFile(segmentName);
    final String loadMetadataDetailsPath =
        CarbonTablePath.getLoadDetailsDir(table.getAbsoluteTableIdentifier().getTablePath())
            + CarbonCommonConstants.FILE_SEPARATOR + loadMetadataDetailsFileName;
    SegmentStatusManager.writeLoadDetailsIntoFile(
        loadMetadataDetailsPath,
        new LoadMetadataDetails[]{loadMetadataDetails}
    );
    return FileFactory.getCarbonFile(loadMetadataDetailsPath);
  }

}

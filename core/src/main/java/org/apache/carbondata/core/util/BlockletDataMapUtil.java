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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.AbstractDFSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;


public class BlockletDataMapUtil {

  public static Map<String, BlockMetaInfo> getBlockMetaInfoMap(
      TableBlockIndexUniqueIdentifier identifier, SegmentIndexFileStore indexFileStore,
      Set<String> filesRead) throws IOException {
    if (identifier.getMergeIndexFileName() != null) {
      CarbonFile indexMergeFile = FileFactory.getCarbonFile(
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getMergeIndexFileName());
      if (indexMergeFile.exists() && !filesRead.contains(indexMergeFile.getPath())) {
        indexFileStore.readAllIIndexOfSegment(new CarbonFile[] { indexMergeFile });
        filesRead.add(indexMergeFile.getPath());
      }
    }
    if (indexFileStore.getFileData(identifier.getIndexFileName()) == null) {
      indexFileStore.readAllIIndexOfSegment(new CarbonFile[] { FileFactory.getCarbonFile(
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getIndexFileName()) });
    }
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    Map<String, BlockMetaInfo> blockMetaInfoMap = new HashMap<>();
    List<DataFileFooter> indexInfo = fileFooterConverter.getIndexInfo(
        identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getIndexFileName(), indexFileStore.getFileData(identifier.getIndexFileName()));
    for (DataFileFooter footer : indexInfo) {
      String blockPath = footer.getBlockInfo().getTableBlockInfo().getFilePath();
      blockMetaInfoMap.put(blockPath, createBlockMetaInfo(blockPath));
    }
    return blockMetaInfoMap;
  }

  private static BlockMetaInfo createBlockMetaInfo(String carbonDataFile) throws IOException {
    CarbonFile carbonFile = FileFactory.getCarbonFile(carbonDataFile);
    if (carbonFile instanceof AbstractDFSCarbonFile) {
      RemoteIterator<LocatedFileStatus> iter =
          ((AbstractDFSCarbonFile)carbonFile).fs.listLocatedStatus(new Path(carbonDataFile));
      LocatedFileStatus fileStatus = iter.next();
      String[] location = fileStatus.getBlockLocations()[0].getHosts();
      long len = fileStatus.getLen();
      return new BlockMetaInfo(location, len);
    } else {
      return new BlockMetaInfo(new String[]{"localhost"}, carbonFile.getSize());
    }
  }

  public static Set<TableBlockIndexUniqueIdentifier> getTableBlockUniqueIdentifiers(Segment segment,
      String tablePath) throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new HashSet<>();
    Map<String, String> indexFiles;
    if (segment.getSegmentFileName() == null) {
      String path = CarbonTablePath.getSegmentPath(tablePath, segment.getSegmentNo());
      indexFiles = new SegmentIndexFileStore().getIndexFilesFromSegment(path);
    } else {
      SegmentFileStore fileStore = new SegmentFileStore(tablePath, segment.getSegmentFileName());
      indexFiles = fileStore.getIndexFiles();
    }
    for (Map.Entry<String, String> indexFileEntry : indexFiles.entrySet()) {
      Path indexFile = new Path(indexFileEntry.getKey());
      tableBlockIndexUniqueIdentifiers.add(
          new TableBlockIndexUniqueIdentifier(indexFile.getParent().toString(), indexFile.getName(),
              indexFileEntry.getValue(), segment.getSegmentNo()));
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * This method will filter out the TableBlockIndexUniqueIdentifiers belongs to that distributable
   *
   * @param tableBlockIndexUniqueIdentifiers
   * @param distributable
   * @return
   */
  public static Set<TableBlockIndexUniqueIdentifier> filterIdentifiersBasedOnDistributable(
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers,
      BlockletDataMapDistributable distributable) {
    Set<TableBlockIndexUniqueIdentifier> validIdentifiers =
        new HashSet<>(tableBlockIndexUniqueIdentifiers.size());
    if (distributable.getFilePath().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
          tableBlockIndexUniqueIdentifiers) {
        if (null != tableBlockIndexUniqueIdentifier.getMergeIndexFileName()) {
          validIdentifiers.add(tableBlockIndexUniqueIdentifier);
        }
      }
    } else {
      for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
          tableBlockIndexUniqueIdentifiers) {
        if (null == tableBlockIndexUniqueIdentifier.getMergeIndexFileName()) {
          validIdentifiers.add(tableBlockIndexUniqueIdentifier);
        }
      }
    }
    return validIdentifiers;
  }

}

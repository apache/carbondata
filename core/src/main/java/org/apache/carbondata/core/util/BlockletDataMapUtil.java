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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.AbstractDFSCarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class BlockletDataMapUtil {

  private static final Log LOG = LogFactory.getLog(BlockletDataMapUtil.class);

  public static Map<String, BlockMetaInfo> getBlockMetaInfoMap(
      TableBlockIndexUniqueIdentifier identifier, SegmentIndexFileStore indexFileStore,
      Set<String> filesRead, Map<String, BlockMetaInfo> fileNameToMetaInfoMapping)
      throws IOException {
    boolean isTransactionalTable = true;
    List<ColumnSchema> tableColumnList = null;
    if (identifier.getMergeIndexFileName() != null
        && indexFileStore.getFileData(identifier.getIndexFileName()) == null) {
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
    CarbonTable carbonTable =
        CarbonMetadata.getInstance().getCarbonTable(identifier.getTableUniqueName());
    if (carbonTable != null) {
      isTransactionalTable = carbonTable.getTableInfo().isTransactionalTable();
      tableColumnList =
          carbonTable.getTableInfo().getFactTable().getListOfColumns();
    }
    for (DataFileFooter footer : indexInfo) {
      if ((!isTransactionalTable) && !isSameColumnSchemaList(footer.getColumnInTable(),
          tableColumnList)) {
        LOG.error("Schema of " + identifier.getIndexFileName()
            + " doesn't match with the table's schema");
        throw new IOException("All the files doesn't have same schema. "
            + "Unsupported operation on nonTransactional table. Check logs.");
      }

      String blockPath = footer.getBlockInfo().getTableBlockInfo().getFilePath();
      if (null == blockMetaInfoMap.get(blockPath)) {
        blockMetaInfoMap.put(blockPath, createBlockMetaInfo(fileNameToMetaInfoMapping, blockPath));
      }
    }
    return blockMetaInfoMap;
  }

  /**
   * This method will create file name to block Meta Info Mapping. This method will reduce the
   * number of namenode calls and using this method one namenode will fetch 1000 entries
   *
   * @param segmentFilePath
   * @return
   * @throws IOException
   */
  public static Map<String, BlockMetaInfo> createCarbonDataFileBlockMetaInfoMapping(
      String segmentFilePath) throws IOException {
    Map<String, BlockMetaInfo> fileNameToMetaInfoMapping = new TreeMap();
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFilePath);
    if (carbonFile instanceof AbstractDFSCarbonFile) {
      PathFilter pathFilter = new PathFilter() {
        @Override public boolean accept(Path path) {
          return CarbonTablePath.isCarbonDataFile(path.getName());
        }
      };
      CarbonFile[] carbonFiles = carbonFile.locationAwareListFiles(pathFilter);
      for (CarbonFile file : carbonFiles) {
        String[] location = file.getLocations();
        long len = file.getSize();
        BlockMetaInfo blockMetaInfo = new BlockMetaInfo(location, len);
        fileNameToMetaInfoMapping.put(file.getPath().toString(), blockMetaInfo);
      }
    }
    return fileNameToMetaInfoMapping;
  }

  private static BlockMetaInfo createBlockMetaInfo(
      Map<String, BlockMetaInfo> fileNameToMetaInfoMapping, String carbonDataFile) {
    FileFactory.FileType fileType = FileFactory.getFileType(carbonDataFile);
    switch (fileType) {
      case LOCAL:
        CarbonFile carbonFile = FileFactory.getCarbonFile(carbonDataFile, fileType);
        return new BlockMetaInfo(new String[] { "localhost" }, carbonFile.getSize());
      default:
        return fileNameToMetaInfoMapping.get(carbonDataFile);
    }
  }

  public static Set<TableBlockIndexUniqueIdentifier> getTableBlockUniqueIdentifiers(Segment segment,
      String getTableUniqueName)
      throws IOException {
    Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new HashSet<>();
    Map<String, String> indexFiles = segment.getCommittedIndexFile();
    for (Map.Entry<String, String> indexFileEntry : indexFiles.entrySet()) {
      Path indexFile = new Path(indexFileEntry.getKey());
      tableBlockIndexUniqueIdentifiers.add(
          new TableBlockIndexUniqueIdentifier(indexFile.getParent().toString(), indexFile.getName(),
              indexFileEntry.getValue(), segment.getSegmentNo(), getTableUniqueName));
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  /**
   * This method will filter out the TableBlockIndexUniqueIdentifier belongs to that distributable
   *
   * @param tableBlockIndexUniqueIdentifiers
   * @param distributable
   * @return
   */
  public static TableBlockIndexUniqueIdentifier filterIdentifiersBasedOnDistributable(
      Set<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers,
      BlockletDataMapDistributable distributable) {
    TableBlockIndexUniqueIdentifier validIdentifier = null;
    String fileName = CarbonTablePath.DataFileUtil.getFileName(distributable.getFilePath());
    for (TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier :
        tableBlockIndexUniqueIdentifiers) {
      if (fileName.equals(tableBlockIndexUniqueIdentifier.getIndexFileName())) {
        validIdentifier = tableBlockIndexUniqueIdentifier;
        break;
      }
    }
    return validIdentifier;
  }

  /**
   * This method will the index files tableBlockIndexUniqueIdentifiers of a merge index file
   *
   * @param identifier
   * @param tableUniqueName
   * @return
   * @throws IOException
   */
  public static List<TableBlockIndexUniqueIdentifier> getIndexFileIdentifiersFromMergeFile(
      TableBlockIndexUniqueIdentifier identifier, SegmentIndexFileStore segmentIndexFileStore,
      String tableUniqueName)
      throws IOException {
    List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers = new ArrayList<>();
    String mergeFilePath =
        identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getIndexFileName();
    segmentIndexFileStore.readMergeFile(mergeFilePath);
    List<String> indexFiles =
        segmentIndexFileStore.getCarbonMergeFileToIndexFilesMap().get(mergeFilePath);
    for (String indexFile : indexFiles) {
      tableBlockIndexUniqueIdentifiers.add(
          new TableBlockIndexUniqueIdentifier(identifier.getIndexFilePath(), indexFile,
              identifier.getIndexFileName(), identifier.getSegmentId(), tableUniqueName));
    }
    return tableBlockIndexUniqueIdentifiers;
  }

  private static boolean isSameColumnSchemaList(List<ColumnSchema> indexFileColumnList,
      List<ColumnSchema> tableColumnList) {
    if (indexFileColumnList.size() != tableColumnList.size()) {
      LOG.error("Index file's column size is " + indexFileColumnList.size()
          + " but table's column size is " + tableColumnList.size());
      return false;
    }
    for (int i = 0; i < tableColumnList.size(); i++) {
      if (!indexFileColumnList.get(i).equalsWithStrictCheck(tableColumnList.get(i))) {
        return false;
      }
    }
    return true;
  }
}
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

package org.apache.carbondata.hadoop.api;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * InputFormat for reading carbondata files without table level metadata support,
 * schema is inferred as following steps:
 * 1. read from schema file is exists
 * 2. read from data file footer
 *
 * @param <T>
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class CarbonFileInputFormat<T> extends CarbonInputFormat<T> implements Serializable {

  // a cache for carbon table, it will be used in task side
  private CarbonTable carbonTable;

  public CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
    CarbonTable carbonTableTemp;
    if (carbonTable == null) {
      // carbon table should be created either from deserialized table info (schema saved in
      // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
      TableInfo tableInfo = getTableInfo(configuration);
      CarbonTable localCarbonTable;
      if (tableInfo != null) {
        localCarbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      } else {
        String schemaPath = CarbonTablePath
            .getSchemaFilePath(getAbsoluteTableIdentifier(configuration).getTablePath());
        if (!FileFactory.isFileExist(schemaPath)) {
          TableInfo tableInfoInfer =
              SchemaReader.inferSchema(getAbsoluteTableIdentifier(configuration), true);
          localCarbonTable = CarbonTable.buildFromTableInfo(tableInfoInfer);
        } else {
          localCarbonTable =
              SchemaReader.readCarbonTableFromStore(getAbsoluteTableIdentifier(configuration));
        }
      }
      this.carbonTable = localCarbonTable;
      return localCarbonTable;
    } else {
      carbonTableTemp = this.carbonTable;
      return carbonTableTemp;
    }
  }

  /**
   * get list of block/blocklet and make them to CarbonInputSplit
   * @param job JobContext with Configuration
   * @return list of CarbonInputSplit
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

    // get all valid segments and set them into the configuration
    // check for externalTable segment (Segment_null)
    // process and resolve the expression

    ReadCommittedScope readCommittedScope;
    if (carbonTable.isTransactionalTable()) {
      readCommittedScope = new LatestFilesReadCommittedScope(
          identifier.getTablePath() + "/Fact/Part0/Segment_null/", job.getConfiguration());
    } else {
      readCommittedScope = getReadCommittedScope(job.getConfiguration());
      if (readCommittedScope == null) {
        readCommittedScope = new LatestFilesReadCommittedScope(identifier.getTablePath(), job
            .getConfiguration());
      } else {
        readCommittedScope.setConfiguration(job.getConfiguration());
      }
    }
    // this will be null in case of corrupt schema file.
    IndexFilter filter = getFilterPredicates(job.getConfiguration());

    // if external table Segments are found, add it to the List
    List<Segment> externalTableSegments = new ArrayList<>();
    Segment seg;
    if (carbonTable.isTransactionalTable()) {
      // SDK some cases write into the Segment Path instead of Table Path i.e. inside
      // the "Fact/Part0/Segment_null". The segment in this case is named as "null".
      // The table is denoted by default as a transactional table and goes through
      // the path of CarbonFileInputFormat. The above scenario is handled in the below code.
      seg = new Segment("null", null, readCommittedScope);
      externalTableSegments.add(seg);
    } else {
      LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();
      for (LoadMetadataDetails load : loadMetadataDetails) {
        seg = new Segment(load.getLoadName(), null, readCommittedScope);
        if (fileLists != null) {
          for (Object fileList : fileLists) {
            String timestamp =
                CarbonTablePath.DataFileUtil.getTimeStampFromFileName(fileList.toString());
            if (timestamp.equals(seg.getSegmentNo())) {
              externalTableSegments.add(seg);
              break;
            }
          }
        } else {
          externalTableSegments.add(seg);
        }
      }
    }
    List<InputSplit> splits = new ArrayList<>();
    boolean useBlockIndex = job.getConfiguration().getBoolean("filter_blocks", true);
    // useBlockIndex would be false in case of SDK when user has not provided any filter, In
    // this case we don't want to load block/blocklet index. It would be true in all other
    // scenarios
    if (filter != null) {
      filter.resolve(false);
    }
    if (useBlockIndex) {
      // do block filtering and get split
      splits = getSplits(job, filter, externalTableSegments);
    } else {
      List<CarbonFile> carbonFiles;
      if (null != this.fileLists) {
        carbonFiles = getAllCarbonDataFiles(this.fileLists);
      } else {
        carbonFiles = getAllCarbonDataFiles(carbonTable.getTablePath());
      }

      List<String> allDeleteDeltaFiles = getAllDeleteDeltaFiles(carbonTable.getTablePath());
      for (CarbonFile carbonFile : carbonFiles) {
        // Segment id is set to null because SDK does not write carbondata files with respect
        // to segments. So no specific name is present for this load.
        CarbonInputSplit split =
            new CarbonInputSplit("null", carbonFile.getAbsolutePath(), 0,
                carbonFile.getLength(), carbonFile.getLocations(), FileFormat.COLUMNAR_V3);
        split.setVersion(ColumnarFormatVersion.V3);
        BlockletDetailInfo info = new BlockletDetailInfo();
        split.setDetailInfo(info);
        info.setBlockSize(carbonFile.getLength());
        info.setVersionNumber(split.getVersion().number());
        info.setUseMinMaxForPruning(false);
        if (CollectionUtils.isNotEmpty(allDeleteDeltaFiles)) {
          split.setDeleteDeltaFiles(
              getDeleteDeltaFiles(carbonFile.getAbsolutePath(), allDeleteDeltaFiles));
        }
        splits.add(split);
      }
      splits.sort(Comparator.comparing(o -> ((CarbonInputSplit) o).getFilePath()));
    }

    setAllColumnProjectionIfNotConfigured(job, carbonTable);
    return splits;
  }

  public void setAllColumnProjectionIfNotConfigured(JobContext job, CarbonTable carbonTable) {
    if (getColumnProjection(job.getConfiguration()) == null) {
      // If the user projection is empty, use default all columns as projections.
      // All column name will be filled inside getSplits, so can update only here.
      String[]  projectionColumns = projectAllColumns(carbonTable);
      setColumnProjection(job.getConfiguration(), projectionColumns);
    }
  }

  private List<CarbonFile> getAllCarbonDataFiles(String tablePath) {
    List<CarbonFile> carbonFiles;
    try {
      carbonFiles = FileFactory.getCarbonFile(tablePath).listFiles(true,
          file -> file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return carbonFiles;
  }

  private List<CarbonFile> getAllCarbonDataFiles(List fileLists) {
    List<CarbonFile> carbonFiles = new LinkedList<>();
    try {
      for (Object fileList : fileLists) {
        carbonFiles.add(FileFactory.getCarbonFile(fileList.toString()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return carbonFiles;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, IndexFilter indexFilter,
      List<Segment> validSegments) throws IOException {
    numSegments = validSegments.size();
    // for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment = getDataBlocksOfSegment(job, carbonTable,
        indexFilter, validSegments, new ArrayList<>(), new ArrayList<>());
    numBlocks = dataBlocksOfSegment.size();
    List<String> allDeleteDeltaFiles = getAllDeleteDeltaFiles(carbonTable.getTablePath());
    if (CollectionUtils.isNotEmpty(allDeleteDeltaFiles)) {
      for (CarbonInputSplit split : dataBlocksOfSegment) {
        split.setDeleteDeltaFiles(getDeleteDeltaFiles(split.getFilePath(), allDeleteDeltaFiles));
      }
    }
    return new LinkedList<>(dataBlocksOfSegment);
  }

  private List<String> getAllDeleteDeltaFiles(String path) {
    List<String> deltaFiles = new ArrayList<>();
    try {
      FileFactory.getCarbonFile(path).listFiles(true, new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          if (file.getName().endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)) {
            deltaFiles.add(file.getAbsolutePath());
            return true;
          }
          return false;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return deltaFiles;
  }

  private String[] getDeleteDeltaFiles(String segmentFilePath, List<String> allDeleteDeltaFiles) {
    List<String> deleteDeltaFiles = new ArrayList<>();
    String segmentFileName = null;
    segmentFilePath = segmentFilePath.replace(CarbonCommonConstants.WINDOWS_FILE_SEPARATOR,
            CarbonCommonConstants.FILE_SEPARATOR);
    String[] pathElements = segmentFilePath.split(CarbonCommonConstants.FILE_SEPARATOR);
    if (ArrayUtils.isNotEmpty(pathElements)) {
      segmentFileName = pathElements[pathElements.length - 1];
    }

    /* DeleteDeltaFiles for a segment will be mapped on the basis of name.
       So extract the expectedDeleteDeltaFileName by removing the
       compressor name and part number from the segmentFileName.
    */

    String expectedDeleteDeltaFileName = null;
    if (segmentFileName != null && !segmentFileName.isEmpty()) {
      int endIndex = segmentFileName.indexOf(CarbonCommonConstants.UNDERSCORE);
      if (endIndex != -1) {
        expectedDeleteDeltaFileName = segmentFileName.substring(0, endIndex);
      }
    }
    String deleteDeltaFullFileName = null;
    for (String deltaFile : allDeleteDeltaFiles) {
      String[] deleteDeltaPathElements = deltaFile.split(Pattern.quote(File.separator));
      if (ArrayUtils.isNotEmpty(deleteDeltaPathElements)) {
        deleteDeltaFullFileName = deleteDeltaPathElements[deleteDeltaPathElements.length - 1];
      }
      int underScoreIndex = deleteDeltaFullFileName.indexOf(CarbonCommonConstants.UNDERSCORE);
      if (underScoreIndex != -1) {
        String deleteDeltaFileName = deleteDeltaFullFileName.substring(0, underScoreIndex);
        if (deleteDeltaFileName.equals(expectedDeleteDeltaFileName)) {
          deleteDeltaFiles.add(deltaFile);
        }
      }
    }
    String[] deleteDeltaFile = new String[deleteDeltaFiles.size()];
    int currentIndex = 0;
    for (String deltaFile : deleteDeltaFiles) {
      deleteDeltaFile[currentIndex++] = deltaFile;
    }
    return deleteDeltaFile;
  }
}

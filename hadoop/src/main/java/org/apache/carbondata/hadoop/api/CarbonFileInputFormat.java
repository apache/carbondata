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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailInfo;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;

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
        if (!FileFactory.isFileExist(schemaPath, FileFactory.getFileType(schemaPath))) {
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
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR
   * are used to get table path to read.
   *
   * @param job
   * @return List<InputSplit> list of CarbonInputSplit
   * @throws IOException
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      // get all valid segments and set them into the configuration
      // check for externalTable segment (Segment_null)
      // process and resolve the expression

      ReadCommittedScope readCommittedScope = null;
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
      PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());
      Expression filter = getFilterPredicates(job.getConfiguration());


      // if external table Segments are found, add it to the List
      List<Segment> externalTableSegments = new ArrayList<Segment>();
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
            for (int i = 0; i < fileLists.size(); i++) {
              if (fileLists.get(i).toString().endsWith(seg.getSegmentNo()
                  + CarbonTablePath.CARBON_DATA_EXT)) {
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
      boolean useBlockDataMap = job.getConfiguration().getBoolean("filter_blocks", true);
      // useBlockDataMap would be false in case of SDK when user has not provided any filter, In
      // this case we don't want to load block/blocklet datamap. It would be true in all other
      // scenarios
      if (useBlockDataMap) {
        // do block filtering and get split
        splits = getSplits(job, filter, externalTableSegments, null, partitionInfo, null);
      } else {
        List<CarbonFile> carbonFiles = null;
        if (null != this.fileLists) {
          carbonFiles = getAllCarbonDataFiles(this.fileLists);
        } else {
          carbonFiles = getAllCarbonDataFiles(carbonTable.getTablePath());
        }

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
          splits.add(split);
        }
        Collections.sort(splits, new Comparator<InputSplit>() {
          @Override public int compare(InputSplit o1, InputSplit o2) {
            return ((CarbonInputSplit) o1).getFilePath()
                .compareTo(((CarbonInputSplit) o2).getFilePath());
          }
        });
      }
      setAllColumnProjectionIfNotConfigured(job, carbonTable);
      return splits;
    }
    return null;
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
      carbonFiles = FileFactory.getCarbonFile(tablePath).listFiles(true, new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT);
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return carbonFiles;
  }

  private List<CarbonFile> getAllCarbonDataFiles(List fileLists) {
    List<CarbonFile> carbonFiles = new LinkedList<CarbonFile>();
    try {
      for (int i = 0; i < fileLists.size(); i++) {
        carbonFiles.add(FileFactory.getCarbonFile(fileLists.get(i).toString()));
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
  private List<InputSplit> getSplits(JobContext job, Expression expression,
      List<Segment> validSegments, BitSet matchedPartitions, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    numSegments = validSegments.size();
    List<InputSplit> result = new LinkedList<InputSplit>();

    // for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, carbonTable, expression, matchedPartitions, validSegments,
            partitionInfo, oldPartitionIdList, new ArrayList<Segment>(), new ArrayList<String>());
    numBlocks = dataBlocksOfSegment.size();
    result.addAll(dataBlocksOfSegment);
    return result;
  }
}

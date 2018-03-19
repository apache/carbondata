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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

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

  protected CarbonTable getOrCreateCarbonTable(Configuration configuration) throws IOException {
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
              SchemaReader.inferSchema(getAbsoluteTableIdentifier(configuration));
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
    AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(job.getConfiguration());
    CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }

    if (getValidateSegmentsToAccess(job.getConfiguration())) {
      // get all valid segments and set them into the configuration
      // check for externalTable segment (Segment_null)
      // process and resolve the expression
      Expression filter = getFilterPredicates(job.getConfiguration());
      TableProvider tableProvider = new SingleTableProvider(carbonTable);
      // this will be null in case of corrupt schema file.
      PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());
      carbonTable.processFilterExpression(filter, null, null);

      FilterResolverIntf filterInterface = carbonTable.resolveFilter(filter, tableProvider);

      String segmentDir = CarbonTablePath.getSegmentPath(identifier.getTablePath(), "null");
      FileFactory.FileType fileType = FileFactory.getFileType(segmentDir);
      if (FileFactory.isFileExist(segmentDir, fileType)) {
        // if external table Segments are found, add it to the List
        List<Segment> externalTableSegments = new ArrayList<Segment>();
        Segment seg = new Segment("null", null);
        externalTableSegments.add(seg);

        Map<String, String> indexFiles =
            new SegmentIndexFileStore().getIndexFilesFromSegment(segmentDir);

        if (indexFiles.size() == 0) {
          throw new RuntimeException("Index file not present to read the carbondata file");
        }
        // do block filtering and get split
        List<InputSplit> splits =
            getSplits(job, filterInterface, externalTableSegments, null, partitionInfo, null);

        return splits;
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  private List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver,
      List<Segment> validSegments, BitSet matchedPartitions, PartitionInfo partitionInfo,
      List<Integer> oldPartitionIdList) throws IOException {

    List<InputSplit> result = new LinkedList<InputSplit>();
    UpdateVO invalidBlockVOForSegmentId = null;
    Boolean isIUDTable = false;

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getOrCreateCarbonTable(job.getConfiguration()).getAbsoluteTableIdentifier();
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);

    isIUDTable = (updateStatusManager.getUpdateStatusDetails().length != 0);

    // for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, absoluteTableIdentifier, filterResolver, matchedPartitions,
            validSegments, partitionInfo, oldPartitionIdList);
    for (CarbonInputSplit inputSplit : dataBlocksOfSegment) {

      // Get the UpdateVO for those tables on which IUD operations being performed.
      if (isIUDTable) {
        invalidBlockVOForSegmentId =
            updateStatusManager.getInvalidTimestampRange(inputSplit.getSegmentId());
      }
      String[] deleteDeltaFilePath = null;
      if (isIUDTable) {
        // In case IUD is not performed in this table avoid searching for
        // invalidated blocks.
        if (CarbonUtil
            .isInvalidTableBlock(inputSplit.getSegmentId(), inputSplit.getPath().toString(),
                invalidBlockVOForSegmentId, updateStatusManager)) {
          continue;
        }
        // When iud is done then only get delete delta files for a block
        try {
          deleteDeltaFilePath = updateStatusManager
              .getDeleteDeltaFilePath(inputSplit.getPath().toString(), inputSplit.getSegmentId());
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      inputSplit.setDeleteDeltaFiles(deleteDeltaFilePath);
      result.add(inputSplit);
    }
    return result;
  }
}

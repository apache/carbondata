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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
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
            identifier.getTablePath() + "/Fact/Part0/Segment_null/");
      } else {
        readCommittedScope = new LatestFilesReadCommittedScope(identifier.getTablePath());
      }
      Expression filter = getFilterPredicates(job.getConfiguration());
      // this will be null in case of corrupt schema file.
      PartitionInfo partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName());
      carbonTable.processFilterExpression(filter, null, null);

      FilterResolverIntf filterInterface = carbonTable.resolveFilter(filter);

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
        List<SegmentDetailVO> allSegments = readCommittedScope.getSegments().getAllSegments();
        for (SegmentDetailVO load : allSegments) {
          seg = new Segment(load.getSegmentId(), null, readCommittedScope);
          externalTableSegments.add(seg);
        }
      }
      // do block filtering and get split
      List<InputSplit> splits =
          getSplits(job, filterInterface, externalTableSegments, null, partitionInfo, null);
      if (getColumnProjection(job.getConfiguration()) == null) {
        // If the user projection is empty, use default all columns as projections.
        // All column name will be filled inside getSplits, so can update only here.
        String[]  projectionColumns = projectAllColumns(carbonTable);
        setColumnProjection(job.getConfiguration(), projectionColumns);
      }
      return splits;
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

    numSegments = validSegments.size();
    List<InputSplit> result = new LinkedList<InputSplit>();

    // for each segment fetch blocks matching filter in Driver BTree
    List<CarbonInputSplit> dataBlocksOfSegment =
        getDataBlocksOfSegment(job, carbonTable, filterResolver, matchedPartitions,
            validSegments, partitionInfo, oldPartitionIdList);
    numBlocks = dataBlocksOfSegment.size();
    result.addAll(dataBlocksOfSegment);
    return result;
  }
}

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

package org.apache.carbondata.core.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper;
import org.apache.carbondata.core.index.dev.secondaryindex.SIExpressionTree.CarbonSIExpression;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.LatestFilesReadCommittedScope;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;

import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

/**
 * Input format for Indexes, it makes the Index pruning distributable.
 */
public class IndexInputFormat extends FileInputFormat<Void, ExtendedBlocklet>
    implements Serializable, Writable {

  private static final transient Logger LOGGER =
      LogServiceFactory.getLogService(IndexInputFormat.class.getName());

  private static final long serialVersionUID = 9189779090091151248L;

  private CarbonTable table;

  private FilterResolverIntf filterResolverIntf;

  private List<Segment> validSegments;

  private List<String> invalidSegments;

  private List<PartitionSpec> partitions;

  private boolean isJobToClearIndexes = false;

  private IndexLevel indexLevel;

  private boolean isFallbackJob = false;

  private String indexToClear = "";

  private ReadCommittedScope readCommittedScope;

  private String taskGroupId = "";

  private String taskGroupDesc = "";

  private String queryId = UUID.randomUUID().toString();

  private transient IndexChooser indexChooser;

  private boolean isWriteToFile = true;

  private boolean isCountStarJob = false;

  // Whether AsyncCall to the Index Server(true in the case of pre-priming)
  private boolean isAsyncCall;

  private CarbonSIExpression indexTablesScanTree;

  IndexInputFormat() {

  }

  IndexInputFormat(CarbonTable table,
      List<Segment> validSegments, List<String> invalidSegments, boolean isJobToClearIndexes,
      String indexToClear) {
    this(table, null, validSegments, invalidSegments, null,
        isJobToClearIndexes, null, false, false);
    this.indexToClear = indexToClear;
  }

  public IndexInputFormat(CarbonTable table, FilterResolverIntf filterResolverIntf,
      List<Segment> validSegments, List<String> invalidSegments, List<PartitionSpec> partitions,
      boolean isJobToClearIndexes, IndexLevel indexLevel, boolean isFallbackJob,
      boolean isAsyncCall) {
    this.table = table;
    this.filterResolverIntf = filterResolverIntf;
    this.validSegments = validSegments;
    if (!validSegments.isEmpty()) {
      this.readCommittedScope = validSegments.get(0).getReadCommittedScope();
    }
    this.invalidSegments = invalidSegments;
    this.partitions = partitions;
    this.isJobToClearIndexes = isJobToClearIndexes;
    this.indexLevel = indexLevel;
    this.isFallbackJob = isFallbackJob;
    this.isAsyncCall = isAsyncCall;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<IndexInputSplitWrapper> distributableList;
    distributableList =
        IndexChooser.getDefaultIndex(table, filterResolverIntf).toDistributable(validSegments);
    List<InputSplit> inputSplits = new ArrayList<>(distributableList.size());
    inputSplits.addAll(distributableList);
    return inputSplits;
  }

  @Override
  public RecordReader<Void, ExtendedBlocklet> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new RecordReader<Void, ExtendedBlocklet>() {
      private Iterator<ExtendedBlocklet> blockletIterator;
      private ExtendedBlocklet currBlocklet;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException {
        IndexInputSplitWrapper distributable = (IndexInputSplitWrapper) inputSplit;
        distributable.getDistributable().getSegment().setReadCommittedScope(readCommittedScope);
        List<Segment> segmentsToLoad = new ArrayList<>();
        segmentsToLoad.add(distributable.getDistributable().getSegment());
        List<ExtendedBlocklet> blocklets = new ArrayList<>();
        if (indexLevel == null) {
          TableIndex defaultIndex = IndexStoreManager.getInstance()
              .getIndex(table, distributable.getDistributable().getIndexSchema());
          blocklets = defaultIndex
              .prune(segmentsToLoad, new IndexFilter(filterResolverIntf), partitions);
          blocklets = IndexUtil
              .pruneIndexes(table, filterResolverIntf, segmentsToLoad, partitions, blocklets,
                  indexChooser);
        } else {
          blocklets = IndexUtil
              .pruneIndexes(table, filterResolverIntf, segmentsToLoad, partitions, blocklets,
                  indexLevel, indexChooser);
        }
        blockletIterator = blocklets.iterator();
      }

      @Override
      public boolean nextKeyValue() {
        boolean hasNext = blockletIterator.hasNext();
        if (hasNext) {
          currBlocklet = blockletIterator.next();
        } else {
          // close all resources when all the results are returned
          close();
        }
        return hasNext;
      }

      @Override
      public Void getCurrentKey() {
        return null;
      }

      @Override
      public ExtendedBlocklet getCurrentValue() {
        return currBlocklet;
      }

      @Override
      public float getProgress() {
        return 0;
      }

      @Override
      public void close() {
        // Clear the Indexes from executor
        if (isFallbackJob) {
          IndexStoreManager.getInstance()
              .clearIndexCache(table.getAbsoluteTableIdentifier(), false);
        }
      }
    };
  }

  public CarbonTable getCarbonTable() {
    return table;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    table.write(out);
    out.writeInt(invalidSegments.size());
    for (String invalidSegment : invalidSegments) {
      out.writeUTF(invalidSegment);
    }
    out.writeBoolean(isJobToClearIndexes);
    out.writeBoolean(isFallbackJob);
    if (indexLevel == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(indexLevel.name());
    }
    out.writeInt(validSegments.size());
    for (Segment segment : validSegments) {
      segment.write(out);
    }
    if (partitions == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(partitions.size());
      for (PartitionSpec partitionSpec : partitions) {
        partitionSpec.write(out);
      }
    }
    if (filterResolverIntf != null) {
      out.writeBoolean(true);
      byte[] filterResolverBytes = ObjectSerializationUtil.convertObjectToString(filterResolverIntf)
          .getBytes(Charset.defaultCharset());
      out.writeInt(filterResolverBytes.length);
      out.write(filterResolverBytes);

    } else {
      out.writeBoolean(false);
    }
    out.writeUTF(indexToClear);
    out.writeUTF(taskGroupId);
    out.writeUTF(taskGroupDesc);
    out.writeUTF(queryId);
    out.writeBoolean(isWriteToFile);
    out.writeBoolean(isCountStarJob);
    out.writeBoolean(isAsyncCall);
    out.writeBoolean(indexTablesScanTree != null);
    if (indexTablesScanTree != null) {
      byte[] siExpressionTreeBytes =
          ObjectSerializationUtil.convertObjectToString(indexTablesScanTree)
              .getBytes(Charset.defaultCharset());
      out.writeInt(siExpressionTreeBytes.length);
      out.write(siExpressionTreeBytes);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.table = new CarbonTable();
    table.readFields(in);
    int invalidSegmentSize = in.readInt();
    invalidSegments = new ArrayList<>(invalidSegmentSize);
    for (int i = 0; i < invalidSegmentSize; i++) {
      invalidSegments.add(in.readUTF());
    }
    this.isJobToClearIndexes = in.readBoolean();
    this.isFallbackJob = in.readBoolean();
    if (in.readBoolean()) {
      this.indexLevel = IndexLevel.valueOf(in.readUTF());
    }
    int validSegmentSize = in.readInt();
    validSegments = new ArrayList<>(validSegmentSize);
    initReadCommittedScope();
    for (int i = 0; i < validSegmentSize; i++) {
      Segment segment = new Segment();
      segment.setReadCommittedScope(readCommittedScope);
      segment.readFields(in);
      validSegments.add(segment);
    }
    if (in.readBoolean()) {
      int numPartitions = in.readInt();
      partitions = new ArrayList<>(numPartitions);
      for (int i = 0; i < numPartitions; i++) {
        PartitionSpec partitionSpec = new PartitionSpec();
        partitionSpec.readFields(in);
        partitions.add(partitionSpec);
      }
    }
    if (in.readBoolean()) {
      byte[] filterResolverBytes = new byte[in.readInt()];
      in.readFully(filterResolverBytes, 0, filterResolverBytes.length);
      this.filterResolverIntf = (FilterResolverIntf) ObjectSerializationUtil
          .convertStringToObject(new String(filterResolverBytes, Charset.defaultCharset()));
    }
    this.indexToClear = in.readUTF();
    this.taskGroupId = in.readUTF();
    this.taskGroupDesc = in.readUTF();
    this.queryId = in.readUTF();
    this.isWriteToFile = in.readBoolean();
    this.isCountStarJob = in.readBoolean();
    this.isAsyncCall = in.readBoolean();
    if (in.readBoolean()) {
      byte[] siExpressionTreeBytes = new byte[in.readInt()];
      in.readFully(siExpressionTreeBytes, 0, siExpressionTreeBytes.length);
      this.indexTablesScanTree = (CarbonSIExpression) ObjectSerializationUtil
          .convertStringToObject(new String(siExpressionTreeBytes, Charset.defaultCharset()));
    }
  }

  private void initReadCommittedScope() throws IOException {
    if (readCommittedScope == null) {
      if (table.isTransactionalTable()) {
        this.readCommittedScope =
            new TableStatusReadCommittedScope(table.getAbsoluteTableIdentifier(),
                FileFactory.getConfiguration());
      } else {
        this.readCommittedScope =
            new LatestFilesReadCommittedScope(table.getTablePath(),
                FileFactory.getConfiguration());
      }
    }
  }

  /**
   * @return Whether the job is fallback or not.
   */
  public boolean isFallbackJob() {
    return isFallbackJob;
  }

  /**
   * @return Whether asyncCall to the IndexServer.
   */
  public boolean ifAsyncCall() {
    return isAsyncCall;
  }

  /**
   * @return Whether the job is to clear cached Indexes or not.
   */
  public boolean isJobToClearIndexes() {
    return isJobToClearIndexes;
  }

  public String getTaskGroupId() {
    return taskGroupId;
  }

  /* setTaskGroupId will be used for Index server to display ExecutionId*/
  public void setTaskGroupId(String taskGroupId) {
    this.taskGroupId = taskGroupId;
  }

  public String getTaskGroupDesc() {
    return taskGroupDesc;
  }

  /* setTaskGroupId will be used for Index server to display Query
  *  If Job name is >CARBON_INDEX_SERVER_JOBNAME_LENGTH
  *  then need to cut as transferring big query to IndexServer will be costly.
  */
  public void setTaskGroupDesc(String taskGroupDesc) {
    int maxJobLength;
    try {
      String maxJobLengthString = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH ,
                      CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH_DEFAULT);
      maxJobLength = Integer.parseInt(maxJobLengthString);
    } catch (Exception e) {
      String maxJobLengthString = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH_DEFAULT);
      maxJobLength = Integer.parseInt(maxJobLengthString);
    }
    if (taskGroupDesc.length() > maxJobLength) {
      this.taskGroupDesc = taskGroupDesc.substring(0, maxJobLength);
    } else {
      this.taskGroupDesc = taskGroupDesc;
    }
  }

  public FilterResolverIntf getFilterResolverIntf() {
    return filterResolverIntf;
  }

  public void setFilterResolverIntf(FilterResolverIntf filterResolverIntf) {
    this.filterResolverIntf = filterResolverIntf;
  }

  public List<String> getInvalidSegments() {
    return invalidSegments;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getIndexToClear() {
    return indexToClear;
  }

  public void setIsWriteToFile(boolean isWriteToFile) {
    this.isWriteToFile = isWriteToFile;
  }

  public boolean isWriteToFile() {
    return isWriteToFile;
  }

  public void setFallbackJob() {
    isFallbackJob = true;
  }

  public List<String> getValidSegmentIds() {
    List<String> validSegments = new ArrayList<>();
    for (Segment segment : this.validSegments) {
      validSegments.add(segment.getSegmentNo());
    }
    return validSegments;
  }

  public List<Segment> getValidSegments() {
    return validSegments;
  }

  public void setValidSegments(List<Segment> validSegments) {
    this.validSegments = validSegments;
  }

  public void createIndexChooser() throws IOException {
    if (null != filterResolverIntf) {
      this.indexChooser = new IndexChooser(table);
    }
  }

  public void setCountStarJob() {
    this.isCountStarJob = true;
  }

  public boolean isCountStarJob() {
    return this.isCountStarJob;
  }

  public List<PartitionSpec> getPartitions() {
    return partitions;
  }

  public ReadCommittedScope getReadCommittedScope() {
    return readCommittedScope;
  }

  public CarbonSIExpression getIndexTablesScanTree() {
    return indexTablesScanTree;
  }

  public void setIndexTablesScanTree(CarbonSIExpression indexTablesScanTree) {
    this.indexTablesScanTree = indexTablesScanTree;
  }
}

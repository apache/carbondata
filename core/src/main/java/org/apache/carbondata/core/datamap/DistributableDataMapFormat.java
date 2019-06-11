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
package org.apache.carbondata.core.datamap;

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
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datastore.impl.FileFactory;
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
 * Input format for datamaps, it makes the datamap pruning distributable.
 */
public class DistributableDataMapFormat extends FileInputFormat<Void, ExtendedBlocklet>
    implements Serializable, Writable {

  private static final transient Logger LOGGER =
      LogServiceFactory.getLogService(DistributableDataMapFormat.class.getName());

  private static final long serialVersionUID = 9189779090091151248L;

  private CarbonTable table;

  private FilterResolverIntf filterResolverIntf;

  private List<Segment> validSegments;

  private List<String> invalidSegments;

  private List<PartitionSpec> partitions;

  private boolean isJobToClearDataMaps = false;

  private DataMapLevel dataMapLevel;

  private boolean isFallbackJob = false;

  private String dataMapToClear = "";

  private ReadCommittedScope readCommittedScope;

  private String taskGroupId = "";

  private String taskGroupDesc = "";

  private String queryId = UUID.randomUUID().toString();

  private transient DataMapChooser dataMapChooser;

  private boolean isWriteToFile = true;

  DistributableDataMapFormat() {

  }

  DistributableDataMapFormat(CarbonTable table,
      List<Segment> validSegments, List<String> invalidSegments, boolean isJobToClearDataMaps,
      String dataMapToClear) throws IOException {
    this(table, null, validSegments, invalidSegments, null,
        isJobToClearDataMaps, null, false);
    this.dataMapToClear = dataMapToClear;
  }

  DistributableDataMapFormat(CarbonTable table, FilterResolverIntf filterResolverIntf,
      List<Segment> validSegments, List<String> invalidSegments, List<PartitionSpec> partitions,
      boolean isJobToClearDataMaps, DataMapLevel dataMapLevel, boolean isFallbackJob)
      throws IOException {
    this.table = table;
    this.filterResolverIntf = filterResolverIntf;
    this.validSegments = validSegments;
    if (!validSegments.isEmpty()) {
      this.readCommittedScope = validSegments.get(0).getReadCommittedScope();
    }
    this.invalidSegments = invalidSegments;
    this.partitions = partitions;
    this.isJobToClearDataMaps = isJobToClearDataMaps;
    this.dataMapLevel = dataMapLevel;
    this.isFallbackJob = isFallbackJob;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<DataMapDistributableWrapper> distributables;
    distributables =
        DataMapChooser.getDefaultDataMap(table, filterResolverIntf).toDistributable(validSegments);
    List<InputSplit> inputSplits = new ArrayList<>(distributables.size());
    inputSplits.addAll(distributables);
    return inputSplits;
  }

  @Override
  public RecordReader<Void, ExtendedBlocklet> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RecordReader<Void, ExtendedBlocklet>() {
      private Iterator<ExtendedBlocklet> blockletIterator;
      private ExtendedBlocklet currBlocklet;
      private List<DataMap> dataMaps;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        DataMapDistributableWrapper distributable = (DataMapDistributableWrapper) inputSplit;
        distributable.getDistributable().getSegment().setReadCommittedScope(readCommittedScope);
        List<Segment> segmentsToLoad = new ArrayList<>();
        segmentsToLoad.add(distributable.getDistributable().getSegment());
        List<ExtendedBlocklet> blocklets = new ArrayList<>();
        if (dataMapLevel == null) {
          TableDataMap defaultDataMap = DataMapStoreManager.getInstance()
              .getDataMap(table, distributable.getDistributable().getDataMapSchema());
          dataMaps = defaultDataMap.getTableDataMaps(distributable.getDistributable());
          blocklets = defaultDataMap
              .prune(segmentsToLoad, new DataMapFilter(filterResolverIntf), partitions);
          blocklets = DataMapUtil
              .pruneDataMaps(table, filterResolverIntf, segmentsToLoad, partitions, blocklets,
                  dataMapChooser);
        } else {
          blocklets = DataMapUtil
              .pruneDataMaps(table, filterResolverIntf, segmentsToLoad, partitions, blocklets,
                  dataMapLevel, dataMapChooser);
        }
        blockletIterator = blocklets.iterator();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
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
      public Void getCurrentKey() throws IOException, InterruptedException {
        return null;
      }

      @Override
      public ExtendedBlocklet getCurrentValue() throws IOException, InterruptedException {
        return currBlocklet;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        if (null != dataMaps) {
          for (DataMap dataMap : dataMaps) {
            dataMap.finish();
          }
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
    out.writeBoolean(isJobToClearDataMaps);
    out.writeBoolean(isFallbackJob);
    if (dataMapLevel == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeUTF(dataMapLevel.name());
    }
    out.writeInt(validSegments.size());
    for (Segment segment : validSegments) {
      segment.write(out);
    }
    if (partitions == null) {
      out.writeBoolean(false);
    } else {
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
    out.writeUTF(dataMapToClear);
    out.writeUTF(taskGroupId);
    out.writeUTF(taskGroupDesc);
    out.writeUTF(queryId);
    out.writeBoolean(isWriteToFile);
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
    this.isJobToClearDataMaps = in.readBoolean();
    this.isFallbackJob = in.readBoolean();
    if (in.readBoolean()) {
      this.dataMapLevel = DataMapLevel.valueOf(in.readUTF());
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
    this.dataMapToClear = in.readUTF();
    this.taskGroupId = in.readUTF();
    this.taskGroupDesc = in.readUTF();
    this.queryId = in.readUTF();
    this.isWriteToFile = in.readBoolean();
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
   * @return Whether the job is to clear cached datamaps or not.
   */
  public boolean isJobToClearDataMaps() {
    return isJobToClearDataMaps;
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
    int maxJobLenth;
    try {
      String maxJobLenthString = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH ,
                      CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH_DEFAULT);
      maxJobLenth = Integer.parseInt(maxJobLenthString);
    } catch (Exception e) {
      String maxJobLenthString = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_JOBNAME_LENGTH_DEFAULT);
      maxJobLenth = Integer.parseInt(maxJobLenthString);
    }
    if (taskGroupDesc.length() > maxJobLenth) {
      this.taskGroupDesc = taskGroupDesc.substring(0, maxJobLenth);
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

  public String getDataMapToClear() {
    return dataMapToClear;
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

  public void createDataMapChooser() throws IOException {
    if (null != filterResolverIntf) {
      this.dataMapChooser = new DataMapChooser(table);
    }
  }
}

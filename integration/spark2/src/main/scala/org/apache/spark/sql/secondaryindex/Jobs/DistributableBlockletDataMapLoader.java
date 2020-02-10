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

package org.apache.spark.sql.secondaryindex.Jobs;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

/**
 * class to load blocklet data map
 */
public class DistributableBlockletDataMapLoader
    extends FileInputFormat<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>
    implements Serializable {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DistributableBlockletDataMapLoader.class.getName());

  private static final long serialVersionUID = 1;

  private CarbonTable table;

  private transient DataMapExprWrapper dataMapExprWrapper;

  private transient List<Segment> validSegments;

  private transient Set<String> keys;

  private ReadCommittedScope readCommittedScope;

  public DistributableBlockletDataMapLoader(CarbonTable table,
      DataMapExprWrapper dataMapExprWrapper, List<Segment> validSegments,
      List<Segment> invalidSegments, List<PartitionSpec> partitions, boolean isJobToClearDataMaps) {
    this.table = table;
    this.dataMapExprWrapper = dataMapExprWrapper;
    this.validSegments = validSegments;
  }

  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    DataMapFactory dataMapFactory =
        DataMapStoreManager.getInstance().getDefaultDataMap(table).getDataMapFactory();
    CacheableDataMap factory = (CacheableDataMap) dataMapFactory;
    List<DataMapDistributable> validDistributables =
        factory.getAllUncachedDistributables(validSegments, dataMapExprWrapper);
    if (!validSegments.isEmpty()) {
      this.readCommittedScope = validSegments.get(0).getReadCommittedScope();
    }
    CarbonBlockLoaderHelper instance = CarbonBlockLoaderHelper.getInstance();
    int distributableSize = validDistributables.size();
    List<InputSplit> inputSplits = new ArrayList<>(distributableSize);
    keys = new HashSet<>();
    Iterator<DataMapDistributable> iterator = validDistributables.iterator();
    while (iterator.hasNext()) {
      BlockletDataMapDistributable next = (BlockletDataMapDistributable) iterator.next();
      String key = next.getSegmentPath();
      if (instance.checkAlreadySubmittedBlock(table.getAbsoluteTableIdentifier(), key)) {
        inputSplits.add(next);
        keys.add(key);
      }
    }
    int sizeOfDistToBeLoaded = inputSplits.size();
    LOGGER.info("Submitted blocks " + sizeOfDistToBeLoaded + ", " + distributableSize
        + " . Rest already considered for load in other job.");
    return inputSplits;
  }

  @Override
  public RecordReader<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>
  createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new RecordReader<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>() {
      private BlockletDataMapIndexWrapper wrapper = null;
      private TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier = null;
      private TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper;
      Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper> cache =
          CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
      private Iterator<TableBlockIndexUniqueIdentifier> iterator;

      @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        BlockletDataMapDistributable segmentDistributable =
            (BlockletDataMapDistributable) inputSplit;
        TableBlockIndexUniqueIdentifier tableSegmentUniqueIdentifier =
            segmentDistributable.getTableBlockIndexUniqueIdentifier();
        Segment segment =
            Segment.toSegment(tableSegmentUniqueIdentifier.getSegmentId(), readCommittedScope);
        iterator =
            BlockletDataMapUtil.getTableBlockUniqueIdentifiers(segment).iterator();
      }

      @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator.hasNext()) {
          TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier = iterator.next();
          this.tableBlockIndexUniqueIdentifier  = tableBlockIndexUniqueIdentifier;
          TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper =
              new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier, table,
                  false, true, true);
          this.tableBlockIndexUniqueIdentifierWrapper = tableBlockIndexUniqueIdentifierWrapper;
          wrapper = cache.get(tableBlockIndexUniqueIdentifierWrapper);
          return true;
        }
        return false;
      }

      @Override public TableBlockIndexUniqueIdentifier getCurrentKey() {
        return tableBlockIndexUniqueIdentifier;
      }

      @Override public BlockletDataMapDetailsWithSchema getCurrentValue() {
        BlockletDataMapDetailsWithSchema blockletDataMapDetailsWithSchema =
            new BlockletDataMapDetailsWithSchema(wrapper, table.getTableInfo().isSchemaModified());
        return blockletDataMapDetailsWithSchema;
      }

      @Override public float getProgress() {
        return 0;
      }

      @Override public void close() {
        if (null != tableBlockIndexUniqueIdentifierWrapper) {
          if (null != wrapper && null != wrapper.getDataMaps() && !wrapper.getDataMaps()
              .isEmpty()) {
            String segmentId =
                tableBlockIndexUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier()
                    .getSegmentId();
            // as segmentId will be same for all the dataMaps and segmentProperties cache is
            // maintained at segment level so it need to be called only once for clearing
            SegmentPropertiesAndSchemaHolder.getInstance()
                .invalidate(segmentId, wrapper.getDataMaps().get(0).getSegmentPropertiesWrapper(),
                    tableBlockIndexUniqueIdentifierWrapper.isAddTableBlockToUnsafeAndLRUCache());
          }
        }
      }

    };
  }

  public void invalidate() {
    if (null != keys) {
      CarbonBlockLoaderHelper instance = CarbonBlockLoaderHelper.getInstance();
      instance.clear(table.getAbsoluteTableIdentifier(), keys);
    }
  }
}

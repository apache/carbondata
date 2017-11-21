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
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input format for datamaps, it makes the datamap pruning distributable.
 */
public class DistributableDataMapFormat extends FileInputFormat<Void, ExtendedBlocklet> implements
    Serializable {

  private AbsoluteTableIdentifier identifier;

  private DataMapExprWrapper dataMapExprWrapper;

  private List<String> validSegments;

  private String className;

  private List<String> partitions;

  public DistributableDataMapFormat(AbsoluteTableIdentifier identifier,
      DataMapExprWrapper dataMapExprWrapper, List<String> validSegments, List<String> partitions, String className) {
    this.identifier = identifier;
    this.dataMapExprWrapper = dataMapExprWrapper;
    this.validSegments = validSegments;
    this.className = className;
    this.partitions = partitions;
  }

  public static void setFilterExp(Configuration configuration, FilterResolverIntf filterExp)
      throws IOException {
    if (filterExp != null) {
      String string = ObjectSerializationUtil.convertObjectToString(filterExp);
      configuration.set(FILTER_EXP, string);
    }
  }

  private static FilterResolverIntf getFilterExp(Configuration configuration) throws IOException {
    String filterString = configuration.get(FILTER_EXP);
    if (filterString != null) {
      Object toObject = ObjectSerializationUtil.convertStringToObject(filterString);
      return (FilterResolverIntf) toObject;
    }
    return null;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<DataMapDistributableWrapper> distributables =
        dataMapExprWrapper.toDistributable(validSegments);
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

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        DataMapDistributableWrapper distributable = (DataMapDistributableWrapper) inputSplit;
        TableDataMap dataMap = DataMapStoreManager.getInstance()
            .getDataMap(identifier, distributable.getDistributable().getDataMapSchema());
        List<ExtendedBlocklet> blocklets = dataMap.prune(
            distributable, dataMapExprWrapper.getFilterResolverIntf(distributable.getUniqueId()), partitions);
        for (ExtendedBlocklet blocklet: blocklets) {
          blocklet.setDataMapUniqueId(distributable.getUniqueId());
        }
        blockletIterator = blocklets.iterator();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNext = blockletIterator.hasNext();
        if (hasNext) {
          currBlocklet = blockletIterator.next();
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

      }
    };
  }

}

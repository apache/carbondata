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
import java.util.LinkedList;
import java.util.List;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.internal.CarbonInputSplit;
import org.apache.carbondata.hadoop.internal.segment.Segment;
import org.apache.carbondata.hadoop.internal.segment.SegmentManager;
import org.apache.carbondata.hadoop.internal.segment.SegmentManagerFactory;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input format of CarbonData file.
 * @param <T>
 */
public class CarbonTableInputFormat<T> extends FileInputFormat<Void, T> {

  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";

  private SegmentManager segmentManager;

  public CarbonTableInputFormat() {
    this.segmentManager = SegmentManagerFactory.getGlobalSegmentManager();
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    switch (((CarbonInputSplit)split).formatType()) {
      case COLUMNAR:
        // TODO: create record reader for columnar format
        break;
      default:
        throw new RuntimeException("Unsupported format type");
    }
    return null;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    // work as following steps:
    // get all current valid segment
    // for each segment, get all input split

    List<InputSplit> output = new LinkedList<>();
    Expression filter = getFilter(job.getConfiguration());
    Segment[] segments = segmentManager.getAllValidSegments();
    FilterResolverIntf filterResolver = CarbonInputFormatUtil.resolveFilter(filter, null);
    for (Segment segment: segments) {
      List<InputSplit> splits = segment.getSplits(job, filterResolver);
      output.addAll(splits);
    }
    return output;
  }

  /**
   * set the table path into configuration
   * @param conf configuration of the job
   * @param tablePath table path string
   */
  public void setTablePath(Configuration conf, String tablePath) {

  }

  /**
   * return the table path in the configuration
   * @param conf configuration of the job
   * @return table path string
   */
  public String getTablePath(Configuration conf) {
    return null;
  }

  /**
   * set projection columns into configuration
   * @param conf configuration of the job
   * @param projection projection
   */
  public void setProjection(Configuration conf, CarbonProjection projection) {

  }

  /**
   * return the projection in the configuration
   * @param conf configuration of the job
   * @return projection
   */
  public CarbonProjection getProjection(Configuration conf) {
    return null;
  }

  /**
   * set filter expression into the configuration
   * @param conf configuration of the job
   * @param filter filter expression
   */
  public void setFilter(Configuration conf, Expression filter) {
    try {
      String filterString = ObjectSerializationUtil.convertObjectToString(filter);
      conf.set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException("Error while setting filter expression to Job", e);
    }
  }

  /**
   * return filter expression in the configuration
   * @param conf configuration of the job
   * @return filter expression
   */
  public Expression getFilter(Configuration conf) {
    Object filter;
    String filterExprString = conf.get(FILTER_PREDICATE);
    if (filterExprString == null) {
      return null;
    }
    try {
      filter = ObjectSerializationUtil.convertStringToObject(filterExprString);
    } catch (IOException e) {
      throw new RuntimeException("Error while reading filter expression", e);
    }
    assert(filter instanceof Expression);
    return (Expression) filter;
  }

  /**
   * Optional API. It can be used by query optimizer to select index based on filter
   * in the configuration of the job. After selecting index internally, index' name will be set
   * in the configuration.
   *
   * The process of selection is simple, just use the default index. Subclass can provide a more
   * advanced selection logic like cost based.
   * @param conf job configuration
   */
  public void selectIndex(Configuration conf) {
    // set the default index in configuration
  }
}

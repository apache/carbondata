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
package org.apache.carbondata.hadoop;

import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;

import org.apache.hadoop.mapreduce.RecordReader;

/**
 * This class will have all the common methods for vector and row based reader
 */
public abstract class AbstractRecordReader<T> extends RecordReader<Void, T> {

  protected int rowCount = 0;

  /**
   * This method will log query result count and querytime
   * @param recordCount
   * @param recorder
   */
  public void logStatistics(int recordCount, QueryStatisticsRecorder recorder) {
    // result size
    QueryStatistic queryStatistic = new QueryStatistic();
    queryStatistic.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, recordCount);
    recorder.recordStatistics(queryStatistic);
  }
}

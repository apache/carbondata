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

package org.apache.carbondata.hadoop.streaming;

import java.io.IOException;

import org.apache.carbondata.core.streaming.CarbonStreamingConstants;
import org.apache.carbondata.processing.csvload.CSVInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Output format to write streaming data to carbondata file
 *
 * @param <V> - type of record
 */
public class CarbonStreamingOutputFormat<K, V> extends FileOutputFormat<K, V> {

  public static long getBlockSize(Configuration conf) {
    return conf.getLong("dfs.block.size",
            CarbonStreamingConstants.DEFAULT_CARBON_STREAM_FILE_BLOCK_SIZE);
  }

  public static void setBlockSize(Configuration conf, long blockSize) {
    conf.setLong("dfs.block.size", blockSize);
  }

  /**
   * When getRecordWriter may need to override
   * to provide correct path including streaming segment name
   */
  @Override
  public CarbonStreamingRecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
          throws IOException, InterruptedException {

    Configuration conf = job.getConfiguration();

    String keyValueSeparator = conf.get(
            CSVInputFormat.DELIMITER,
            CSVInputFormat.DELIMITER_DEFAULT);

    return new CarbonStreamingRecordWriter<K, V>(
            conf,
            getDefaultWorkFile(job, null),
            keyValueSeparator);
  }

}

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

package org.apache.carbondata.streaming;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Stream output format
 */
public class CarbonStreamOutputFormat extends FileOutputFormat<Void, Object> {

  static final byte[] CARBON_SYNC_MARKER =
      "@carbondata_sync".getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));

  public static final String CARBON_ENCODER_ROW_BUFFER_SIZE = "carbon.stream.row.buffer.size";

  public static final int CARBON_ENCODER_ROW_BUFFER_SIZE_DEFAULT = 1024;

  public static final String CARBON_STREAM_BLOCKLET_ROW_NUMS = "carbon.stream.blocklet.row.nums";

  public static final int CARBON_STREAM_BLOCKLET_ROW_NUMS_DEFAULT = 32000;

  public static final String CARBON_STREAM_CACHE_SIZE = "carbon.stream.cache.size";

  public static final int CARBON_STREAM_CACHE_SIZE_DEFAULT = 32 * 1024 * 1024;

  private static final String LOAD_Model = "mapreduce.output.carbon.load.model";

  private static final String SEGMENT_ID = "carbon.segment.id";

  @Override
  public RecordWriter<Void, Object> getRecordWriter(TaskAttemptContext job)
      throws IOException {
    return new CarbonStreamRecordWriter(job);
  }

  public static void setCarbonLoadModel(Configuration hadoopConf, CarbonLoadModel carbonLoadModel)
      throws IOException {
    if (carbonLoadModel != null) {
      hadoopConf.set(LOAD_Model, ObjectSerializationUtil.convertObjectToString(carbonLoadModel));
    }
  }

  public static CarbonLoadModel getCarbonLoadModel(Configuration hadoopConf) throws IOException {
    String value = hadoopConf.get(LOAD_Model);
    if (value == null) {
      return null;
    } else {
      return (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(value);
    }
  }

  public static void setSegmentId(Configuration hadoopConf, String segmentId) {
    if (segmentId != null) {
      hadoopConf.set(SEGMENT_ID, segmentId);
    }
  }

  public static String getSegmentId(Configuration hadoopConf) {
    return hadoopConf.get(SEGMENT_ID);
  }

}

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

package org.apache.carbondata.store;

import java.io.IOException;

import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.store.api.CarbonFileWriter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

class CarbonFileWriterImpl implements CarbonFileWriter {

  private RecordWriter<NullWritable, StringArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private StringArrayWritable writable;

  CarbonFileWriterImpl(CarbonTableOutputFormat format, TaskAttemptContext context)
      throws IOException {
    this.recordWriter = format.getRecordWriter(context);
    this.context = context;
    this.writable = new StringArrayWritable();
  }

  @Override
  public void writeRow(String[] fields) throws IOException {
    writable.set(fields);
    try {
      recordWriter.write(NullWritable.get(), writable);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      recordWriter.close(context);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}

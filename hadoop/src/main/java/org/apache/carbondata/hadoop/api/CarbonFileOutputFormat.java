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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.processing.loading.DataLoadExecutor;
import org.apache.carbondata.processing.loading.csvinput.StringArrayWritable;
import org.apache.carbondata.processing.loading.iterator.CarbonOutputIteratorWrapper;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarbonFileOutputFormat extends FileOutputFormat<NullWritable, StringArrayWritable> {

  private static final String WRITE_TEMP_FILE = "mapreduce.carbonfile.tempfile.pathname";

  public static void setWriteTempFile(TaskAttemptContext context, String writTempFile) {
    if (writTempFile != null) {
      context.getConfiguration().set(WRITE_TEMP_FILE, writTempFile);
    }
  }

  public static String getWriteTempFile(TaskAttemptContext context) {
    return context.getConfiguration().get(WRITE_TEMP_FILE);
  }

  @Override
  public RecordWriter<NullWritable, StringArrayWritable> getRecordWriter(TaskAttemptContext job)
      throws IOException {
    final CarbonLoadModel loadModel = CarbonTableOutputFormat.getLoadModel(job.getConfiguration());
    loadModel.setWriteTempPath(new String[]{CarbonFileOutputFormat.getWriteTempFile(job)});
    loadModel.setFileLevelLoad(true);
    loadModel.setTaskNo(System.nanoTime() + "");
    final CarbonOutputIteratorWrapper iteratorWrapper = new CarbonOutputIteratorWrapper();
    final DataLoadExecutor dataLoadExecutor = DataLoadExecutor.newInstance(loadModel);
    ExecutorService executorService = Executors.newFixedThreadPool(
        1,
        new CarbonThreadFactory("CarbonFileOutputFormat::CarbonRecordWriter"));
    // It should be started in new thread as the underlying iterator uses blocking queue.
    Future future = executorService.submit(new Thread() {
      @Override public void run() {
        try {
          dataLoadExecutor.execute(new CarbonIterator[] { iteratorWrapper });
        } catch (Exception e) {
          dataLoadExecutor.close();
          throw new RuntimeException(e);
        }
      }
    });

    return new CarbonTableOutputFormat.CarbonRecordWriter(
        iteratorWrapper, dataLoadExecutor, loadModel, future, executorService);
  }
}

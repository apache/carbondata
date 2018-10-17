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

package org.apache.carbondata.processing.loading.csvinput;

import java.io.IOException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import com.univocity.parsers.common.TextParsingException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * It is wrapper iterator around @{@link RecordReader}.
 */
public class CSVRecordReaderIterator extends CarbonIterator<Object []> {

  private RecordReader<NullWritable, StringArrayWritable> recordReader;

  /**
   * It is just a little hack to make recordreader as iterator. Usually we cannot call hasNext
   * multiple times on record reader as it moves another line. To avoid that situation like hasNext
   * only tells whether next row is present or not and next will move the pointer to next row after
   * consuming it.
   */
  private boolean isConsumed;

  private InputSplit split;

  private TaskAttemptContext context;

  public CSVRecordReaderIterator(RecordReader<NullWritable, StringArrayWritable> recordReader,
      InputSplit split, TaskAttemptContext context) {
    this.recordReader = recordReader;
    this.split = split;
    this.context = context;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!isConsumed) {
        isConsumed = recordReader.nextKeyValue();
        return isConsumed;
      }
      return true;
    } catch (Exception e) {
      if (e instanceof TextParsingException) {
        throw new CarbonDataLoadingException(
            CarbonDataProcessorUtil.trimErrorMessage(e.getMessage()));
      }
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override
  public Object[] next() {
    try {
      String[] data = recordReader.getCurrentValue().get();
      isConsumed = false;
      return data;
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  @Override
  public void initialize() {
    try {
      recordReader.initialize(split, context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      recordReader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

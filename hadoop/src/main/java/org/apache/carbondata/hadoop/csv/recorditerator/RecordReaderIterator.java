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
package org.apache.carbondata.hadoop.csv.recorditerator;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.hadoop.io.StringArrayWritable;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * It is wrapper iterator around @{@link RecordReader}.
 */
public class RecordReaderIterator extends CarbonIterator<Object []> {

  private RecordReader<NullWritable, StringArrayWritable> recordReader;

  /**
   * It is just a little hack to make recordreader as iterator. Usually we cannot call hasNext
   * multiple times on record reader as it moves another line. To avoid that situation like hasNext
   * only tells whether next row is present or not and next will move the pointer to next row after
   * consuming it.
   */
  private boolean isConsumed;

  public RecordReaderIterator(RecordReader<NullWritable, StringArrayWritable> recordReader) {
    this.recordReader = recordReader;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!isConsumed) {
        isConsumed = recordReader.nextKeyValue();
        return isConsumed;
      }
      return isConsumed;
    } catch (Exception e) {
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
}

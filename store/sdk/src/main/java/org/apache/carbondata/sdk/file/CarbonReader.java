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

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.RecordReader;

public class CarbonReader<T> {

  private List<RecordReader<Void, T>> readers;

  private RecordReader<Void, T> currentReader;

  private int index;

  CarbonReader(List<RecordReader<Void, T>> readers) {
    if (readers.size() == 0) {
      throw new IllegalArgumentException("no reader");
    }
    this.readers = readers;
    this.index = 0;
    this.currentReader = readers.get(0);
  }

  public boolean hasNext() throws IOException, InterruptedException {
    if (currentReader.nextKeyValue()) {
      return true;
    } else {
      if (index == readers.size() - 1) {
        // no more readers
        return false;
      } else {
        index++;
        currentReader = readers.get(index);
        return currentReader.nextKeyValue();
      }
    }
  }

  public T readNextRow() throws IOException, InterruptedException {
    return currentReader.getCurrentValue();
  }

  public static CarbonReaderBuilder builder(String tablePath) {
    return new CarbonReaderBuilder(tablePath);
  }
}

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.util.CarbonTaskInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

import org.apache.hadoop.mapreduce.RecordReader;


/**
 * Reader for carbondata file
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class CarbonReader<T> {

  private List<RecordReader<Void, T>> readers;

  private RecordReader<Void, T> currentReader;

  private int index;

  private boolean initialise;

  /**
   * Call {@link #builder(String)} to construct an instance
   */
  CarbonReader(List<RecordReader<Void, T>> readers) {
    if (readers.size() == 0) {
      throw new IllegalArgumentException("no reader");
    }
    this.initialise = true;
    this.readers = readers;
    this.index = 0;
    this.currentReader = readers.get(0);
    CarbonTaskInfo carbonTaskInfo = new CarbonTaskInfo();
    carbonTaskInfo.setTaskId(CarbonUtil.generateUUID());
    ThreadLocalTaskInfo.setCarbonTaskInfo(carbonTaskInfo);
  }

  /**
   * Return true if has next row
   */
  public boolean hasNext() throws IOException, InterruptedException {
    validateReader();
    if (currentReader.nextKeyValue()) {
      return true;
    } else {
      if (index == readers.size() - 1) {
        // no more readers
        return false;
      } else {
        index++;
        // current reader is closed
        currentReader.close();
        currentReader = readers.get(index);
        return currentReader.nextKeyValue();
      }
    }
  }

  /**
   * Read and return next row object
   */
  public T readNextRow() throws IOException, InterruptedException {
    validateReader();
    return currentReader.getCurrentValue();
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   *
   * @param tablePath table store path
   * @param tableName table name
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(String tablePath, String tableName) {
    return new CarbonReaderBuilder(tablePath, tableName);
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   * Default value of table name is table + tablePath + time
   *
   * @param tablePath table path
   * @return CarbonReaderBuilder object
   */
  public static CarbonReaderBuilder builder(String tablePath) {
    UUID uuid = UUID.randomUUID();
    String tableName = "UnknownTable" + uuid;
    return builder(tablePath, tableName);
  }

  /**
   * Return a new list of {@link CarbonReader} objects
   *
   * @param maxSplits
   */
  public List<CarbonReader> split(int maxSplits) throws IOException {
    validateReader();
    if (maxSplits < 1) {
      throw new RuntimeException(
          this.getClass().getSimpleName() + ".split: maxSplits must be positive");
    }

    List<CarbonReader> carbonReaders = new ArrayList<>();

    // If maxSplits < readers.size
    // Split the reader into maxSplits splits with each
    // element contains >= 1 CarbonRecordReader objects
    if (maxSplits < this.readers.size()) {
      for (int i = 0; i < maxSplits; ++i) {
        carbonReaders.add(new CarbonReader<>(this.readers
            .subList((int) Math.ceil((float) (i * this.readers.size()) / maxSplits),
                (int) Math.ceil((float) ((i + 1) * this.readers.size()) / maxSplits))));
      }
    }
    // If maxSplits >= readers.size
    // Split the reader into reader.size splits with each
    // element contains exactly 1 CarbonRecordReader object
    else {
      for (int i = 0; i < this.readers.size(); ++i) {
        carbonReaders.add(new CarbonReader<>(this.readers.subList(i, i + 1)));
      }
    }

    this.initialise = false;
    return carbonReaders;
  }

  /**
   * Close reader
   *
   * @throws IOException
   */
  public void close() throws IOException {
    validateReader();
    this.currentReader.close();
    this.initialise = false;
  }

  /**
   * Validate the reader
   */
  private void validateReader() {
    if (!this.initialise) {
      throw new RuntimeException(this.getClass().getSimpleName() +
          " not initialise, please create it first.");
    }
  }
}

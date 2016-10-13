/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.processing.newflow.iterators;

import java.io.IOException;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.mapred.RecordReader;

/**
 * This iterator iterates RecordReader.
 */
public class RecordReaderIterator extends CarbonIterator<Object[]> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RecordReaderIterator.class.getName());

  private RecordReader<Void, CarbonArrayWritable> recordReader;

  private CarbonArrayWritable data = new CarbonArrayWritable();

  public RecordReaderIterator(RecordReader<Void, CarbonArrayWritable> recordReader) {
    this.recordReader = recordReader;
  }

  @Override public boolean hasNext() {
    try {
      return recordReader.next(null, data);
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return false;
  }

  @Override public Object[] next() {
    return data.get();
  }

}

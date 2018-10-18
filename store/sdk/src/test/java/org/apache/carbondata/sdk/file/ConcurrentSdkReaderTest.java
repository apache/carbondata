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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.core.metadata.datatype.DataTypes;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOExceptionWithCause;
import org.junit.*;

/**
 * multi-thread Test suite for {@link CarbonReader}
 */
public class ConcurrentSdkReaderTest extends TestCase {

  private static final String dataDir = "./testReadFiles";

  public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void writeTestData(long numRows, int segmentSize) {
    cleanTestData();

    Field[] fields = new Field[2];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("intField", DataTypes.INT);

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("table_blocksize", Integer.toString(segmentSize));

    CarbonWriterBuilder builder =
        CarbonWriter.builder().outputPath(dataDir).withTableProperties(tableProperties)
            .withCsvInput(new Schema(fields));

    try {
      CarbonWriter writer = builder.build();

      for (long i = 0; i < numRows; ++i) {
        writer.write(new String[] { "robot_" + i, String.valueOf(i) });
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test public void testReadParallely() throws IOException {
    long numRows = 10000000;
    int segmentSize = 10;
    short numThreads = 4;
    writeTestData(numRows, segmentSize);

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    long count;

    try {
      CarbonReader reader = CarbonReader.builder(dataDir).build();
      count = 0;
      long start = System.currentTimeMillis();
      while (reader.hasNext()) {
        reader.readNextRow();
        count += 1;
      }
      long end = System.currentTimeMillis();
      System.out.println("[Sequential read] Time:" + (end - start));
      Assert.assertEquals(numRows, count);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    try {
      CarbonReader reader = CarbonReader.builder(dataDir).build();
      List<CarbonReader> multipleReaders = reader.split(numThreads);
      List<Future> results = new ArrayList<>();
      count = 0;
      long start = System.currentTimeMillis();
      for (CarbonReader reader_i : multipleReaders) {
        results.add(executorService.submit(new ReadLogic(reader_i)));
      }
      for (Future result_i : results) {
        count += (long) result_i.get();
      }
      long end = System.currentTimeMillis();
      System.out.println("[Parallel read] Time:" + (end - start));
      Assert.assertEquals(numRows, count);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    cleanTestData();
  }

  class ReadLogic implements Callable<Long> {
    CarbonReader reader;

    ReadLogic(CarbonReader reader) {
      this.reader = reader;
    }

    @Override public Long call() throws IOException {
      long count = 0;
      try {
        while (reader.hasNext()) {
          reader.readNextRow();
          count += 1;
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
      return count;
    }
  }

}
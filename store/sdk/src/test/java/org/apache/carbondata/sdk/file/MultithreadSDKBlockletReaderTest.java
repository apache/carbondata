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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * multi-thread Test suite for {@link CarbonReader}
 */
public class MultithreadSDKBlockletReaderTest {

  private static final String dataDir = "./testReadFiles";

  @Before @After public void cleanTestData() {
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  private void writeDataMultipleFiles(int numFiles, long numRowsPerFile) {
    Field[] fields = new Field[2];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("intField", DataTypes.INT);

    for (int numFile = 0; numFile < numFiles; ++numFile) {
      CarbonWriterBuilder builder =
          CarbonWriter.builder().outputPath(dataDir).withCsvInput(new Schema(fields))
              .withBlockletSize(1).withBlockletSize(2).writtenBy("ConcurrentSdkReaderTest");
      try {
        CarbonWriter writer = builder.build();

        for (long i = 0; i < numRowsPerFile; ++i) {
          writer.write(new String[] { "robot_" + i, String.valueOf(i) });
        }
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test public void testReadParallely() throws IOException, InterruptedException {
    int numFiles = 2;
    int numRowsPerFile = 1000000;
    short numThreads;
    writeDataMultipleFiles(numFiles, numRowsPerFile);
    long count;

    InputSplit[] splits = CarbonReader.builder(dataDir).getSplits(true);
    Assert.assertEquals(splits.length, 8);
    numThreads = (short) splits.length;
    // Concurrent Reading
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    try {
      try {
        List<ReadLogic> tasks = new ArrayList<>();
        List<Future<Long>> results;
        count = 0;
        for (InputSplit split : splits) {
          tasks.add(new ReadLogic(split));
        }
        long start = System.currentTimeMillis();
        results = executorService.invokeAll(tasks);
        for (Future result_i : results) {
          count += (long) result_i.get();
        }
        long end = System.currentTimeMillis();
        System.out.println("[Parallel read] Time: " + (end - start) + " ms");
        Assert.assertEquals(numFiles * numRowsPerFile, count);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
  }

  class ReadLogic implements Callable<Long> {
    InputSplit split;

    ReadLogic(InputSplit split) {
      this.split = split;
    }

    @Override
    public Long call() throws IOException, InterruptedException {
      long count = 0;
      CarbonReader reader = CarbonReader.builder(split).build();
      try {
        while (reader.hasNext()) {
          reader.readNextRow();
          count += 1;
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      } finally {
        reader.close();
      }
      return count;
    }
  }

}
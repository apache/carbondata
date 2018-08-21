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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * multi-thread Test suite for {@link CSVCarbonWriter}
 */
public class ConcurrentSdkWriterTest {

  @Test public void testWriteFiles() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    Field[] fields = new Field[2];
    fields[0] = new Field("name", DataTypes.STRING);
    fields[1] = new Field("age", DataTypes.INT);

    int poolSize = 6;
    ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
    try {
      CarbonWriterBuilder builder =
          CarbonWriter.builder().outputPath(path).uniqueIdentifier(555).taskNo(123);
      CarbonWriter writer = builder.buildWriterForCSVInput(new Schema(fields));
      // write in multi-thread
      for (int i = 0; i < poolSize; i++) {
        executorService.submit(new writeLogic(writer));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.HOURS);
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // read the files and verify the count
    CarbonReader reader;
    try {
      reader =
          CarbonReader.builder(path, "_temp").projection(new String[] { "name", "age" }).build();
      int i = 0;
      while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        i++;
      }
      // count should be 60 records
      Assert.assertEquals(i, 60);
      reader.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    FileUtils.deleteDirectory(new File(path));
  }

  class writeLogic implements Runnable {
    CarbonWriter writer;

    writeLogic(CarbonWriter writer) {
      this.writer = writer;
    }

    @Override public void run() {

      try {
        for (int i = 0; i < 10; i++) {
          writer.write(new String[] { "robot" + (i % 10), String.valueOf(i),
              String.valueOf((double) i / 2) });
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}

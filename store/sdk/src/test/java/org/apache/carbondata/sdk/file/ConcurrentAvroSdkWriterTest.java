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

import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * multi-thread Test suite for {@link CSVCarbonWriter}
 */
public class ConcurrentAvroSdkWriterTest {

  private static final int recordsPerItr = 10;
  private static final short numOfThreads = 4;

  @Test public void testWriteFiles() throws IOException {
    String path = "./testWriteFiles";
    FileUtils.deleteDirectory(new File(path));

    String mySchema =
        "{" + "  \"name\": \"address\", " + "   \"type\": \"record\", " + "    \"fields\": [  "
            + "  { \"name\": \"name\", \"type\": \"string\"}, "
            + "  { \"name\": \"age\", \"type\": \"int\"}, " + "  { " + "    \"name\": \"address\", "
            + "      \"type\": { " + "    \"type\" : \"record\", "
            + "        \"name\" : \"my_address\", " + "        \"fields\" : [ "
            + "    {\"name\": \"street\", \"type\": \"string\"}, "
            + "    {\"name\": \"city\", \"type\": \"string\"} " + "  ]} " + "  } " + "] " + "}";

    String json =
        "{\"name\":\"bob\", \"age\":10, \"address\" : {\"street\":\"abc\", \"city\":\"bang\"}}";

    // conversion to GenericData.Record
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(mySchema);
    GenericData.Record record = TestUtil.jsonToAvro(json, mySchema);

    ExecutorService executorService = Executors.newFixedThreadPool(numOfThreads);
    try {
      CarbonWriterBuilder builder =
          CarbonWriter.builder().outputPath(path).withThreadSafe(numOfThreads);
      CarbonWriter writer = builder.withAvroInput(avroSchema).writtenBy("ConcurrentAvroSdkWriterTest").build();
      // write in multi-thread
      for (int i = 0; i < numOfThreads; i++) {
        executorService.submit(new WriteLogic(writer, record));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.HOURS);
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // read the files and verify the count
    CarbonReader reader;
    try {
      reader =
          CarbonReader.builder(path, "_temp2122").projection(new String[] { "name", "age" }).build();
      int i = 0;
      while (reader.hasNext()) {
        Object[] row = (Object[]) reader.readNextRow();
        i++;
      }
      Assert.assertEquals(i, numOfThreads * recordsPerItr);
      reader.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    FileUtils.deleteDirectory(new File(path));
  }

  class WriteLogic implements Runnable {
    CarbonWriter writer;
    GenericData.Record record;

    WriteLogic(CarbonWriter writer, GenericData.Record record) {
      this.writer = writer;
      this.record = record;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < recordsPerItr; i++) {
          writer.write(record);
        }
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    }
  }

}

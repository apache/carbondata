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

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.BATCH_SEPARATOR;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.ROW_SEPARATOR;

/**
 * CarbonReader test for big data
 */
public class CarbonReaderForBigDataTest {
  public static void main(String[] args) throws InterruptedException, InvalidLoadOptionException, IOException {
    System.out.println("start to read data");
    String path = "../../../../Downloads/carbon-data-big/dir2";
    if (args.length > 0) {
      path = args[0];
    }
    double num = 1000000000.0;
    String originPath = "../../../../Downloads/carbon-data";
    String newPath = "../../../../Downloads/carbon-data-big";
    boolean writeNewData = false;
    if (writeNewData) {
      extendData(originPath, newPath);
    }

    Configuration conf = new Configuration();
    if (args.length > 3) {
      conf.set("fs.s3a.access.key", args[1]);
      conf.set("fs.s3a.secret.key", args[2]);
      conf.set("fs.s3a.endpoint", args[3]);
    }
    int printNum = 1000000;
//    readNextRow(path, num, conf, printNum);
//    readNextStringRow(path, num, conf, printNum);
//    readNextBatchRow(path, num, conf, 100000, printNum, false);
    readNextBatchStringRow(path, num, conf, 100000, printNum, false);
  }

  public static void readNextRow(String path, double num, Configuration conf, int printNum) {
    System.out.println("readNextRow");
    try {
      // Read data
      long startTime = System.nanoTime();
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .build();

      long startReadTime = System.nanoTime();
      double buildTime = (startReadTime - startTime) / num;
      System.out.println("build time is " + buildTime);
      int i = 0;
      while (reader.hasNext()) {
        Object[] data = (Object[]) reader.readNextRow();
        for (int x = 0; x < data.length; x++) {
          Object column = data[x];
        }
        i++;
        if (i > 0 && i % printNum == 0) {
          Long point = System.nanoTime();
          System.out.print(i + ": time is " + ((point - startReadTime) / num)
              + " s, speed is " + (printNum / ((point - startReadTime) / num)) + " records/s \t");
          for (int j = 0; j < data.length; j++) {
            System.out.print(data[j] + "\t\t");
          }
          System.out.println();
          startReadTime = System.nanoTime();
        }
      }
      long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + buildTime
          + " s, \ttotal read time is " + ((endReadTime - startTime) / num - buildTime)
          + " s, \taverage speed is " + (i / ((endReadTime - startTime) / num - buildTime))
          + " records/s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public static void readNextStringRow(String path, double num, Configuration conf, int printNum) {
    System.out.println("readNextRow");
    try {
      // Read data
      long startTime = System.nanoTime();
      CarbonReader reader = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .build();

      long startReadTime = System.nanoTime();
      double buildTime = (startReadTime - startTime) / num;
      System.out.println("build time is " + buildTime);
      int i = 0;
      while (reader.hasNext()) {
        Object[] data = reader.readNextStringRow().split(ROW_SEPARATOR);
        for (int x = 0; x < data.length; x++) {
          Object column = data[x];
        }
        i++;
        if (i > 0 && i % printNum == 0) {
          Long point = System.nanoTime();
          System.out.print(i + ": time is " + ((point - startReadTime) / num)
              + " s, speed is " + (printNum / ((point - startReadTime) / num)) + " records/s \t");
          for (int j = 0; j < data.length; j++) {
            System.out.print(data[j] + "\t\t");
          }
          System.out.println();
          startReadTime = System.nanoTime();
        }
      }
      long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + buildTime
          + " s, \ttotal read time is " + ((endReadTime - startTime) / num - buildTime)
          + " s, \taverage speed is " + (i / ((endReadTime - startTime) / num - buildTime))
          + " records/s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  /**
   * read next batch row
   *
   * @param path     data path
   * @param num      number for time
   * @param conf     configuration
   * @param batch    batch size
   * @param printNum print number for each batch
   */
  public static void readNextBatchRow(String path, double num,
                                      Configuration conf, int batch, int printNum, boolean withRowRecordReader) {
    System.out.println("readNextBatchRow");
    try {
      // Read data
      Long startTime = System.nanoTime();
      CarbonReaderBuilder carbonReaderBuilder = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .withBatch(batch);

      CarbonReader reader;
      if (withRowRecordReader) {
        reader = carbonReaderBuilder.withRowRecordReader().build();
      } else {
        reader = carbonReaderBuilder.build();
      }

      long startReadTime = System.nanoTime();
      long readBatchTime = 0L;
      long hasNextTime = 0L;
      long readBatchTotalTime = 0L;
      long hasNextTotalTime = 0L;
      System.out.println("build time is " + (startReadTime - startTime) / num);
      int i = 0;
      long startHasNext = System.nanoTime();
      long startBatch = System.nanoTime();

      while (reader.hasNext()) {
        long endHasNext = System.nanoTime();
        Object[] batchRow = reader.readNextBatchRow();
        for (int k = 0; k < batchRow.length; k++) {
          Object[] data = (Object[]) batchRow[k];
          for (int x = 0; x < data.length; x++) {
            Object column = data[x];
          }
          i++;
          if (k == batchRow.length - 1 && i % printNum != 0) {
            hasNextTime = hasNextTime + (endHasNext - startHasNext);
            long endBatchReadTime = System.nanoTime();
            readBatchTime = readBatchTime + (endBatchReadTime - endHasNext);
          }
          if (i > 0 && i % printNum == 0) {
            hasNextTime = hasNextTime + (endHasNext - startHasNext);
            long endBatchReadTime = System.nanoTime();
            readBatchTime = readBatchTime + (endBatchReadTime - endHasNext);

            Long endBatch = System.nanoTime();
            System.out.print(i + ": time is " + (endBatch - startBatch) / num
                + " s, \tspeed is " + (printNum / ((endBatch - startBatch) / num))
                + " records/s, \thasNext time is " + (hasNextTime / num)
                + " s,\t readNextBatch time is \t" + (readBatchTime / num) + "s.\t");
            hasNextTotalTime += hasNextTime;
            readBatchTotalTime += readBatchTime;
            hasNextTime = 0L;
            readBatchTime = 0L;
            for (int j = 0; j < data.length; j++) {
              System.out.print(data[j] + "\t\t");
            }
            System.out.println();
            startBatch = System.nanoTime();
          }
        }
        startHasNext = System.nanoTime();
      }
      Long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + (startReadTime - startTime) / num
          + " s, \ttotal read time is " + (endReadTime - startReadTime) / num
          + " s, \taverage speed is " + (i / ((endReadTime - startReadTime) / num))
          + " records/s. hasNext total time is " + hasNextTotalTime / num
          + " s, readNextBatch total time is " + readBatchTotalTime / num + " s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public static void readNextBatchStringRow(String path, double num,
                                            Configuration conf, int batch, int printNum, boolean withRowRecordReader) {
    System.out.println("readNextBatchRow");
    try {
      // Read data
      Long startTime = System.nanoTime();
      CarbonReaderBuilder carbonReaderBuilder = CarbonReader
          .builder(path, "_temp")
          .withHadoopConf(conf)
          .withBatch(batch);

      CarbonReader reader;
      if (withRowRecordReader) {
        reader = carbonReaderBuilder.withRowRecordReader().build();
      } else {
        reader = carbonReaderBuilder.build();
      }

      long startReadTime = System.nanoTime();
      long readBatchTime = 0L;
      long hasNextTime = 0L;
      long readBatchTotalTime = 0L;
      long hasNextTotalTime = 0L;
      System.out.println("build time is " + (startReadTime - startTime) / num);
      int i = 0;
      long startHasNext = System.nanoTime();
      long startBatch = System.nanoTime();

      while (reader.hasNext()) {
        long endHasNext = System.nanoTime();
        String[] batchRow = reader.readNextBatchStringRow().split(BATCH_SEPARATOR);
        for (int k = 0; k < batchRow.length; k++) {
          Object[] data = batchRow[k].split(ROW_SEPARATOR);
          for (int x = 0; x < data.length; x++) {
            Object column = data[x];
          }
          i++;
          if (k == batchRow.length - 1 && i % printNum != 0) {
            hasNextTime = hasNextTime + (endHasNext - startHasNext);
            long endBatchReadTime = System.nanoTime();
            readBatchTime = readBatchTime + (endBatchReadTime - endHasNext);
          }
          if (i > 0 && i % printNum == 0) {
            hasNextTime = hasNextTime + (endHasNext - startHasNext);
            long endBatchReadTime = System.nanoTime();
            readBatchTime = readBatchTime + (endBatchReadTime - endHasNext);

            Long endBatch = System.nanoTime();
            System.out.print(i + ": time is " + (endBatch - startBatch) / num
                + " s, \tspeed is " + (printNum / ((endBatch - startBatch) / num))
                + " records/s, \thasNext time is " + (hasNextTime / num)
                + " s,\t readNextBatch time is \t" + (readBatchTime / num) + "s.\t");
            hasNextTotalTime += hasNextTime;
            readBatchTotalTime += readBatchTime;
            hasNextTime = 0L;
            readBatchTime = 0L;
            for (int j = 0; j < data.length; j++) {
              System.out.print(data[j] + "\t\t");
            }
            System.out.println();
            startBatch = System.nanoTime();
          }
        }
        startHasNext = System.nanoTime();
      }
      Long endReadTime = System.nanoTime();
      System.out.println("total lines is " + i + ", build time is " + (startReadTime - startTime) / num
          + " s, \ttotal read time is " + (endReadTime - startReadTime) / num
          + " s, \taverage speed is " + (i / ((endReadTime - startReadTime) / num))
          + " records/s. hasNext total time is " + hasNextTotalTime / num
          + " s, readNextBatch total time is " + readBatchTotalTime / num + " s.");
      reader.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public static Schema readSchema(String path) throws IOException {
    File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if (name == null) {
          return false;
        }
        return name.endsWith("carbondata");
      }
    });
    if (dataFiles == null || dataFiles.length < 1) {
      throw new RuntimeException("Carbon index file not exists.");
    }
    Schema schema = CarbonSchemaReader
        .readSchemaInDataFile(dataFiles[0].getAbsolutePath())
        .asOriginOrder();
    return schema;
  }

  /**
   * extend data
   * read origin path data and generate new data in new path,
   * the new data is bigger than origin data
   *
   * @param originPath origin path of data
   * @param newPath    new path of data
   * @throws IOException
   * @throws InterruptedException
   * @throws InvalidLoadOptionException
   */
  public static void extendData(String originPath, String newPath)
      throws IOException, InterruptedException, InvalidLoadOptionException {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "2048")
        .addProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB, "4048");

    Map<String, String> map = new HashMap<>();
    map.put("complex_delimiter_level_1", "#");

    // Read data
    CarbonReader reader = CarbonReader
        .builder(originPath, "_temp")
        .build();

    int i = 1;
    int num = 100000;
    int ram = num;
    Random random = new Random();
    Object[] objects = new Object[3000];
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      if (!(row[0] == null
          || row[1] == null
          || row[2] == null
          || row[0].equals("null")
          || row[1].equals("null")
          || row[2].equals("null"))) {
        System.out.print(i + ":\t");

        objects[i - 1] = row;

        i++;
      }
      System.out.println();
    }
    reader.close();

    CarbonWriterBuilder builder = CarbonWriter.builder()
        .outputPath(newPath)
        .withLoadOptions(map)
        .withBlockSize(128)
        .withCsvInput(readSchema(originPath));
    CarbonWriter writer = builder.build();

    for (int index = 0; index < i - 2; index++) {
      System.out.println(index);
      Object[] row = (Object[]) objects[index];
      String[] strings = new String[row.length];
      for (int j = 2; j < row.length; j++) {
        if (row[j] instanceof Long) {
          strings[j] = new Timestamp((Long) row[j] / 1000).toString();
        } else {
          strings[j] = (String) row[j];
        }
      }
      for (int k = 0; k < num; k++) {
        strings[0] = "Source" + random.nextInt(ram) + row[0];
        strings[1] = "Target" + random.nextInt(ram) + row[1];
        writer.write(strings);
      }

      if (index > 1 && index % 400 == 0) {
        System.out.println("start to close and build again");
        for (int a = 0; a < strings.length; a++) {
          System.out.print(strings[a] + "\t\t\t\t");
        }
        writer.close();
        Thread.sleep(2000);
        writer = builder.build();
      }
    }

    writer.close();
  }

}

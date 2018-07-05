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

package org.apache.carbondata.examples.sdk;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;


/**
 * Example for testing read many files on S3
 */
public class SDKS3MultiFilesExample {
  public static void main(String[] args) throws Exception {
    LogServiceFactory.getLogService().isInfoEnabled();
    LogService logger = LogServiceFactory.getLogService(SDKS3MultiFilesExample.class.getName());
    if (args == null || args.length < 3) {
      logger.error("Usage: java SDKS3MultiFilesExample: <access-key> <secret-key>"
          + "<s3-endpoint> [table-path-on-s3] [rows] [batch number]");
      System.exit(0);
    }

    String path = "s3a://sdk/WriterOutput";
    if (args.length > 3) {
      path = args[3];
    }

    int num = 150;
    if (args.length > 4) {
      num = Integer.parseInt(args[4]);
    }
    int batch = 40;
    if (args.length > 5) {
      batch = Integer.parseInt(args[5]);
    }

    Field[] fields = new Field[9];
    fields[0] = new Field("stringField", DataTypes.STRING);
    fields[1] = new Field("shortField", DataTypes.SHORT);
    fields[2] = new Field("intField", DataTypes.INT);
    fields[3] = new Field("longField", DataTypes.LONG);
    fields[4] = new Field("doubleField", DataTypes.DOUBLE);
    fields[5] = new Field("boolField", DataTypes.BOOLEAN);
    fields[6] = new Field("dateField", DataTypes.DATE);
    fields[7] = new Field("timeField", DataTypes.TIMESTAMP);
    fields[8] = new Field("decimalField", DataTypes.createDecimalType(8, 2));

    long writeStartTime = System.currentTimeMillis();
    CarbonWriter writer = CarbonWriter.builder()
        .setAccessKey(args[0])
        .setSecretKey(args[1])
        .setEndPoint(args[2])
        .outputPath(path)
        .buildWriterForCSVInput(new Schema(fields));
    long writeBuildTime = System.currentTimeMillis();
    System.out.println("Finished build writer time: "
        + (writeBuildTime - writeStartTime) / 1000.0 + " s");

    int closeNum = 1;
    for (int i = 0; i < num; i++) {
      if (i > 0 && (i % batch) == 0) {
        writer.close();
        writer = null;
        writer = CarbonWriter.builder()
            .setAccessKey(args[0])
            .setSecretKey(args[1])
            .setEndPoint(args[2])
            .outputPath(path)
            .buildWriterForCSVInput(new Schema(fields));

        System.out.println("Finished write the " + closeNum +
            " batch, the number of each batch is " + batch);
        closeNum++;
      }
      String[] row2 = new String[]{
          "robot" + (i % 10),
          String.valueOf(i % Short.MAX_VALUE),
          String.valueOf(i),
          String.valueOf(Long.MAX_VALUE - i),
          String.valueOf((double) i / 2),
          String.valueOf(true),
          "2019-03-02",
          "2019-02-12 03:03:34",
          "12.345"
      };
      writer.write(row2);
    }
    System.out.println("Finished write the " + closeNum +
        " batch, the number of each batch is " + batch);
    writer.close();
    System.out.println("Finished write data time: "
        + (System.currentTimeMillis() - writeStartTime) / 1000.0 + " s");

    // Read data
    System.out.println("\nStart build reader: " +
        new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()));
    long startTime = System.currentTimeMillis();
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .projection(new String[]{
            "stringField"
            , "shortField"
            , "intField"
            , "longField"
            , "doubleField"
            , "boolField"
            , "dateField"
            , "timeField"
            , "decimalField"})
        .build();
    long buildTime = System.currentTimeMillis();
    System.out.println("Finished build reader time: " + (buildTime - startTime) / 1000.0 + " s");
    System.out.println("\nData:");
    long day = 24L * 3600 * 1000;
    int i = 0;
    while (reader.hasNext()) {
      Object[] row = (Object[]) reader.readNextRow();
      if (i < 100) {
        System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
            i, row[0], row[1], row[2], row[3], row[4], row[5],
            new Date((day * ((int) row[6]))), new Timestamp((long) row[7] / 1000), row[8]
        ));

        long rowTime = System.currentTimeMillis();
        if (i == 0) {
          System.out.println("Read first row time: "
              + (rowTime - startTime) / 1000.0 + " s");
        }
      }
      i++;
    }
    System.out.println("Finished read, the count of rows is:" + i);
    long runTime = System.currentTimeMillis();
    System.out.println("Read time:" + (runTime - startTime) / 1000.0 + " s");
    reader.close();
  }
}

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

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.commons.io.FileUtils;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonSchemaReader;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

/**
 * Example fo CarbonReader with close method
 * After readNextRow of CarbonReader, User should close the reader,
 * otherwise main will continue run some time
 */
public class CarbonReaderExample {
    public static void main(String[] args) {
        String path = "./testWriteFiles";
        try {
            FileUtils.deleteDirectory(new File(path));

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

            CarbonWriter writer = CarbonWriter.builder()
                .outputPath(path)
                .buildWriterForCSVInput(new Schema(fields));

            for (int i = 0; i < 10; i++) {
                String[] row2 = new String[]{
                    "robot" + (i % 10),
                    String.valueOf(i),
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
            writer.close();

            File[] dataFiles = new File(path).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name == null) {
                        return false;
                    }
                    return name.endsWith("carbonindex");
                }
            });
            if (dataFiles == null || dataFiles.length < 1) {
                throw new RuntimeException("Carbon index file not exists.");
            }
            Schema schema = CarbonSchemaReader
                .readSchemaInIndexFile(dataFiles[0].getAbsolutePath())
                .asOriginOrder();
            // Transform the schema
            String[] strings = new String[schema.getFields().length];
            for (int i = 0; i < schema.getFields().length; i++) {
                strings[i] = (schema.getFields())[i].getFieldName();
            }

            // Read data
            CarbonReader reader = CarbonReader
                .builder(path, "_temp")
                .projection(strings)
                .build();

            System.out.println("\nData:");
            long day = 24L * 3600 * 1000;
            int i = 0;
            while (reader.hasNext()) {
                Object[] row = (Object[]) reader.readNextRow();
                System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
                    i, row[0], row[1], row[2], row[3], row[4], row[5],
                    new Date((day * ((int) row[6]))), new Timestamp((long) row[7] / 1000), row[8]
                ));
                i++;
            }
            System.out.println("\nFinished");

            // Read data
            CarbonReader reader2 = CarbonReader
                .builder(path, "_temp")
                .build();

            System.out.println("\nData:");
            i = 0;
            while (reader2.hasNext()) {
              Object[] row = (Object[]) reader2.readNextRow();
              System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t",
                  i, row[0], new Date((day * ((int) row[1]))), new Timestamp((long) row[2] / 1000),
                  row[3], row[4], row[5], row[6], row[7], row[8]
              ));
              i++;
            }
            System.out.println("\nFinished");
            reader.close();
            FileUtils.deleteDirectory(new File(path));
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}

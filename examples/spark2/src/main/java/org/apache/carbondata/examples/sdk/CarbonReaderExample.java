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
import java.util.List;

import org.apache.commons.io.FileUtils;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.sdk.file.*;

/**
 * Example for testing CarbonReader
 */
public class CarbonReaderExample {
    public static void main(String[] args) throws Exception {
        String path = "./testWriteFiles";
        FileUtils.deleteDirectory(new File(path));

        Field[] fields = new Field[2];
        fields[0] = new Field("name", DataTypes.STRING);
        fields[1] = new Field("age", DataTypes.INT);

        CarbonWriterBuilder builder = CarbonWriter.builder()
                .withSchema(new Schema(fields))
                .isTransactionalTable(true)
                .outputPath(path)
                .persistSchemaFile(true);

        CarbonWriter writer = builder.buildWriterForCSVInput();

        for (int i = 0; i < 10; i++) {
            writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
        }
        writer.close();

        // Read data
        CarbonReader reader = CarbonReader.builder(path, "_temp")
                .projection(new String[]{"name", "age"}).build();

        int i = 0;
        System.out.println("\nData:");
        while (reader.hasNext()) {
            Object[] row = (Object[]) reader.readNextRow();
            System.out.println(row[0] + " " + row[1]);
        }
        System.out.println("\nFinished");
        reader.close();
        FileUtils.deleteDirectory(new File(path));
    }
}

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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

/**
 * Example for testing CarbonWriter on S3
 */
public class CarbonWriterS3Example {
    public static void main(String[] args) throws Exception {
        LogService logger = LogServiceFactory.getLogService(CarbonWriterS3Example.class.getName());
        if (args == null || args.length < 3) {
            logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>" +
                    "<s3-endpoint> [table-path-on-s3] [persistSchema] [transactionalTable]");
            System.exit(0);
        }

        String path = "s3a://sdk/WriterOutput";
        if (args.length > 3) {
            path=args[3];
        }

        int num = 3;
        if (args.length > 4) {
            num = Integer.parseInt(args[4]);
        }

        Boolean persistSchema = true;
        if (args.length > 5) {
            if (args[5].equalsIgnoreCase("true")) {
                persistSchema = true;
            } else {
                persistSchema = false;
            }
        }

        Boolean transactionalTable = true;
        if (args.length > 6) {
            if (args[6].equalsIgnoreCase("true")) {
                transactionalTable = true;
            } else {
                transactionalTable = false;
            }
        }

        Field[] fields = new Field[2];
        fields[0] = new Field("name", DataTypes.STRING);
        fields[1] = new Field("age", DataTypes.INT);
        CarbonWriterBuilder builder = CarbonWriter.builder()
                .withSchema(new Schema(fields))
                .setAccessKey(args[0])
                .setSecretKey(args[1])
                .setEndPoint(args[2])
                .outputPath(path)
                .persistSchemaFile(persistSchema)
                .isTransactionalTable(transactionalTable);

        CarbonWriter writer = builder.buildWriterForCSVInput();

        for (int i = 0; i < num; i++) {
            writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
        }
        writer.close();
        // Read data
        CarbonReader reader = CarbonReader
                .builder(path, "_temp")
                .projection(new String[]{"name", "age"})
                .build();

        System.out.println("\nData:");
        int i = 0;
        while (i < 20 && reader.hasNext()) {
            Object[] row = (Object[]) reader.readNextRow();
            System.out.println(row[0] + " " + row[1]);
            i++;
        }
        System.out.println("\nFinished");
        // TODO
        //        reader.close();
    }
}

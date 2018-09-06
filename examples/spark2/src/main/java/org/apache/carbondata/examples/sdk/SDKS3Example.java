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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.file.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;

/**
 * Example for testing CarbonWriter on S3
 */
public class SDKS3Example {
    public static void main(String[] args) throws Exception {
        LogService logger = LogServiceFactory.getLogService(SDKS3Example.class.getName());
        if (args == null || args.length < 3) {
            logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>"
                + "<s3-endpoint> [table-path-on-s3] [rows]");
            System.exit(0);
        }

        String backupProperty = CarbonProperties.getInstance()
            .getProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
                CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH_DEFAULT);
        CarbonProperties.getInstance()
            .addProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH, "true");

        String path = "s3a://sdk/WriterOutput";
        if (args.length > 3) {
            path=args[3];
        }

        int num = 3;
        if (args.length > 4) {
            num = Integer.parseInt(args[4]);
        }

        Configuration conf = new Configuration(false);
        conf.set(Constants.ACCESS_KEY, args[0]);
        conf.set(Constants.SECRET_KEY, args[1]);
        conf.set(Constants.ENDPOINT, args[2]);

        Field[] fields = new Field[2];
        fields[0] = new Field("name", DataTypes.STRING);
        fields[1] = new Field("age", DataTypes.INT);
        CarbonWriterBuilder builder = CarbonWriter.builder().outputPath(path).withHadoopConf(conf);
        CarbonWriter writer = builder.withCsvInput(new Schema(fields)).build();

        for (int i = 0; i < num; i++) {
            writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
        }
        writer.close();
        // Read data

        EqualToExpression equalToExpression = new EqualToExpression(
            new ColumnExpression("name", DataTypes.STRING),
            new LiteralExpression("robot1", DataTypes.STRING));

        CarbonReader reader = CarbonReader
            .builder(path, "_temp")
            .projection(new String[]{"name", "age"})
            .filter(equalToExpression)
            .withHadoopConf(conf)
            .build();

        System.out.println("\nData:");
        int i = 0;
        while (i < 20 && reader.hasNext()) {
            Object[] row = (Object[]) reader.readNextRow();
            System.out.println(row[0] + " " + row[1]);
            i++;
        }
        System.out.println("\nFinished");
        reader.close();

        CarbonProperties.getInstance()
            .addProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
                backupProperty);
    }
}

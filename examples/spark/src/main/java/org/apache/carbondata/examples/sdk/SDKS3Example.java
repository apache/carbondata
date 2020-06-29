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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.log4j.Logger;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * Example for testing CarbonWriter on S3
 */
public class SDKS3Example {
    public static void main(String[] args) throws Exception {
        Logger logger = LogServiceFactory.getLogService(SDKS3Example.class.getName());
        if (args == null || args.length < 3) {
            logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>"
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3221
                + "<s3-endpoint> [table-path-on-s3] [rows] [Number of writes]");
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

        int rows = 3;
        if (args.length > 4) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3221
            rows = Integer.parseInt(args[4]);
        }
        int num = 3;
        if (args.length > 5) {
            num = Integer.parseInt(args[5]);
        }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3000
        Configuration conf = new Configuration(true);
        conf.set(Constants.ACCESS_KEY, args[0]);
        conf.set(Constants.SECRET_KEY, args[1]);
        conf.set(Constants.ENDPOINT, args[2]);

        Field[] fields = new Field[2];
        fields[0] = new Field("name", DataTypes.STRING);
        fields[1] = new Field("age", DataTypes.INT);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3221
        for (int j = 0; j < num; j++) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3000
            CarbonWriter writer = CarbonWriter
                .builder()
                .outputPath(path)
                .withHadoopConf(conf)
                .withCsvInput(new Schema(fields))
                .writtenBy("SDKS3Example")
                .build();

            for (int i = 0; i < rows; i++) {
                writer.write(new String[]{"robot" + (i % 10), String.valueOf(i)});
            }
            writer.close();
        }
        // Read data

        EqualToExpression equalToExpression = new EqualToExpression(
            new ColumnExpression("name", DataTypes.STRING),
            new LiteralExpression("robot1", DataTypes.STRING));

        CarbonReader reader = CarbonReader
            .builder(path, "_temp")
            .projection(new String[]{"name", "age"})
            .filter(equalToExpression)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2961
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2392

        // Read without filter
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3221
        CarbonReader reader2 = CarbonReader
            .builder(path, "_temp")
            .projection(new String[]{"name", "age"})
            .withHadoopConf(ACCESS_KEY, args[0])
            .withHadoopConf(SECRET_KEY, args[1])
            .withHadoopConf(ENDPOINT, args[2])
            .build();

        System.out.println("\nData:");
        i = 0;
        while (i < 20 && reader2.hasNext()) {
            Object[] row = (Object[]) reader2.readNextRow();
            System.out.println(row[0] + " " + row[1]);
            i++;
        }
        System.out.println("\nFinished");
        reader2.close();

        CarbonProperties.getInstance()
            .addProperty(CarbonLoadOptionConstants.ENABLE_CARBON_LOAD_DIRECT_WRITE_TO_STORE_PATH,
                backupProperty);
    }
}

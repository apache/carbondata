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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.sdk.file.CarbonReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * Example for testing carbonReader on S3
 */
public class SDKS3ReadExample {
    public static void main(String[] args) throws Exception {
        Logger logger = LogServiceFactory.getLogService(SDKS3ReadExample.class.getName());
        if (args == null || args.length < 3) {
            logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>"
                + "<s3-endpoint> [table-path-on-s3]");
            System.exit(0);
        }

        String path = "s3a://sdk/WriterOutput";
        if (args.length > 3) {
            path=args[3];
        }

        // Read data

        EqualToExpression equalToExpression = new EqualToExpression(
            new ColumnExpression("name", DataTypes.STRING),
            new LiteralExpression("robot1", DataTypes.STRING));

        Configuration configuration = new Configuration();
        configuration.set(ACCESS_KEY, args[0]);
        configuration.set(SECRET_KEY, args[1]);
        configuration.set(ENDPOINT, args[2]);
        CarbonReader reader = CarbonReader
            .builder(path, "_temp")
            .projection(new String[]{"name", "age"})
            .filter(equalToExpression)
            .withHadoopConf(configuration)
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

        // Read without filter
        CarbonReader reader2 = CarbonReader
            .builder(path, "_temp")
            .projection(new String[]{"name", "age"})
            .withHadoopConf(configuration)
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
    }
}

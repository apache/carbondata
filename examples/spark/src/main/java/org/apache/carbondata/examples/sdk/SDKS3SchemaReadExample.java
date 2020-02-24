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
import org.apache.carbondata.sdk.file.CarbonSchemaReader;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static org.apache.hadoop.fs.s3a.Constants.*;

/**
 * Example for testing carbonSchemaReader on S3
 */
public class SDKS3SchemaReadExample {
  public static void main(String[] args) throws Exception {
    Logger logger = LogServiceFactory.getLogService(SDKS3SchemaReadExample.class.getName());
    if (args == null || args.length < 3) {
      logger.error("Usage: java CarbonS3Example: <access-key> <secret-key>"
          + "<s3-endpoint> [table-path-on-s3]");
      System.exit(0);
    }

    String path = "s3a://sdk/WriterOutput/carbondata2/";

    if (args.length > 3) {
      path = args[3];
    }

    Configuration configuration = new Configuration();
    configuration.set(ACCESS_KEY, args[0]);
    configuration.set(SECRET_KEY, args[1]);
    configuration.set(ENDPOINT, args[2]);

    // method 1 to read schema
    Schema schema = CarbonSchemaReader.readSchema(path, true, configuration);
    System.out.println("Schema length is " + schema.getFieldsLength());
    Field[] fields = schema.getFields();
    for (int i = 0; i < fields.length; i++) {
      System.out.println(fields[i] + "\t");
    }

    // method 2 to read schema
    Schema schema2 = CarbonSchemaReader.readSchema(path, configuration);
    System.out.println("Schema length is " + schema2.getFieldsLength());
    Field[] fields2 = schema2.getFields();
    for (int i = 0; i < fields2.length; i++) {
      System.out.println(fields2[i] + "\t");
    }
  }
}

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

package org.apache.carbon.flink;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

public final class CarbonS3WriterFactory extends CarbonWriterFactory {

  private static final long serialVersionUID = 2302824357711095245L;

  @Override
  public String getType() {
    return CarbonS3WriterFactoryBuilder.TYPE;
  }

  @Override
  protected CarbonS3Writer create0() throws IOException {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String writeTempPath = writerProperties.getProperty(CarbonS3Property.DATA_TEMP_PATH);
    if (writeTempPath == null) {
      throw new IllegalArgumentException(
          "Writer property [" + CarbonS3Property.DATA_TEMP_PATH + "] is not set."
      );
    }
    final String writerIdentifier = UUID.randomUUID().toString();
    final String writePath = writeTempPath + writerIdentifier.replace("-", "") + "/";
    final CarbonTable table = this.getTable();
    return new CarbonS3Writer(this, writerIdentifier, table,
        writePath, this.getS3Configuration());
  }

  @Override
  protected CarbonS3Writer create0(final String identifier, final String path)
      throws IOException {
    return new CarbonS3Writer(this, identifier, this.getTable(),
        path, this.getS3Configuration());
  }

  @Override
  protected CarbonTable getTable() throws IOException {
    this.setS3Configuration(this.getS3Configuration());
    return super.getTable();
  }

  private org.apache.hadoop.conf.Configuration getS3Configuration() {
    final Properties writerProperties = this.getConfiguration().getWriterProperties();
    final String accessKey = writerProperties.getProperty(CarbonS3Property.ACCESS_KEY);
    final String secretKey = writerProperties.getProperty(CarbonS3Property.SECRET_KEY);
    final String endpoint = writerProperties.getProperty(CarbonS3Property.ENDPOINT);
    if (accessKey == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.ACCESS_KEY + "] is not set."
      );
    }
    if (secretKey == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.SECRET_KEY + "] is not set."
      );
    }
    if (endpoint == null) {
      throw new IllegalArgumentException(
              "Writer property [" + CarbonS3Property.ENDPOINT + "] is not set."
      );
    }
    final org.apache.hadoop.conf.Configuration configuration =
        new org.apache.hadoop.conf.Configuration(true);
    configuration.set("fs.s3.access.key", accessKey);
    configuration.set("fs.s3.secret.key", secretKey);
    configuration.set("fs.s3.endpoint", endpoint);
    configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("fs.s3a.access.key", accessKey);
    configuration.set("fs.s3a.secret.key", secretKey);
    configuration.set("fs.s3a.endpoint", endpoint);
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return configuration;
  }

  private void setS3Configuration(final org.apache.hadoop.conf.Configuration configuration) {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
  }

}

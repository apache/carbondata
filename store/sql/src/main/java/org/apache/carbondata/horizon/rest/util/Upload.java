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

package org.apache.carbondata.horizon.rest.util;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * a util to upload a local file to s3
 */

public class Upload {

  private static Configuration configuration = null;

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: Upload <local file> <s3 path> [overwrite]");
      return;
    }

    String sourceFile = args[0];
    String targetFile = args[1];
    boolean isOverWrite = false;
    if (args.length >= 3) {
      isOverWrite = Boolean.valueOf(args[2]);
    }

    upload(sourceFile, targetFile, isOverWrite);
  }

  public static void upload(String sourceFile, String targetFile, boolean isOverWrite)
      throws IOException {
    Configuration hadoopConf = getConfiguration();

    Path sourcePath = new Path(sourceFile);
    FileSystem sourceFileSystem = sourcePath.getFileSystem(hadoopConf);
    if (!sourceFileSystem.exists(sourcePath)) {
      throw new IOException("source file not exists: " + sourceFile);
    }

    Path targetPath = new Path(targetFile);
    FileSystem targetFileSystem = targetPath.getFileSystem(hadoopConf);
    if (targetFileSystem.exists(targetPath) && !isOverWrite) {
      throw new IOException("target file exists: " + targetFile);
    }

    IOUtils.copyBytes(sourceFileSystem.open(sourcePath),
        targetFileSystem.create(targetPath, isOverWrite), 1024 * 4, true);

    sourceFileSystem.close();
    targetFileSystem.close();
  }

  public static synchronized Configuration getConfiguration() {
    if (configuration == null) {
      configuration = new Configuration();
      Properties properties = System.getProperties();
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        Object key = entry.getKey();
        Object value = entry.getValue();
        if (key instanceof String && value instanceof String) {
          String keyStr = (String) key;
          if (keyStr.startsWith("hadoop.")) {
            configuration.set(keyStr.substring("hadoop.".length()), (String) value);
          }
        }
      }
    }

    return configuration;
  }
}

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

package org.apache.carbondata.core.datastore.impl;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;

import org.apache.hadoop.conf.Configuration;

/**
 * Interface to  create CarbonFile Instance specific to the FileSystem where the patch belongs.
 */
public interface FileTypeInterface {

  /**
   * Return the correct CarbonFile instance.
   *
   * @param path          path of the file
   * @param configuration configuration
   * @return CarbonFile instance
   */
  public CarbonFile getCarbonFile(String path, Configuration configuration);

  /**
   * Check if the FileSystem mapped with the given path is supported or not.
   *
   * @param path path of the file
   * @return true if supported, fasle if not supported
   */
  public boolean isPathSupported(String path);
}


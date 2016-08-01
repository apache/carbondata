/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.path;

import java.io.File;

import org.carbondata.core.carbon.CarbonTableIdentifier;

import org.apache.hadoop.fs.Path;

/**
 * Helps to get Store content paths.
 */
public class CarbonStorePath extends Path {

  private String storePath;

  public CarbonStorePath(String storePathString) {
    super(storePathString);
    this.storePath = storePathString;
  }

  /**
   * gets CarbonTablePath object to manage table paths
   */
  public static CarbonTablePath getCarbonTablePath(String storePath,
      CarbonTableIdentifier tableIdentifier) {
    CarbonTablePath carbonTablePath = new CarbonTablePath(tableIdentifier,
        storePath + File.separator + tableIdentifier.getDatabaseName() + File.separator
            + tableIdentifier.getTableName());

    return carbonTablePath;
  }

  /**
   * gets CarbonTablePath object to manage table paths
   */
  public CarbonTablePath getCarbonTablePath(CarbonTableIdentifier tableIdentifier) {
    return CarbonStorePath.getCarbonTablePath(storePath, tableIdentifier);
  }

  @Override public boolean equals(Object o) {
    if (!(o instanceof CarbonStorePath)) {
      return false;
    }
    CarbonStorePath path = (CarbonStorePath)o;
    return storePath.equals(path.storePath) && super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode() + storePath.hashCode();
  }
}

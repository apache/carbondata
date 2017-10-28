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
package org.apache.carbondata.core.util.path;

import java.io.File;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;

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
   *
   * @param storePath the store path of the segment
   * @param tableIdentifier identifier of carbon table that the segment belong to
   * @return the store location of the segment
   */
  public static CarbonTablePath getCarbonTablePath(String storePath,
      CarbonTableIdentifier tableIdentifier) {
    return new CarbonTablePath(tableIdentifier,
        storePath + File.separator + tableIdentifier.getDatabaseName() + File.separator
            + tableIdentifier.getTableName());
  }

  public static CarbonTablePath getCarbonTablePath(String storePath,
      String dbName, String tableName) {
    return new CarbonTablePath(storePath, dbName, tableName);
  }

  public static CarbonTablePath getCarbonTablePath(AbsoluteTableIdentifier identifier) {
    CarbonTableIdentifier id = identifier.getCarbonTableIdentifier();
    return new CarbonTablePath(id, identifier.getTablePath());
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

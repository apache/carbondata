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

import org.apache.hadoop.conf.Configuration;
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
      CarbonTableIdentifier tableIdentifier, Configuration configuration) {
    return new CarbonTablePath(tableIdentifier,
        storePath + File.separator + tableIdentifier.getDatabaseName() + File.separator
            + tableIdentifier.getTableName(), configuration);
  }

  public static CarbonTablePath getCarbonTablePath(String storePath,
      String dbName, String tableName, Configuration configuration) {
    return new CarbonTablePath(storePath, dbName, tableName, configuration);
  }

  public static CarbonTablePath getCarbonTablePath(AbsoluteTableIdentifier identifier,
      Configuration configuration) {
    CarbonTableIdentifier id = identifier.getCarbonTableIdentifier();
    return new CarbonTablePath(id, identifier.getTablePath(), configuration);
  }

  /**
   * gets CarbonTablePath object to manage table paths
   */
  public CarbonTablePath getCarbonTablePath(CarbonTableIdentifier tableIdentifier,
      Configuration configuration) {
    return CarbonStorePath.getCarbonTablePath(storePath, tableIdentifier, configuration);
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

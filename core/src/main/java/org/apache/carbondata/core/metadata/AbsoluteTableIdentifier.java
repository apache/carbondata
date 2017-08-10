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
package org.apache.carbondata.core.metadata;

import java.io.File;
import java.io.Serializable;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;

/**
 * identifier which will have store path and carbon table identifier
 */
public class AbsoluteTableIdentifier implements Serializable {

  /**
   * serializable version
   */
  private static final long serialVersionUID = 4695047103484427506L;

  /**
   * path of the store
   */
  private String storePath;


  private boolean isLocalPath;

  /**
   * carbon table identifier which will have table name and table database
   * name
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  public AbsoluteTableIdentifier(String storePath, CarbonTableIdentifier carbonTableIdentifier) {
    //TODO this should be moved to common place where path handling will be handled
    this.storePath = FileFactory.getUpdatedFilePath(storePath);
    isLocalPath = storePath.startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX);
    this.carbonTableIdentifier = carbonTableIdentifier;
  }

  /**
   * @return the storePath
   */
  public String getStorePath() {
    return storePath;
  }

  /**
   * @return the carbonTableIdentifier
   */
  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return carbonTableIdentifier;
  }

  public static AbsoluteTableIdentifier from(String storePath, String dbName, String tableName) {
    CarbonTableIdentifier identifier = new CarbonTableIdentifier(dbName, tableName, "");
    return new AbsoluteTableIdentifier(storePath, identifier);
  }

  /**
   * By using the tablePath this method will prepare a AbsoluteTableIdentifier with
   * dummy tableId(Long.toString(System.currentTimeMillis()).
   * This instance could not be used to uniquely identify the table, this is just
   * to get the database name, table name and store path to load the schema.
   * @param tablePath
   * @return returns AbsoluteTableIdentifier with dummy tableId
   */
  public static AbsoluteTableIdentifier fromTablePath(String tablePath) {
    String formattedTablePath = tablePath.replace('\\', '/');
    String[] names = formattedTablePath.split("/");
    if (names.length < 3) {
      throw new IllegalArgumentException("invalid table path: " + tablePath);
    }

    String tableName = names[names.length - 1];
    String dbName = names[names.length - 2];
    String storePath = formattedTablePath.substring(0,
        formattedTablePath.lastIndexOf(dbName + CarbonCommonConstants.FILE_SEPARATOR + tableName)
            - 1);

    CarbonTableIdentifier identifier =
        new CarbonTableIdentifier(dbName, tableName, Long.toString(System.currentTimeMillis()));
    return new AbsoluteTableIdentifier(storePath, identifier);
  }

  public String getTablePath() {
    return getStorePath() + File.separator + getCarbonTableIdentifier().getDatabaseName() +
        File.separator + getCarbonTableIdentifier().getTableName();
  }

  public String appendWithLocalPrefix(String path) {
    if (isLocalPath) {
      return CarbonCommonConstants.LOCAL_FILE_PREFIX + path;
    } else {
      return path;
    }
  }

  /**
   * to get the hash code
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((carbonTableIdentifier == null) ? 0 : carbonTableIdentifier.hashCode());
    result = prime * result + ((storePath == null) ? 0 : storePath.hashCode());
    return result;
  }

  /**
   * to check this class is equal to
   * other object passed
   *
   * @param obj other object
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AbsoluteTableIdentifier)) {
      return false;
    }
    AbsoluteTableIdentifier other = (AbsoluteTableIdentifier) obj;
    if (carbonTableIdentifier == null) {
      if (other.carbonTableIdentifier != null) {
        return false;
      }
    } else if (!carbonTableIdentifier.equals(other.carbonTableIdentifier)) {
      return false;
    }
    if (storePath == null) {
      if (other.storePath != null) {
        return false;
      }
    } else if (!storePath.equals(other.storePath)) {
      return false;
    }
    return true;
  }

  public String uniqueName() {
    return storePath + "/" + carbonTableIdentifier.toString().toLowerCase();
  }
}

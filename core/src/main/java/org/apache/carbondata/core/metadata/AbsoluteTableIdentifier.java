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
   * path of the table
   */
  private String tablePath;


  /**
   * carbon table identifier which will have table name and table database
   * name
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  private AbsoluteTableIdentifier(String tablePath, CarbonTableIdentifier carbonTableIdentifier) {
    //TODO this should be moved to common place where path handling will be handled
    this.tablePath = FileFactory.getUpdatedFilePath(tablePath);
    this.carbonTableIdentifier = carbonTableIdentifier;
  }

  /**
   * @return the carbonTableIdentifier
   */
  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return carbonTableIdentifier;
  }

  public static AbsoluteTableIdentifier from(String tablePath, String dbName, String tableName,
      String tableId) {
    CarbonTableIdentifier identifier = new CarbonTableIdentifier(dbName, tableName, tableId);
    return new AbsoluteTableIdentifier(tablePath, identifier);
  }

  public static AbsoluteTableIdentifier from(String tablePath, String dbName, String tableName) {
    return from(tablePath, dbName, tableName, "");
  }

  public static AbsoluteTableIdentifier from(
      String tablePath,
      CarbonTableIdentifier carbonTableIdentifier) {
    return new AbsoluteTableIdentifier(tablePath, carbonTableIdentifier);
  }

  public String getTablePath() {
    return tablePath;
  }

  public String appendWithLocalPrefix(String path) {
    if (tablePath.startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX)) {
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
    result = prime * result + ((tablePath == null) ? 0 : tablePath.hashCode());
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
    if (tablePath == null) {
      if (other.tablePath != null) {
        return false;
      }
    } else if (!tablePath.equals(other.tablePath)) {
      return false;
    }
    return true;
  }

  public String uniqueName() {
    return tablePath + "/" + carbonTableIdentifier.toString().toLowerCase();
  }

  public String getDatabaseName() {
    return carbonTableIdentifier.getDatabaseName();
  }

  public String getTableName() {
    return carbonTableIdentifier.getTableName();
  }

  public String toString() {
    return carbonTableIdentifier.toString();
  }

}

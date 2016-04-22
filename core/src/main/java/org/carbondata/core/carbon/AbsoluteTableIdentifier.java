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
package org.carbondata.core.carbon;

import java.io.Serializable;

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

  /**
   * carbon table identifier which will have table name and table database
   * name
   */
  private CarbonTableIdentifier carbonTableIdentifier;

  /**
   * @return the storePath
   */
  public String getStorePath() {
    return storePath;
  }

  /**
   * @param storePath the storePath to set
   */
  public void setStorePath(String storePath) {
    this.storePath = storePath;
  }

  /**
   * @return the carbonTableIdentifier
   */
  public CarbonTableIdentifier getCarbonTableIdentifier() {
    return carbonTableIdentifier;
  }

  /**
   * @param carbonTableIdentifier the carbonTableIdentifier to set
   */
  public void setCarbonTableIdentifier(CarbonTableIdentifier carbonTableIdentifier) {
    this.carbonTableIdentifier = carbonTableIdentifier;
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
}

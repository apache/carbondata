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

package org.carbondata.query.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class CarbonSegmentHeader implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 8170452732144940835L;

  /**
   * dimensions
   */
  private int[] dims;

  /**
   * cube name
   */
  private String cubeUniqueName;

  /**
   * table name
   */
  private String factTableName;

  /**
   * predicates
   */
  private List<CarbonPredicates> preds;

  /**
   * hash code
   */
  private transient int hashcode;

  /**
   * start key
   */
  private long[] startKey;

  /**
   * end key
   */
  private long[] endKey;

  public CarbonSegmentHeader(String cubeUniqueName, String factTableName) {
    this.cubeUniqueName = cubeUniqueName;
    this.factTableName = factTableName;
  }

  /**
   * @return the dims
   */
  public int[] getDims() {
    return dims;
  }

  /**
   * @param dims the dims to set
   */
  public void setDims(int[] dims) {
    this.dims = dims;
  }

  /**
   * @return the cubeName
   */
  public String getCubeName() {
    return cubeUniqueName;
  }

  /**
   * @return the factTableName
   */
  public String getFactTableName() {
    return factTableName;
  }

  /**
   * @return the preds
   */
  public List<CarbonPredicates> getPreds() {
    return preds;
  }

  /**
   * @param preds the preds to set
   */
  public void setPreds(List<CarbonPredicates> preds) {
    this.preds = preds;
  }

  @Override public int hashCode() {
    if (hashcode == 0) {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((cubeUniqueName == null) ? 0 : cubeUniqueName.hashCode());
      result = prime * result + Arrays.hashCode(dims);
      result = prime * result + ((factTableName == null) ? 0 : factTableName.hashCode());
      hashcode = result;
    }

    return hashcode;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof CarbonSegmentHeader) {
      if (this == obj) {
        return true;
      }

      CarbonSegmentHeader other = (CarbonSegmentHeader) obj;

      if (!cubeUniqueName.equals(other.cubeUniqueName) || !Arrays.equals(dims, other.dims)
          || !factTableName.equals(other.factTableName) || preds != null && !preds
          .equals(other.preds)) {
        return false;
      }

      return true;
    }

    return false;

  }

  /**
   * equalsForSubSet
   *
   * @param other
   * @return
   */
  public boolean equalsForSubSet(CarbonSegmentHeader other) {

    if (!cubeUniqueName.equals(other.cubeUniqueName) || !isSubSet(other.dims) || !factTableName
        .equals(other.factTableName) || preds != null && !preds.equals(other.preds)) {
      return false;
    }

    return true;
  }

  private boolean isSubSet(int[] otherDims) {
    for (int i = 0; i < otherDims.length; i++) {
      boolean found = false;
      for (int j = 0; j < dims.length; j++) {
        if (otherDims[i] == dims[j]) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  public long[] getStartKey() {
    return startKey;
  }

  public void setStartKey(long[] ls) {
    this.startKey = ls;
  }

  public long[] getEndKey() {
    return endKey;
  }

  public void setEndKey(long[] ls) {
    this.endKey = ls;
  }

}

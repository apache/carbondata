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

package com.huawei.unibi.molap.metadata;

import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.olap.Exp;
import com.huawei.unibi.molap.olap.SqlStatement.Type;

/**
 * Calculated measure instance
 */
public class CalculatedMeasure extends Measure {

  private static final long serialVersionUID = 7678164364921738949L;

  private transient Exp exp;

  private Dimension distCountDim;

  public CalculatedMeasure(String colName, int ordinal, String aggName, String aggClassName,
      String name, Type dataType, Cube cube) {
  }

  public CalculatedMeasure(Exp exp, String name) {
    super(null, -1, null, null, name, null, null, false);
    this.exp = exp;
  }

  public CalculatedMeasure(String name) {
    super(null, -1, null, null, name, null, null, false);
  }

  /**
   * @return the exp
   */
  public Exp getExp() {
    return exp;
  }

  /**
   * @param exp the exp to set
   */
  public void setExp(Exp exp) {
    this.exp = exp;
  }

  /**
   * @return the distCountDim
   */
  public Dimension getDistCountDim() {
    return distCountDim;
  }

  /**
   * @return the distCountDim
   */
  public void setDistCountDim(Dimension distCountDim) {
    this.distCountDim = distCountDim;
  }

  @Override public boolean equals(Object obj) {
    Measure that = null;

    if (obj instanceof Measure) {

      that = (Measure) obj;
      return that.getName().equals(getName());
    }
    // Added this to fix Find bug
    // Symmetric issue
    if (obj instanceof Dimension) {
      return super.equals(obj);
    }
    return false;

  }

  @Override public int hashCode() {
    return getName().hashCode();
  }

}

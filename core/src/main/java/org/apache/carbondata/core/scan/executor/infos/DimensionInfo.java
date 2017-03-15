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

package org.apache.carbondata.core.scan.executor.infos;

/**
 * This method will information about the query dimensions whether they exist in particular block
 * and their default value
 */
public class DimensionInfo {

  /**
   * flag to check whether a given dimension exists in a given block
   */
  private boolean[] dimensionExists;

  /**
   * maintains default value for each dimension
   */
  private Object[] defaultValues;

  /**
   * @param dimensionExists
   * @param defaultValues
   */
  public DimensionInfo(boolean[] dimensionExists, Object[] defaultValues) {
    this.dimensionExists = dimensionExists;
    this.defaultValues = defaultValues;
  }

  /**
   * @return
   */
  public boolean[] getDimensionExists() {
    return dimensionExists;
  }

  /**
   * @return
   */
  public Object[] getDefaultValues() {
    return defaultValues;
  }
}

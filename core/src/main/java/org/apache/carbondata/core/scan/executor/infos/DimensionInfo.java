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

import org.apache.carbondata.core.metadata.datatype.DataType;

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
   * flag to check whether there exist a dictionary column in the query which
   * does not exist in the current block
   */
  private boolean isDictionaryColumnAdded;

  /**
   * flag to check whether there exist a no dictionary column in the query which
   * does not exist in the current block
   */
  private boolean isNoDictionaryColumnAdded;

  /**
   * count of dictionary column not existing in the current block
   */
  private int newDictionaryColumnCount;

  /**
   * count of no dictionary columns not existing in the current block
   */
  private int newNoDictionaryColumnCount;

  /**
   * count of complex columns not existing in the current block
   */
  private int newComplexColumnCount;

  /**
   * flag to check whether there exist a complex column in the query which
   * does not exist in the current block
   */
  private boolean isComplexColumnAdded;
  /**
  * maintains the block datatype
  */
  public DataType[] dataType;

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

  public boolean isDictionaryColumnAdded() {
    return isDictionaryColumnAdded;
  }

  public void setDictionaryColumnAdded(boolean dictionaryColumnAdded) {
    isDictionaryColumnAdded = dictionaryColumnAdded;
  }

  public boolean isNoDictionaryColumnAdded() {
    return isNoDictionaryColumnAdded;
  }

  public void setNoDictionaryColumnAdded(boolean noDictionaryColumnAdded) {
    isNoDictionaryColumnAdded = noDictionaryColumnAdded;
  }

  public int getNewDictionaryColumnCount() {
    return newDictionaryColumnCount;
  }

  public void setNewDictionaryColumnCount(int newDictionaryColumnCount) {
    this.newDictionaryColumnCount = newDictionaryColumnCount;
  }

  public int getNewNoDictionaryColumnCount() {
    return newNoDictionaryColumnCount;
  }

  public void setNewNoDictionaryColumnCount(int newNoDictionaryColumnCount) {
    this.newNoDictionaryColumnCount = newNoDictionaryColumnCount;
  }

  public boolean isComplexColumnAdded() {
    return isComplexColumnAdded;
  }

  public void setComplexColumnAdded(boolean complexColumnAdded) {
    isComplexColumnAdded = complexColumnAdded;
  }

  public int getNewComplexColumnCount() {
    return newComplexColumnCount;
  }

  public void setNewComplexColumnCount(int newComplexColumnCount) {
    this.newComplexColumnCount = newComplexColumnCount;
  }
}

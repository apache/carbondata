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
 * Info class which store all the details
 * which is required during aggregation
 */
public class MeasureInfo {

  /**
   * selected query measure ordinal
   * which will be used to read the measures chunk data
   * this will be storing the index of the measure in measures chunk
   */
  private int[] measureOrdinals;

  /**
   * This parameter will be used to
   * check whether particular measure is present
   * in the table block, if not then its default value will be used
   */
  private boolean[] measureExists;

  /**
   * this default value will be used to when some measure is not present
   * in the table block, in case of restructuring of the table if user is adding any
   * measure then in older block that measure wont be present so for measure default value
   * will be used to aggregate in the older table block query execution
   */
  private Object[] defaultValues;

  /**
   * Datatype of each measure;
   */
  private DataType[] measureDataTypes;

  /**
   * @return the measureOrdinal
   */
  public int[] getMeasureOrdinals() {
    return measureOrdinals;
  }

  /**
   * @param measureOrdinal the measureOrdinal to set
   */
  public void setMeasureOrdinals(int[] measureOrdinal) {
    this.measureOrdinals = measureOrdinal;
  }

  /**
   * @return the measureExists
   */
  public boolean[] getMeasureExists() {
    return measureExists;
  }

  /**
   * @param measureExists the measureExists to set
   */
  public void setMeasureExists(boolean[] measureExists) {
    this.measureExists = measureExists;
  }

  /**
   * @return the defaultValues
   */
  public Object[] getDefaultValues() {
    return defaultValues;
  }

  /**
   * @param defaultValues the defaultValues to set
   */
  public void setDefaultValues(Object[] defaultValues) {
    this.defaultValues = defaultValues;
  }

  public DataType[] getMeasureDataTypes() {
    return measureDataTypes;
  }

  public void setMeasureDataTypes(DataType[] measureDataTypes) {
    this.measureDataTypes = measureDataTypes;
  }
}

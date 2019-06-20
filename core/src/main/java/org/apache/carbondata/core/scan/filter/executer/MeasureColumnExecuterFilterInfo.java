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
package org.apache.carbondata.core.scan.filter.executer;

import java.util.AbstractCollection;
import java.util.HashSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import it.unimi.dsi.fastutil.booleans.BooleanOpenHashSet;
import it.unimi.dsi.fastutil.bytes.ByteOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.shorts.ShortOpenHashSet;

/**
 * Below class will be used to keep all the filter values based on data type
 * for measure column.
 * In this class there are multiple type of set is used to avoid conversion of
 * primitive type to primitive object to avoid gc which cause perofrmace degrade when
 * number of records are high
 */
public class MeasureColumnExecuterFilterInfo {

  Object[] filterKeys;

  /**
   * filter set used for filtering the measure value based on data type
   */
  private AbstractCollection filterSet;

  public void setFilterKeys(Object[] filterKeys, DataType dataType) {
    this.filterKeys = filterKeys;
    if (dataType == DataTypes.BOOLEAN) {
      filterSet = new BooleanOpenHashSet();
    } else if (dataType == DataTypes.BYTE) {
      filterSet = new ByteOpenHashSet();
    } else if (dataType == DataTypes.SHORT) {
      filterSet = new ShortOpenHashSet();
    } else if (dataType == DataTypes.INT) {
      filterSet = new IntOpenHashSet();
    } else if (dataType == DataTypes.FLOAT) {
      filterSet = new FloatOpenHashSet();
    } else if (dataType == DataTypes.LONG) {
      filterSet = new LongOpenHashSet();
    } else if (dataType == DataTypes.DOUBLE) {
      filterSet = new DoubleOpenHashSet();
    } else if (DataTypes.isDecimal(dataType)) {
      filterSet = new HashSet();
    } else {
      throw new IllegalArgumentException("Invalid data type");
    }
    for (int i = 0; i < filterKeys.length; i++) {
      if (null != filterKeys[i]) {
        filterSet.add(filterKeys[i]);
      }
    }
  }

  public Object[] getFilterKeys() {
    return filterKeys;
  }

  public AbstractCollection getFilterSet() {
    return filterSet;
  }
}

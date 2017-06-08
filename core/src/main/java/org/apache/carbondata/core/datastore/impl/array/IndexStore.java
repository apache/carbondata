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
package org.apache.carbondata.core.datastore.impl.array;

import org.apache.carbondata.core.datastore.IndexKey;

/**
 * Interface for storing and retrieving index.
 */
public interface IndexStore {

  byte[] getMin(int index, int colIndex);

  byte[] getMax(int index, int colIndex);

  // TODO It can be removed later and use only {getMin(int index, int colIndex)} instead
  byte[][] getMins(int index);

  byte[][] getMaxs(int index);

  IndexKey getIndexKey(int index);

  boolean isKeyAvailableAtIndex(int index);

  int getRowCount(int index);

  int getIndexKeyCount();

  void clear();

}

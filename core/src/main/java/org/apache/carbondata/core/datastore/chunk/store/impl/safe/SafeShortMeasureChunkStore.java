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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

/**
 * Below class will be used to store the measure values of short data type
 *
 */
public class SafeShortMeasureChunkStore extends
    SafeAbstractMeasureDataChunkStore<short[]> {

  /**
   * data
   */
  private short[] data;

  public SafeShortMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
  }

  /**
   * Below method will be used to put short array data
   *
   * @param data
   */
  @Override
  public void putData(short[] data) {
    this.data = data;
  }

  /**
   * to get the short value
   *
   * @param index
   * @return shot value based on index
   */
  @Override
  public short getShort(int index) {
    return data[index];
  }

}

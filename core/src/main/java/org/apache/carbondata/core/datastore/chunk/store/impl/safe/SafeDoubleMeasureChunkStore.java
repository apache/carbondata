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
 * Below class will be used to store the measure values of double data type
 */
public class SafeDoubleMeasureChunkStore extends
    SafeAbstractMeasureDataChunkStore<double[]> {

  /**
   * data
   */
  private double[] data;

  public SafeDoubleMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
  }

  /**
   * Below method will be used to store double array data
   *
   * @param data
   */
  @Override
  public void putData(double[] data) {
    this.data = data;
  }

  /**
   * to get the double value
   *
   * @param index
   * @return double value based on index
   */
  @Override
  public double getDouble(int index) {
    return this.data[index];
  }
}

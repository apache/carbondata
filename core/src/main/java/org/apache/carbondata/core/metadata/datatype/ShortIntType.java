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

package org.apache.carbondata.core.metadata.datatype;

class ShortIntType extends DataType {

  static final DataType SHORT_INT =
      new ShortIntType(DataTypes.SHORT_INT_TYPE_ID, 14, "SHORT_INT", 3);

  private ShortIntType(int id, int precedenceOrder, String name, int sizeInBytes) {
    super(id, precedenceOrder, name, sizeInBytes);
  }

  @Override
  public int getSizeBits() {
    throw new UnsupportedOperationException("Should not call this from SHORT_INT type");
  }

  // this function is needed to ensure singleton pattern while supporting java serialization
  private Object readResolve() {
    return DataTypes.SHORT_INT;
  }
}

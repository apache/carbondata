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
package org.apache.carbondata.core.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * dictionary writer interface
 */
public interface CarbonDictionaryWriter extends Closeable {
  /**
   * write method that accepts one value at a time
   * This method can be used when data is huge and memory is les. In that
   * case data can be stored to a file and an iterator can iterate over it and
   * pass one value at a time
   *
   * @param value unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  void write(String value) throws IOException;

  /**
   * write method that accepts list of byte arrays as value
   * This can be used when data is less, then string can be converted
   * to byte array for each value and added to a list
   *
   * @param valueList list of byte array. Each byte array is unique dictionary value
   * @throws IOException if an I/O error occurs
   */
  void write(List<byte[]> valueList) throws IOException;


  void commit() throws IOException;
}

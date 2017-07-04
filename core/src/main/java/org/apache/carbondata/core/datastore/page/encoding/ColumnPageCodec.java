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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;

/**
 *  Codec for a column page data, implementation should not keep state across pages,
 *  caller may use the same object to apply multiple pages.
 */
public interface ColumnPageCodec {

  /**
   * Codec name will be stored in BlockletHeader (DataChunk3)
   */
  String getName();

  /**
   * encode a column page and return the encoded data
   */
  EncodedColumnPage encode(ColumnPage input) throws MemoryException, IOException;

  /**
   * decode byte array from offset to a column page
   * @param input encoded byte array
   * @param offset startoffset of the input to decode
   * @param length length of data to decode
   * @return decoded data
   */
  ColumnPage decode(byte[] input, int offset, int length) throws MemoryException;

}

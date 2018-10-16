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
import java.util.BitSet;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

public interface ColumnPageDecoder {

  /**
   * Apply decoding algorithm on input byte array and return decoded column page
   */
  ColumnPage decode(byte[] input, int offset, int length) throws MemoryException, IOException;

  /**
   *  Apply decoding algorithm on input byte array and fill the vector here.
   */
  void decodeAndFillVector(byte[] input, int offset, int length, ColumnVectorInfo vectorInfo,
      BitSet nullBits, boolean isLVEncoded) throws MemoryException, IOException;

  ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
      throws MemoryException, IOException;
}

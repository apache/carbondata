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

package org.apache.carbondata.core.keygenerator.mdkey;

public class MultiDimKeyVarLengthGenerator extends AbstractKeyGenerator {

  private static final long serialVersionUID = 9134778127271586515L;
  /**
   *
   */
  protected int[][] byteRangesForKeys;
  private Bits bits;

  public MultiDimKeyVarLengthGenerator(int[] lens) {
    bits = new Bits(lens);
    byteRangesForKeys = new int[lens.length][];
    int keys = lens.length;
    for (int i = 0; i < keys; i++) {
      byteRangesForKeys[i] = bits.getKeyByteOffsets(i);
    }
  }

  @Override
  public byte[] generateKey(int[] keys) {

    return bits.getBytes(keys);
  }

  @Override
  public long[] getKeyArray(byte[] key, int offset) {

    return bits.getKeyArray(key, offset);
  }

  @Override
  public int getDimCount() {

    return bits.getDimCount();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MultiDimKeyVarLengthGenerator) {
      MultiDimKeyVarLengthGenerator other = (MultiDimKeyVarLengthGenerator) obj;
      return bits.equals(other.bits);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return bits.hashCode();
  }

}

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

import java.util.Arrays;

import org.apache.carbondata.core.util.ByteUtil;

public class ByteArrayHolder implements Comparable<ByteArrayHolder> {

  /**
   * mdkey
   */
  private byte[] mdKey;

  /**
   * primary key
   */
  private int primaryKey;

  /**
   * @param mdKey
   * @param primaryKey
   */
  public ByteArrayHolder(byte[] mdKey, int primaryKey) {
    this.mdKey = mdKey;
    this.primaryKey = primaryKey;
  }

  @Override public int compareTo(ByteArrayHolder o) {
    return ByteUtil.compare(mdKey, o.mdKey);
  }

  @Override public boolean equals(Object obj) {
    // TODO Auto-generated method stub
    if (obj instanceof ByteArrayHolder) {
      if (0 == ByteUtil.compare(mdKey, ((ByteArrayHolder) obj).mdKey)) {
        return true;
      }
    }
    return false;
  }

  @Override public int hashCode() {
    int prime = 31;
    int result = prime * Arrays.hashCode(mdKey);
    result = result + prime * primaryKey;
    return result;
  }

  public byte[] getMdKey() {
    return mdKey;
  }

  public int getPrimaryKey() {
    return primaryKey;
  }

}


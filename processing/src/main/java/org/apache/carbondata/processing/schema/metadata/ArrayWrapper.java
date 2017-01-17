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

package org.apache.carbondata.processing.schema.metadata;

import java.io.Serializable;
import java.util.Arrays;

public class ArrayWrapper implements Serializable {

  /**
   * Comment for <code>serialVersionUID</code>
   */
  private static final long serialVersionUID = -2016551342632572869L;

  /**
   * data
   */
  private int[] data;

  public ArrayWrapper(int[] data) {
    if (data == null) {
      throw new IllegalArgumentException();
    }
    this.data = data;
  }

  @Override public boolean equals(Object other) {
    if (other instanceof ArrayWrapper) {
      return Arrays.equals(data, ((ArrayWrapper) other).data);
    } else {
      return false;
    }

  }

  @Override public int hashCode() {
    return Arrays.hashCode(data);
  }

  public int[] getData() {
    return data;
  }

  public void setData(int[] data) {
    this.data = data;
  }
}

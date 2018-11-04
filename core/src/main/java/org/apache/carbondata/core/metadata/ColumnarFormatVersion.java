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

package org.apache.carbondata.core.metadata;

public enum ColumnarFormatVersion {
  V1((short)1),
  V2((short)2),
  V3((short)3);

  private short version;
  ColumnarFormatVersion(short version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "ColumnarFormatV" + version;
  }

  public short number() {
    return version;
  }

  public static ColumnarFormatVersion valueOf(short version) {
    switch (version) {
      case 0:
        // before multiple reader support, for existing carbon file, it is version 1
        return V1;
      case 1:
        // after multiple reader support, user can write new file with version 1
        return V1;
      case 2:
        return V2;
      case 3:
        return V3;
      default:
        throw new UnsupportedOperationException("Unsupported columnar format version: " + version);
    }
  }
}

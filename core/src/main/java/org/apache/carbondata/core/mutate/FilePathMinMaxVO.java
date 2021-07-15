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

package org.apache.carbondata.core.mutate;

import java.io.Serializable;

/**
 * VO class for filePath and Min Max for each blocklet
 */
public class FilePathMinMaxVO implements Serializable {

  private String filePath;

  private byte[] min;

  private byte[] max;

  public FilePathMinMaxVO(String filePath, byte[] min, byte[] max) {
    this.filePath = filePath;
    this.min = min;
    this.max = max;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public byte[] getMin() {
    return min;
  }

  public void setMin(byte[] min) {
    this.min = min;
  }

  public byte[] getMax() {
    return max;
  }

  public void setMax(byte[] max) {
    this.max = max;
  }
}
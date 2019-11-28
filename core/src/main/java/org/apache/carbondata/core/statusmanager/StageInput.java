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

package org.apache.carbondata.core.statusmanager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.hadoop.CarbonInputSplit;

import org.apache.hadoop.mapreduce.InputSplit;

public class StageInput {

  /**
   * the base path of files
   */
  private String base;

  /**
   * the list of (file, length) in this StageInput
   */
  private Map<String, Long> files;

  public StageInput() {

  }

  public StageInput(String base, Map<String, Long> files) {
    this.base = base;
    this.files = files;
  }

  public String getBase() {
    return base;
  }

  public void setBase(String base) {
    this.base = base;
  }

  public Map<String, Long> getFiles() {
    return files;
  }

  public void setFiles(Map<String, Long> files) {
    this.files = files;
  }

  public List<InputSplit> createSplits() {
    return
        files.entrySet().stream().filter(
            entry -> entry.getKey().endsWith(CarbonCommonConstants.FACT_FILE_EXT)
        ).map(
            entry -> CarbonInputSplit.from("-1", "0",
                base + CarbonCommonConstants.FILE_SEPARATOR + entry.getKey(),
                0, entry.getValue(), ColumnarFormatVersion.V3, null)
        ).collect(Collectors.toList());
  }

}

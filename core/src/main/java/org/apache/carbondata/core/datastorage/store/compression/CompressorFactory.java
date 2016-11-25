/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.datastorage.store.compression;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

public class CompressorFactory {

  private static Compressor snappy;
  private static Compressor dummy;

  public static Compressor getInstance() {
    String compressor = CarbonProperties.getInstance().getProperty(CarbonCommonConstants.COMPRESSOR,
        CarbonCommonConstants.DEFAULT_COMPRESSOR);
    switch (compressor) {
      case "snappy":
        if (snappy == null) {
          snappy = new SnappyCompressor();
        }
        return snappy;
      case "dummy":
        if (dummy == null) {
          dummy = new DummyCompressor();
        }
        return dummy;
      default:
        throw new RuntimeException("Unsupported compressor: " + compressor);
    }
  }
}

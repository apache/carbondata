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

package org.apache.carbondata.core.datastore.compression;

import org.apache.carbondata.core.api.CarbonProperties;

public class CompressorFactory {

  private static final CompressorFactory COMPRESSOR_FACTORY = new CompressorFactory();

  private final Compressor snappyCompressor;

  private CompressorFactory() {
    String compressorType = CarbonProperties.COMPRESSOR.getOrDefault();
    switch (compressorType) {
      case "snappy":
        snappyCompressor = new SnappyCompressor();
        break;
      default:
        throw new RuntimeException(
            "Invalid compressor type provided! Please provide valid compressor type");
    }
  }

  public static CompressorFactory getInstance() {
    return COMPRESSOR_FACTORY;
  }

  public Compressor getCompressor() {
    return getCompressor(CarbonProperties.COMPRESSOR.getOrDefault());
  }

  public Compressor getCompressor(String name) {
    if (name.equalsIgnoreCase("snappy")) {
      return snappyCompressor;
    } else {
      throw new UnsupportedOperationException(name + " compressor is not supported");
    }
  }

}

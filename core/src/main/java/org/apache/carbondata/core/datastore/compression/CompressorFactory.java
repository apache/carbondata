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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

public class CompressorFactory {
  private static final CompressorFactory COMPRESSOR_FACTORY = new CompressorFactory();

  private final Map<String, SupportedCompressor> compressors = new HashMap<>();

  public enum SupportedCompressor {
    SNAPPY("snappy", SnappyCompressor.class),
    ZSTD("zstd", ZstdCompressor.class);

    private String name;
    private Class<Compressor> compressorClass;
    private transient Compressor compressor;

    SupportedCompressor(String name, Class compressorCls) {
      this.name = name;
      this.compressorClass = compressorCls;
    }

    public String getName() {
      return name;
    }

    /**
     * we will load the compressor only if it is needed
     */
    public Compressor getCompressor() {
      if (this.compressor == null) {
        try {
          this.compressor = compressorClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException("Exception occurs while getting compressor for " + name);
        }
      }
      return this.compressor;
    }
  }

  private CompressorFactory() {
    for (SupportedCompressor supportedCompressor : SupportedCompressor.values()) {
      compressors.put(supportedCompressor.getName(), supportedCompressor);
    }
  }

  public static CompressorFactory getInstance() {
    return COMPRESSOR_FACTORY;
  }

  /**
   * get the default compressor.
   * This method can only be called in data load procedure to compress column page.
   * In query procedure, we should read the compressor information from the metadata
   * in datafiles when we want to decompress the content.
   */
  public Compressor getCompressor() {
    String compressorType = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.COMPRESSOR, CarbonCommonConstants.DEFAULT_COMPRESSOR);
    if (!compressors.containsKey(compressorType)) {
      throw new UnsupportedOperationException(
          "Invalid compressor type provided! Currently we only support "
              + Arrays.toString(SupportedCompressor.values()));
    }
    return getCompressor(compressorType);
  }

  public Compressor getCompressor(String name) {
    if (compressors.containsKey(name.toLowerCase())) {
      return compressors.get(name.toLowerCase()).getCompressor();
    }
    throw new UnsupportedOperationException(
        name + " compressor is not supported, currently we only support "
            + Arrays.toString(SupportedCompressor.values()));
  }
}

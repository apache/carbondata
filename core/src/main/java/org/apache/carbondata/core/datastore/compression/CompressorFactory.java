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

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class CompressorFactory {
  private static final Logger LOGGER = LogServiceFactory.getLogService(
      CompressorFactory.class.getName());
  private static final CompressorFactory COMPRESSOR_FACTORY = new CompressorFactory();

  private final Map<String, Compressor> allSupportedCompressors = new HashMap<>();

  public enum NativeSupportedCompressor {
    SNAPPY("snappy", SnappyCompressor.class),
    ZSTD("zstd", ZstdCompressor.class),
    GZIP("gzip", GzipCompressor.class);

    private String name;
    private Class<Compressor> compressorClass;
    private transient Compressor compressor;

    NativeSupportedCompressor(String name, Class compressorCls) {
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
          throw new RuntimeException("Exception occurs while getting compressor for " + name, e);
        }
      }
      return this.compressor;
    }
  }

  private CompressorFactory() {
    for (NativeSupportedCompressor nativeSupportedCompressor : NativeSupportedCompressor.values()) {
      allSupportedCompressors.put(nativeSupportedCompressor.getName(),
          nativeSupportedCompressor.getCompressor());
    }
  }

  public static CompressorFactory getInstance() {
    return COMPRESSOR_FACTORY;
  }

  /**
   * register the compressor using reflection.
   * If the class name of the compressor has already been registered before, it will return false;
   * If the reflection fails to work or the compressor name has problem, it will throw
   * RunTimeException; If it is registered successfully, it will return true.
   *
   * @param compressorClassName full class name of the compressor
   * @return true if register successfully, false if failed.
   */
  private Compressor registerColumnCompressor(String compressorClassName) {
    if (allSupportedCompressors.containsKey(compressorClassName)) {
      return allSupportedCompressors.get(compressorClassName);
    }

    Class clazz;
    try {
      clazz = Class.forName(compressorClassName);
      Object instance = clazz.newInstance();
      if (instance instanceof Compressor) {
        if (!((Compressor) instance).getName().equals(compressorClassName)) {
          throw new RuntimeException(String.format("For not carbondata native supported compressor,"
              + " the result of method getName() should be the full class name. Expected '%s',"
              + " found '%s'", compressorClassName, ((Compressor) instance).getName()));
        }
        allSupportedCompressors.put(compressorClassName, (Compressor) instance);
        LOGGER.info(String.format(
            "successfully register compressor %s to carbondata", compressorClassName));
        return (Compressor) instance;
      } else {
        throw new RuntimeException(
            String.format("Compressor '%s' should be a subclass of '%s'",
                compressorClassName, Compressor.class.getCanonicalName()));
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOGGER.error(String.format("Failed to register compressor '%s'", compressorClassName), e);
      throw new RuntimeException(
          String.format("Failed to load compressor '%s', currently carbondata supports %s",
              compressorClassName, StringUtils.join(allSupportedCompressors.keySet(), ", ")), e);
    }
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
    return getCompressor(compressorType);
  }

  public Compressor getCompressor(String name) {
    String internalCompressorName = getInternalCompressorName(name);
    if (null == internalCompressorName) {
      // maybe this is a new compressor, we will try to register it
      return registerColumnCompressor(name);
    } else {
      return allSupportedCompressors.get(internalCompressorName);
    }
  }

  // if we specify the compressor name in table property, carbondata now will convert the
  // property value to lowercase, so here we will ignore the case and find the real name.
  private String getInternalCompressorName(String name) {
    for (String key : allSupportedCompressors.keySet()) {
      if (key.equalsIgnoreCase(name)) {
        return key;
      }
    }
    return null;
  }
}

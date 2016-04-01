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

package org.carbondata.core.carbon.metadata.leafnode.compressor;

/**
 * Wrapper for the encoder and the custom class name for custom encoder.
 */
public class CompressorInfo {

    /**
     * compression type selected to compress the data
     */
    private CompressionCodec compressionType;

    /**
     * class name if compression type selected is custom
     */
    private String customClassName;

    /**
     * @return the compressionType
     */
    public CompressionCodec getCompressionType() {
        return compressionType;
    }

    /**
     * @param compressionType the compressionType to set
     */
    public void setCompressionType(CompressionCodec compressionType) {
        this.compressionType = compressionType;
    }

    /**
     * @return the customClassName
     */
    public String getCustomClassName() {
        return customClassName;
    }

    /**
     * @param customClassName the customClassName to set
     */
    public void setCustomClassName(String customClassName) {
        this.customClassName = customClassName;
    }

}

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
package org.carbondata.core.carbon.metadata.encoder;

import java.io.Serializable;

/**
 * encode class for storing encoding information
 */
public class Encoder implements Serializable {

    /**
     * serialization version
     */
    private static final long serialVersionUID = 6593257193918882905L;

    /**
     * type of encoding selected
     */
    private Encoding encoding;

    /**
     * custom class name when encoding type is custom
     */
    private String customClassName;

    /**
     * @return the encoding
     */
    public Encoding getEncoding() {
        return encoding;
    }

    /**
     * @param encoding the encoding to set
     */
    public void setEncoding(Encoding encoding) {
        this.encoding = encoding;
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

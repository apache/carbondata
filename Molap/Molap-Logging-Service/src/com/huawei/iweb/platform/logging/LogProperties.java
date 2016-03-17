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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2011
 * =====================================
 *
 */

package com.huawei.iweb.platform.logging;

/**
 * For Log Properties.
 * 
 * @author m00902711
 * 
 */
public enum LogProperties {
    /**
     * UserName.
     */
    USER_NAME("USER_NAME"),
    /**
     * Machine IP.
     */
    MACHINE_IP("MACHINE_IP"),
    /**
     * CLIENT IP.
     */
    CLIENT_IP("CLIENT_IP"),
    /**
     * Operations
     */
    OPERATRION("OPERATRION"),
    /**
     * FEATURE.
     */
    FEATURE("FEATURE"),
    /**
     * MODULE.
     */
    MODULE("MODULE"),
    /**
     * TYPE.
     */
    TYPE("TYPE"),
    /**
     * EVENT ID.
     */
    EVENT_ID("EVENT_ID");
    /**
     * key
     */
    private String key;

    /**
     * specifies keys for Log properties.
     * 
     * @param key
     */
    private LogProperties(String key)
    {
        this.key = key;
    }

    /**
     * returns key
     * 
     * @return String
     */
    public String getKey()
    {
        return key;
    }

    /**
     * returns in String format
     * 
     * @return String
     */
    public String toString()
    {
        return key;
    }
}

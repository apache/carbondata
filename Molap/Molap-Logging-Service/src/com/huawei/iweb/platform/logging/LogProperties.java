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

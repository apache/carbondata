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

package com.huawei.iweb.platform.logging.impl;

import java.util.Locale;
import java.util.ResourceBundle;

import com.huawei.iweb.platform.logging.LocaleLogMessageFinder;
import com.huawei.iweb.platform.logging.LogEvent;

/**
 * Locale Based Message finder for the Log Events
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 */
public class ResourceBundleMessageFinder implements LocaleLogMessageFinder
{

    private static final String LOG_BUNDLE_NAME = "LogResource";

    /**
     * constructor
     */
    public ResourceBundleMessageFinder()
    {

    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LocaleLogMessageFinder#findLogEventMessage
     *      (com.huawei.iweb.platform.logging.LogEvent)
     * @param event
     *            events
     * @return String
     */
    public String findLogEventMessage(LogEvent event)
    {
        return findLogEventMessage(Locale.getDefault(), event);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LocaleLogMessageFinder#findLogEventMessage
     *      (java.util.Locale, com.huawei.iweb.platform.logging.LogEvent)
     * @param locale
     *            for Internationalization
     * @param event
     *            for log events
     * @return String
     * 
     */
    public String findLogEventMessage(Locale locale, LogEvent event)
    {
        String message = null;
        try
        {

            String location = event.getModuleName() + LOG_BUNDLE_NAME;
            ResourceBundle bundle = ResourceBundle.getBundle(location, locale,
                    event.getClass().getClassLoader());
            message = bundle.getString(event.getEventCode());

        }
        catch(NullPointerException e)
        {
            return null;
        }
        return message;
    }
}
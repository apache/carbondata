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

import java.util.Locale;

/**
 * Log based on Locale.
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 */
public interface LocaleLogMessageFinder
{

    /**
     * return Log Event Message
     * 
     * @param event
     *            log event.
     * @return String
     */
    String findLogEventMessage(LogEvent event);

    /**
     * returns log event message based on Locale.
     * 
     * @param locale
     *            based on Locale.
     * @param event
     *            based on event.
     * @return String
     */
    String findLogEventMessage(Locale locale, LogEvent event);

}
/**
 * 
 * Copyright Notice ===================================== This file contains
 * proprietary information of Huawei Technologies India Pvt Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2011
 * =====================================
 * 
 */
package com.huawei.iweb.platform.logging;

/**
 * for Log Services
 * 
 * @author R72411
 * @version 1.0
 * @created 08-Oct-2008 10:37:40
 */
public interface LogService
{

    /**
     * Tells if the debug mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isDebugEnabled();

    /**
     * Tells if the INFO mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isInfoEnabled();

    /**
     * Tells if the WARN mode of the logger is enabled
     * 
     * @return boolean
     * 
     */
    boolean isWarnEnabled();

    /**
     * This method has been deprecated. use method <code>info</code> instead of
     * audit
     * 
     * @param event events
     * @param inserts for inserts
     */
    void debug(LogEvent event , Object... inserts);

    /**
     * for debugs
     * 
     * @param event events
     * @param throwable throwable
     * @param inserts for inserts
     */
    void debug(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for Errors.
     * 
     * @param event events
     * @param inserts inserts
     */
    void error(LogEvent event , Object... inserts);

    /**
     * for errors and exception.
     * 
     * @param event events
     * @param throwable throwable.
     * @param inserts inserts.
     */
    void error(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for Info
     * 
     * @param event events.
     * @param inserts inserts.
     */
    void info(LogEvent event , Object... inserts);

    /**
     * for Info
     * 
     * @param event events
     * @param throwable throwable
     * @param inserts inserts
     */
    void info(LogEvent event , Throwable throwable , Object... inserts);

    /**
     * for Secure Logs
     * 
     * @param event events
     * @param inserts inserts
     */
    void secure(LogEvent event , Object... inserts);

    /**
     * for audit
     * 
     * @param event events
     * @param inserts inserts
     */
    void audit(LogEvent event , Object... inserts);

    /**
     * for audit
     * 
     * @param msg Message
     */
    void audit(String msg);

    /**
     * for warn
     * 
     * @param event events
     * @param inserts inserts
     * 
     */
    void warn(LogEvent event , Object... inserts);

    /**
     * Added for alarm message
     * 
     * @param msg
     * 
     */
    void alarm(String msg);

}

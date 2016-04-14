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

package org.carbondata.common.logging.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.carbondata.common.logging.*;

/**
 * Default Implementation of the <code>LogService</code>
 */
public final class StandardLogService implements LogService {
    /**
     * for Resource Bundleing
     */
    private static final LocaleLogMessageFinder MESSAGE_RESOLVER =
            new ResourceBundleMessageFinder();
    private static final String QUERY_ID = "queryID:";
    private static final String PARTITION_ID = "[partitionID:";
    private static final String CARBON_AUDIT_LOG_PATH = "carbon.auditlog.file.path";
    private static final String AUDIT_LOG_DEFAULT_PATH = "logs/CarbonAudit.log";
    private static final String CARBON_AUDIT_LOG_ROLLING_UP_SIZE = "carbon.auditlog.max.file.size";
    private static final String AUDIT_LOG_DEFAULT_ROLLING_UP_SIZE = "10MB";
    private static final String CARBON_AUDIT_LOG_MAX_BACKUP = "carbon.auditlog.max.backup.files";
    private static final String AUDIT_LOG_DEFAULT_MAX_BACKUP = "10";
    private static final String CARBON_AUDIT_LOG_LEVEL = "carbon.logging.level";
    private static final String AUDIT_LOG_DEFAULT_LEVEL = "INFO";
    private static boolean doLog = true;
    private Logger logger;

    /**
     * Constructor.
     *
     * @param clazzName for which the Logging is required
     */
    public StandardLogService(String clazzName) {
        String auditLogPath = AUDIT_LOG_DEFAULT_PATH;
        String rollupSize = AUDIT_LOG_DEFAULT_ROLLING_UP_SIZE;
        String maxBackup = AUDIT_LOG_DEFAULT_MAX_BACKUP;
        String logLevel = AUDIT_LOG_DEFAULT_LEVEL;

        Properties props = new Properties();
        Properties carbonProps = FileUtil.getCarbonProperties();

        if (null != carbonProps) {
            if (null != carbonProps.getProperty(CARBON_AUDIT_LOG_PATH)) {
                auditLogPath = carbonProps.getProperty(CARBON_AUDIT_LOG_PATH);
            }

            if (null != carbonProps.getProperty(CARBON_AUDIT_LOG_ROLLING_UP_SIZE)) {
                rollupSize = carbonProps.getProperty(CARBON_AUDIT_LOG_ROLLING_UP_SIZE);
            }

            if (null != carbonProps.getProperty(CARBON_AUDIT_LOG_MAX_BACKUP)) {
                maxBackup = carbonProps.getProperty(CARBON_AUDIT_LOG_MAX_BACKUP);
            }

            if (null != carbonProps.getProperty(CARBON_AUDIT_LOG_LEVEL)) {
                logLevel = carbonProps.getProperty(CARBON_AUDIT_LOG_LEVEL);
            }
        }

        props.setProperty("log4j.rootLogger", logLevel + ",stdout,AUDL");

        props.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d %-5p [%c] %m%n");
        props.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.AUDL",
                "org.carbondata.common.logging.AuditExtendedRollingFileAppender");

        props.setProperty("log4j.appender.AUDL.File", auditLogPath);
        props.setProperty("log4j.appender.AUDL.threshold",
                "AUDIT#org.carbondata.common.logging.AuditLevel");
        props.setProperty("log4j.appender.AUDL.layout.ConversionPattern",
                "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n");
        props.setProperty("log4j.appender.AUDL.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.AUDL.MaxFileSize", rollupSize);
        props.setProperty("log4j.appender.AUDL.MaxBackupIndex", maxBackup);

        props.setProperty("log4j.logger.com.huawei", logLevel + ",stdout");
        props.setProperty("log4j.logger.com.huawei", logLevel + ",AUDL");

        /*PropertyConfigurator.configure(props);*/
        logger = Logger.getLogger(clazzName);

    }

    public StandardLogService() {
        this("Carbon");
    }

    /**
     * returns is DO Log
     *
     * @return the doLog
     */
    public static boolean isDoLog() {
        return doLog;
    }

    /**
     * set Do Log
     *
     * @param doLog the doLog to set
     */
    public static void setDoLog(boolean doLog) {
        StandardLogService.doLog = doLog;
    }

    public static String getPartitionID(String cubeName) {
        return cubeName.substring(cubeName.lastIndexOf('_') + 1, cubeName.length());
    }

    public static void setThreadName(String partitionID, String queryID) {
        StringBuffer b = new StringBuffer(PARTITION_ID);
        b.append(partitionID);
        if (null != queryID) {
            b.append(";queryID:");
            b.append(queryID);
        }
        b.append("]");
        Thread.currentThread().setName(getThreadName() + b.toString());
    }

    private static String getThreadName() {
        String name = Thread.currentThread().getName();
        int index = name.indexOf(PARTITION_ID);
        if (index > -1) {
            name = name.substring(0, index);
        } else {
            name = '[' + name + ']';
        }
        return name.trim();
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isWarnEnabled() {
        return logger.isEnabledFor(org.apache.log4j.Level.WARN);
    }

    public void audit(LogEvent event, Object... inserts) {
        logMessage(Level.AUDIT, event, null, inserts);
    }

    public void debug(LogEvent event, Object... inserts) {
        if (logger.isDebugEnabled()) {
            logMessage(Level.DEBUG, event, null, inserts);
        }
    }

    public void debug(LogEvent event, Throwable throwable, Object... inserts) {

        if (logger.isDebugEnabled()) {
            logMessage(Level.DEBUG, event, throwable, inserts);
        }
    }

    public void error(LogEvent event, Object... inserts) {
        logMessage(Level.ERROR, event, null, inserts);
    }

    public void error(LogEvent event, Throwable throwable, Object... inserts) {
        logMessage(Level.ERROR, event, throwable, inserts);
    }

    public void info(LogEvent event, Object... inserts) {
        if (logger.isInfoEnabled()) {
            logMessage(Level.INFO, event, null, inserts);
        }
    }

    public void info(LogEvent event, Throwable throwable, Object... inserts) {
        if (logger.isInfoEnabled()) {
            logMessage(Level.INFO, event, null, inserts);
        }
    }

    /**
     * Utility Method to log the the Message.
     *
     * @param logLevel  level for which logging is required.
     * @param event     Logevent of this Log
     * @param throwable
     * @param inserts
     */
    private void logMessage(Level logLevel, LogEvent event, Throwable throwable,
            Object... inserts) {
        if (StandardLogService.doLog) {
            try {
                String message = getMessage(event);
                if (null == message) {
                    message = createMessageForMissingEntry(event, inserts);
                } else {
                    message = String.format(message, inserts);
                    message = MessageFormat.format(message, inserts);
                }
                //Append the partition id and query id if exist
                StringBuffer buff = new StringBuffer(Thread.currentThread().getName());
                buff.append(" ");
                buff.append(message);
                message = buff.toString();
                if (Level.ERROR.toString().equalsIgnoreCase(logLevel.toString())) {
                    logErrorMessage(throwable, message);
                } else if (Level.DEBUG.toString().equalsIgnoreCase(logLevel.toString())) {
                    logDebugMessage(throwable, message);
                } else if (Level.INFO.toString().equalsIgnoreCase(logLevel.toString())) {
                    logInfoMessage(throwable, message);
                } else if (Level.WARN.toString().equalsIgnoreCase(logLevel.toString())) {
                    logWarnMessage(throwable, message);
                } else if (Level.AUDIT.toString().equalsIgnoreCase(logLevel.toString())) {
                    audit(message);
                }

            } catch (Throwable t) {
                logger.error(t);
            }
        }
    }

    private void logErrorMessage(Throwable throwable, String message) {

        if (null == throwable) {
            logger.error(message);
        } else {
            logger.error(message, throwable);
        }
    }

    private void logInfoMessage(Throwable throwable, String message) {

        if (null == throwable) {
            logger.info(message);
        } else {
            logger.info(message, throwable);
        }
    }

    private void logDebugMessage(Throwable throwable, String message) {

        if (null == throwable) {
            logger.debug(message);
        } else {
            logger.debug(message, throwable);
        }
    }

    private void logWarnMessage(Throwable throwable, String message) {

        if (null == throwable) {
            logger.warn(message);
        } else {
            logger.warn(message, throwable);
        }
    }

    private String getMessage(LogEvent event) {
        String message = MESSAGE_RESOLVER.findLogEventMessage(event);
        return message;
    }

    private String createMessageForMissingEntry(LogEvent event, Object... inserts) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("A message for key ");
        messageBuilder.append(event.getEventCode());
        messageBuilder.append(" could not be found.");

        if (inserts != null && inserts.length > 0) {
            messageBuilder.append(" The supplied inserts were: ");

            for (int i = 0; i < inserts.length; i++) {
                messageBuilder.append(inserts[i]);

                if (i < inserts.length - 1) {
                    messageBuilder.append(", ");
                }
            }
        }
        return messageBuilder.toString();
    }

    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    public void warn(LogEvent event, Object... inserts) {
        if (isWarnEnabled()) {
            logMessage(Level.WARN, event, null, inserts);
        }
    }

    public void setEventProperties(String propertyName, String propertyValue) {
        MDC.put(propertyName, propertyValue);
    }

    /**
     * log audit log
     *
     * @param msg audit log message
     */
    @Override
    public void audit(String msg) {
        String hostName = "";

        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostName = "localhost";
        }

        logger.log(AuditLevel.AUDIT, "[" + hostName + "]" + msg);
    }

}

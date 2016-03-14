/**
 * 
 * Copyright Notice ===================================== This file contains
 * proprietary information of Huawei Technologies India Pvt Ltd. Copying or
 * reproduction without prior written approval is prohibited. Copyright (c) 2011
 * =====================================
 * 
 */

package com.huawei.iweb.platform.logging.impl;

import static com.huawei.iweb.platform.logging.Level.AUDIT;
import static com.huawei.iweb.platform.logging.Level.DEBUG;
import static com.huawei.iweb.platform.logging.Level.ERROR;
import static com.huawei.iweb.platform.logging.Level.INFO;
import static com.huawei.iweb.platform.logging.Level.SECURE;
import static com.huawei.iweb.platform.logging.Level.WARN;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.huawei.iweb.platform.logging.AlarmLevel;
import com.huawei.iweb.platform.logging.AuditLevel;
import com.huawei.iweb.platform.logging.Level;
import com.huawei.iweb.platform.logging.LocaleLogMessageFinder;
import com.huawei.iweb.platform.logging.LogEvent;
import com.huawei.iweb.platform.logging.LogService;

/**
 * Default Implementation of the <code>LogService</code>
 * 
 * @author S72079
 * @version 1.0
 * @created 17-Oct-2015 10:37:41
 */
public final class StandardLogService implements LogService
{
    /**
     * for Resource Bundleing
     */
    private static final LocaleLogMessageFinder MESSAGE_RESOLVER = new ResourceBundleMessageFinder();

    private static boolean doLog = true;

    private Logger logger;
    
    private static final String QUERY_ID = "queryID:";

    private static final String PARTITION_ID = "[partitionID:";
    
    private static final String CARBON_AUDIT_LOG_PATH = "carbon.auditlog.file.path";
    
    private static final String AUDIT_LOG_DEFAULT_PATH = "logs/MolapAudit.log";
    
    private static final String CARBON_AUDIT_LOG_ROLLING_UP_SIZE = "carbon.auditlog.max.file.size";
    
    private static final String AUDIT_LOG_DEFAULT_ROLLING_UP_SIZE = "10MB";
    
    private static final String CARBON_AUDIT_LOG_MAX_BACKUP = "carbon.auditlog.max.backup.files";
    
    private static final String AUDIT_LOG_DEFAULT_MAX_BACKUP = "10";
    
    private static final String CARBON_AUDIT_LOG_LEVEL = "carbon.logging.level";
    
    private static final String AUDIT_LOG_DEFAULT_LEVEL = "INFO";

    /**
     * Constructor.
     * 
     * @param clazzName for which the Logging is required
     */
    public StandardLogService(String clazzName)
    {
    	String auditLogPath = AUDIT_LOG_DEFAULT_PATH;
    	String rollupSize = AUDIT_LOG_DEFAULT_ROLLING_UP_SIZE;
    	String maxBackup = AUDIT_LOG_DEFAULT_MAX_BACKUP;
    	String logLevel = AUDIT_LOG_DEFAULT_LEVEL;
    	
    	Properties props = new Properties();
    	Properties molapProps = FileUtil.getMolapProperties();
    	
    	if(null != molapProps)
    	{
    		if(null != molapProps.getProperty(CARBON_AUDIT_LOG_PATH))
    		{
    			auditLogPath = molapProps.getProperty(CARBON_AUDIT_LOG_PATH);
    		}
    		
    		if(null != molapProps.getProperty(CARBON_AUDIT_LOG_ROLLING_UP_SIZE))
    		{
    			rollupSize = molapProps.getProperty(CARBON_AUDIT_LOG_ROLLING_UP_SIZE);
    		}
    		
    		if(null != molapProps.getProperty(CARBON_AUDIT_LOG_MAX_BACKUP))
    		{
    			maxBackup = molapProps.getProperty(CARBON_AUDIT_LOG_MAX_BACKUP);
    		}
    		
    		if(null != molapProps.getProperty(CARBON_AUDIT_LOG_LEVEL))
    		{
    			logLevel = molapProps.getProperty(CARBON_AUDIT_LOG_LEVEL);
    		}
    	}
    	
    	props.setProperty("log4j.rootLogger", logLevel+",stdout,AUDL");
    	
    	props.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
    	props.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d %-5p [%c] %m%n");
    	props.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
    	props.setProperty("log4j.appender.AUDL", "com.huawei.iweb.platform.logging.AuditExtendedRollingFileAppender");
    	
    	props.setProperty("log4j.appender.AUDL.File", auditLogPath);
    	props.setProperty("log4j.appender.AUDL.threshold", "AUDIT#com.huawei.iweb.platform.logging.AuditLevel");
    	props.setProperty("log4j.appender.AUDL.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n");
    	props.setProperty("log4j.appender.AUDL.layout", "org.apache.log4j.PatternLayout");
    	props.setProperty("log4j.appender.AUDL.MaxFileSize", rollupSize);
    	props.setProperty("log4j.appender.AUDL.MaxBackupIndex", maxBackup);
    	
    	props.setProperty("log4j.logger.com.huawei", logLevel+",stdout");
    	props.setProperty("log4j.logger.com.huawei", logLevel+",AUDL");
    	
    	/*PropertyConfigurator.configure(props);*/
    	logger = Logger.getLogger(clazzName);
    	
    }
    
    public StandardLogService()
    {
    	this("Carbon");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#isDebugEnabled()
     */
    public boolean isDebugEnabled()
    {
        return logger.isDebugEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#isDebugEnabled()
     */
    public boolean isWarnEnabled()
    {
        return logger.isEnabledFor(org.apache.log4j.Level.WARN);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#secure(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * @param event events
     * @param inserts inserts
     */
    public void secure(LogEvent event , Object... inserts)
    {
        logMessage(SECURE , event , null , inserts);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#audit(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * 
     * @param event events
     * @param inserts inserts
     */
    public void audit(LogEvent event , Object... inserts)
    {
        logMessage(AUDIT , event , null , inserts);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#debug(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * @param event events
     * @param inserts inserts
     * 
     */
    public void debug(LogEvent event , Object... inserts)
    {

        if (logger.isDebugEnabled())
        {
            logMessage(DEBUG , event , null , inserts);
        }

    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#debug(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Throwable, java.lang.Object[])
     * 
     * @param event events
     * @param throwable exception to be logged
     * @param inserts inserts
     * 
     */
    public void debug(LogEvent event , Throwable throwable , Object... inserts)
    {

        if (logger.isDebugEnabled())
        {
            logMessage(DEBUG , event , throwable , inserts);
        }
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#error(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * 
     * @param event events
     * @param inserts inserts
     * 
     */
    public void error(LogEvent event , Object... inserts)
    {
        logMessage(ERROR , event , null , inserts);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#error(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Throwable, java.lang.Object[])
     * @param event events
     * @param throwable exception to be logged
     * @param inserts inserts
     */
    public void error(LogEvent event , Throwable throwable , Object... inserts)
    {
        logMessage(ERROR , event , throwable , inserts);
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#info(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * @param event events
     * @param inserts inserts
     */
    public void info(LogEvent event , Object... inserts)
    {
        if (logger.isInfoEnabled())
        {
            logMessage(INFO , event , null , inserts);
        }
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#info(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Throwable, java.lang.Object[])
     * @param event events
     * @param throwable exception to be logged
     * @param inserts inserts
     */
    public void info(LogEvent event , Throwable throwable , Object... inserts)
    {
        if (logger.isInfoEnabled())
        {
            logMessage(INFO , event , null , inserts);
        }
    }

    /**
     * Utility Method to log the the Message.
     * 
     * @param logLevel level for which logging is required.
     * @param event Logevent of this Log
     * @param throwable
     * @param inserts
     */
    private void logMessage(Level logLevel , LogEvent event ,
            Throwable throwable , Object... inserts)
    {

        if (StandardLogService.doLog)
        {
            try
            {
                String message = getMessage(event);
                if (null == message)
                {
                    message = createMessageForMissingEntry(event , inserts);
                }
                else
                {
                    message = String.format(message , inserts);
                    message = MessageFormat.format(message , inserts);
                }
                //Append the partition id and query id if exist
                StringBuffer buff= new StringBuffer(Thread.currentThread().getName());
                buff.append(" ");
                buff.append(message);
                message=buff.toString();
                if (ERROR.toString().equalsIgnoreCase(logLevel.toString()))
                {
                    logErrorMessage(throwable , message);
                }
                else if (DEBUG.toString().equalsIgnoreCase(logLevel.toString()))
                {
                    logDebugMessage(throwable , message);
                }
                else if (INFO.toString().equalsIgnoreCase(logLevel.toString()))
                {
                    logInfoMessage(throwable , message);
                }
                else if (WARN.toString().equalsIgnoreCase(logLevel.toString()))
                {
                    logWarnMessage(throwable , message);
                }
                else if (AUDIT.toString().equalsIgnoreCase(logLevel.toString()))
                {
                    audit(message);
                }

            }
            catch (Throwable t)
            {
                // t.printStackTrace();
                logger.error(t);
            }
        }
    }

    /**
     * 
     * @Description : logErrorMessage
     * @param throwable
     * @param message
     */
    private void logErrorMessage(Throwable throwable , String message)
    {

        if (null == throwable)
        {
            logger.error(message);
        }
        else
        {
            logger.error(message , throwable);
        }
    }

    /**
     * 
     * @Description : logDebugMessage
     * @param throwable
     * @param message
     */
    private void logInfoMessage(Throwable throwable , String message)
    {

        if (null == throwable)
        {
            logger.info(message);
        }
        else
        {
            logger.info(message , throwable);
        }
    }

    /**
     * 
     * @Description : logDebugMessage
     * @param throwable
     * @param message
     */
    private void logDebugMessage(Throwable throwable , String message)
    {

        if (null == throwable)
        {
            logger.debug(message);
        }
        else
        {
            logger.debug(message , throwable);
        }
    }


    /**
     * 
     * @Description : logWarnMessage
     * @param throwable
     * @param message
     */
    private void logWarnMessage(Throwable throwable , String message)
    {

        if (null == throwable)
        {
            logger.warn(message);
        }
        else
        {
            logger.warn(message , throwable);
        }
    }

    /**
     * return Message
     * 
     * @param event
     * @return String
     */
    private String getMessage(LogEvent event)
    {
        String message = MESSAGE_RESOLVER.findLogEventMessage(event);
        return message;
    }

    /**
     * returns Message created for Event
     * 
     * @param event
     * @param inserts
     * @return
     */
    private String createMessageForMissingEntry(LogEvent event ,
            Object... inserts)
    {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("A message for key ");
        messageBuilder.append(event.getEventCode());
        messageBuilder.append(" could not be found.");

        if (inserts != null && inserts.length > 0)
        {
            messageBuilder.append(" The supplied inserts were: ");

            for (int i = 0; i < inserts.length; i++)
            {
                messageBuilder.append(inserts[i]);

                if (i < inserts.length - 1)
                {
                    messageBuilder.append(", ");
                }
            }
        }
        return messageBuilder.toString();
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#isInfoEnabled()
     * @return boolean
     */
    public boolean isInfoEnabled()
    {
        return logger.isInfoEnabled();
    }

    /**
     * (non-Javadoc)
     * 
     * @see com.huawei.iweb.platform.logging.LogService#warn(com.huawei.iweb.platform
     *      .logging.LogEvent, java.lang.Object[])
     * @param event events
     * @param inserts ..
     */
    public void warn(LogEvent event , Object... inserts)
    {
        if (isWarnEnabled())
        {
            logMessage(WARN , event , null , inserts);
        }
    }

    /**
     * sets Events Properties.
     * 
     * @param propertyName property Name
     * @param propertyValue property Value
     */
    public void setEventProperties(String propertyName , String propertyValue)
    {
        MDC.put(propertyName , propertyValue);
    }

    /**
     * returns is DO Log
     * 
     * @return the doLog
     */
    public static boolean isDoLog()
    {
        return doLog;
    }

    /**
     * set Do Log
     * 
     * @param doLog the doLog to set
     */
    public static void setDoLog(boolean doLog)
    {
        StandardLogService.doLog = doLog;
    }

    /**
     * log audit log
     * 
     * @param msg audit log message
     */
    @Override
    public void audit(String msg)
    {
    	String hostName = "";
    	
    	try
		{
			hostName = InetAddress.getLocalHost().getHostName();
		} 
    	catch (UnknownHostException e)
		{
    		hostName = "localhost";
		}
    	
        logger.log(AuditLevel.AUDIT , "[" + hostName + "]" + msg);
    }


    /**
     * 
     * @see com.huawei.iweb.platform.logging.LogService#audit(java.lang.String)
     * 
     */
    @Override
    public void alarm(String msg)
    {
        logger.log(AlarmLevel.ALARM , msg);
    }
    public static String getPartitionID(String cubeName)
    {
        return cubeName.substring(cubeName.lastIndexOf('_')+1,cubeName.length());
    }
    
    public static void setThreadName(String partitionID,String queryID)
    {
        StringBuffer b= new StringBuffer(PARTITION_ID);
        b.append(partitionID);
        if(null!=queryID)
        {
            b.append(";queryID:");
            b.append(queryID);
        }
        b.append("]");
        Thread.currentThread().setName(getThreadName()+b.toString());
    }

    private static String getThreadName()
    {
       String name=Thread.currentThread().getName();
       int index=name.indexOf(PARTITION_ID);
       if(index>-1)
       {
           name=name.substring(0,index);
       }
       else
       {
    	   name='['+name+']';
       }
        return name.trim();
    }
    
}

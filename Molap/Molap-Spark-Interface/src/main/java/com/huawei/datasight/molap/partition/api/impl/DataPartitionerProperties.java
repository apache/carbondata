/**
 * 
 */
package com.huawei.datasight.molap.partition.api.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.huawei.datasight.molap.spark.util.MolapSparkInterFaceLogEvent;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;

/**
 * @author R00900208
 *
 */
public final class DataPartitionerProperties {
	 private static final LogService LOGGER = LogServiceFactory
	            .getLogService(DataPartitionerProperties.class.getName());
	
	private static DataPartitionerProperties instance;
	
	private Properties properties;
	
	private DataPartitionerProperties()
	{
		properties = loadProperties();
	}
	
	public static DataPartitionerProperties getInstance()
	{
		if(instance == null)
		{
			instance = new DataPartitionerProperties();
		}
		return instance;
	}
	
	public String getValue(String key, String defaultVal)
	{
		return properties.getProperty(key, defaultVal);
	}
	
	public String getValue(String key)
	{
		return properties.getProperty(key);
	}

    /**
     * 
     * Read the properties from CSVFilePartitioner.properties
     */
    private Properties loadProperties()
    {
        Properties props = new Properties();

        File file = new File("DataPartitioner.properties");
        FileInputStream fis = null;
        try
        {
            if(file.exists())
            {
                fis = new FileInputStream(file);

                props.load(fis);
            }
        }
        catch(Exception e)
        {
//            e.printStackTrace();
           	LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());
          
        } 
        finally
        {
            if(null != fis)
            {
                try
                {
                    fis.close();
                }
                catch(IOException e)
                {
//                    e.printStackTrace();
                   	LOGGER.error(MolapSparkInterFaceLogEvent.UNIBI_MOLAP_SPARK_INTERFACE_MSG, e, e.getMessage());
                    
                }
            }
        }
        
        return props;

    }
}

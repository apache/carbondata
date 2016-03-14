package com.huawei.unibi.molap.engine.util;

//import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.columnar.TIMESTAMP;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.olap.SqlStatement;
import com.huawei.unibi.molap.util.MolapProperties;

public final class DataTypeConverter
{
    
    private DataTypeConverter()
    {
        
    }
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(DataTypeConverter.class.getName());
    
    public static Object getDataBasedOnDataType(String data, SqlStatement.Type dataType)
    {
        
        if(null==data)
        {
            return null;
        }
        try
        {
        switch(dataType)
        {
            case INT:
                if(data.isEmpty())
                {
                    return null;
                }
                return Integer.parseInt(data);
            case DOUBLE:
                if(data.isEmpty())
                {
                    return null;
                }
                return Double.parseDouble(data);
            case LONG:
                if(data.isEmpty())
                {
                    return null;
                }
                return Long.parseLong(data);
            case BOOLEAN:
                if(data.isEmpty())
                {
                    return null;
                }
                return Boolean.parseBoolean(data);
            case TIMESTAMP:
                if(data.isEmpty())
                {
                    return null;
                }
                SimpleDateFormat parser = new SimpleDateFormat(MolapProperties.getInstance().
                        getProperty(MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT,
                                MolapCommonConstants.MOLAP_TIMESTAMP_DEFAULT_FORMAT));
                Date dateToStr;
                try
                {
                    dateToStr = parser.parse(data);
                    return dateToStr.getTime()*1000;
                }
                catch(ParseException e)
                {
                    LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Cannot convert" + TIMESTAMP.toString() + " to Time/Long type value"+e.getMessage());
                    return null;
                }
            default:
                return data;
        }
       }
        catch(NumberFormatException ex)
        {
//            if(data.isEmpty())
//            {
//                return null;
//            }
//            else
//            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while converting data type"+data);
                return null;
//            }
        }

    }
    
}

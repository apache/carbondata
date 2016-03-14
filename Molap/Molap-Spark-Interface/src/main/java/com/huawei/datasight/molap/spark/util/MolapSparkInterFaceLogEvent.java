package com.huawei.datasight.molap.spark.util;

import com.huawei.iweb.platform.logging.LogEvent;

public enum MolapSparkInterFaceLogEvent implements LogEvent {
	
	UNIBI_MOLAP_SPARK_INTERFACE_MSG("molap.Spark.Interface");
	
	private String eventCode;

    private MolapSparkInterFaceLogEvent(final String eventCode)
    {
        this.eventCode = eventCode;
    }

	@Override
	public String getEventCode() 
	{
		return eventCode;
	}

	@Override
	public String getModuleName()
	{
		return "MOLAP_SPARK_INTERFACE";
	}

}

package com.huawei.datasight.molap.datastats.load; 

import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;

import com.huawei.datasight.molap.datastats.load.FactDataHandler;
import com.huawei.datasight.molap.datastats.load.LevelMetaInfo;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;

public class FactDataHandlerTest
{

	@Test
	public void testInitialise_falseAggKeyBlock()
	{
		MolapProperties.getInstance().addProperty( MolapCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK, "false");
		MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY, "false");
		LevelMetaInfo levelInfo=new MockUp<LevelMetaInfo>()
		{
			@Mock
			public int[] getDimCardinality()
			{
				return new int[]{10,12};
			}
			
		}.getMockInstance();
		
		Cube cube=new MockUp<Cube>()
		{
			@Mock
			public String getCubeName()
			{
				return "default_test";
			}
			@Mock
			public String getSchemaName()
			{
				return "default";
			}
			@Mock
			public String getMode()
			{
				return "store";
				
			}
			@Mock
			public String getFactTableName()
			{
				return "factTable";
			}
			
		}.getMockInstance();
		
		FactDataHandler factHandler=new FactDataHandler(cube,levelInfo, "factTable", 0, null);
	}
	@Test
	public void testInitialise_trueAggKeyBlock_TestHighCardinality()
	{
		MolapProperties.getInstance().addProperty( MolapCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK, "true");
		LevelMetaInfo levelInfo=new MockUp<LevelMetaInfo>()
				{
					@Mock
					public int[] getDimCardinality()
					{
						return new int[]{1000010,12};
					}
					
				}.getMockInstance();
				
				Cube cube=new MockUp<Cube>()
				{
					@Mock
					public String getCubeName()
					{
						return "default_test";
					}
					@Mock
					public String getSchemaName()
					{
						return "default";
					}
					@Mock
					public String getMode()
					{
						return "store";
						
					}
					@Mock
					public String getFactTableName()
					{
						return "factTable";
					}
					
				}.getMockInstance();
				
				FactDataHandler factHandler=new FactDataHandler(cube,levelInfo, "factTable", 0, null);
	}
}

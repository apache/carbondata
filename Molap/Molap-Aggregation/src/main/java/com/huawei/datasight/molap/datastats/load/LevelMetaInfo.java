package com.huawei.datasight.molap.datastats.load;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

/**
 * This class will have information about level metadata
 * @author A00902717
 *
 */
public class LevelMetaInfo
{
	
	
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(LevelMetaInfo.class.getName());
    
    private int[] dimCardinality;
    
	public LevelMetaInfo(MolapFile file,String tableName)
	{
		initialise(file,tableName);
	}
	private void initialise(MolapFile file,final String tableName)
	{
		
		if (file.isDirectory())
		{
			MolapFile[] files = file.listFiles(new MolapFileFilter()
			{
				public boolean accept(MolapFile pathname)
				{
					return (!pathname.isDirectory())
							&& pathname
									.getName()
									.startsWith(
											MolapCommonConstants.LEVEL_METADATA_FILE)
							&& pathname.getName().endsWith(
									tableName + ".metadata");
				}

			});
			try
			{
				dimCardinality = MolapUtil
						.getCardinalityFromLevelMetadataFile(files[0]
								.getAbsolutePath());
			}
			catch (MolapUtilException e)
			{
				LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
			}
		}
		
	}
	public int[] getDimCardinality()
	{
		return dimCardinality;
	}
	
	

}

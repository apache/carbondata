package com.huawei.unibi.molap;
import junit.framework.TestCase;

import com.huawei.unibi.molap.api.dataloader.DataLoadModel;




/**
 * @author S71730
 *
 */
public class DataLoadModelTest extends TestCase 
{
	
	public void testgetCubeName()
	{
		DataLoadModel model= new DataLoadModel();
		model.isCsvLoad();
	}
	

}

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnElYTj28oj1SbubylISpmjDIU8J3SoEzaxxBwHcKUI/LgRpyc9O35pSLk7yZQzvS8RFg
jcQJXhDij4pcwSPbH65reZYoRurra+6wx5PTIXHvOcxG5Uj6h9fBK2CguqBjjw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.etl;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : ETLGraphExecuter.java
 * Class Description : 
 * Version 1.0
 */
public class ETLGraphExecuter {


	/**
	 *  graph transformation object
	 */
    private Trans trans;
	
	/**
     * 
     * Comment for <code>LOGGER</code>
     * 
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(ETLGraphExecuter.class.getName());
    
    /**
     * executeGraph which generate the kettle graph
     * @param graphFile
     * @throws DataLoadingException
     */
	public void executeGraph(final String graphFile) throws DataLoadingException{
		TransMeta transMeta = null;
		// initialize environment variables
	    try {
	    	KettleEnvironment.init(false);
	    	LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Kettle environment initialized");
	    } catch (KettleException ke) {
	    	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Unable to initialize Kettle Environment "+ke.getMessage());
	    }	    
  
	    
            try {
				transMeta = new TransMeta(graphFile);
					        transMeta.setFilename(graphFile);
            trans = new Trans(transMeta);
            trans.setLogLevel(LogLevel.NOTHING);
            trans.execute(null);
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph execution is started "+graphFile);
            
            }catch (KettleXMLException e) {
            	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Unable to start execution of graph "+e.getMessage());
            	throw new DataLoadingException("Unable to start execution of graph ",e);
				
			}catch (KettleException e)
			{
				LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, "Unable to start execution of graph "+e.getMessage());
				throw new DataLoadingException("Unable to start execution of graph ",e);
			}
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Waiting for graph execution to finish");            
            trans.waitUntilFinished();
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph execution is finished.");
           
            
            if (trans.getErrors() > 0)
            {
            	LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph Execution had errors");
            	throw new DataLoadingException();
            }                  
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph execution task is over with No error.");
	}
	
	/**
	 *  Interrupts all child threads run by kettle to execute the graph
	 */
	public void interruptGraphExecution()
	{
		LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph Execution is interrupted");
		if(null != trans)
		{
			trans.killAll();
			LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Graph execution steps are killed.");
		}
	}
}

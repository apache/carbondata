/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBkxA9lJLJrfYxl51J4L2LTfZUXhtjP5zb/nAIHJGsIfWp+6AUFfjVBEM/5UfABx2C6nI
RjDMnp23psFgTiNcXfWNAYb0unfXrC9krL3zweVC7MpN1gPXHpOSWwMkAljKOA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage;

import java.util.HashSet;
import java.util.Set;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;

/**
 * Maintains a list of queries on which are working on the given slice. Once all
 * the queries execution finished, informs the cube store to clear the cache for
 * the slice.
 * 
 * @author K00900207
 * 
 */
public class SliceListener 
{
	/**
	 * On which slice this is working on 
	 */
	private InMemoryCube slice;
	
	/**
	 * Queries executing currently on this slice 
	 */
	private Set<Long> queries = new HashSet<Long>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
	
	/**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(SliceListener.class.getName());
	

	/**
	 * Add the given query to dependents.
	 * 
	 * @param queryID
	 */
	public void registerQuery(Long queryID)
	{
		queries.add(queryID);
	}
	
    /**
     * @return
     */
	public String getCubeUniqueName()
	{
		return slice.getCubeUniqueName();
	}
	
	/**
	 * @param slice
	 */
	public SliceListener(InMemoryCube slice) 
	{
		this.slice = slice;
	}
	
	public void fireQueryFinish(Long queryId)
	{
	    LOGGER.info(
                MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,"SliceListener: query finished " + queryId);
		//System.out.println("SliceListener: query finished " + queryId);
		//Don't remove till some one makes it as in active
		if(!slice.isActive())
		{
			queries.remove(queryId);
		}
		
		if(queries.size() == 0)
		{
			// to avoid ConcurrentModificationException while sliceListiterating 
			//By:Sojer z00218041
            /**
             * This code is commented for checkstyle issue where
             * SLICE_LIST_CONCURRENT = false; where the loop will never run
             * 
             * @author C00900810
             */
            // try
            // {
            // while(InMemoryCubeStore.getInstance().isSliceListConcurrent())
            // {
            // Thread.sleep(1);
            // }
            // }
            // catch(InterruptedException e)
            // {
            // LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
            // "InterruptedException");
            // }
			LOGGER.error(
	                MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,"SliceListener: Unregistering slice " + slice.getID());
			//System.out.println("SliceListener: Unregistering slice " + slice.getID());
			
			//Yes this slice is ready to clear
			InMemoryCubeStore.getInstance().unRegisterSlice(slice.getCubeUniqueName(), slice);
			slice.clean();
			
			// By Sojer z00218041 if the query is in waiting and old execution
            // finished, change QUERY_EXECUTE_STATUS and deal with cache
            InMemoryCubeStore.getInstance().afterClearQueriesAndCubes(slice.getCubeUniqueName());
		}
	}
	
	/**
	 * Is there any more queries pending to notify? 
	 */
	public boolean stillListening()
	{
		return queries.size() != 0;
	}
}
